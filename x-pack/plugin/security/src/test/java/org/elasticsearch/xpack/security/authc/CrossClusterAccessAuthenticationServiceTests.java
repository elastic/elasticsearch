/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.ssl.PemUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationResult;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.transport.CrossClusterApiKeySignatureManager;
import org.elasticsearch.xpack.security.transport.X509CertificateSignature;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.security.cert.X509Certificate;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CrossClusterAccessAuthenticationServiceTests extends ESTestCase {

    private ThreadContext threadContext;
    private ClusterService clusterService;
    private ApiKeyService apiKeyService;
    private AuthenticationService authenticationService;
    private CrossClusterAccessAuthenticationService crossClusterAccessAuthenticationService;
    private CrossClusterApiKeySignatureManager.Verifier verifier;
    private CrossClusterApiKeySignatureManager.Signer signer;

    @Before
    public void init() throws Exception {
        this.threadContext = new ThreadContext(Settings.EMPTY);
        this.apiKeyService = mock(ApiKeyService.class);
        this.authenticationService = mock(AuthenticationService.class);
        this.verifier = mock(CrossClusterApiKeySignatureManager.Verifier.class);
        this.signer = mock(CrossClusterApiKeySignatureManager.Signer.class);
        this.clusterService = mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        when(clusterService.state().getMinTransportVersion()).thenReturn(TransportVersion.current());
        when(clusterService.threadPool().getThreadContext()).thenReturn(threadContext);
        crossClusterAccessAuthenticationService = new CrossClusterAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authenticationService,
            verifier
        );
    }

    public void testAuthenticationSuccessOnSuccessfulAuthentication() throws IOException, ExecutionException, InterruptedException {
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo()
        );
        crossClusterAccessHeaders.writeToContext(threadContext, null);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final ArgumentCaptor<Authentication> authenticationCapture = ArgumentCaptor.forClass(Authentication.class);
        doNothing().when(auditableRequest).authenticationSuccess(authenticationCapture.capture());
        doAnswer(invocationOnMock -> {
            AuthenticationToken authenticationToken = (AuthenticationToken) invocationOnMock.getArguments()[2];
            assertThat(authenticationToken.principal(), is(crossClusterAccessHeaders.credentials().principal()));
            assertThat(authenticationToken.credentials(), is(crossClusterAccessHeaders.credentials().credentials()));
            return new Authenticator.Context(
                threadContext,
                auditableRequest,
                mock(Realms.class),
                (AuthenticationToken) invocationOnMock.getArguments()[2]
            );
        }).when(authenticationService).newContext(anyString(), any(), any());
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(any(Authenticator.Context.class), listenerCaptor.capture());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        crossClusterAccessAuthenticationService.authenticate("action", mock(TransportRequest.class), future);
        final Authentication apiKeyAuthentication = AuthenticationTestHelper.builder().apiKey().build(false);
        listenerCaptor.getValue().onResponse(apiKeyAuthentication);
        future.get();

        final Authentication expectedAuthentication = apiKeyAuthentication.toCrossClusterAccess(
            crossClusterAccessHeaders.getCleanAndValidatedSubjectInfo()
        );
        verify(auditableRequest).authenticationSuccess(expectedAuthentication);
        verifyNoMoreInteractions(auditableRequest);
    }

    public void testExceptionProcessingRequestOnInvalidCrossClusterAccessSubjectInfo() throws IOException {
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            new CrossClusterAccessSubjectInfo(
                // Invalid internal user
                AuthenticationTestHelper.builder().internal(InternalUsers.XPACK_USER).build(),
                new RoleDescriptorsIntersection(
                    new RoleDescriptor("invalid_role", new String[] { "all" }, null, null, null, null, null, null, null, null, null, null)
                )
            )
        );
        crossClusterAccessHeaders.writeToContext(threadContext, null);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final ArgumentCaptor<Authentication> authenticationCapture = ArgumentCaptor.forClass(Authentication.class);
        doNothing().when(auditableRequest).authenticationSuccess(authenticationCapture.capture());
        when(auditableRequest.exceptionProcessingRequest(any(), any())).thenAnswer(
            i -> new ElasticsearchSecurityException("potato", (Exception) i.getArguments()[0])
        );
        doAnswer(invocationOnMock -> {
            AuthenticationToken authenticationToken = (AuthenticationToken) invocationOnMock.getArguments()[2];
            assertThat(authenticationToken.principal(), is(crossClusterAccessHeaders.credentials().principal()));
            assertThat(authenticationToken.credentials(), is(crossClusterAccessHeaders.credentials().credentials()));
            return new Authenticator.Context(
                threadContext,
                auditableRequest,
                mock(Realms.class),
                (AuthenticationToken) invocationOnMock.getArguments()[2]
            );
        }).when(authenticationService).newContext(anyString(), any(), any());
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(any(Authenticator.Context.class), listenerCaptor.capture());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        crossClusterAccessAuthenticationService.authenticate("action", mock(TransportRequest.class), future);
        final Authentication apiKeyAuthentication = AuthenticationTestHelper.builder().apiKey().build(false);
        listenerCaptor.getValue().onResponse(apiKeyAuthentication);

        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);

        assertThat(actual.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            actual.getCause().getCause().getMessage(),
            containsString("received cross cluster request from an unexpected internal user [" + InternalUsers.XPACK_USER.principal() + "]")
        );
        verify(auditableRequest).exceptionProcessingRequest(
            any(Exception.class),
            credentialsArgMatches(crossClusterAccessHeaders.credentials())
        );
        verifyNoMoreInteractions(auditableRequest);
    }

    public void testAuthenticationSuccessfulCrossClusterApiKeySignature() throws IOException, GeneralSecurityException, ExecutionException,
        InterruptedException {
        var subjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo();
        var apiKeyHeader = CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader();
        var certs = PemUtils.readCertificates(List.of(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")))
            .stream()
            .map(cert -> (X509Certificate) cert)
            .toArray(X509Certificate[]::new);
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(apiKeyHeader, subjectInfo);

        when(signer.sign(anyString(), anyString())).thenReturn(new X509CertificateSignature(certs, "", mock(BytesReference.class)));
        crossClusterAccessHeaders.writeToContext(threadContext, signer);

        when(verifier.verify(any(X509CertificateSignature.class), anyString(), anyString())).thenReturn(true);

        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final ArgumentCaptor<Authentication> authenticationCapture = ArgumentCaptor.forClass(Authentication.class);
        doNothing().when(auditableRequest).authenticationSuccess(authenticationCapture.capture());

        var authContext = new Authenticator.Context(
            threadContext,
            auditableRequest,
            mock(Realms.class),
            crossClusterAccessHeaders.credentials()
        );
        var action = "action";
        var request = mock(TransportRequest.class);
        when(authenticationService.newContext(anyString(), any(TransportRequest.class), any(ApiKeyService.ApiKeyCredentials.class)))
            .thenReturn(authContext);

        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(any(Authenticator.Context.class), listenerCaptor.capture());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        crossClusterAccessAuthenticationService.authenticate(action, request, future);

        final Authentication apiKeyAuthentication = AuthenticationTestHelper.builder().apiKey().build(false);
        listenerCaptor.getValue().onResponse(apiKeyAuthentication);
        future.get();

        final Authentication expectedAuthentication = apiKeyAuthentication.toCrossClusterAccess(
            crossClusterAccessHeaders.getCleanAndValidatedSubjectInfo()
        );
        verify(auditableRequest).authenticationSuccess(expectedAuthentication);
        verifyNoMoreInteractions(auditableRequest);
    }

    public void testAuthenticationExceptionOnBadCrossClusterApiKeySignature() throws IOException, GeneralSecurityException {
        var subjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo();
        var apiKeyHeader = CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader();
        var certs = PemUtils.readCertificates(List.of(getDataPath("/org/elasticsearch/xpack/security/signature/signing_rsa.crt")))
            .stream()
            .map(cert -> (X509Certificate) cert)
            .toArray(X509Certificate[]::new);
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(apiKeyHeader, subjectInfo);

        var verifyMock = when(verifier.verify(any(X509CertificateSignature.class), anyString(), anyString()));
        boolean badCert = randomBoolean();

        if (badCert) {
            verifyMock.thenThrow(new GeneralSecurityException("bad certificate"));
        } else {
            verifyMock.thenReturn(false);
        }

        when(signer.sign(anyString(), anyString())).thenReturn(new X509CertificateSignature(certs, "", mock(BytesReference.class)));
        crossClusterAccessHeaders.writeToContext(threadContext, signer);

        var auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        doAnswer(invocationOnMock -> invocationOnMock.getArguments()[0]).when(auditableRequest).exceptionProcessingRequest(any(), any());

        var authContext = new Authenticator.Context(
            threadContext,
            auditableRequest,
            mock(Realms.class),
            crossClusterAccessHeaders.credentials()
        );
        var action = "action";
        var request = mock(TransportRequest.class);
        when(authenticationService.newContext(anyString(), any(TransportRequest.class), any(ApiKeyService.ApiKeyCredentials.class)))
            .thenReturn(authContext);

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        crossClusterAccessAuthenticationService.authenticate(action, request, future);

        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);

        assertThat(actual.getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(
            actual.getMessage(),
            containsString(
                (badCert
                    ? "Failed to verify cross cluster api key signature certificate from ["
                    : "Invalid cross cluster api key signature from [") + X509CertificateSignature.certificateToString(certs[0]) + "]"
            )
        );
    }

    public void testNoInteractionWithAuditableRequestOnInitialAuthenticationFailure() throws IOException {
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo()
        );
        crossClusterAccessHeaders.writeToContext(threadContext, null);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        doAnswer(invocationOnMock -> {
            AuthenticationToken authenticationToken = (AuthenticationToken) invocationOnMock.getArguments()[2];
            assertThat(authenticationToken.principal(), is(crossClusterAccessHeaders.credentials().principal()));
            assertThat(authenticationToken.credentials(), is(crossClusterAccessHeaders.credentials().credentials()));
            return new Authenticator.Context(
                threadContext,
                auditableRequest,
                mock(Realms.class),
                (AuthenticationToken) invocationOnMock.getArguments()[2]
            );
        }).when(authenticationService).newContext(anyString(), any(), any());
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(any(Authenticator.Context.class), listenerCaptor.capture());

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        crossClusterAccessAuthenticationService.authenticate("action", mock(TransportRequest.class), future);
        final ElasticsearchSecurityException authenticationFailure = new ElasticsearchSecurityException("authentication failure");
        listenerCaptor.getValue().onFailure(authenticationFailure);

        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);
        assertThat(actual.getCause(), equalTo(authenticationFailure));
        verifyNoInteractions(auditableRequest);
    }

    public void testTerminateExceptionBubblesUpWithAuthenticateHeaders() {
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<AuthenticationResult<User>>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(apiKeyService)
            .tryAuthenticate(any(), any(ApiKeyService.ApiKeyCredentials.class), listenerCaptor.capture());

        final PlainActionFuture<Void> future = new PlainActionFuture<>();
        crossClusterAccessAuthenticationService.tryAuthenticate(
            new ApiKeyService.ApiKeyCredentials(UUIDs.randomBase64UUID(), UUIDs.randomBase64UUIDSecureString(), ApiKey.Type.CROSS_CLUSTER),
            future
        );
        Exception ex = new IllegalArgumentException("terminator");
        listenerCaptor.getValue().onResponse(AuthenticationResult.terminate("authentication failure", ex));

        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);
        assertThat(actual.getCause(), equalTo(ex));
    }

    private static AuthenticationToken credentialsArgMatches(AuthenticationToken credentials) {
        return argThat(arg -> arg.principal().equals(credentials.principal()) && arg.credentials().equals(credentials.credentials()));
    }
}
