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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.transport.TransportRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.AuthenticationToken;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.InternalUsers;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.elasticsearch.transport.RemoteClusterPortSettings.TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class CrossClusterAccessAuthenticationServiceTests extends ESTestCase {

    private ClusterService clusterService;
    private ApiKeyService apiKeyService;
    private AuthenticationService authenticationService;

    @Before
    public void init() throws Exception {
        this.apiKeyService = mock(ApiKeyService.class);
        this.authenticationService = mock(AuthenticationService.class);
        this.clusterService = mockClusterServiceWithMinTransportVersion(TransportVersion.CURRENT);
    }

    public void testAuthenticateThrowsOnUnsupportedMinVersions() throws IOException {
        clusterService = mockClusterServiceWithMinTransportVersion(
            TransportVersionUtils.randomVersionBetween(
                random(),
                TransportVersion.MINIMUM_COMPATIBLE,
                TransportVersionUtils.getPreviousVersion(TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR)
            )
        );
        final var authcContext = mock(Authenticator.Context.class, Mockito.RETURNS_DEEP_STUBS);
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo()
        );
        crossClusterAccessHeaders.writeToContext(threadContext);
        when(authcContext.getThreadContext()).thenReturn(threadContext);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final ArgumentCaptor<AuthenticationToken> authenticationTokenCapture = ArgumentCaptor.forClass(AuthenticationToken.class);
        doNothing().when(authcContext).addAuthenticationToken(authenticationTokenCapture.capture());
        when(authcContext.getMostRecentAuthenticationToken()).thenAnswer(ignored -> authenticationTokenCapture.getValue());
        when(authcContext.getRequest()).thenReturn(auditableRequest);
        when(auditableRequest.exceptionProcessingRequest(any(), any())).thenAnswer(
            i -> new ElasticsearchSecurityException("potato", (Exception) i.getArguments()[0])
        );
        when(authenticationService.newContext(anyString(), any(), anyBoolean())).thenReturn(authcContext);
        final CrossClusterAccessAuthenticationService service = new CrossClusterAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authenticationService
        );

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate("action", mock(TransportRequest.class), future);
        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);

        assertThat(actual.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        assertThat(
            actual.getCause().getCause().getMessage(),
            equalTo(
                "all nodes must have transport version ["
                    + TRANSPORT_VERSION_ADVANCED_REMOTE_CLUSTER_SECURITY_CCR
                    + "] or higher to support cross cluster requests through the dedicated remote cluster port"
            )
        );
        verify(auditableRequest).exceptionProcessingRequest(
            any(Exception.class),
            credentialsArgMatches(crossClusterAccessHeaders.credentials())
        );
        verifyNoMoreInteractions(auditableRequest);
    }

    public void testAuthenticationSuccessOnSuccessfulAuthentication() throws IOException, ExecutionException, InterruptedException {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo()
        );
        crossClusterAccessHeaders.writeToContext(threadContext);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final ArgumentCaptor<Authentication> authenticationCapture = ArgumentCaptor.forClass(Authentication.class);
        doNothing().when(auditableRequest).authenticationSuccess(authenticationCapture.capture());
        final Authenticator.Context authcContext = mock(Authenticator.Context.class, Mockito.RETURNS_DEEP_STUBS);
        when(authcContext.getThreadContext()).thenReturn(threadContext);
        when(authcContext.getRequest()).thenReturn(auditableRequest);
        when(authenticationService.newContext(anyString(), any(), anyBoolean())).thenReturn(authcContext);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(eq(authcContext), listenerCaptor.capture());
        final CrossClusterAccessAuthenticationService service = new CrossClusterAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authenticationService
        );

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate("action", mock(TransportRequest.class), future);
        final Authentication apiKeyAuthentication = AuthenticationTestHelper.builder().apiKey().build(false);
        listenerCaptor.getValue().onResponse(apiKeyAuthentication);
        future.get();

        final Authentication expectedAuthentication = apiKeyAuthentication.toCrossClusterAccess(
            crossClusterAccessHeaders.getCleanAndValidatedSubjectInfo()
        );
        verify(auditableRequest).authenticationSuccess(expectedAuthentication);
        verifyNoMoreInteractions(auditableRequest);
        verify(authcContext).addAuthenticationToken(credentialsArgMatches(crossClusterAccessHeaders.credentials()));
    }

    public void testExceptionProcessingRequestOnInvalidCrossClusterAccessSubjectInfo() throws IOException {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            new CrossClusterAccessSubjectInfo(
                // Invalid internal user
                AuthenticationTestHelper.builder().internal(InternalUsers.XPACK_USER).build(),
                new RoleDescriptorsIntersection(
                    new RoleDescriptor("invalid_role", new String[] { "all" }, null, null, null, null, null, null, null, null)
                )
            )
        );
        crossClusterAccessHeaders.writeToContext(threadContext);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final ArgumentCaptor<Authentication> authenticationCapture = ArgumentCaptor.forClass(Authentication.class);
        doNothing().when(auditableRequest).authenticationSuccess(authenticationCapture.capture());
        final Authenticator.Context authcContext = mock(Authenticator.Context.class, Mockito.RETURNS_DEEP_STUBS);
        final ArgumentCaptor<AuthenticationToken> authenticationTokenCapture = ArgumentCaptor.forClass(AuthenticationToken.class);
        doNothing().when(authcContext).addAuthenticationToken(authenticationTokenCapture.capture());
        when(authcContext.getMostRecentAuthenticationToken()).thenAnswer(ignored -> authenticationTokenCapture.getValue());
        when(authcContext.getThreadContext()).thenReturn(threadContext);
        when(authcContext.getRequest()).thenReturn(auditableRequest);
        when(auditableRequest.exceptionProcessingRequest(any(), any())).thenAnswer(
            i -> new ElasticsearchSecurityException("potato", (Exception) i.getArguments()[0])
        );
        when(authenticationService.newContext(anyString(), any(), anyBoolean())).thenReturn(authcContext);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(eq(authcContext), listenerCaptor.capture());
        final CrossClusterAccessAuthenticationService service = new CrossClusterAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authenticationService
        );

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate("action", mock(TransportRequest.class), future);
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

    public void testNoInteractionWithAuditableRequestOnInitialAuthenticationFailure() throws IOException {
        final var threadContext = new ThreadContext(Settings.EMPTY);
        final var crossClusterAccessHeaders = new CrossClusterAccessHeaders(
            CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
            AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo()
        );
        crossClusterAccessHeaders.writeToContext(threadContext);
        final AuthenticationService.AuditableRequest auditableRequest = mock(AuthenticationService.AuditableRequest.class);
        final Authenticator.Context authcContext = mock(Authenticator.Context.class, Mockito.RETURNS_DEEP_STUBS);
        when(authcContext.getThreadContext()).thenReturn(threadContext);
        when(authcContext.getRequest()).thenReturn(auditableRequest);
        when(authenticationService.newContext(anyString(), any(), anyBoolean())).thenReturn(authcContext);
        @SuppressWarnings("unchecked")
        final ArgumentCaptor<ActionListener<Authentication>> listenerCaptor = ArgumentCaptor.forClass(ActionListener.class);
        doAnswer(i -> null).when(authenticationService).authenticate(eq(authcContext), listenerCaptor.capture());
        final CrossClusterAccessAuthenticationService service = new CrossClusterAccessAuthenticationService(
            clusterService,
            apiKeyService,
            authenticationService
        );

        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate("action", mock(TransportRequest.class), future);
        final ElasticsearchSecurityException authenticationFailure = new ElasticsearchSecurityException("authentication failure");
        listenerCaptor.getValue().onFailure(authenticationFailure);

        final ExecutionException actual = expectThrows(ExecutionException.class, future::get);
        assertThat(actual.getCause(), equalTo(authenticationFailure));
        verifyNoInteractions(auditableRequest);
    }

    private static AuthenticationToken credentialsArgMatches(AuthenticationToken credentials) {
        return argThat(arg -> arg.principal().equals(credentials.principal()) && arg.credentials().equals(credentials.credentials()));
    }

    private static ClusterService mockClusterServiceWithMinTransportVersion(final TransportVersion transportVersion) {
        final ClusterService clusterService = mock(ClusterService.class, Mockito.RETURNS_DEEP_STUBS);
        when(clusterService.state().getMinTransportVersion()).thenReturn(transportVersion);
        return clusterService;
    }
}
