/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authc;

import io.netty.channel.Channel;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.transport.Header;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequest;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyAction;
import org.elasticsearch.xpack.core.security.action.apikey.CreateCrossClusterApiKeyRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authc.support.AuthenticationContextSerializer;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.InternalUsers;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.NONE;
import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.WAIT_UNTIL;
import static org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo.CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.CrossClusterAccessHeaders.CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY;
import static org.elasticsearch.xpack.security.transport.X509CertificateSignature.CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CrossClusterAccessAuthenticationServiceIntegTests extends SecurityIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        final Settings.Builder builder = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        builder.put("xpack.security.audit.enabled", "true");
        builder.put("xpack.security.audit.logfile.events.emit_request_body", "true");
        return builder.build();
    }

    public void testInvalidHeaders() throws IOException {
        final String encodedCrossClusterAccessApiKey = getEncodedCrossClusterAccessApiKey();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final CrossClusterAccessAuthenticationService service = getCrossClusterAccessAuthenticationService(nodeName);

        try (var ignored = threadContext.stashContext()) {
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    equalTo("cross cluster access header [" + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY + "] is required")
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            new CrossClusterAccessHeaders(
                ApiKeyService.withApiKeyPrefix("abc"),
                AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo()
            ).writeToContext(threadContext, null);
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    equalTo(
                        "cross cluster access header ["
                            + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY
                            + "] value must be a valid API key credential"
                    )
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            final String randomApiKey = Base64.getEncoder()
                .encodeToString((UUIDs.base64UUID() + ":" + UUIDs.randomBase64UUIDSecureString()).getBytes(StandardCharsets.UTF_8));
            threadContext.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, ApiKeyService.withApiKeyPrefix(randomApiKey));
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    equalTo("cross cluster access header [" + CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY + "] is required")
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            final var internalUser = randomValueOtherThan(InternalUsers.SYSTEM_USER, AuthenticationTestHelper::randomInternalUser);
            new CrossClusterAccessHeaders(
                encodedCrossClusterAccessApiKey,
                new CrossClusterAccessSubjectInfo(
                    AuthenticationTestHelper.builder().internal(internalUser).build(),
                    RoleDescriptorsIntersection.EMPTY
                )
            ).writeToContext(threadContext, null);
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    equalTo("received cross cluster request from an unexpected internal user [" + internalUser.principal() + "]")
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            Authentication authentication = AuthenticationTestHelper.builder().crossClusterAccess().build();
            new CrossClusterAccessHeaders(
                encodedCrossClusterAccessApiKey,
                new CrossClusterAccessSubjectInfo(authentication, RoleDescriptorsIntersection.EMPTY)
            ).writeToContext(threadContext, null);

            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    containsString(
                        "subject ["
                            + authentication.getEffectiveSubject().getUser().principal()
                            + "] has type ["
                            + authentication.getEffectiveSubject().getType()
                            + "] but nested cross cluster access is not supported"
                    )
                )
            );
        }
    }

    public void testAuthenticateHeadersSuccess() throws IOException {
        final String encodedCrossClusterAccessApiKey = getEncodedCrossClusterAccessApiKey();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final CrossClusterAccessAuthenticationService service = getCrossClusterAccessAuthenticationService(nodeName);
        Channel mockChannel = mock(Channel.class);
        Header mockHeader = mock(Header.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
        when(mockHeader.getActionName()).thenReturn("test:action");

        try (var ignored = threadContext.stashContext()) {
            addRandomizedHeaders(threadContext, encodedCrossClusterAccessApiKey);
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            Map<String, String> headers = withRandomizedAdditionalSecurityHeaders(
                Map.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, encodedCrossClusterAccessApiKey)
            );
            final ApiKeyService.ApiKeyCredentials credentials = service.extractApiKeyCredentialsFromHeaders(headers);
            service.tryAuthenticate(credentials, mockChannel, mockHeader, future);
            future.actionGet();
        }
    }

    public void testGetApiKeyCredentialsFromHeaders() {
        final String nodeName = internalCluster().getRandomNodeName();
        final CrossClusterAccessAuthenticationService service = getCrossClusterAccessAuthenticationService(nodeName);

        {
            ElasticsearchSecurityException ex = expectThrows(
                ElasticsearchSecurityException.class,
                () -> service.extractApiKeyCredentialsFromHeaders(withRandomizedAdditionalSecurityHeaders(Map.of()))
            );
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(
                ex.getCause().getMessage(),
                containsString(
                    "Cross cluster requests through the dedicated remote cluster server port require transport header ["
                        + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY
                        + "] but none found. "
                        + "Please ensure you have configured remote cluster credentials on the cluster originating the request."
                )
            );
        }

        {
            ElasticsearchSecurityException ex = expectThrows(
                ElasticsearchSecurityException.class,
                () -> service.extractApiKeyCredentialsFromHeaders(
                    withRandomizedAdditionalSecurityHeaders(
                        Map.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, ApiKeyService.withApiKeyPrefix("abc"))
                    )
                )
            );
            assertThat(ex.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(
                ex.getCause().getMessage(),
                containsString(
                    "cross cluster access header ["
                        + CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY
                        + "] value must be a valid API key credential"
                )
            );
        }

    }

    public void testAuthenticateHeadersFailure() throws IOException {
        final EncodedKeyWithId encodedCrossClusterAccessApiKeyWithId = getEncodedCrossClusterAccessApiKeyWithId();
        final EncodedKeyWithId encodedRestApiKeyWithId = getEncodedRestApiKeyWithId();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final CrossClusterAccessAuthenticationService service = getCrossClusterAccessAuthenticationService(nodeName);
        Channel mockChannel = mock(Channel.class);
        Header mockHeader = mock(Header.class);
        when(mockChannel.remoteAddress()).thenReturn(new InetSocketAddress("127.0.0.1", 12345));
        when(mockHeader.getActionName()).thenReturn("test:action");

        try (var ignored = threadContext.stashContext()) {
            addRandomizedHeaders(threadContext, encodedCrossClusterAccessApiKeyWithId.encoded);
            final Map<String, String> headers = withRandomizedAdditionalSecurityHeaders(
                Map.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, encodedRestApiKeyWithId.encoded)
            );
            final ApiKeyService.ApiKeyCredentials credentials = service.extractApiKeyCredentialsFromHeaders(headers);
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            service.tryAuthenticate(credentials, mockChannel, mockHeader, future);
            final ExecutionException actualException = expectThrows(ExecutionException.class, future::get);
            assertThat(actualException.getCause(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(
                actualException.getCause().getMessage(),
                containsString("authentication expected API key type of [" + ApiKey.Type.CROSS_CLUSTER.value() + "]")
            );
        }

        try (var ignored = threadContext.stashContext()) {
            addRandomizedHeaders(threadContext, encodedCrossClusterAccessApiKeyWithId.encoded);
            final String wrongApiKeyWithCorrectId = ApiKeyService.withApiKeyPrefix(
                Base64.getEncoder()
                    .encodeToString(
                        (encodedCrossClusterAccessApiKeyWithId.id + ":" + UUIDs.randomBase64UUIDSecureString()).getBytes(
                            StandardCharsets.UTF_8
                        )
                    )
            );
            final Map<String, String> headers = withRandomizedAdditionalSecurityHeaders(
                Map.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, wrongApiKeyWithCorrectId)
            );
            final ApiKeyService.ApiKeyCredentials credentials = service.extractApiKeyCredentialsFromHeaders(headers);
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            service.tryAuthenticate(credentials, mockChannel, mockHeader, future);
            final ExecutionException actualException = expectThrows(ExecutionException.class, future::get);
            assertThat(actualException.getCause(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(actualException.getCause().getMessage(), containsString("invalid credentials for API key"));
        }

        try (var ignored = threadContext.stashContext()) {
            addRandomizedHeaders(threadContext, encodedCrossClusterAccessApiKeyWithId.encoded);
            final String wrongApiKey = ApiKeyService.withApiKeyPrefix(
                Base64.getEncoder()
                    .encodeToString((UUIDs.base64UUID() + ":" + UUIDs.randomBase64UUIDSecureString()).getBytes(StandardCharsets.UTF_8))
            );
            final Map<String, String> headers = withRandomizedAdditionalSecurityHeaders(
                Map.of(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, wrongApiKey)
            );
            final ApiKeyService.ApiKeyCredentials credentials = service.extractApiKeyCredentialsFromHeaders(headers);
            final PlainActionFuture<Void> future = new PlainActionFuture<>();
            service.tryAuthenticate(credentials, mockChannel, mockHeader, future);
            final ExecutionException actualException = expectThrows(ExecutionException.class, future::get);
            assertThat(actualException.getCause(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(actualException.getCause().getMessage(), containsString("unable to find apikey with id"));
        }
    }

    private Map<String, String> withRandomizedAdditionalSecurityHeaders(Map<String, String> headers) throws IOException {
        var map = new HashMap<>(headers);
        if (randomBoolean()) {
            map.put(
                "Authorization",
                randomFrom(
                    CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader(),
                    UsernamePasswordToken.basicAuthHeaderValue(
                        SecuritySettingsSource.TEST_USER_NAME,
                        new SecureString(SecuritySettingsSource.TEST_USER_NAME.toCharArray())
                    ),
                    UsernamePasswordToken.basicAuthHeaderValue("user", new SecureString(randomAlphaOfLength(20).toCharArray()))
                )
            );
        }
        if (randomBoolean()) {
            map.put(AuthenticationField.AUTHENTICATION_KEY, AuthenticationTestHelper.builder().build().encode());
        }
        return Map.copyOf(map);
    }

    private void addRandomizedHeaders(ThreadContext threadContext, String validEncodedApiKey) throws IOException {
        // Headers in thread context should have no impact on tryAuthenticate
        if (randomBoolean()) {
            new CrossClusterAccessHeaders(
                validEncodedApiKey,
                randomFrom(
                    new CrossClusterAccessSubjectInfo(AuthenticationTestHelper.builder().build(), RoleDescriptorsIntersection.EMPTY),
                    new CrossClusterAccessSubjectInfo(
                        AuthenticationTestHelper.builder().crossClusterAccess().build(),
                        RoleDescriptorsIntersection.EMPTY
                    )
                )
            ).writeToContext(threadContext, null);
        } else {
            if (randomBoolean()) {
                threadContext.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, validEncodedApiKey);
            }
        }
        if (randomBoolean()) {
            new AuthenticationContextSerializer().writeToContext(AuthenticationTestHelper.builder().build(), threadContext);
        }
        if (randomBoolean()) {
            threadContext.putHeader("Authorization", CrossClusterAccessHeadersTests.randomEncodedApiKeyHeader());
        }
    }

    private String getEncodedCrossClusterAccessApiKey() throws IOException {
        return getEncodedCrossClusterAccessApiKeyWithId().encoded;
    }

    private EncodedKeyWithId getEncodedCrossClusterAccessApiKeyWithId() throws IOException {
        final CreateCrossClusterApiKeyRequest request = CreateCrossClusterApiKeyRequest.withNameAndAccess("cross_cluster_access_key", """
            {"search": [{"names": ["*"]}]}""");
        request.setRefreshPolicy(randomFrom(NONE, IMMEDIATE, WAIT_UNTIL));
        final CreateApiKeyResponse response = client().execute(CreateCrossClusterApiKeyAction.INSTANCE, request).actionGet();
        return new EncodedKeyWithId(
            response.getId(),
            ApiKeyService.withApiKeyPrefix(
                Base64.getEncoder().encodeToString((response.getId() + ":" + response.getKey()).getBytes(StandardCharsets.UTF_8))
            )
        );
    }

    private EncodedKeyWithId getEncodedRestApiKeyWithId() {
        final CreateApiKeyRequest request = new CreateApiKeyRequest(randomAlphaOfLength(10), null, null, null);
        request.setRefreshPolicy(randomFrom(NONE, IMMEDIATE, WAIT_UNTIL));
        final CreateApiKeyResponse response = client().execute(CreateApiKeyAction.INSTANCE, request).actionGet();
        return new EncodedKeyWithId(
            response.getId(),
            ApiKeyService.withApiKeyPrefix(
                Base64.getEncoder().encodeToString((response.getId() + ":" + response.getKey()).getBytes(StandardCharsets.UTF_8))
            )
        );
    }

    record EncodedKeyWithId(String id, String encoded) {}

    private void authenticateAndAssertExpectedErrorMessage(
        CrossClusterAccessAuthenticationService service,
        Consumer<String> errorMessageAssertion
    ) {
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(ClusterStateAction.NAME, new SearchRequest(), future);
        final ExecutionException actualException = expectThrows(ExecutionException.class, future::get);
        assertThat(actualException.getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(actualException.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        errorMessageAssertion.accept(actualException.getCause().getCause().getMessage());
    }

    private static CrossClusterAccessAuthenticationService getCrossClusterAccessAuthenticationService(String nodeName) {
        RemoteClusterAuthenticationService service = internalCluster().getInstance(RemoteClusterAuthenticationService.class, nodeName);
        if (service instanceof CrossClusterAccessAuthenticationService) {
            return (CrossClusterAccessAuthenticationService) service;
        } else {
            throw new AssertionError(
                "expected ["
                    + CrossClusterAccessAuthenticationService.class.getCanonicalName()
                    + "] but got  ["
                    + service.getClass().getSimpleName()
                    + "]"
            );
        }
    }

    public void testAuditLogForBadCrossClusterApiKeySignature() throws Exception {
        final EncodedKeyWithId validApiKey = getEncodedCrossClusterAccessApiKeyWithId();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final CrossClusterAccessAuthenticationService service = getCrossClusterAccessAuthenticationService(nodeName);

        // 1. Prepare Corrupted Headers (Valid Key + Invalid Signature)
        final String subjectInfoHeader = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo().encode();
        // Use a clearly invalid signature header to force the verifier to fail
        final String maliciousSignatureHeader = "version=1;certs=BAD;signature=DATA";

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(CROSS_CLUSTER_ACCESS_CREDENTIALS_HEADER_KEY, validApiKey.encoded);
            threadContext.putHeader(CROSS_CLUSTER_ACCESS_SUBJECT_INFO_HEADER_KEY, subjectInfoHeader);
            threadContext.putHeader(CROSS_CLUSTER_ACCESS_SIGNATURE_HEADER_KEY, maliciousSignatureHeader);

            // 2. Execute the Authentication, expecting failure
            final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
            service.authenticate(ClusterStateAction.NAME, new SearchRequest(), future);

            // Assert the correct exception is thrown
            final ExecutionException actualException = expectThrows(ExecutionException.class, future::get);
            assertThat(actualException.getCause(), instanceOf(ElasticsearchSecurityException.class));
            assertThat(actualException.getCause().getMessage(), containsString("Invalid cross cluster api key signature from ["));
        }
    }








}
