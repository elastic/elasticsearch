/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.remoteaccess;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.security.authc.ApiKeyService;
import org.elasticsearch.xpack.security.authc.RemoteAccessAuthenticationService;
import org.elasticsearch.xpack.security.authc.RemoteAccessHeaders;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;
import static org.elasticsearch.xpack.security.authc.RemoteAccessHeaders.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RemoteAccessAuthenticationServiceIntegTests extends SecurityIntegTestCase {

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    public void testInvalidHeaders() throws IOException {
        final String encodedRemoteAccessApiKey = getEncodedRemoteAccessApiKey();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final RemoteAccessAuthenticationService service = internalCluster().getInstance(RemoteAccessAuthenticationService.class, nodeName);

        try (var ignored = threadContext.stashContext()) {
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(msg, equalTo("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required"))
            );
        }

        try (var ignored = threadContext.stashContext()) {
            new RemoteAccessHeaders(ApiKeyService.withApiKeyPrefix("abc"), AuthenticationTestHelper.randomRemoteAccessAuthentication())
                .writeToContext(threadContext);
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    equalTo(
                        "remote access header ["
                            + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY
                            + "] value must be a valid API key credential"
                    )
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            final String randomApiKey = Base64.getEncoder()
                .encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, ApiKeyService.withApiKeyPrefix(randomApiKey));
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(msg, equalTo("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required"))
            );
        }

        try (var ignored = threadContext.stashContext()) {
            final var internalUser = randomValueOtherThan(SystemUser.INSTANCE, AuthenticationTestHelper::randomInternalUser);
            new RemoteAccessHeaders(
                encodedRemoteAccessApiKey,
                new RemoteAccessAuthentication(
                    AuthenticationTestHelper.builder().internal(internalUser).build(),
                    RoleDescriptorsIntersection.EMPTY
                )
            ).writeToContext(threadContext);
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    equalTo("received cross cluster request from an unexpected internal user [" + internalUser.principal() + "]")
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            new RemoteAccessHeaders(
                encodedRemoteAccessApiKey,
                AuthenticationTestHelper.randomRemoteAccessAuthentication(
                    new RoleDescriptorsIntersection(
                        randomValueOtherThanMany(
                            rd -> false == (rd.hasClusterPrivileges()
                                || rd.hasApplicationPrivileges()
                                || rd.hasConfigurableClusterPrivileges()
                                || rd.hasRunAs()
                                || rd.hasRemoteIndicesPrivileges()),
                            () -> RoleDescriptorTests.randomRoleDescriptor()
                        )
                    )
                )
            ).writeToContext(threadContext);
            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    containsString(
                        "role descriptor for remote access can only contain index privileges but other privileges found for subject"
                    )
                )
            );
        }

        try (var ignored = threadContext.stashContext()) {
            Authentication authentication = AuthenticationTestHelper.builder().apiKey().build();
            new RemoteAccessHeaders(
                encodedRemoteAccessApiKey,
                new RemoteAccessAuthentication(authentication, RoleDescriptorsIntersection.EMPTY)
            ).writeToContext(threadContext);

            authenticateAndAssertExpectedErrorMessage(
                service,
                msg -> assertThat(
                    msg,
                    containsString(
                        "subject ["
                            + authentication.getEffectiveSubject().getUser().principal()
                            + "] has type ["
                            + authentication.getEffectiveSubject().getType()
                            + "] which is not supported for remote access"
                    )
                )
            );
        }
    }

    public void testSystemUserIsMappedToCrossClusterInternalRole() throws InterruptedException, IOException, ExecutionException {
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final RemoteAccessAuthenticationService service = internalCluster().getInstance(RemoteAccessAuthenticationService.class, nodeName);

        try (var ignored = threadContext.stashContext()) {
            new RemoteAccessHeaders(
                getEncodedRemoteAccessApiKey(),
                new RemoteAccessAuthentication(
                    AuthenticationTestHelper.builder().internal(SystemUser.INSTANCE).build(),
                    new RoleDescriptorsIntersection(new RoleDescriptor("role", null, null, null, null, null, null, null))
                )
            ).writeToContext(threadContext);

            final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
            service.authenticate(ClusterStateAction.NAME, new SearchRequest(), future);
            final Authentication actualAuthentication = future.get();

            assertNotNull(actualAuthentication);
            assertThat(actualAuthentication.getEffectiveSubject().getUser(), is(SystemUser.INSTANCE));
            @SuppressWarnings("unchecked")
            List<RemoteAccessAuthentication.RoleDescriptorsBytes> rds = (List<
                RemoteAccessAuthentication.RoleDescriptorsBytes>) actualAuthentication.getAuthenticatingSubject()
                    .getMetadata()
                    .get(AuthenticationField.REMOTE_ACCESS_ROLE_DESCRIPTORS_KEY);
            assertThat(rds.size(), equalTo(1));
            assertThat(rds.get(0).toRoleDescriptors(), equalTo(Set.of(RemoteAccessAuthenticationService.CROSS_CLUSTER_INTERNAL_ROLE)));
        }
    }

    private String getEncodedRemoteAccessApiKey() {
        final CreateApiKeyResponse response = new CreateApiKeyRequestBuilder(client().admin().cluster()).setName("remote_access_key").get();
        return ApiKeyService.withApiKeyPrefix(
            Base64.getEncoder().encodeToString((response.getId() + ":" + response.getKey()).getBytes(StandardCharsets.UTF_8))
        );
    }

    private void authenticateAndAssertExpectedErrorMessage(
        RemoteAccessAuthenticationService service,
        Consumer<String> errorMessageAssertion
    ) {
        final PlainActionFuture<Authentication> future = new PlainActionFuture<>();
        service.authenticate(ClusterStateAction.NAME, new SearchRequest(), future);
        final ExecutionException actualException = expectThrows(ExecutionException.class, future::get);
        assertThat(actualException.getCause(), instanceOf(ElasticsearchSecurityException.class));
        assertThat(actualException.getCause().getCause(), instanceOf(IllegalArgumentException.class));
        errorMessageAssertion.accept(actualException.getCause().getCause().getMessage());
    }
}
