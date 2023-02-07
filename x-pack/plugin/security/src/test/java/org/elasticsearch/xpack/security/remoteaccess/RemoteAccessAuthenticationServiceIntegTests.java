/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.remoteaccess;

import org.elasticsearch.ElasticsearchSecurityException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyRequestBuilder;
import org.elasticsearch.xpack.core.security.action.apikey.CreateApiKeyResponse;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationServiceField;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.SystemUser;
import org.elasticsearch.xpack.security.authc.RemoteAccessAuthenticationService;
import org.junit.BeforeClass;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.elasticsearch.xpack.core.security.authc.AuthenticationField.AUTHENTICATION_KEY;
import static org.elasticsearch.xpack.core.security.authc.RemoteAccessAuthentication.REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY;
import static org.elasticsearch.xpack.security.transport.SecurityServerTransportInterceptor.REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RemoteAccessAuthenticationServiceIntegTests extends SecurityIntegTestCase {

    @BeforeClass
    public static void checkFeatureFlag() {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());
    }

    public void testInvalidHeaders() throws InterruptedException, IOException {
        final String encodedRemoteAccessApiKey = getEncodedRemoteAccessApiKey();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final RemoteAccessAuthenticationService service = internalCluster().getInstance(RemoteAccessAuthenticationService.class, nodeName);

        try (var ignored = threadContext.stashContext()) {
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    equalTo("remote access header [" + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY + "] is required")
                );
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "abc");
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    equalTo(
                        "remote access header ["
                            + REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY
                            + "] value must be a valid API key credential"
                    )
                );
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(AUTHENTICATION_KEY, AuthenticationTestHelper.builder().build().encode());
            // Optionally include remote access headers; the request should fail due to authentication header either way
            if (randomBoolean()) {
                threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + encodedRemoteAccessApiKey);
                AuthenticationTestHelper.randomRemoteAccessAuthentication().writeToContext(threadContext);
            }
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(ex.getCause().getMessage(), equalTo("authentication header is not allowed with remote access"));
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(AuthenticationServiceField.RUN_AS_USER_HEADER, AuthenticationTestHelper.builder().build().encode());
            // Optionally include remote access headers; the request should fail due to authentication header either way
            if (randomBoolean()) {
                threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + encodedRemoteAccessApiKey);
                AuthenticationTestHelper.randomRemoteAccessAuthentication().writeToContext(threadContext);
            }
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(ex.getCause().getMessage(), equalTo("run-as header is not allowed with remote access"));
            });
        }

        try (var ignored = threadContext.stashContext()) {
            final String randomApiKey = Base64.getEncoder()
                .encodeToString((UUIDs.base64UUID() + ":" + UUIDs.base64UUID()).getBytes(StandardCharsets.UTF_8));
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + randomApiKey);
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    equalTo("remote access header [" + REMOTE_ACCESS_AUTHENTICATION_HEADER_KEY + "] is required")
                );
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + encodedRemoteAccessApiKey);
            final var internalUser = randomValueOtherThan(SystemUser.INSTANCE, AuthenticationTestHelper::randomInternalUser);
            new RemoteAccessAuthentication(
                AuthenticationTestHelper.builder().internal(internalUser).build(),
                RoleDescriptorsIntersection.EMPTY
            ).writeToContext(threadContext);
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    equalTo("received cross cluster request from an unexpected internal user [" + internalUser.principal() + "]")
                );
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + encodedRemoteAccessApiKey);
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
            ).writeToContext(threadContext);
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    containsString(
                        "role descriptor for remote access can only contain index privileges but other privileges found for subject"
                    )
                );
            });
        }

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + encodedRemoteAccessApiKey);
            Authentication authentication = AuthenticationTestHelper.builder().apiKey().build();
            new RemoteAccessAuthentication(authentication, RoleDescriptorsIntersection.EMPTY).writeToContext(threadContext);
            authenticateAndAssertExpectedFailure(service, ex -> {
                assertThat(ex, instanceOf(ElasticsearchSecurityException.class));
                assertThat(
                    ex.getCause().getMessage(),
                    containsString(
                        "subject ["
                            + authentication.getEffectiveSubject().getUser().principal()
                            + "] has type ["
                            + authentication.getEffectiveSubject().getType()
                            + "] which is not supported for remote access"
                    )
                );
            });
        }
    }

    public void testSystemUserIsMappedToCrossClusterInternalRole() throws InterruptedException, IOException {
        final String encodedRemoteAccessApiKey = getEncodedRemoteAccessApiKey();
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final RemoteAccessAuthenticationService service = internalCluster().getInstance(RemoteAccessAuthenticationService.class, nodeName);

        try (var ignored = threadContext.stashContext()) {
            threadContext.putHeader(REMOTE_ACCESS_CLUSTER_CREDENTIAL_HEADER_KEY, "ApiKey " + encodedRemoteAccessApiKey);
            new RemoteAccessAuthentication(
                AuthenticationTestHelper.builder().internal(SystemUser.INSTANCE).build(),
                new RoleDescriptorsIntersection(new RoleDescriptor("role", null, null, null, null, null, null, null))
            ).writeToContext(threadContext);

            final AtomicReference<Authentication> actual = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);
            service.authenticate(ClusterStateAction.NAME, new SearchRequest(), false, new LatchedActionListener<>(new ActionListener<>() {
                @Override
                public void onResponse(Authentication authentication) {
                    actual.set(authentication);
                }

                @Override
                public void onFailure(Exception e) {
                    fail();
                }
            }, latch));
            latch.await();
            final Authentication actualAuthentication = actual.get();
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
        return Base64.getEncoder().encodeToString((response.getId() + ":" + response.getKey()).getBytes(StandardCharsets.UTF_8));
    }

    private void authenticateAndAssertExpectedFailure(RemoteAccessAuthenticationService service, Consumer<Exception> assertions)
        throws InterruptedException {
        final AtomicReference<Exception> actual = new AtomicReference<>();
        final AtomicReference<Authentication> actualAuthentication = new AtomicReference<>();
        final CountDownLatch latch = new CountDownLatch(1);
        service.authenticate(SearchAction.NAME, new SearchRequest(), false, new LatchedActionListener<>(new ActionListener<>() {
            @Override
            public void onResponse(Authentication authentication) {
                actualAuthentication.set(authentication);
            }

            @Override
            public void onFailure(Exception e) {
                actual.set(e);
            }
        }, latch));
        latch.await();
        assertNull(actualAuthentication.get());
        final Exception actualException = actual.get();
        assertNotNull(actual);
        assertions.accept(actualException);
    }
}
