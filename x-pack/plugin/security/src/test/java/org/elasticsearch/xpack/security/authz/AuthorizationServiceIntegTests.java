/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationTestHelper;
import org.elasticsearch.xpack.core.security.authc.CrossClusterAccessSubjectInfo;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorTests;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.authz.privilege.ClusterPrivilegeResolver;
import org.elasticsearch.xpack.core.security.authz.privilege.IndexPrivilege;
import org.elasticsearch.xpack.core.security.support.NativeRealmValidationUtil;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.xpack.core.security.authz.AuthorizationServiceField.AUTHORIZATION_INFO_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class AuthorizationServiceIntegTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    public void testGetRoleDescriptorsIntersectionForRemoteCluster() throws IOException, InterruptedException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final String concreteClusterAlias = randomAlphaOfLength(10);
        final String roleName = randomAlphaOfLength(5);
        getSecurityClient().putRole(
            new RoleDescriptor(
                roleName,
                randomSubsetOf(ClusterPrivilegeResolver.names()).toArray(String[]::new),
                randomBoolean()
                    ? null
                    : new RoleDescriptor.IndicesPrivileges[] {
                        RoleDescriptor.IndicesPrivileges.builder()
                            .privileges(randomSubsetOf(randomIntBetween(1, 4), IndexPrivilege.names()))
                            .indices(generateRandomStringArray(5, randomIntBetween(3, 9), false, false))
                            .allowRestrictedIndices(randomBoolean())
                            .build() },
                null,
                null,
                generateRandomStringArray(5, randomIntBetween(2, 8), false, true),
                null,
                null,
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    new RoleDescriptor.RemoteIndicesPrivileges(
                        RoleDescriptor.IndicesPrivileges.builder()
                            .indices(shuffledList(List.of("index1", "index2")))
                            .privileges(shuffledList(List.of("read", "write")))
                            .build(),
                        randomNonEmptySubsetOf(List.of(concreteClusterAlias, "*")).toArray(new String[0])
                    ) },
                null
            )
        );
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final AuthorizationService authzService = internalCluster().getInstance(AuthorizationService.class, nodeName);
        final Authentication authentication = Authentication.newRealmAuthentication(
            new User(randomAlphaOfLengthBetween(5, 16), roleName),
            new Authentication.RealmRef(
                randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16),
                randomAlphaOfLengthBetween(5, 16),
                nodeName
            )
        );
        final RoleDescriptorsIntersection actual = authorizeThenGetRoleDescriptorsIntersectionForRemoteCluster(
            threadContext,
            authzService,
            authentication,
            concreteClusterAlias
        );
        final String generatedRoleName = actual.roleDescriptorsList().iterator().next().iterator().next().getName();
        assertNull(NativeRealmValidationUtil.validateRoleName(generatedRoleName, false));
        assertThat(generatedRoleName, not(equalTo(roleName)));
        assertThat(
            actual,
            equalTo(
                new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                generatedRoleName,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder()
                                        .indices("index1", "index2")
                                        .privileges("read", "write")
                                        .build() },
                                null,
                                null,
                                null,
                                null,
                                null
                            )
                        )
                    )
                )
            )
        );
    }

    public void testCrossClusterAccessWithInvalidRoleDescriptors() {
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final AuthorizationService authzService = internalCluster().getInstance(AuthorizationService.class, nodeName);
        final CrossClusterAccessSubjectInfo crossClusterAccessSubjectInfo = AuthenticationTestHelper.randomCrossClusterAccessSubjectInfo(
            new RoleDescriptorsIntersection(
                randomValueOtherThanMany(rd -> false == rd.hasPrivilegesOtherThanIndex(), () -> RoleDescriptorTests.randomRoleDescriptor())
            )
        );
        final Authentication authentication = AuthenticationTestHelper.builder()
            .crossClusterAccess(randomAlphaOfLength(42), crossClusterAccessSubjectInfo)
            .build();
        try (var ignored = threadContext.stashContext()) {
            // A request ID is set during authentication and is required for authorization; since we are not authenticating, set it
            // explicitly
            AuditUtil.generateRequestId(threadContext);
            final var future = new PlainActionFuture<Void>();
            // Authorize to trigger role resolution and (failed) validation
            authzService.authorize(authentication, AuthenticateAction.INSTANCE.name(), AuthenticateRequest.INSTANCE, future);
            final IllegalArgumentException actual = expectThrows(IllegalArgumentException.class, future::actionGet);
            final String expectedPrincipal = crossClusterAccessSubjectInfo.getAuthentication().getEffectiveSubject().getUser().principal();
            assertThat(
                actual.getMessage(),
                equalTo(
                    "Role descriptor for cross cluster access can only contain index privileges but other privileges found for subject ["
                        + expectedPrincipal
                        + "]"
                )
            );
        }
    }

    private RoleDescriptorsIntersection authorizeThenGetRoleDescriptorsIntersectionForRemoteCluster(
        final ThreadContext threadContext,
        final AuthorizationService authzService,
        final Authentication authentication,
        final String concreteClusterAlias
    ) throws InterruptedException {
        try (var ignored = threadContext.stashContext()) {
            assertThat(threadContext.getTransient(AUTHORIZATION_INFO_KEY), nullValue());
            final AtomicReference<RoleDescriptorsIntersection> actual = new AtomicReference<>();
            final CountDownLatch latch = new CountDownLatch(1);
            // A request ID is set during authentication and is required for authorization; since we are not authenticating, set it
            // explicitly
            AuditUtil.generateRequestId(threadContext);

            // Get Role Descriptors for remote cluster should work regardless whether threadContext has existing authz info
            if (randomBoolean()) {
                // Authorize to populate thread context with authz info
                // Note that if the outer listener throws, we will not count down on the latch, however, we also won't get to the await call
                // since the exception will be thrown before -- so no deadlock
                authzService.authorize(
                    authentication,
                    AuthenticateAction.INSTANCE.name(),
                    AuthenticateRequest.INSTANCE,
                    ActionTestUtils.assertNoFailureListener(nothing -> {
                        authzService.getRoleDescriptorsIntersectionForRemoteCluster(
                            concreteClusterAlias,
                            authentication.getEffectiveSubject(),
                            new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(newValue -> {
                                assertThat(threadContext.getTransient(AUTHORIZATION_INFO_KEY), not(nullValue()));
                                actual.set(newValue);
                            }), latch)
                        );
                    })
                );
            } else {
                authzService.getRoleDescriptorsIntersectionForRemoteCluster(
                    concreteClusterAlias,
                    authentication.getEffectiveSubject(),
                    new LatchedActionListener<>(ActionTestUtils.assertNoFailureListener(newValue -> {
                        assertThat(threadContext.getTransient(AUTHORIZATION_INFO_KEY), nullValue());
                        actual.set(newValue);
                    }), latch)
                );
            }

            latch.await();
            // Validate original authz info is restored after call complete
            assertThat(threadContext.getTransient(AUTHORIZATION_INFO_KEY), nullValue());
            return actual.get();
        }
    }
}
