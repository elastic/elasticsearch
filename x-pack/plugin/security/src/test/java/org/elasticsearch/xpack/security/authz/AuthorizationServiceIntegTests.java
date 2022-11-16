/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.transport.TcpTransport;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authc.AuthenticationField;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptorsIntersection;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;

public class AuthorizationServiceIntegTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    public void testRetrieveRemoteAccessRoleDescriptorsIntersectionForNonInternalUser() throws IOException, InterruptedException {
        assumeTrue("untrusted remote cluster feature flag must be enabled", TcpTransport.isUntrustedRemoteClusterEnabled());

        final String concreteClusterAlias = randomAlphaOfLength(10);
        final String roleName = randomAlphaOfLength(5);
        getSecurityClient().putRole(
            new RoleDescriptor(
                roleName,
                null,
                null,
                null,
                null,
                null,
                null,
                null,
                new RoleDescriptor.RemoteIndicesPrivileges[] {
                    new RoleDescriptor.RemoteIndicesPrivileges(
                        RoleDescriptor.IndicesPrivileges.builder().indices("index").privileges("read").build(),
                        randomNonEmptySubsetOf(List.of(concreteClusterAlias, "*")).toArray(new String[0])
                    ) }
            )
        );
        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final AuthorizationService authzService = internalCluster().getInstance(AuthorizationService.class, nodeName);
        final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
        final String type = randomAlphaOfLengthBetween(5, 16);
        final Authentication authentication = Authentication.newRealmAuthentication(
            new User(randomAlphaOfLengthBetween(5, 16), roleName),
            new Authentication.RealmRef(realmName, type, nodeName)
        );
        final CountDownLatch latch = new CountDownLatch(1);
        final AtomicReference<RoleDescriptorsIntersection> actual = new AtomicReference<>();
        // This is set during authentication; since we are not authenticating set it explicitly as it's required for the `authorize`
        // call below
        AuditUtil.generateRequestId(threadContext);
        // Authorize to populate thread context with authz info
        ActionListener<Void> authzListener = ActionTestUtils.assertNoFailureListener(ignored -> {
            authzService.retrieveRemoteAccessRoleDescriptorsIntersection(
                concreteClusterAlias,
                authentication.getEffectiveSubject(),
                ActionTestUtils.assertNoFailureListener(roleDescriptorsIntersection -> {
                    actual.set(roleDescriptorsIntersection);
                    latch.countDown();
                })
            );
        });
        authzService.authorize(authentication, AuthenticateAction.INSTANCE.name(), AuthenticateRequest.INSTANCE, authzListener);
        latch.await();
        assertThat(
            actual.get(),
            equalTo(
                new RoleDescriptorsIntersection(
                    List.of(
                        Set.of(
                            new RoleDescriptor(
                                roleName,
                                null,
                                new RoleDescriptor.IndicesPrivileges[] {
                                    RoleDescriptor.IndicesPrivileges.builder().indices("index").privileges("read").build() },
                                null,
                                null,
                                null,
                                Map.of(AuthenticationField.REMOTE_ACCESS_ROLE_NAMES_KEY, List.of(roleName)),
                                null
                            )
                        )
                    )
                )
            )
        );
    }

}
