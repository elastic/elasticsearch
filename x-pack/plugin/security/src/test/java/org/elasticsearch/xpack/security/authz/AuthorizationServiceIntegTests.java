/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security.authz;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.core.security.SecurityContext;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateAction;
import org.elasticsearch.xpack.core.security.action.user.AuthenticateRequest;
import org.elasticsearch.xpack.core.security.authc.Authentication;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.elasticsearch.xpack.security.audit.AuditUtil;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

public class AuthorizationServiceIntegTests extends SecurityIntegTestCase {

    @Override
    protected boolean addMockHttpTransport() {
        return false; // need real http
    }

    public void test() throws ExecutionException, InterruptedException, IOException {
        getSecurityClient().putRole(
            new RoleDescriptor(
                "remote_role",
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
                        "remote"
                    ) }
            )
        );

        final String nodeName = internalCluster().getRandomNodeName();
        final ThreadContext threadContext = internalCluster().getInstance(SecurityContext.class, nodeName).getThreadContext();
        final AuthorizationService authzService = internalCluster().getInstance(AuthorizationService.class, nodeName);
        final String realmName = randomBoolean() ? "realm" : randomAlphaOfLengthBetween(1, 16);
        final String type = randomAlphaOfLengthBetween(5, 16);
        final Authentication authentication = Authentication.newRealmAuthentication(
            new User("remote_user", "remote_role"),
            new Authentication.RealmRef(realmName, type, nodeName)
        );

        try (ThreadContext.StoredContext ignore = threadContext.stashContext()) {
            // authentication.writeToContext(threadContext);
            AuditUtil.generateRequestId(threadContext);
            // Authorize to populate thread context with authz info
            authzService.authorize(
                authentication,
                AuthenticateAction.INSTANCE.name(),
                AuthenticateRequest.INSTANCE,
                ActionListener.wrap(ignored -> {
                    authzService.retrieveRemoteAccessRoleDescriptorsIntersection(
                        "remote",
                        authentication.getEffectiveSubject(),
                        ActionListener.wrap(
                            actual -> { assertThat(actual.roleDescriptorsList().size(), equalTo(1)); },
                            ex -> fail("No errors expected")
                        )
                    );
                }, ex -> fail("No errors expected"))
            );
        }
    }

}
