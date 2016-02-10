/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.shield.authz.permission;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthAction;
import org.elasticsearch.action.admin.cluster.state.ClusterStateAction;
import org.elasticsearch.action.admin.cluster.stats.ClusterStatsAction;
import org.elasticsearch.client.Client;
import org.elasticsearch.license.plugin.action.get.GetLicenseAction;
import org.elasticsearch.shield.action.user.AuthenticateRequestBuilder;
import org.elasticsearch.shield.action.user.ChangePasswordRequestBuilder;
import org.elasticsearch.shield.user.User;
import org.elasticsearch.shield.action.user.AuthenticateAction;
import org.elasticsearch.shield.action.user.AuthenticateRequest;
import org.elasticsearch.shield.action.user.ChangePasswordAction;
import org.elasticsearch.shield.action.user.ChangePasswordRequest;
import org.elasticsearch.shield.action.user.DeleteUserAction;
import org.elasticsearch.shield.action.user.PutUserAction;
import org.elasticsearch.shield.action.user.UserRequest;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportRequest;

import java.util.Iterator;

import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;

/**
 * Unit tests for the {@link DefaultRole}
 */
public class DefaultRoleTests extends ESTestCase {

    public void testDefaultRoleHasNoIndicesPrivileges() {
        Iterator<IndicesPermission.Group> iter = DefaultRole.INSTANCE.indices().iterator();
        assertThat(iter.hasNext(), is(false));
    }

    public void testDefaultRoleHasNoRunAsPrivileges() {
        assertThat(DefaultRole.INSTANCE.runAs().isEmpty(), is(true));
    }

    public void testDefaultRoleAllowsUser() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final TransportRequest request = changePasswordRequest ?
                new ChangePasswordRequestBuilder(mock(Client.class)).username(user.principal()).request() :
                new AuthenticateRequestBuilder(mock(Client.class)).username(user.principal()).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        assertThat(request, instanceOf(UserRequest.class));

        assertThat(DefaultRole.INSTANCE.cluster().check(action, request, user), is(true));
    }

    public void testDefaultRoleDoesNotAllowNonMatchingUsername() {
        final User user = new User("joe");
        final boolean changePasswordRequest = randomBoolean();
        final String username = randomFrom("", "joe" + randomAsciiOfLengthBetween(1, 5), randomAsciiOfLengthBetween(3, 10));
        final TransportRequest request = changePasswordRequest ?
                new ChangePasswordRequestBuilder(mock(Client.class)).username(username).request() :
                new AuthenticateRequestBuilder(mock(Client.class)).username(username).request();
        final String action = changePasswordRequest ? ChangePasswordAction.NAME : AuthenticateAction.NAME;
        assertThat(request, instanceOf(UserRequest.class));

        assertThat(DefaultRole.INSTANCE.cluster().check(action, request, user), is(false));

        final User user2 = new User("admin", new String[] { "bar" }, user);
        if (request instanceof ChangePasswordRequest) {
            ((ChangePasswordRequest)request).username("joe");
        } else {
            ((AuthenticateRequest)request).username("joe");
        }
        // run as should not be checked by this role, it is up to the caller to provide the correct user
        assertThat(DefaultRole.INSTANCE.cluster().check(action, request, user2), is(false));
        assertThat(DefaultRole.INSTANCE.cluster().check(action, request, user), is(true));
    }

    public void testDefaultRoleDoesNotAllowOtherActions() {
        final User user = mock(User.class);
        final TransportRequest request = mock(TransportRequest.class);
        final String action = randomFrom(PutUserAction.NAME, DeleteUserAction.NAME, ClusterHealthAction.NAME, ClusterStateAction.NAME,
                ClusterStatsAction.NAME, GetLicenseAction.NAME);

        assertThat(DefaultRole.INSTANCE.cluster().check(action, request, user), is(false));
        verifyZeroInteractions(user, request);
    }
}
