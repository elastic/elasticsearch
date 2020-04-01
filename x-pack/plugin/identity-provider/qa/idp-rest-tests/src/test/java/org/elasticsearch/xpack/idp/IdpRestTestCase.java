/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.idp;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.ApplicationResourcePrivileges;
import org.elasticsearch.client.security.user.privileges.IndicesPrivileges;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public abstract class IdpRestTestCase extends ESRestTestCase {

    private RestHighLevelClient highLevelAdminClient;

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("idp_user", new SecureString("idp-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    private RestHighLevelClient getHighLevelAdminClient() {
        if (highLevelAdminClient == null) {
            highLevelAdminClient = new RestHighLevelClient(
                adminClient(),
                ignore -> {
                },
                List.of()) {
            };
        }
        return highLevelAdminClient;
    }

    protected User createUser(String username, SecureString password, String... roles) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final User user = new User(username, List.of(roles), Map.of(), username + " in " + getTestName(), username + "@test.example.com");
        final PutUserRequest request = PutUserRequest.withPassword(user, password.getChars(), true, RefreshPolicy.WAIT_UNTIL);
        client.security().putUser(request, RequestOptions.DEFAULT);
        return user;
    }

    protected void deleteUser(String username) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final DeleteUserRequest request = new DeleteUserRequest(username, RefreshPolicy.WAIT_UNTIL);
        client.security().deleteUser(request, RequestOptions.DEFAULT);
    }

    protected void createRole(String name, Collection<String> clusterPrivileges, Collection<IndicesPrivileges> indicesPrivileges,
                              Collection<ApplicationResourcePrivileges> applicationPrivileges) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final Role role = Role.builder()
            .name(name)
            .clusterPrivileges(clusterPrivileges)
            .indicesPrivileges(indicesPrivileges)
            .applicationResourcePrivileges(applicationPrivileges)
            .build();
        client.security().putRole(new PutRoleRequest(role, null), RequestOptions.DEFAULT);
    }

    protected void deleteRole(String name) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final DeleteRoleRequest request = new DeleteRoleRequest(name, RefreshPolicy.WAIT_UNTIL);
        client.security().deleteRole(request, RequestOptions.DEFAULT);
    }
}
