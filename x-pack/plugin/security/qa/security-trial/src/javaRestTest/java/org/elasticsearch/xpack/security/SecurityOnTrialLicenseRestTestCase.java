/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.CreateTokenResponse;
import org.elasticsearch.client.security.DeleteRoleRequest;
import org.elasticsearch.client.security.DeleteUserRequest;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetApiKeyResponse;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.PutRoleRequest;
import org.elasticsearch.client.security.PutUserRequest;
import org.elasticsearch.client.security.RefreshPolicy;
import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.client.security.user.User;
import org.elasticsearch.client.security.user.privileges.Role;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken.basicAuthHeaderValue;

public abstract class SecurityOnTrialLicenseRestTestCase extends ESRestTestCase {
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
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder()
            .put(ThreadContext.PREFIX + ".Authorization", token)
            .build();
    }

    protected void createUser(String username, SecureString password, List<String> roles) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().putUser(PutUserRequest.withPassword(new User(username, roles), password.getChars(), true,
            RefreshPolicy.WAIT_UNTIL), RequestOptions.DEFAULT);
    }

    protected void createRole(String name, Collection<String> clusterPrivileges) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final Role role = Role.builder().name(name).clusterPrivileges(clusterPrivileges).build();
        client.security().putRole(new PutRoleRequest(role, null), RequestOptions.DEFAULT);
    }

    /**
     * @return A tuple of (access-token, refresh-token)
     */
    protected Tuple<String, String> createOAuthToken(String username, SecureString password) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final CreateTokenRequest request = CreateTokenRequest.passwordGrant(username, password.getChars());
        final CreateTokenResponse response = client.security().createToken(request, RequestOptions.DEFAULT);
        return Tuple.tuple(response.getAccessToken(), response.getRefreshToken());
    }

    protected void deleteUser(String username) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().deleteUser(new DeleteUserRequest(username), RequestOptions.DEFAULT);
    }

    protected void deleteRole(String name) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().deleteRole(new DeleteRoleRequest(name), RequestOptions.DEFAULT);
    }

    protected void invalidateApiKeysForUser(String username) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        client.security().invalidateApiKey(InvalidateApiKeyRequest.usingUserName(username), RequestOptions.DEFAULT);
    }

    protected ApiKey getApiKey(String id) throws IOException {
        final RestHighLevelClient client = getHighLevelAdminClient();
        final GetApiKeyResponse response = client.security().getApiKey(GetApiKeyRequest.usingApiKeyId(id, false), RequestOptions.DEFAULT);
        assertThat(response.getApiKeyInfos(), Matchers.iterableWithSize(1));
        return response.getApiKeyInfos().get(0);
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
}
