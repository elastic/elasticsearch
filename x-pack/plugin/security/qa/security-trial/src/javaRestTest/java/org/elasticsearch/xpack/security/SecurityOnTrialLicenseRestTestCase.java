/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.security.CreateTokenRequest;
import org.elasticsearch.client.security.CreateTokenResponse;
import org.elasticsearch.client.security.GetApiKeyRequest;
import org.elasticsearch.client.security.GetApiKeyResponse;
import org.elasticsearch.client.security.InvalidateApiKeyRequest;
import org.elasticsearch.client.security.support.ApiKey;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Collection;
import java.util.List;

@SuppressWarnings("removal")
public abstract class SecurityOnTrialLicenseRestTestCase extends ESRestTestCase {
    private RestHighLevelClient highLevelAdminClient;
    private TestSecurityClient securityClient;

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue("admin_user", new SecureString("admin-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue("security_test_user", new SecureString("security-test-password".toCharArray()));
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    protected TestSecurityClient getSecurityClient() {
        if (securityClient == null) {
            securityClient = new TestSecurityClient(adminClient());
        }
        return securityClient;
    }

    protected void createUser(String username, SecureString password, List<String> roles) throws IOException {
        getSecurityClient().putUser(new User(username, roles.toArray(String[]::new)), password);
    }

    protected void createRole(String name, Collection<String> clusterPrivileges) throws IOException {
        final RoleDescriptor role = new RoleDescriptor(
            name,
            clusterPrivileges.toArray(String[]::new),
            new RoleDescriptor.IndicesPrivileges[0],
            new String[0]
        );
        getSecurityClient().putRole(role);
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
        getSecurityClient().deleteUser(username);
    }

    protected void deleteRole(String name) throws IOException {
        getSecurityClient().deleteRole(name);
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
            highLevelAdminClient = new RestHighLevelClient(adminClient(), ignore -> {}, List.of()) {
            };
        }
        return highLevelAdminClient;
    }
}
