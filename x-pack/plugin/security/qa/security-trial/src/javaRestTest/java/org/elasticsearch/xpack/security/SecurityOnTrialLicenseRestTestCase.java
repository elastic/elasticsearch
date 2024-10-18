/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.LocalClusterConfigProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.action.apikey.ApiKey;
import org.elasticsearch.xpack.core.security.authc.support.UsernamePasswordToken;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.ClassRule;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.cluster.local.model.User.ROOT_USER_ROLE;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public abstract class SecurityOnTrialLicenseRestTestCase extends ESRestTestCase {
    private TestSecurityClient securityClient;

    public static LocalClusterConfigProvider commonTrialSecurityClusterConfig = cluster -> cluster.nodes(2)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.ml.enabled", "false")
        .setting("xpack.license.self_generated.type", "trial")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.ssl.diagnose.trust", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .setting("xpack.security.remote_cluster_client.ssl.enabled", "false")
        .keystore("cluster.remote.my_remote_cluster_a.credentials", "cluster_a_credentials")
        .keystore("cluster.remote.my_remote_cluster_b.credentials", "cluster_b_credentials")
        .keystore("cluster.remote.my_remote_cluster_a_1.credentials", "cluster_a_credentials")
        .keystore("cluster.remote.my_remote_cluster_a_2.credentials", "cluster_a_credentials")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user("admin_user", "admin-password", ROOT_USER_ROLE, true)
        .user("security_test_user", "security-test-password", "security_test_role", false)
        .user("x_pack_rest_user", "x-pack-test-password", ROOT_USER_ROLE, true)
        .user("cat_test_user", "cat-test-password", "cat_test_role", false);

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local().apply(commonTrialSecurityClusterConfig).build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

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
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null
        );
        getSecurityClient().putRole(role);
    }

    /**
     * @return A tuple of (access-token, refresh-token)
     */
    protected Tuple<String, String> createOAuthToken(String username, SecureString password) throws IOException {
        final TestSecurityClient securityClient = new TestSecurityClient(adminClient());
        final TestSecurityClient.OAuth2Token token = securityClient.createToken(
            new UsernamePasswordToken(username, new SecureString(password.getChars()))
        );
        return new Tuple<>(token.accessToken(), token.getRefreshToken());
    }

    protected void deleteUser(String username) throws IOException {
        getSecurityClient().deleteUser(username);
    }

    protected void deleteRole(String name) throws IOException {
        getSecurityClient().deleteRole(name);
    }

    protected void invalidateApiKeysForUser(String username) throws IOException {
        getSecurityClient().invalidateApiKeysForUser(username);
    }

    protected ApiKey getApiKey(String id) throws IOException {
        final TestSecurityClient client = getSecurityClient();
        return client.getApiKey(id);
    }

    protected void upsertRole(String roleDescriptor, String roleName) throws IOException {
        Request createRoleRequest = roleRequest(roleDescriptor, roleName);
        Response createRoleResponse = adminClient().performRequest(createRoleRequest);
        assertOK(createRoleResponse);
    }

    protected Request roleRequest(String roleDescriptor, String roleName) {
        Request createRoleRequest;
        if (randomBoolean()) {
            createRoleRequest = new Request(randomFrom(HttpPut.METHOD_NAME, HttpPost.METHOD_NAME), "/_security/role/" + roleName);
            createRoleRequest.setJsonEntity(roleDescriptor);
        } else {
            createRoleRequest = new Request(HttpPost.METHOD_NAME, "/_security/role");
            createRoleRequest.setJsonEntity(Strings.format("""
                {"roles": {"%s": %s}}
                """, roleName, roleDescriptor));
        }
        return createRoleRequest;
    }

    @SuppressWarnings("unchecked")
    protected void assertSendRequestThrowsError(Request request, String expectedError) throws IOException {
        String errorMessage;
        if (request.getEndpoint().endsWith("/role")) {
            Map<String, Object> response = responseAsMap(adminClient().performRequest(request));

            Map<String, Object> errors = (Map<String, Object>) response.get("errors");
            Map<String, Object> failedItems = (Map<String, Object>) errors.get("details");
            assertEquals(failedItems.size(), 1);
            Map<String, Object> error = (Map<String, Object>) failedItems.values().stream().findFirst().orElseThrow();
            errorMessage = (String) error.get("reason");
        } else {
            ResponseException e = expectThrows(ResponseException.class, () -> adminClient().performRequest(request));
            assertEquals(400, e.getResponse().getStatusLine().getStatusCode());
            errorMessage = e.getMessage();
        }
        assertThat(errorMessage, containsString(expectedError));
    }

    protected void fetchRoleAndAssertEqualsExpected(final String roleName, final RoleDescriptor expectedRoleDescriptor) throws IOException {
        final Response getRoleResponse = adminClient().performRequest(new Request("GET", "/_security/role/" + roleName));
        assertOK(getRoleResponse);
        final Map<String, RoleDescriptor> actual = responseAsParser(getRoleResponse).map(
            HashMap::new,
            p -> RoleDescriptor.parserBuilder().allowDescription(true).build().parse(expectedRoleDescriptor.getName(), p)
        );
        assertThat(actual, equalTo(Map.of(expectedRoleDescriptor.getName(), expectedRoleDescriptor)));
    }

    protected Map<String, Object> upsertRoles(String roleDescriptorsByName) throws IOException {
        Request request = rolesRequest(roleDescriptorsByName);
        Response response = adminClient().performRequest(request);
        assertOK(response);
        return responseAsMap(response);
    }

    protected Request rolesRequest(String roleDescriptorsByName) {
        Request rolesRequest;
        rolesRequest = new Request(HttpPost.METHOD_NAME, "/_security/role");
        rolesRequest.setJsonEntity(org.elasticsearch.core.Strings.format(roleDescriptorsByName));
        return rolesRequest;
    }
}
