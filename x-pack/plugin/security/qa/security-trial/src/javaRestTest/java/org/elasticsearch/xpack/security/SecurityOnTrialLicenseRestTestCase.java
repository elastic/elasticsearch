/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.test.TestSecurityClient;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
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
import java.util.List;

import static org.elasticsearch.test.cluster.local.model.User.ROOT_USER_ROLE;

public abstract class SecurityOnTrialLicenseRestTestCase extends ESRestTestCase {
    private TestSecurityClient securityClient;

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
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
        .user("cat_test_user", "cat-test-password", "cat_test_role", false)
        .build();

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
            new RoleDescriptor.IndicesPrivileges[0],
            new String[0]
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
}
