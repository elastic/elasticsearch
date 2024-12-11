/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import static org.elasticsearch.xpack.security.QueryRoleIT.assertQuery;
import static org.elasticsearch.xpack.security.QueryRoleIT.waitForMigrationCompletion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.oneOf;

public class QueryableReservedRolesIT extends ESRestTestCase {

    protected static final String REST_USER = "security_test_user";
    private static final SecureString REST_PASSWORD = new SecureString("security-test-password".toCharArray());
    private static final String ADMIN_USER = "admin_user";
    private static final SecureString ADMIN_PASSWORD = new SecureString("admin-password".toCharArray());

    protected static final String READ_SECURITY_USER = "read_security_user";
    private static final SecureString READ_SECURITY_PASSWORD = new SecureString("read-security-password".toCharArray());

    @BeforeClass
    public static void setup() {
        new ReservedRolesStore();
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .nodes(2)
        .distribution(DistributionType.DEFAULT)
        .setting("xpack.license.self_generated.type", "basic")
        .setting("xpack.security.enabled", "true")
        .setting("xpack.security.ssl.diagnose.trust", "true")
        .setting("xpack.security.http.ssl.enabled", "false")
        .setting("xpack.security.transport.ssl.enabled", "false")
        .setting("xpack.security.authc.token.enabled", "true")
        .setting("xpack.security.authc.api_key.enabled", "true")
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .user(REST_USER, REST_PASSWORD.toString(), "security_test_role", false)
        .user(READ_SECURITY_USER, READ_SECURITY_PASSWORD.toString(), "read_security_user_role", false)
        .systemProperty("es.queryable_built_in_roles_enabled", "true")
        .build();

    @Override
    protected String getTestRestCluster() {
        return cluster.getHttpAddresses();
    }

    @Override
    protected Settings restAdminSettings() {
        String token = basicAuthHeaderValue(ADMIN_USER, ADMIN_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    @Override
    protected Settings restClientSettings() {
        String token = basicAuthHeaderValue(REST_USER, REST_PASSWORD);
        return Settings.builder().put(ThreadContext.PREFIX + ".Authorization", token).build();
    }

    public void testQueryDeleteOrUpdateReservedRoles() throws Exception {
        waitForMigrationCompletion(adminClient(), SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION);

        final String[] allReservedRoles = ReservedRolesStore.names().toArray(new String[0]);
        assertQuery(client(), """
            { "query": { "bool": { "must": { "term": { "metadata._reserved": true } } } }, "size": 100 }
            """, allReservedRoles.length, roles -> {
            assertThat(roles, iterableWithSize(allReservedRoles.length));
            for (var role : roles) {
                assertThat((String) role.get("name"), is(oneOf(allReservedRoles)));
            }
        });

        final String roleName = randomFrom(allReservedRoles);
        assertQuery(client(), String.format("""
            { "query": { "bool": { "must": { "term": { "name": "%s" } } } } }
            """, roleName), 1, roles -> {
            assertThat(roles, iterableWithSize(1));
            assertThat((String) roles.get(0).get("name"), equalTo(roleName));
        });

        assertCannotDeleteReservedRoles();
        assertCannotCreateOrUpdateReservedRole(roleName);
    }

    public void testGetReservedRoles() throws Exception {
        final String[] allReservedRoles = ReservedRolesStore.names().toArray(new String[0]);
        final String roleName = randomFrom(allReservedRoles);
        Request request = new Request("GET", "/_security/role/" + roleName);
        Response response = adminClient().performRequest(request);
        assertOK(response);
        var responseMap = responseAsMap(response);
        assertThat(responseMap.size(), equalTo(1));
        assertThat(responseMap.containsKey(roleName), is(true));
    }

    private void assertCannotDeleteReservedRoles() throws Exception {
        {
            String roleName = randomFrom(ReservedRolesStore.names());
            Request request = new Request("DELETE", "/_security/role/" + roleName);
            var e = expectThrows(ResponseException.class, () -> adminClient().performRequest(request));
            assertThat(e.getMessage(), containsString("role [" + roleName + "] is reserved and cannot be deleted"));
        }
        {
            Request request = new Request("DELETE", "/_security/role/");
            request.setJsonEntity(
                """
                    {
                      "names": [%s]
                    }
                    """.formatted(
                    ReservedRolesStore.names().stream().map(name -> "\"" + name + "\"").reduce((a, b) -> a + ", " + b).orElse("")
                )
            );
            Response response = adminClient().performRequest(request);
            assertOK(response);
            String responseAsString = responseAsMap(response).toString();
            for (String roleName : ReservedRolesStore.names()) {
                assertThat(responseAsString, containsString("role [" + roleName + "] is reserved and cannot be deleted"));
            }
        }
    }

    private void assertCannotCreateOrUpdateReservedRole(String roleName) throws Exception {
        Request request = new Request(randomBoolean() ? "PUT" : "POST", "/_security/role/" + roleName);
        request.setJsonEntity("""
            {
              "cluster": ["all"],
              "indices": [
                {
                  "names": ["*"],
                  "privileges": ["all"]
                }
              ]
            }
            """);
        var e = expectThrows(ResponseException.class, () -> adminClient().performRequest(request));
        assertThat(e.getMessage(), containsString("Role [" + roleName + "] is reserved and may not be used."));
    }

}
