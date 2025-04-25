/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.security;

import com.carrotsearch.randomizedtesting.annotations.TestCaseOrdering;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.test.AnnotationTestOrdering;
import org.elasticsearch.test.AnnotationTestOrdering.Order;
import org.elasticsearch.test.cluster.ElasticsearchCluster;
import org.elasticsearch.test.cluster.MutableSettingsProvider;
import org.elasticsearch.test.cluster.local.distribution.DistributionType;
import org.elasticsearch.test.cluster.local.model.User;
import org.elasticsearch.test.cluster.util.resource.Resource;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.elasticsearch.xpack.core.security.test.TestRestrictedIndices;
import org.elasticsearch.xpack.security.support.QueryableBuiltInRolesSynchronizer;
import org.elasticsearch.xpack.security.support.SecurityMigrations;
import org.junit.BeforeClass;
import org.junit.ClassRule;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.elasticsearch.xpack.security.QueryRoleIT.assertQuery;
import static org.elasticsearch.xpack.security.QueryRoleIT.waitForMigrationCompletion;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.iterableWithSize;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.oneOf;

@TestCaseOrdering(AnnotationTestOrdering.class)
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

    private static MutableSettingsProvider clusterSettings = new MutableSettingsProvider() {
        {
            put("xpack.license.self_generated.type", "basic");
            put("xpack.security.enabled", "true");
            put("xpack.security.http.ssl.enabled", "false");
            put("xpack.security.transport.ssl.enabled", "false");
        }
    };

    @ClassRule
    public static ElasticsearchCluster cluster = ElasticsearchCluster.local()
        .distribution(DistributionType.DEFAULT)
        .nodes(2)
        .settings(clusterSettings)
        .rolesFile(Resource.fromClasspath("roles.yml"))
        .user(ADMIN_USER, ADMIN_PASSWORD.toString(), User.ROOT_USER_ROLE, true)
        .user(REST_USER, REST_PASSWORD.toString(), "security_test_role", false)
        .user(READ_SECURITY_USER, READ_SECURITY_PASSWORD.toString(), "read_security_user_role", false)
        .systemProperty("es.queryable_built_in_roles_enabled", "true")
        .plugin("queryable-reserved-roles-test")
        .build();

    private static Set<String> PREVIOUS_RESERVED_ROLES;
    private static Set<String> CONFIGURED_RESERVED_ROLES;

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

    @Order(10)
    public void testQueryDeleteOrUpdateReservedRoles() throws Exception {
        waitForMigrationCompletion(adminClient(), SecurityMigrations.ROLE_METADATA_FLATTENED_MIGRATION_VERSION);

        final String[] allReservedRoles = ReservedRolesStore.names().toArray(new String[0]);
        assertBusy(() -> {
            assertQuery(client(), """
                { "query": { "bool": { "must": { "term": { "metadata._reserved": true } } } }, "size": 100 }
                """, allReservedRoles.length, roles -> {
                assertThat(roles, iterableWithSize(allReservedRoles.length));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(allReservedRoles)));
                }
            });
        }, 30, TimeUnit.SECONDS);

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

    @Order(11)
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

    @Order(20)
    public void testRestartForConfiguringReservedRoles() throws Exception {
        configureReservedRoles(List.of("editor", "viewer", "kibana_system", "apm_system", "beats_system", "logstash_system"));
        cluster.restart(false);
        closeClients();
    }

    @Order(30)
    public void testConfiguredReservedRoles() throws Exception {
        assert CONFIGURED_RESERVED_ROLES != null;

        // Test query roles API
        assertBusy(() -> {
            assertQuery(client(), """
                { "query": { "bool": { "must": { "term": { "metadata._reserved": true } } } }, "size": 100 }
                """, CONFIGURED_RESERVED_ROLES.size(), roles -> {
                assertThat(roles, iterableWithSize(CONFIGURED_RESERVED_ROLES.size()));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(CONFIGURED_RESERVED_ROLES.toArray(new String[0]))));
                }
            });
        }, 30, TimeUnit.SECONDS);

        // Test get roles API
        assertBusy(() -> {
            final Response response = adminClient().performRequest(new Request("GET", "/_security/role"));
            assertOK(response);
            final Map<String, Object> responseMap = responseAsMap(response);
            assertThat(responseMap.keySet(), equalTo(CONFIGURED_RESERVED_ROLES));
        });
    }

    @Order(40)
    public void testRestartForConfiguringReservedRolesAndClosingIndex() throws Exception {
        configureReservedRoles(List.of("editor", "viewer"));
        closeSecurityIndex();
        cluster.restart(false);
        closeClients();
    }

    @Order(50)
    public void testConfiguredReservedRolesAfterClosingAndOpeningIndex() throws Exception {
        assert CONFIGURED_RESERVED_ROLES != null;
        assert PREVIOUS_RESERVED_ROLES != null;
        assertThat(PREVIOUS_RESERVED_ROLES, is(not(equalTo(CONFIGURED_RESERVED_ROLES))));

        // Test configured roles did not get updated because the security index is closed
        assertMetadataContainsBuiltInRoles(PREVIOUS_RESERVED_ROLES);

        // Open the security index
        openSecurityIndex();

        // Test that the roles are now updated after index got opened
        assertBusy(() -> {
            assertQuery(client(), """
                { "query": { "bool": { "must": { "term": { "metadata._reserved": true } } } }, "size": 100 }
                """, CONFIGURED_RESERVED_ROLES.size(), roles -> {
                assertThat(roles, iterableWithSize(CONFIGURED_RESERVED_ROLES.size()));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(CONFIGURED_RESERVED_ROLES.toArray(new String[0]))));
                }
            });
        }, 30, TimeUnit.SECONDS);

    }

    @Order(60)
    public void testDeletingAndCreatingSecurityIndexTriggersSynchronization() throws Exception {
        deleteSecurityIndex();

        assertBusy(this::assertSecurityIndexDeleted, 30, TimeUnit.SECONDS);

        // Creating a user will trigger .security index creation
        createUser("superman", "superman", "superuser");

        // Test that the roles are now updated after index got created
        assertBusy(() -> {
            assertQuery(client(), """
                { "query": { "bool": { "must": { "term": { "metadata._reserved": true } } } }, "size": 100 }
                """, CONFIGURED_RESERVED_ROLES.size(), roles -> {
                assertThat(roles, iterableWithSize(CONFIGURED_RESERVED_ROLES.size()));
                for (var role : roles) {
                    assertThat((String) role.get("name"), is(oneOf(CONFIGURED_RESERVED_ROLES.toArray(new String[0]))));
                }
            });
        }, 30, TimeUnit.SECONDS);
    }

    private void createUser(String name, String password, String role) throws IOException {
        Request request = new Request("PUT", "/_security/user/" + name);
        request.setJsonEntity("{ \"password\": \"" + password + "\", \"roles\": [ \"" + role + "\"] }");
        assertOK(adminClient().performRequest(request));
    }

    private void deleteSecurityIndex() throws IOException {
        final Request deleteRequest = new Request("DELETE", INTERNAL_SECURITY_MAIN_INDEX_7);
        deleteRequest.setOptions(RequestOptions.DEFAULT.toBuilder().setWarningsHandler(ESRestTestCase::ignoreSystemIndexAccessWarnings));
        final Response response = adminClient().performRequest(deleteRequest);
        try (InputStream is = response.getEntity().getContent()) {
            assertTrue((boolean) XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true).get("acknowledged"));
        }
    }

    private void assertMetadataContainsBuiltInRoles(Set<String> builtInRoles) throws IOException {
        final Request request = new Request("GET", "_cluster/state/metadata/" + INTERNAL_SECURITY_MAIN_INDEX_7);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, String> builtInRolesDigests = ObjectPath.createFromResponse(response)
            .evaluate("metadata.indices.\\.security-7." + QueryableBuiltInRolesSynchronizer.METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST_KEY);
        assertThat(builtInRolesDigests.keySet(), equalTo(builtInRoles));
    }

    private void assertSecurityIndexDeleted() throws IOException {
        final Request request = new Request("GET", "_cluster/state/metadata/" + INTERNAL_SECURITY_MAIN_INDEX_7);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, String> securityIndexMetadata = ObjectPath.createFromResponse(response)
            .evaluate("metadata.indices.\\.security-7");
        assertThat(securityIndexMetadata, is(nullValue()));
    }

    private void configureReservedRoles(List<String> reservedRoles) throws Exception {
        PREVIOUS_RESERVED_ROLES = CONFIGURED_RESERVED_ROLES;
        CONFIGURED_RESERVED_ROLES = new HashSet<>();
        CONFIGURED_RESERVED_ROLES.add("superuser"); // superuser must always be included
        CONFIGURED_RESERVED_ROLES.addAll(reservedRoles);
        clusterSettings.put("xpack.security.reserved_roles.include", Strings.collectionToCommaDelimitedString(CONFIGURED_RESERVED_ROLES));
    }

    private void closeSecurityIndex() throws Exception {
        Request request = new Request("POST", "/" + TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7 + "/_close");
        request.setOptions(
            expectWarnings(
                "this request accesses system indices: [.security-7], but in a future major version, "
                    + "direct access to system indices will be prevented by default"
            )
        );
        Response response = adminClient().performRequest(request);
        assertOK(response);
    }

    private void openSecurityIndex() throws Exception {
        Request request = new Request("POST", "/" + TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7 + "/_open");
        request.setOptions(
            expectWarnings(
                "this request accesses system indices: [.security-7], but in a future major version, "
                    + "direct access to system indices will be prevented by default"
            )
        );
        Response response = adminClient().performRequest(request);
        assertOK(response);
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
