/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.elasticsearch.xpack.core.security.user.User;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SecurityIndexFieldMigrationIT extends AbstractUpgradeTestCase {

    public void testMetadataCanBeQueriedThroughUserAPIInUpgradedCluster() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createUser(
                "test-user",
                "100%-security-guaranteed",
                new String[] { "master-of-the-world", "some-other-role" },
                Map.of("test_key", "test_value")
            );
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            final Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/user");
            request.setJsonEntity("""
                {"query":{"term":{"metadata.test_key":"test_value"}}}""");
            final Response response = client().performRequest(request);
            assertOK(response);
            final Map<String, Object> responseMap = responseAsMap(response);
            @SuppressWarnings("unchecked")
            final List<Map<String, Object>> users = (List<Map<String, Object>>) responseMap.get("users");

            assertEquals(1, users.size());
        }
    }

    public void testMetadataMigratedAfterUpgrade() throws IOException {
        String testUser = "test-user";
        String testRole = "test-role";
        String metaKey = "test_key";
        String metaValue = "test_value";
        // TODO Add test for role mapping and privilege
        Map<String, Object> testMetadata = Map.of(metaKey, metaValue);
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createUser(testUser, "100%-security-guaranteed", new String[] { "superuser" }, testMetadata);
            assertEntityInSecurityIndex("user", testUser);
            createRole(testRole, testMetadata);
            assertEntityInSecurityIndex("role", testRole);
        }
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            assertEntityInSecurityIndex("user", testUser, metaKey, metaValue);
            assertEntityInSecurityIndex("role", testRole, metaKey, metaValue);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertEntityInSecurityIndex(String type, String value, String metaKey, String metaValue) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(
            String.format(
                Locale.ROOT,
                """
                    {"query":{"bool":{"must":[{"term":{"_id":"%s-%s"}},{"term":{"metadata_flattened.%s":"%s"}}]}}}""",
                type,
                value,
                metaKey,
                metaValue
            )
        );
        addExpectWarningOption(options);
        request.setOptions(options);

        Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        Map<String, Object> hits = ((Map<String, Object>) responseMap.get("hits"));
        assertEquals(1, ((List<Object>) hits.get("hits")).size());
    }

    @SuppressWarnings("unchecked")
    private void assertEntityInSecurityIndex(String type, String id) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"query":{"term":{"_id":"%s-%s"}}}""", type, id));
        addExpectWarningOption(options);
        request.setOptions(options);
        Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        Map<String, Object> hits = ((Map<String, Object>) responseMap.get("hits"));
        assertEquals(1, ((List<Object>) hits.get("hits")).size());
    }

    private void addExpectWarningOption(RequestOptions.Builder options) {
        Set<String> expectedWarnings = Set.of(
            "this request accesses system indices: [.security-7],"
                + " but in a future major version, direct access to system indices will be prevented by default"
        );

        options.setWarningsHandler(warnings -> {
            final Set<String> actual = Set.copyOf(warnings);
            // Return true if the warnings aren't what we expected; the client will treat them as a fatal error.
            return actual.equals(expectedWarnings) == false;
        });

    }

    private void createUser(String username, String password, String[] roles, Map<String, Object> metadata) throws IOException {
        final Request request = new Request("POST", "/_security/user/" + username);
        BytesReference source = BytesReference.bytes(
            jsonBuilder().map(
                Map.of(
                    User.Fields.USERNAME.getPreferredName(),
                    username,
                    User.Fields.ROLES.getPreferredName(),
                    roles,
                    User.Fields.FULL_NAME.getPreferredName(),
                    "Mr User Usersson",
                    User.Fields.EMAIL.getPreferredName(),
                    "user@user.com",
                    User.Fields.METADATA.getPreferredName(),
                    metadata,
                    User.Fields.PASSWORD.getPreferredName(),
                    password,
                    User.Fields.ENABLED.getPreferredName(),
                    true
                )
            )
        );
        request.setJsonEntity(source.utf8ToString());
        assertOK(adminClient().performRequest(request));
        refreshSecurityIndex();
    }

    private void createRole(String roleName, Map<String, Object> metadata) throws IOException {
        final Request request = new Request("POST", "/_security/role/" + roleName);
        BytesReference source = BytesReference.bytes(
            jsonBuilder().map(
                Map.of(
                    RoleDescriptor.Fields.CLUSTER.getPreferredName(),
                    List.of("cluster:monitor/xpack/license/get"),
                    RoleDescriptor.Fields.METADATA.getPreferredName(),
                    metadata
                )
            )
        );
        request.setJsonEntity(source.utf8ToString());
        assertOK(adminClient().performRequest(request));
        refreshSecurityIndex();
    }

    private void refreshSecurityIndex() throws IOException {
        Request request = new Request("POST", "/.security-7/_refresh");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        addExpectWarningOption(options);
        request.setOptions(options);
        assertOK(adminClient().performRequest(request));
    }
}
