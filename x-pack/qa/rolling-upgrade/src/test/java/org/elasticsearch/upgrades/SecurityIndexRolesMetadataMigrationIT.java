/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import org.elasticsearch.client.Node;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SecurityIndexRolesMetadataMigrationIT extends AbstractUpgradeTestCase {

    public void testRoleMigration() throws Exception {
        String oldTestRole = "old-test-role";
        String mixed1TestRole = "mixed1-test-role";
        String mixed2TestRole = "mixed2-test-role";
        String upgradedTestRole = "upgraded-test-role";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createRoleWithMetadata(oldTestRole, Map.of("meta", "test"));
            assertDocInSecurityIndex(oldTestRole);
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            if (FIRST_MIXED_ROUND) {
                createRoleWithMetadata(mixed1TestRole, Map.of("meta", "test"));
                assertDocInSecurityIndex(mixed1TestRole);
            } else {
                createRoleWithMetadata(mixed2TestRole, Map.of("meta", "test"));
                assertDocInSecurityIndex(mixed2TestRole);
            }
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            createRoleWithMetadata(upgradedTestRole, Map.of("meta", "test"));
            waitForSecurityMigrationCompletion(adminClient(), 1);
            assertMigratedDocInSecurityIndex(oldTestRole, "meta", "test");
            assertMigratedDocInSecurityIndex(mixed1TestRole, "meta", "test");
            assertMigratedDocInSecurityIndex(mixed2TestRole, "meta", "test");
            assertMigratedDocInSecurityIndex(upgradedTestRole, "meta", "test");
            // queries all roles by metadata
            assertAllRoles(client(), "mixed1-test-role", "mixed2-test-role", "old-test-role", "upgraded-test-role");
        }
    }

    @SuppressWarnings("unchecked")
    private void assertMigratedDocInSecurityIndex(String roleName, String metaKey, String metaValue) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(
            String.format(
                Locale.ROOT,
                """
                    {"query":{"bool":{"must":[{"term":{"_id":"%s-%s"}},{"term":{"metadata_flattened.%s":"%s"}}]}}}""",
                "role",
                roleName,
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
    private void assertDocInSecurityIndex(String id) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"query":{"term":{"_id":"%s-%s"}}}""", "role", id));
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

    @SuppressWarnings("unchecked")
    private static void assertNoMigration(RestClient adminClient) throws Exception {
        Request request = new Request("GET", "_cluster/state/metadata/" + INTERNAL_SECURITY_MAIN_INDEX_7);
        Response response = adminClient.performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        Map<String, Object> indicesMetadataMap = (Map<String, Object>) ((Map<String, Object>) responseMap.get("metadata")).get("indices");
        assertTrue(indicesMetadataMap.containsKey(INTERNAL_SECURITY_MAIN_INDEX_7));
        assertFalse(
            ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).containsKey(MIGRATION_VERSION_CUSTOM_KEY)
        );
    }

    private void createRoleWithMetadata(String roleName, Map<String, Object> metadata) throws IOException {
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
        assertOK(client().performRequest(request));
    }

    private void assertCannotQueryRolesByMetadata(RestClient client) {
        List<Node> originalNodes = client.getNodes();
        try {
            // try the query on every node (upgraded or not)
            for (Node node : originalNodes) {
                client.setNodes(List.of(node));
                String metadataQuery = """
                    {"query":{"exists":{"field":"metadata.test"}}}""";
                Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
                request.setJsonEntity(metadataQuery);
                ResponseException e = expectThrows(ResponseException.class, () -> client.performRequest(request));
                if (e.getResponse().getStatusLine().getStatusCode() == 400) {
                    // this is an old node that doesn't know about the API
                    // note that 7.17 shows different error messages from "no handler found for uri"
                } else if (e.getResponse().getStatusLine().getStatusCode() == 503) {
                    // this is an upgraded node, but migration does not work
                    assertThat(e.getMessage(), containsString("Cannot query or sort role metadata until automatic migration completed"));
                } else {
                    fail(e, "Unexpected exception type");
                }
            }
        } finally {
            client.setNodes(originalNodes);
        }
    }

    private void assertAllRoles(RestClient client, String... roleNames) throws IOException {
        // this queries all roles by metadata
        String metadataQuery = """
            {"query":{"bool":{"must":[{"exists":{"field":"metadata.meta"}}]}},"sort":["name"]}""";
        Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
        request.setJsonEntity(metadataQuery);
        Response response = client.performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        assertThat(responseMap.get("total"), is(roleNames.length));
        assertThat(responseMap.get("count"), is(roleNames.length));
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> roles = new ArrayList<>((List<Map<String, Object>>) responseMap.get("roles"));
        assertThat(roles.size(), is(responseMap.get("count")));
        for (int i = 0; i < roleNames.length; i++) {
            assertThat(roles.get(i).get("name"), equalTo(roleNames[i]));
        }
    }
}
