/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.upgrades;

import com.carrotsearch.randomizedtesting.annotations.Name;

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
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.equalTo;

public class SecurityIndexRolesMetadataMigrationIT extends AbstractXpackRollingUpgradeWithSecurityTestCase {

    public SecurityIndexRolesMetadataMigrationIT(@Name("upgradedNodes") int upgradedNodes) {
        super(upgradedNodes);
    }

    public void testRoleMigration() throws Exception {
        String oldTestRole = "old-test-role";
        String mixed1TestRole = "mixed1-test-role";
        String mixed2TestRole = "mixed2-test-role";
        String upgradedTestRole = "upgraded-test-role";
        if (isOldCluster()) {
            createRoleWithMetadata(oldTestRole, Map.of("meta", "test"));
            assertDocInSecurityIndex(oldTestRole);
        } else if (isFirstMixedCluster()) {
            createRoleWithMetadata(mixed1TestRole, Map.of("meta", "test"));
            assertDocInSecurityIndex(mixed1TestRole);
        } else if (isMixedCluster()) {
            createRoleWithMetadata(mixed2TestRole, Map.of("meta", "test"));
            assertDocInSecurityIndex(mixed2TestRole);
        } else if (isUpgradedCluster()) {
            createRoleWithMetadata(upgradedTestRole, Map.of("meta", "test"));
            waitForSecurityMigrationCompletion(adminClient(), 1);
            assertMigratedDocInSecurityIndex(oldTestRole, "meta", "test");
            assertMigratedDocInSecurityIndex(mixed1TestRole, "meta", "test");
            assertMigratedDocInSecurityIndex(mixed2TestRole, "meta", "test");
            assertMigratedDocInSecurityIndex(upgradedTestRole, "meta", "test");
            // query all roles by metadata - use assertBusy to handle the case where the node handling the query is not yet aware of the
            // successful migration
            assertBusy(() -> assertAllRoles(client(), "mixed1-test-role", "mixed2-test-role", "old-test-role", "upgraded-test-role"));
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

    private void assertAllRoles(RestClient client, String... roleNames) throws IOException {
        // this queries all roles by metadata
        String metadataQuery = """
            {"query":{"bool":{"must":[{"exists":{"field":"metadata.meta"}}]}},"sort":["name"]}""";
        Request request = new Request(randomFrom("POST", "GET"), "/_security/_query/role");
        request.setJsonEntity(metadataQuery);
        Response response = null;
        try {
            response = client.performRequest(request);
        } catch (ResponseException e) {
            fail(e);
        }
        assertNotNull(response);
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
