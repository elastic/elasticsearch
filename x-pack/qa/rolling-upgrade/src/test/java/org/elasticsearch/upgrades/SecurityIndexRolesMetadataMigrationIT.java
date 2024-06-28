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

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;

public class SecurityIndexRolesMetadataMigrationIT extends AbstractUpgradeTestCase {

    public void testMetadataMigratedAfterUpgrade() throws Exception {
        String testRole = "test-role";
        String metaKey = "test_key";
        String metaValue = "test_value";

        Map<String, Object> testMetadata = Map.of(metaKey, metaValue);
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createRole(testRole, testMetadata);
            assertEntityInSecurityIndex(testRole);
        }
        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            refreshSecurityIndex();
            waitForMigrationCompletion();
            assertEntityInSecurityIndex(testRole, metaKey, metaValue);
        }
    }

    public void testMetadataWrittenAfterUpgradeWithoutMigration() throws IOException {
        String testRole = "another-test-role";
        String metaKey = "another-test_key";
        String metaValue = "another-test_value";

        Map<String, Object> testMetadata = Map.of(metaKey, metaValue);

        if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            createRole(testRole, testMetadata);
            assertEntityInSecurityIndex(testRole, metaKey, metaValue);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertEntityInSecurityIndex(String roleName, String metaKey, String metaValue) throws IOException {
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
    private void assertEntityInSecurityIndex(String id) throws IOException {
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
    private void waitForMigrationCompletion() throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/.security-7");
        assertBusy(() -> {
            Response response = adminClient().performRequest(request);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            assertTrue(
                ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) responseMap.get("metadata")).get("indices")).get(
                    ".security-7"
                )).containsKey("migration_version")
            );
        });
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
