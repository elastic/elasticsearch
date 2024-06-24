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
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;

public class SecurityIndexRolesMetadataMigrationIT extends AbstractUpgradeTestCase {

    public void testRoleMigration() throws Exception {
        String oldTestRole = "old-test-role";
        String oldMetaKey = "old-meta-test-key";
        String oldMetaValue = "old-meta-test-value";
        String mixedTestRole = "mixed-test-role";
        String mixedMetaKey = "mixed-meta-test-key";
        String mixedMetaValue = "mixed-meta-test-value";
        String upgradedTestRole = "upgraded-test-role";
        String upgradedMetaKey = "upgraded-meta-test-key";
        String upgradedMetaValue = "upgraded-meta-test-value";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createRoleWithMetadata(oldTestRole, Map.of(oldMetaKey, oldMetaValue));
            assertDocInSecurityIndex(oldTestRole);
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            createRoleWithMetadata(mixedTestRole, Map.of(mixedMetaKey, mixedMetaValue));
            assertDocInSecurityIndex(mixedTestRole);
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            createRoleWithMetadata(upgradedTestRole, Map.of(upgradedMetaKey, upgradedMetaValue));

            waitForMigrationCompletion();
            assertMigratedDocInSecurityIndex(oldTestRole, oldMetaKey, oldMetaValue);
            assertMigratedDocInSecurityIndex(mixedTestRole, mixedMetaKey, mixedMetaValue);
            assertMigratedDocInSecurityIndex(upgradedTestRole, upgradedMetaKey, upgradedMetaValue);
        }
    }

    @SuppressWarnings("unchecked")
    private void assertMigratedDocInSecurityIndex(String roleName, String metaKey, String metaValue) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(Strings.format("""
            {"query":{"bool":{"must":[
                {"term":{"_id":"%s-%s"}},
                {"term":{"metadata_flattened.%s":"%s"}},
                {"term":{"name":"%2$s"}}]}}}""", "role", roleName, metaKey, metaValue));
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
    private void waitForMigrationCompletion() throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/.security-7");
        assertBusy(() -> {
            Response response = adminClient().performRequest(request);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            Map<String, Object> indexMetadata = (Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) responseMap.get(
                "metadata"
            )).get("indices")).get(".security-7");
            assertTrue(indexMetadata.containsKey(MIGRATION_VERSION_CUSTOM_KEY));
            Map<String, Object> migrationVersionMap = (Map<String, Object>) indexMetadata.get(MIGRATION_VERSION_CUSTOM_KEY);
            // latest migration version as of this writing
            assertTrue(Integer.parseInt((String) migrationVersionMap.get(MIGRATION_VERSION_CUSTOM_DATA_KEY)) >= 2);
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
