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
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;
import org.junit.Before;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_DATA_KEY;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.equalTo;

public class SecurityIndexRolesMetadataMigrationIT extends AbstractUpgradeTestCase {

    private static Boolean upgradingBeforeRoleMigration = false;

    @Before
    public void checkBeforeHasNoRoleMigrationFeature() {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            if (clusterHasFeature("security.migration_framework") == false
                || clusterHasFeature("security.roles_metadata_flattened") == false) {
                upgradingBeforeRoleMigration = true;
            }
        }
        assumeTrue("Only valid when upgrading from versions without role migration", upgradingBeforeRoleMigration);
    }

    public void testRoleMigration() throws Exception {
        String oldTestRole = "old-test-role";
        String oldMetaKey = "old-meta-test-key";
        String oldMetaValue = "old-meta-test-value";
        String mixed1TestRole = "mixed1-test-role";
        String mixed1MetaKey = "mixed1-meta-test-key";
        String mixed1MetaValue = "mixed1-meta-test-value";
        String mixed2TestRole = "mixed2-test-role";
        String mixed2MetaKey = "mixed2-meta-test-key";
        String mixed2MetaValue = "mixed2-meta-test-value";
        String upgradedTestRole = "upgraded-test-role";
        String upgradedMetaKey = "upgraded-meta-test-key";
        String upgradedMetaValue = "upgraded-meta-test-value";
        if (CLUSTER_TYPE == ClusterType.OLD) {
            createRoleWithMetadata(oldTestRole, Map.of(oldMetaKey, oldMetaValue));
            assertDocInSecurityIndex(oldTestRole);
            assertNoMigration(adminClient());
        } else if (CLUSTER_TYPE == ClusterType.MIXED) {
            if (FIRST_MIXED_ROUND) {
                createRoleWithMetadata(mixed1TestRole, Map.of(mixed1MetaKey, mixed1MetaValue));
                assertDocInSecurityIndex(mixed1TestRole);
                assertNoMigration(adminClient());
            } else {
                createRoleWithMetadata(mixed2TestRole, Map.of(mixed2MetaKey, mixed2MetaValue));
                assertDocInSecurityIndex(mixed2TestRole);
                assertNoMigration(adminClient());
            }
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            createRoleWithMetadata(upgradedTestRole, Map.of(upgradedMetaKey, upgradedMetaValue));
            waitForMigrationCompletion(adminClient(), null);
            assertMigratedDocInSecurityIndex(oldTestRole, oldMetaKey, oldMetaValue);
            assertMigratedDocInSecurityIndex(mixed1TestRole, mixed1MetaKey, mixed1MetaValue);
            assertMigratedDocInSecurityIndex(mixed2TestRole, mixed2MetaKey, mixed2MetaValue);
            assertMigratedDocInSecurityIndex(upgradedTestRole, upgradedMetaKey, upgradedMetaValue);
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

    @SuppressWarnings("unchecked")
    private static void waitForMigrationCompletion(RestClient adminClient, @Nullable Integer migrationVersion) throws Exception {
        final Request request = new Request("GET", "_cluster/state/metadata/" + INTERNAL_SECURITY_MAIN_INDEX_7);
        assertBusy(() -> {
            Response response = adminClient.performRequest(request);
            assertOK(response);
            Map<String, Object> responseMap = responseAsMap(response);
            Map<String, Object> indicesMetadataMap = (Map<String, Object>) ((Map<String, Object>) responseMap.get("metadata")).get(
                "indices"
            );
            assertTrue(indicesMetadataMap.containsKey(INTERNAL_SECURITY_MAIN_INDEX_7));
            assertTrue(
                ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).containsKey(MIGRATION_VERSION_CUSTOM_KEY)
            );
            if (migrationVersion != null) {
                assertTrue(
                    ((Map<String, Object>) ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).get(
                        MIGRATION_VERSION_CUSTOM_KEY
                    )).containsKey(MIGRATION_VERSION_CUSTOM_DATA_KEY)
                );
                assertThat(
                    (Integer) ((Map<String, Object>) ((Map<String, Object>) indicesMetadataMap.get(INTERNAL_SECURITY_MAIN_INDEX_7)).get(
                        MIGRATION_VERSION_CUSTOM_KEY
                    )).get(MIGRATION_VERSION_CUSTOM_DATA_KEY),
                    equalTo(migrationVersion)
                );
            }
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
}
