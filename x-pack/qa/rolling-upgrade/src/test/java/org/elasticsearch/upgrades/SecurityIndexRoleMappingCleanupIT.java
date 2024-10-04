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
import org.elasticsearch.xpack.core.security.authc.support.mapper.ExpressionRoleMapping;
import org.elasticsearch.xpack.core.security.authz.RoleDescriptor;

import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.xpack.core.security.action.UpdateIndexMigrationVersionAction.MIGRATION_VERSION_CUSTOM_KEY;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.contains;

public class SecurityIndexRoleMappingCleanupIT extends AbstractUpgradeTestCase {
    public void testRoleMigration() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // Create some role mappings that clash with operator defined ones and some that do not
            createNativeRoleMapping("kibana_role_mapping", Map.of("meta", "test"));
            createNativeRoleMapping("no_name_conflict", Map.of("meta", "test"));
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            waitForMigrationCompletion(adminClient());
            // Make sure all role mapping still there (since they are operator defined)
            assertAllRoleMappings(client(), "no_name_conflict", "kibana_role_mapping", "fleet_role_mapping");
            // Make sure migrated mapping is only in cluster state
            assertMigratedMappingNotInSecurityIndex("kibana_role_mapping");
            // Make sure not migrated mapping is still in security index
            assertNotMigratedMappingInSecurityIndex("no_name_conflict");
            // Make sure we can create a conflicting role mapping
            createNativeRoleMapping("kibana_role_mapping", Map.of("meta", "test"));
            // TODO Delete native role mapping here and confirm file based is still here
            // TODO this could look different depending on what approach we take
            assertAllRoleMappings(client(), "kibana_role_mapping", "no_name_conflict", "fleet_role_mapping");
        }
    }

    @SuppressWarnings("unchecked")
    private void assertMigratedMappingNotInSecurityIndex(String mappingName) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"query":{"bool":{"must":[{"term":{"_id":"%s_%s"}}]}}}""", "role-mapping", mappingName));
        addExpectWarningOption(options);
        request.setOptions(options);

        Response response = adminClient().performRequest(request);
        assertOK(response);
        final Map<String, Object> responseMap = responseAsMap(response);

        Map<String, Object> hits = ((Map<String, Object>) responseMap.get("hits"));
        assertEquals(0, ((List<Object>) hits.get("hits")).size());
    }

    @SuppressWarnings("unchecked")
    private void assertNotMigratedMappingInSecurityIndex(String mappingName) throws IOException {
        final Request request = new Request("POST", "/.security/_search");
        RequestOptions.Builder options = request.getOptions().toBuilder();
        request.setJsonEntity(String.format(Locale.ROOT, """
            {"query":{"bool":{"must":[{"term":{"_id":"%s_%s"}}]}}}""", "role-mapping", mappingName));
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
    private static void waitForMigrationCompletion(RestClient adminClient) throws Exception {
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
        });
    }

    private void createNativeRoleMapping(String roleMappingName, Map<String, Object> metadata) throws IOException {
        final Request request = new Request("POST", "/_security/role_mapping/" + roleMappingName);
        BytesReference source = BytesReference.bytes(
            jsonBuilder().map(
                Map.of(
                    ExpressionRoleMapping.Fields.ROLES.getPreferredName(),
                    List.of("superuser"),
                    ExpressionRoleMapping.Fields.ENABLED.getPreferredName(),
                    true,
                    ExpressionRoleMapping.Fields.RULES.getPreferredName(),
                    Map.of("field", Map.of("username", "role-mapping-test-user")),
                    RoleDescriptor.Fields.METADATA.getPreferredName(),
                    metadata
                )
            )
        );
        request.setJsonEntity(source.utf8ToString());
        assertOK(client().performRequest(request));
    }

    private void assertAllRoleMappings(RestClient client, String... roleNames) throws IOException {
        Request request = new Request("GET", "/_security/role_mapping");
        Response response = client.performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);

        for (String roleName : roleNames) {
            assertThat(responseMap.keySet(), contains(roleName));
        }

        assertThat(responseMap.get("count"), is(roleNames.length));
    }
}
