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

import static org.elasticsearch.TransportVersions.SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE;
import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class SecurityIndexRoleMappingCleanupIT extends AbstractUpgradeTestCase {

    public void testCleanupDuplicateMappingsCreatedWithBug() throws Exception {
        assumeTrue(
            "Role mappings are in cluster state after transport version: " + SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE,
            minimumTransportVersion().after(SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE)
        );

        if (CLUSTER_TYPE == ClusterType.OLD) {
            assumeTrue(
                "Cleanup only needed before security.role_mapping_cleanup feature available in cluster",
                clusterHasFeature("security.role_mapping_cleanup") == false
            );
            // Since the old cluster has role mappings in cluster state, but doesn't check duplicates, create a duplicate
            createNativeRoleMapping("operator_role_mapping_1", Map.of("meta", "test"));
            createNativeRoleMapping("no_name_conflict", Map.of("meta", "test"));
            assertAllRoleMappings(client(), "no_name_conflict", "operator_role_mapping_1");
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            waitForSecurityMigrationCompletion(adminClient(), 2);
            assertRoleMappingsExistsInClusterState("operator_role_mapping_1", "operator_role_mapping_2");
            // In the old cluster we might have created these (depending on the node features), so make sure they were removed
            assertFalse(roleMappingExistsInSecurityIndex("operator_role_mapping_1"));
            assertFalse(roleMappingExistsInSecurityIndex("operator_role_mapping_2"));
            // Make sure we can create a conflicting role mapping again
            createNativeRoleMapping("operator_role_mapping_1", Map.of("meta", "test"));
        }
    }

    public void testCleanupDuplicateMappingsCreatedBeforeBug() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            assumeTrue(
                "Role mappings are in cluster state after transport version: " + SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE,
                minimumTransportVersion().before(SECURITY_ROLE_MAPPINGS_IN_CLUSTER_STATE)
            );
            createNativeRoleMapping("no_name_conflict", Map.of("meta", "test"));
            // Assume operator defined role mappings were created in the native store
            assertAllRoleMappings(client(), "no_name_conflict", "operator_role_mapping_1", "operator_role_mapping_2");
        } else if (CLUSTER_TYPE == ClusterType.UPGRADED) {
            waitForSecurityMigrationCompletion(adminClient(), 2);
            assertRoleMappingsExistsInClusterState("operator_role_mapping_1", "operator_role_mapping_2");
            // In the old cluster we might have created these (depending on the version), so make sure they were removed
            assertFalse(roleMappingExistsInSecurityIndex("operator_role_mapping_1"));
            assertFalse(roleMappingExistsInSecurityIndex("operator_role_mapping_2"));
            // Make sure we can create a conflicting role mapping
            createNativeRoleMapping("operator_role_mapping_1", Map.of("meta", "test"));
        }
    }

    @SuppressWarnings("unchecked")
    private void assertRoleMappingsExistsInClusterState(String... roleMappingNames) throws IOException {
        final Request request = new Request("GET", "_cluster/state/metadata/");
        Response response = adminClient().performRequest(request);
        assertOK(response);
        Map<String, Object> responseMap = responseAsMap(response);
        Map<String, Object> metadata = ((Map<String, Object>) responseMap.get("metadata"));
        assertNotNull(metadata);
        List<Map<String, Object>> clusterStateRoleMappings = (List<Map<String, Object>>) ((Map<String, Object>) metadata.get(
            "role_mappings"
        )).get("role_mappings");

        assertNotNull(clusterStateRoleMappings);
        assertEquals(clusterStateRoleMappings.size(), roleMappingNames.length);

        List<String> roleMappingHandlers = (List<String>) ((Map<String, Object>) ((Map<String, Object>) ((Map<String, Object>) ((Map<
            String,
            Object>) metadata.get("reserved_state")).get("file_settings")).get("handlers")).get("role_mappings")).get("keys");

        assertNotNull(roleMappingHandlers);
        assertEquals(clusterStateRoleMappings.size(), roleMappingHandlers.size());
        assertThat(roleMappingHandlers, containsInAnyOrder(roleMappingNames));
    }

    @SuppressWarnings("unchecked")
    private boolean roleMappingExistsInSecurityIndex(String mappingName) throws IOException {
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
        return ((List<Object>) hits.get("hits")).isEmpty() == false;
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

        assertThat(responseMap.keySet(), containsInAnyOrder(roleNames));
        assertThat(responseMap.size(), is(roleNames.length));
    }
}
