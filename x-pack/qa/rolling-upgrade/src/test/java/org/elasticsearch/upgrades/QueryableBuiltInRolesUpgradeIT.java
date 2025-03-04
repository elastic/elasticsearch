/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.apache.http.client.methods.HttpGet;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xpack.core.security.authz.store.ReservedRolesStore;
import org.junit.Before;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;
import static org.elasticsearch.common.xcontent.support.XContentMapValues.extractValue;
import static org.elasticsearch.xpack.core.security.test.TestRestrictedIndices.INTERNAL_SECURITY_MAIN_INDEX_7;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class QueryableBuiltInRolesUpgradeIT extends AbstractUpgradeTestCase {

    private static final String QUERYABLE_BUILT_IN_ROLES_NODE_FEATURE = "security.queryable_built_in_roles";
    public static final String INDEX_METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST = "queryable_built_in_roles_digest";

    @Before
    public void initializeReservedRolesStore() {
        new ReservedRolesStore();
    }

    /**
     * Test upgrades from an older cluster versions that do not support queryable built-in roles feature.
     */
    public void testBuiltInRolesSyncedOnClusterUpgrade() throws Exception {
        final int numberOfNodes = 3; // defined in build.gradle
        waitForNodes(numberOfNodes);

        final Set<TestNodeInfo> nodes = collectNodeInfos(adminClient());
        assertThat("cluster should have " + numberOfNodes + " nodes", nodes.size(), equalTo(numberOfNodes));

        final Set<TestNodeInfo> newVersionNodes = nodes.stream().filter(TestNodeInfo::isUpgradedVersionCluster).collect(toSet());
        final Set<TestNodeInfo> oldVersionNodes = nodes.stream().filter(TestNodeInfo::isOriginalVersionCluster).collect(toSet());

        assumeTrue(
            "Old version nodes must not support queryable feature",
            oldVersionNodes.stream().noneMatch(TestNodeInfo::supportsQueryableBuiltInRolesFeature)
        );
        assumeTrue(
            "New version nodes must support queryable feature",
            newVersionNodes.stream().allMatch(TestNodeInfo::supportsQueryableBuiltInRolesFeature)
        );

        switch (CLUSTER_TYPE) {
            case OLD, MIXED -> {
                // none of the old version nodes should support the queryable feature,
                // hence the built-in roles should not exist in the security index
                // in the mixed version cluster we do not attempt to sync the built-in roles
                assertBuiltInRolesNotIndexed();
            }
            case UPGRADED -> {
                // the built-in roles should be synced after the upgrade
                assertBusy(() -> assertBuiltInRolesIndexed(ReservedRolesStore.names()), 45, TimeUnit.SECONDS);
            }
        }
    }

    record TestNodeInfo(String nodeId, String version, Set<String> features) {

        public boolean isOriginalVersionCluster() {
            return AbstractUpgradeTestCase.isOriginalCluster(this.version());
        }

        public boolean isUpgradedVersionCluster() {
            return false == isOriginalVersionCluster();
        }

        public boolean supportsQueryableBuiltInRolesFeature() {
            return features().contains(QUERYABLE_BUILT_IN_ROLES_NODE_FEATURE);
        }

    }

    private static Set<TestNodeInfo> collectNodeInfos(RestClient adminClient) throws IOException {
        final Request request = new Request("GET", "_cluster/state");
        request.addParameter("filter_path", "nodes_features");

        final Response response = adminClient.performRequest(request);

        Map<String, Set<String>> nodeFeatures = null;
        var responseData = responseAsMap(response);
        if (responseData.get("nodes_features") instanceof List<?> nodesFeatures) {
            nodeFeatures = nodesFeatures.stream()
                .map(Map.class::cast)
                .collect(Collectors.toUnmodifiableMap(nodeFeatureMap -> nodeFeatureMap.get("node_id").toString(), nodeFeatureMap -> {
                    @SuppressWarnings("unchecked")
                    var features = (List<String>) nodeFeatureMap.get("features");
                    return new HashSet<>(features);
                }));
        }

        Map<String, String> nodeVersions = nodesVersions();
        assertThat(nodeVersions, is(notNullValue()));
        // old cluster may not support node features, so we can treat it as if no features are supported
        if (nodeFeatures == null) {
            Set<TestNodeInfo> nodes = new HashSet<>(nodeVersions.size());
            for (String nodeId : nodeVersions.keySet()) {
                nodes.add(new TestNodeInfo(nodeId, nodeVersions.get(nodeId), Set.of()));
            }
            return nodes;
        } else {
            assertThat(nodeVersions.keySet(), containsInAnyOrder(nodeFeatures.keySet().toArray()));
            Set<TestNodeInfo> nodes = new HashSet<>(nodeVersions.size());
            for (String nodeId : nodeVersions.keySet()) {
                nodes.add(new TestNodeInfo(nodeId, nodeVersions.get(nodeId), nodeFeatures.get(nodeId)));
            }
            return nodes;
        }
    }

    private static void waitForNodes(int numberOfNodes) throws IOException {
        final Request request = new Request(HttpGet.METHOD_NAME, "/_cluster/health");
        request.addParameter("wait_for_nodes", String.valueOf(numberOfNodes));
        final Response response = client().performRequest(request);
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> nodesVersions() throws IOException {
        final Response response = client().performRequest(new Request(HttpGet.METHOD_NAME, "_nodes/_all"));
        assertThat(response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        final Map<String, Object> nodes = (Map<String, Object>) extractValue(responseAsMap(response), "nodes");
        assertNotNull("Nodes info is null", nodes);
        final Map<String, String> nodesVersions = Maps.newMapWithExpectedSize(nodes.size());
        for (Map.Entry<String, Object> node : nodes.entrySet()) {
            nodesVersions.put(node.getKey(), (String) extractValue((Map<?, ?>) node.getValue(), "version"));
        }
        return nodesVersions;
    }

    private void assertBuiltInRolesIndexed(Set<String> expectedBuiltInRoles) throws IOException {
        final Map<String, String> builtInRoles = readSecurityIndexBuiltInRolesMetadata();
        assertThat(builtInRoles, is(notNullValue()));
        assertThat(builtInRoles.keySet(), containsInAnyOrder(expectedBuiltInRoles.toArray()));
    }

    private void assertBuiltInRolesNotIndexed() throws IOException {
        final Map<String, String> builtInRoles = readSecurityIndexBuiltInRolesMetadata();
        assertThat(builtInRoles, is(nullValue()));
    }

    private Map<String, String> readSecurityIndexBuiltInRolesMetadata() throws IOException {
        final Request request = new Request("GET", "_cluster/state/metadata/" + INTERNAL_SECURITY_MAIN_INDEX_7);
        final Response response = adminClient().performRequest(request);
        assertOK(response);
        return ObjectPath.createFromResponse(response)
            .evaluate("metadata.indices.\\.security-7." + INDEX_METADATA_QUERYABLE_BUILT_IN_ROLES_DIGEST);
    }
}
