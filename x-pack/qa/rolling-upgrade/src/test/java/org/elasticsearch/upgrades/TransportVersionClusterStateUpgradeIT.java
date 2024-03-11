/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.Build;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.test.rest.RestTestLegacyFeatures;

import java.util.Map;

import static org.elasticsearch.cluster.ClusterState.INFERRED_TRANSPORT_VERSION;
import static org.elasticsearch.cluster.ClusterState.VERSION_INTRODUCING_TRANSPORT_VERSIONS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.oneOf;

public class TransportVersionClusterStateUpgradeIT extends AbstractUpgradeTestCase {

    public void testReadsInferredTransportVersions() throws Exception {
        // waitUntil because the versions fixup on upgrade happens in the background so may need a retry
        assertTrue(waitUntil(() -> {
            try {
                // check several responses in order to sample from a selection of nodes
                for (int i = getClusterHosts().size(); i > 0; i--) {
                    if (runTransportVersionsTest() == false) {
                        return false;
                    }
                }
                return true;
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }));
    }

    private boolean runTransportVersionsTest() throws Exception {
        final var clusterState = ObjectPath.createFromResponse(
            client().performRequest(new Request("GET", "/_cluster/state" + randomFrom("", "/nodes") + randomFrom("", "?local")))
        );
        final var description = clusterState.toString();

        final var nodeIds = clusterState.evaluateMapKeys("nodes");
        final Map<String, String> versionsByNodeId = Maps.newHashMapWithExpectedSize(nodeIds.size());
        for (final var nodeId : nodeIds) {
            versionsByNodeId.put(nodeId, clusterState.evaluate("nodes." + nodeId + ".version"));
        }

        final var hasTransportVersions = clusterState.evaluate("transport_versions") != null;
        final var hasNodesVersions = clusterState.evaluate("nodes_versions") != null;
        assertFalse(description, hasNodesVersions && hasTransportVersions);

        switch (CLUSTER_TYPE) {
            case OLD -> {
                if (clusterHasFeature(RestTestLegacyFeatures.TRANSPORT_VERSION_SUPPORTED) == false) {
                    // Before 8.8.0 there was only DiscoveryNode#version
                    assertFalse(description, hasTransportVersions);
                    assertFalse(description, hasNodesVersions);
                } else if (clusterHasFeature(RestTestLegacyFeatures.STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION) == false) {
                    // In [8.8.0, 8.11.0) we exposed just transport_versions
                    assertTrue(description, hasTransportVersions);
                    assertFalse(description, hasNodesVersions);
                } else {
                    // From 8.11.0 onwards we exposed nodes_versions
                    assertFalse(description, hasTransportVersions);
                    assertTrue(description, hasNodesVersions);
                }
            }
            case MIXED -> {
                if (clusterHasFeature(RestTestLegacyFeatures.TRANSPORT_VERSION_SUPPORTED) == false) {
                    // Responding node might be <8.8.0 (so no extra versions) or >=8.11.0 (includes nodes_versions)
                    assertFalse(description, hasTransportVersions);
                } else if (clusterHasFeature(RestTestLegacyFeatures.STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION) == false) {
                    // Responding node might be in [8.8.0, 8.11.0) (transport_versions) or >=8.11.0 (includes nodes_versions) but not both
                    assertTrue(description, hasNodesVersions || hasTransportVersions);
                } else {
                    // Responding node is ≥8.11.0 so has nodes_versions for sure
                    assertFalse(description, hasTransportVersions);
                    assertTrue(description, hasNodesVersions);
                }
            }
            case UPGRADED -> {
                // All nodes are Version.CURRENT, ≥8.11.0, so we definitely have nodes_versions
                assertFalse(description, hasTransportVersions);
                assertTrue(description, hasNodesVersions);
                assertThat(description, versionsByNodeId.values(), everyItem(equalTo(Build.current().version())));
            }
        }

        if (hasTransportVersions) {
            // Upgrading from [8.8.0, 8.11.0) and the responding node is still on the old version
            assertFalse(description, clusterHasFeature(RestTestLegacyFeatures.STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION));
            assertTrue(description, clusterHasFeature(RestTestLegacyFeatures.TRANSPORT_VERSION_SUPPORTED));
            assertNotEquals(description, ClusterType.UPGRADED, CLUSTER_TYPE);

            // transport_versions includes the correct version for all nodes, no inference is needed
            assertEquals(description, nodeIds.size(), clusterState.evaluateArraySize("transport_versions"));
            for (int i = 0; i < nodeIds.size(); i++) {
                final var path = "transport_versions." + i;
                final String nodeId = clusterState.evaluate(path + ".node_id");
                final var nodeDescription = nodeId + "/" + description;
                final var transportVersion = TransportVersion.fromString(clusterState.evaluate(path + ".transport_version"));
                final var nodeVersion = versionsByNodeId.get(nodeId);
                assertNotNull(nodeDescription, nodeVersion);
                if (nodeVersion.equals(Build.current().version())) {
                    assertEquals(nodeDescription, TransportVersion.current(), transportVersion);
                } else {
                    // There's no relationship between node versions and transport versions anymore, although we can be sure of this:
                    assertThat(nodeDescription, transportVersion, greaterThanOrEqualTo(INFERRED_TRANSPORT_VERSION));
                }
            }
        } else if (hasNodesVersions) {
            // Either upgrading from ≥8.11.0 (the responding node might be old or new), or from <8.8.0 (the responding node is new)
            assertFalse(
                description,
                clusterHasFeature(RestTestLegacyFeatures.STATE_REPLACED_TRANSPORT_VERSION_WITH_NODES_VERSION) == false
                    && CLUSTER_TYPE == ClusterType.OLD
            );

            // nodes_versions includes _a_ version for all nodes; it might be correct, or it might be inferred if we're upgrading from
            // <8.8.0 and the master is still an old node or the TransportVersionsFixupListener hasn't run yet
            assertEquals(description, nodeIds.size(), clusterState.evaluateArraySize("nodes_versions"));
            for (int i = 0; i < nodeIds.size(); i++) {
                final var path = "nodes_versions." + i;
                final String nodeId = clusterState.evaluate(path + ".node_id");
                final var nodeDescription = nodeId + "/" + description;
                final var transportVersion = TransportVersion.fromString(clusterState.evaluate(path + ".transport_version"));
                final var nodeVersion = versionsByNodeId.get(nodeId);
                assertNotNull(nodeDescription, nodeVersion);
                if (nodeVersion.equals(Build.current().version())) {
                    // Either the responding node is upgraded or the upgrade is trivial; if the responding node is upgraded but the master
                    // is not then its transport version may be temporarily inferred as 8.8.0 until TransportVersionsFixupListener runs.
                    assertThat(
                        nodeDescription,
                        transportVersion,
                        clusterHasFeature(RestTestLegacyFeatures.TRANSPORT_VERSION_SUPPORTED)
                            ? equalTo(TransportVersion.current())
                            : oneOf(TransportVersion.current(), INFERRED_TRANSPORT_VERSION)
                    );
                    if (CLUSTER_TYPE == ClusterType.UPGRADED && transportVersion.equals(INFERRED_TRANSPORT_VERSION)) {
                        // TransportVersionsFixupListener should run soon, retry
                        logger.info("{} - not fixed up yet, retrying", nodeDescription);
                        return false;
                    }
                } else {
                    var version = parseLegacyVersion(nodeVersion);
                    // All non-semantic versions are after 8.8.0 and have transport version
                    var transportVersionIntroduced = version.map(v -> v.after(VERSION_INTRODUCING_TRANSPORT_VERSIONS)).orElse(true);
                    if (transportVersionIntroduced) {
                        // There's no relationship between node versions and transport versions anymore, although we can be sure of this:
                        assertThat(nodeDescription, transportVersion, greaterThan(INFERRED_TRANSPORT_VERSION));
                    } else {
                        // Responding node is not upgraded, and no later than 8.8.0, so we infer its version correctly.
                        assertEquals(nodeDescription, TransportVersion.fromId(version.get().id()), transportVersion);
                    }
                }
            }
        }

        return true;
    }
}
