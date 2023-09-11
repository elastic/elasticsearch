/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.upgrades;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.Version;
import org.elasticsearch.client.Request;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.test.rest.ObjectPath;

import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.oneOf;

public class TransportVersionClusterStateUpgradeIT extends AbstractUpgradeTestCase {

    private static final Version VERSION_INTRODUCING_TRANSPORT_VERSIONS = Version.V_8_8_0;
    private static final Version VERSION_INTRODUCING_NODES_VERSIONS = Version.V_8_11_0;
    private static final TransportVersion FIRST_TRANSPORT_VERSION = TransportVersions.V_8_8_0;

    public void testReadsInferredTransportVersions() throws Exception {
        assertEquals(VERSION_INTRODUCING_TRANSPORT_VERSIONS.id(), FIRST_TRANSPORT_VERSION.id());

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
        final Map<String, Version> versionsByNodeId = Maps.newHashMapWithExpectedSize(nodeIds.size());
        for (final var nodeId : nodeIds) {
            versionsByNodeId.put(nodeId, Version.fromString(clusterState.evaluate("nodes." + nodeId + ".version")));
        }

        final var hasTransportVersions = clusterState.evaluate("transport_versions") != null;
        final var hasNodesVersions = clusterState.evaluate("nodes_versions") != null;
        assertFalse(description, hasNodesVersions && hasTransportVersions);

        switch (CLUSTER_TYPE) {
            case OLD -> {
                if (UPGRADE_FROM_VERSION.before(VERSION_INTRODUCING_TRANSPORT_VERSIONS)) {
                    assertFalse(description, hasTransportVersions);
                    assertFalse(description, hasNodesVersions);
                } else if (UPGRADE_FROM_VERSION.before(VERSION_INTRODUCING_NODES_VERSIONS)) {
                    assertTrue(description, hasTransportVersions);
                    assertFalse(description, hasNodesVersions);
                } else {
                    assertFalse(description, hasTransportVersions);
                    assertTrue(description, hasNodesVersions);
                }
            }
            case MIXED -> {
                if (UPGRADE_FROM_VERSION.before(VERSION_INTRODUCING_TRANSPORT_VERSIONS)) {
                    assertFalse(description, hasTransportVersions);
                } else if (UPGRADE_FROM_VERSION.before(VERSION_INTRODUCING_NODES_VERSIONS)) {
                    assertTrue(description, hasNodesVersions || hasTransportVersions);
                } else {
                    assertFalse(description, hasTransportVersions);
                    assertTrue(description, hasNodesVersions);
                }
            }
            case UPGRADED -> {
                assertFalse(description, hasTransportVersions);
                assertTrue(description, hasNodesVersions);
                assertTrue(description, versionsByNodeId.values().stream().allMatch(v -> v.equals(Version.CURRENT)));
            }
        }

        if (hasTransportVersions) {
            assertTrue(description, UPGRADE_FROM_VERSION.before(VERSION_INTRODUCING_NODES_VERSIONS));
            assertTrue(description, UPGRADE_FROM_VERSION.onOrAfter(VERSION_INTRODUCING_TRANSPORT_VERSIONS));
            assertNotEquals(description, ClusterType.UPGRADED, CLUSTER_TYPE);

            assertEquals(description, nodeIds.size(), clusterState.evaluateArraySize("transport_versions"));
            for (int i = 0; i < nodeIds.size(); i++) {
                final var path = "transport_versions." + i;
                final String nodeId = clusterState.evaluate(path + ".node_id");
                final var nodeDescription = nodeId + "/" + description;
                final var transportVersion = TransportVersion.fromString(clusterState.evaluate(path + ".transport_version"));
                final var nodeVersion = versionsByNodeId.get(nodeId);
                assertNotNull(nodeDescription, nodeVersion);
                if (nodeVersion.equals(Version.CURRENT)) {
                    assertEquals(nodeDescription, TransportVersion.current(), transportVersion);
                } else if (nodeVersion.after(VERSION_INTRODUCING_TRANSPORT_VERSIONS)) {
                    assertTrue(nodeDescription, transportVersion.after(FIRST_TRANSPORT_VERSION));
                } else {
                    assertEquals(nodeDescription, FIRST_TRANSPORT_VERSION, transportVersion);
                }
            }
        } else if (hasNodesVersions) {
            assertFalse(description, UPGRADE_FROM_VERSION.before(VERSION_INTRODUCING_NODES_VERSIONS) && CLUSTER_TYPE == ClusterType.OLD);
            assertEquals(description, nodeIds.size(), clusterState.evaluateArraySize("nodes_versions"));
            for (int i = 0; i < nodeIds.size(); i++) {
                final var path = "nodes_versions." + i;
                final String nodeId = clusterState.evaluate(path + ".node_id");
                final var nodeDescription = nodeId + "/" + description;
                final var transportVersion = TransportVersion.fromString(clusterState.evaluate(path + ".transport_version"));
                final var nodeVersion = versionsByNodeId.get(nodeId);
                assertNotNull(nodeDescription, nodeVersion);
                if (nodeVersion.equals(Version.CURRENT)) {
                    assertThat(
                        nodeDescription,
                        transportVersion,
                        UPGRADE_FROM_VERSION.onOrAfter(VERSION_INTRODUCING_TRANSPORT_VERSIONS)
                            ? equalTo(TransportVersion.current())
                            : oneOf(TransportVersion.current(), FIRST_TRANSPORT_VERSION)
                    );
                    if (CLUSTER_TYPE == ClusterType.UPGRADED && transportVersion.equals(FIRST_TRANSPORT_VERSION)) {
                        logger.info("{} - not fixed up yet, retrying", nodeDescription);
                        return false;
                    }
                } else if (nodeVersion.after(VERSION_INTRODUCING_TRANSPORT_VERSIONS)) {
                    assertTrue(nodeDescription, transportVersion.after(FIRST_TRANSPORT_VERSION));
                } else {
                    assertEquals(nodeDescription, TransportVersion.fromId(nodeVersion.id()), transportVersion);
                }
            }
        }

        return true;
    }
}
