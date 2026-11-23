/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.single_node;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.rest.ESRestTestCase;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

/**
 * Reads back what decides the default of {@code esql.federation.enabled} on the node under test. The default is a function
 * of the node type ({@code stateless.enabled}) and the build; the test JVM shares the build with the node it starts, but
 * only the node knows its type, so a suite whose expectation depends on the default asks the node rather than assuming it
 * is stateful. This matters because these suites are shipped as a test artifact and may run against a cluster that this
 * repository's cluster builder did not define.
 */
final class FederationNodeSettings {

    private FederationNodeSettings() {}

    /** Whether the cluster's nodes are stateless, read from {@code GET _nodes/settings}; every node must agree. */
    @SuppressWarnings("unchecked")
    static boolean isStateless(RestClient client) throws IOException {
        Map<String, Object> response = ESRestTestCase.entityAsMap(client.performRequest(new Request("GET", "/_nodes/settings")));
        Map<String, Object> nodes = (Map<String, Object>) response.get("nodes");
        assertFalse("no nodes reported their settings", nodes.isEmpty());
        Boolean stateless = null;
        for (Object node : nodes.values()) {
            Map<String, Object> settings = (Map<String, Object>) ((Map<String, Object>) node).get("settings");
            boolean nodeStateless = DiscoveryNode.isStateless(Settings.builder().loadFromMap(settings).build());
            if (stateless == null) {
                stateless = nodeStateless;
            } else {
                assertEquals("nodes of one cluster must share a node type", stateless, nodeStateless);
            }
        }
        return stateless;
    }
}
