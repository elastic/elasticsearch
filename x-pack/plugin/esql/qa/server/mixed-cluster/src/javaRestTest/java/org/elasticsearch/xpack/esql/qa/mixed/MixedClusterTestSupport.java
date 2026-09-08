/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.qa.mixed;

import org.elasticsearch.Version;
import org.elasticsearch.test.rest.ObjectPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Pure version and node-selection support shared by mixed-cluster REST suites.
 */
final class MixedClusterTestSupport {

    private MixedClusterTestSupport() {}

    /**
     * Returns the BWC version supplied by the regular test task or its serverless overlay.
     */
    static Version bwcVersion() {
        String value = System.getProperty("tests.old_cluster_version");
        if (value == null) {
            value = System.getProperty("tests.serverless.bwc_stack_version");
        }
        if (value == null) {
            throw new IllegalStateException("BWC version system property is not set");
        }
        return Version.fromString(value.replace("-SNAPSHOT", ""));
    }

    /**
     * Selects the live nodes whose version either matches the BWC version or the current version.
     */
    static List<Node> nodesForCoordinator(ObjectPath nodesInfo, boolean oldCoordinator) throws IOException {
        Map<String, Object> nodes = nodesInfo.evaluate("nodes");
        List<Node> selected = new ArrayList<>();
        for (String id : nodes.keySet()) {
            Version version = Version.fromString(((String) nodesInfo.evaluate("nodes." + id + ".version")).replace("-SNAPSHOT", ""));
            if (version.equals(bwcVersion()) == oldCoordinator) {
                selected.add(
                    new Node(
                        nodesInfo.evaluate("nodes." + id + ".name"),
                        version,
                        nodesInfo.evaluate("nodes." + id + ".http.publish_address")
                    )
                );
            }
        }
        if (selected.isEmpty()) {
            throw new IllegalStateException(
                "No " + (oldCoordinator ? "old" : "current") + " nodes found in mixed cluster for BWC version [" + bwcVersion() + "]"
            );
        }
        return List.copyOf(selected);
    }

    /**
     * Returns the comma-separated HTTP publish addresses used by the REST test framework.
     */
    static String httpAddressesForCoordinator(ObjectPath nodesInfo, boolean oldCoordinator) throws IOException {
        return String.join(",", nodesForCoordinator(nodesInfo, oldCoordinator).stream().map(Node::httpAddress).toList());
    }

    /**
     * Identifies one live mixed-cluster node and its HTTP publish address.
     */
    record Node(String name, Version version, String httpAddress) {}
}
