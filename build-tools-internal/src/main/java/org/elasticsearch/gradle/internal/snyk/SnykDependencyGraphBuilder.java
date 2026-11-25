/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.snyk;

import org.elasticsearch.gradle.internal.snyk.SnykDependencyGraph.SnykDependencyNode;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class SnykDependencyGraphBuilder {

    private final Map<String, SnykDependencyNode> nodes = new LinkedHashMap<>();
    private final Set<SnykDependencyGraph.SnykDependencyPkg> pkgs = new LinkedHashSet<>();

    private SnykDependencyNode currentNode;
    private final String gradleVersion;

    public SnykDependencyGraphBuilder(String gradleVersion) {
        this.gradleVersion = gradleVersion;
    }

    public SnykDependencyNode addNode(String nodeId, String pkgIdPrefix, String version) {
        String pkgId = pkgIdPrefix + "@" + version;
        pkgs.add(new SnykDependencyGraph.SnykDependencyPkg(pkgId));
        if (currentNode != null) {
            currentNode.addDep(pkgId);
        }
        SnykDependencyNode existing = nodes.get(nodeId);
        if (existing != null) {
            return existing;
        }
        SnykDependencyNode node = new SnykDependencyNode(nodeId, pkgId);
        nodes.put(nodeId, node);
        return node;
    }

    private static String packagePrefix(String nodeId) {
        return nodeId.substring(0, nodeId.lastIndexOf('@'));
    }

    private static String packageVersion(String nodeId) {
        return nodeId.substring(nodeId.lastIndexOf('@') + 1);
    }

    private void loadGraph(String parentNodeId, Map<String, List<String>> depsByNodeId, Set<String> visited) {
        SnykDependencyNode parent = nodes.get(parentNodeId);
        this.currentNode = parent;
        depsByNodeId.getOrDefault(parentNodeId, List.of()).forEach(nodeId -> {
            addNode(nodeId, packagePrefix(nodeId), packageVersion(nodeId));
            if (visited.add(nodeId)) {
                loadGraph(nodeId, depsByNodeId, visited);
            }
            this.currentNode = parent;
        });
    }

    public SnykDependencyGraph build() {
        return new SnykDependencyGraph(gradleVersion, new LinkedHashSet<>(nodes.values()), pkgs);
    }

    public void walkGraph(String rootPkgId, String version, Map<String, List<String>> depsByNodeId) {
        addNode("root-node", rootPkgId, version);
        loadGraph("root-node", depsByNodeId, new LinkedHashSet<>());
    }
}
