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
import org.gradle.api.artifacts.ResolvedDependency;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class SnykDependencyGraphBuilder {

    private Map<String, SnykDependencyNode> nodes = new LinkedHashMap<>();
    private Set<SnykDependencyGraph.SnykDependencyPkg> pkgs = new LinkedHashSet<>();

    private SnykDependencyNode currentNode;
    private String gradleVersion;

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

    public SnykDependencyNode addDependency(ResolvedDependency dep) {
        String pkgPrefix = dep.getModuleGroup() + ":" + dep.getModuleName();
        String nodeId = pkgPrefix + "@" + dep.getModuleVersion();
        return addNode(nodeId, pkgPrefix, dep.getModuleVersion());
    }

    private void loadGraph(SnykDependencyNode parent, Set<ResolvedDependency> deps) {
        this.currentNode = parent;
        deps.forEach(dep -> {
            SnykDependencyGraph.SnykDependencyNode snykDependencyNode = addDependency(dep);
            loadGraph(snykDependencyNode, dep.getChildren());
            this.currentNode = parent;
        });
    }

    public SnykDependencyGraph build() {
        return new SnykDependencyGraph(gradleVersion, new LinkedHashSet<>(nodes.values()), pkgs);
    }

    public void walkGraph(String rootPkgId, String version, Set<ResolvedDependency> deps) {
        SnykDependencyNode root = addNode("root-node", rootPkgId, version);
        loadGraph(root, deps);
    }
}
