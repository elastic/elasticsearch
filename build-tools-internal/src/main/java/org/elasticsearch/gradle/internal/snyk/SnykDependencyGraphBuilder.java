/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import org.elasticsearch.gradle.internal.snyk.SnykDependencyGraph.SnykDependencyNode;
import org.gradle.api.artifacts.ResolvedDependency;

import java.util.LinkedHashSet;
import java.util.Set;

public class SnykDependencyGraphBuilder {

    private Set<SnykDependencyNode> nodes = new LinkedHashSet<>();
    private Set<SnykDependencyGraph.SnykDependencyPkg> pkgs = new LinkedHashSet<>();

    private SnykDependencyNode currentNode;
    private String gradleVersion;

    public SnykDependencyGraphBuilder(String gradleVersion) {
        this.gradleVersion = gradleVersion;
    }

    public SnykDependencyNode addNode(String nodeId, String pkgIdPrefix, String version) {
        String pkgId = pkgIdPrefix + "@" + version;
        SnykDependencyNode node = new SnykDependencyNode(nodeId, pkgId);
        SnykDependencyGraph.SnykDependencyPkg pkg = new SnykDependencyGraph.SnykDependencyPkg(pkgId);
        nodes.add(node);
        if (currentNode != null) {
            currentNode.addDep(pkgId);
        }
        pkgs.add(pkg);
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
        return new SnykDependencyGraph(gradleVersion, nodes, pkgs);
    }

    public void walkGraph(String rootPkgId, String version, Set<ResolvedDependency> deps) {
        SnykDependencyNode root = addNode("root-node", rootPkgId, version);
        loadGraph(root, deps);
    }
}
