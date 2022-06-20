/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.internal.snyk;

import org.gradle.api.artifacts.Configuration;

import org.gradle.api.DefaultTask;
import org.gradle.api.artifacts.ResolvedDependency;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.TaskAction;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class GenerateSnykDependencyGraph extends DefaultTask {

    private Configuration configuration;

    private final SnykGraph graph = new SnykGraph("root-node");

    @Internal
    public SnykGraph getGraph() {
        return graph;
    }

    @TaskAction
    void resolveGraph() {
        Set<ResolvedDependency> firstLevelModuleDependencies = configuration.getResolvedConfiguration().getFirstLevelModuleDependencies();
        createGraph(firstLevelModuleDependencies);
    }

    private Map<String, Object> createGraph(Iterable<ResolvedDependency> deps) {
        String rootId = "root-node";
        Set currentChain = new HashSet();
        loadGraph(deps, graph, rootId, currentChain);
        return graph.nodes;
    }

    private void loadGraph(Iterable<ResolvedDependency> deps, SnykGraph graph, String parentId, Set currentChain) {
        System.out.println("GenerateSnykDependencyGraph.loadGraph");
        System.out.println("parentId = " + parentId);
        deps.forEach( dep -> {
            String childId = dep.getModuleGroup() + ":" + dep.getModuleName() + "@" + dep.getModuleVersion();
            if (graph.getNodes().get(childId) == null) {
                Map<String, Object> childDependency = Map.of("name", dep.getModuleGroup() +
                        ":" + dep.getModuleName(), "version", dep.getModuleVersion()
                );
                graph.setNode(childId, childDependency);
            }
            //  In Gradle 2, there can be several instances of the same dependency present at each level,
            //  each for a different configuration. In this case, we need to merge the dependencies.
            if (currentChain.contains(childId) == false && dep.getChildren() != null ) {
                currentChain.add(childId);
                loadGraph(dep.getChildren(), graph, childId, currentChain);
            }
            graph.setEdge(parentId, childId);

        });
    }

    public void setConfiguration(Configuration config) {
        this.configuration = config;
    }

}
