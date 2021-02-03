/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.Jdk;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.tasks.Nested;

import java.util.Collection;
import java.util.concurrent.Callable;

public interface TestClustersAware extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    default void useCluster(ElasticsearchCluster cluster) {
        if (cluster.getPath().equals(getProject().getPath()) == false) {
            throw new TestClustersException("Task " + getPath() + " can't use test cluster from" + " another project " + cluster);
        }

        // Add configured distributions as task dependencies so they are built before starting the cluster
        cluster.getNodes()
            .stream()
            .flatMap(node -> node.getDistributions().stream())
            .forEach(distro -> dependsOn(getProject().provider(() -> distro.maybeFreeze())));

        // Add legacy BWC JDK runtime as a dependency so it's downloaded before starting the cluster if necessary
        cluster.getNodes().stream().map(node -> (Callable<Jdk>) node::getBwcJdk).forEach(this::dependsOn);
        cluster.getNodes().forEach(node -> dependsOn((Callable<Collection<Configuration>>) node::getPluginAndModuleConfigurations));
        getClusters().add(cluster);
    }

    default void beforeStart() {}

}
