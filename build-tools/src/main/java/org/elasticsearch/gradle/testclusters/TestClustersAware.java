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
import org.gradle.api.provider.Provider;
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

        cluster.getNodes().stream().map(node -> (Callable<Jdk>) node::getBwcJdk).forEach(this::dependsOn);
        cluster.getNodes().all(node -> node.getDistributions().stream()
                .forEach(distro -> dependsOn(getProject().provider(() -> distro.maybeFreeze()))));
        cluster.getNodes().all(node -> dependsOn((Callable<Collection<Configuration>>) node::getPluginAndModuleConfigurations));
        getClusters().add(cluster);
    }

    default void useCluster(Provider<ElasticsearchCluster> cluster) {
        useCluster(cluster.get());
    }

    default void beforeStart() {}
}
