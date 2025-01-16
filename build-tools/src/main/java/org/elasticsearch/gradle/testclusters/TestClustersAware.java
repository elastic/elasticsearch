/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.ElasticsearchDistribution;
import org.gradle.api.Task;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.services.ServiceReference;
import org.gradle.api.tasks.Nested;

import java.util.Collection;

import static org.elasticsearch.gradle.testclusters.TestClustersPlugin.REGISTRY_SERVICE_NAME;
import static org.elasticsearch.gradle.testclusters.TestClustersPlugin.TEST_CLUSTER_TASKS_SERVICE;

public interface TestClustersAware extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    @ServiceReference(REGISTRY_SERVICE_NAME)
    Property<TestClustersRegistry> getRegistery();

    @ServiceReference(TEST_CLUSTER_TASKS_SERVICE)
    Property<TestClustersPlugin.TaskEventsService> getTasksService();

    default void useCluster(ElasticsearchCluster cluster) {
        if (cluster.getPath().equals(getProject().getPath()) == false) {
            throw new TestClustersException("Task " + getPath() + " can't use test cluster from" + " another project " + cluster);
        }
        if (cluster.getName().equals(getName())) {
            for (ElasticsearchNode node : cluster.getNodes()) {
                for (ElasticsearchDistribution distro : node.getDistributions()) {
                    ElasticsearchDistribution frozenDistro = distro.maybeFreeze();
                    dependsOn(frozenDistro);
                }
            }
            dependsOn(cluster.getPluginAndModuleConfigurations());
        }
        getClusters().add(cluster);
    }

    default void useCluster(Provider<ElasticsearchCluster> cluster) {
        useCluster(cluster.get());
    }

    default void beforeStart() {
        getTasksService().get().register(this);
    }

    default void enableDebug() {
        int debugPort = 5007;
        for (ElasticsearchCluster cluster : getClusters()) {
            for (ElasticsearchNode node : cluster.getNodes()) {
                getLogger().lifecycle("Running elasticsearch in debug mode, {} expecting running debug server on port {}", node, debugPort);
                node.jvmArgs("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + debugPort);
                debugPort += 1;
            }
        }
    }

    default void enableCliDebug() {
        int cliDebugPort = 5107;
        for (ElasticsearchCluster cluster : getClusters()) {
            for (ElasticsearchNode node : cluster.getNodes()) {
                getLogger().lifecycle(
                    "Running cli launcher in debug mode, {} expecting running debug server on port {}",
                    node,
                    cliDebugPort
                );
                node.cliJvmArgs("-agentlib:jdwp=transport=dt_socket,server=n,suspend=y,address=" + cliDebugPort);
                cliDebugPort += 1;
            }
        }
    }

    default void enableEntitlements() {
        for (ElasticsearchCluster cluster : getClusters()) {
            for (ElasticsearchNode node : cluster.getNodes()) {
                node.cliJvmArgs("-Des.entitlements.enabled=true");
            }
        }
    }
}
