package org.elasticsearch.gradle.testclusters;

import org.elasticsearch.gradle.Jdk;
import org.gradle.api.Task;
import org.gradle.api.tasks.Nested;

import java.util.Collection;
import java.util.concurrent.Callable;

interface TestClustersAware extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    default void useCluster(ElasticsearchCluster cluster) {
        if (cluster.getPath().equals(getProject().getPath()) == false) {
            throw new TestClustersException("Task " + getPath() + " can't use test cluster from" + " another project " + cluster);
        }

        // Add configured distributions as task dependencies so they are built before starting the cluster
        cluster.getNodes().stream().flatMap(node -> node.getDistributions().stream()).forEach(distro -> dependsOn(distro.getExtracted()));

        // Add legacy BWC JDK runtime as a dependency so it's downloaded before starting the cluster if necessary
        cluster.getNodes().stream().map(node -> (Callable<Jdk>) node::getBwcJdk).forEach(this::dependsOn);

        getClusters().add(cluster);
    }

    default void beforeStart() {}

}
