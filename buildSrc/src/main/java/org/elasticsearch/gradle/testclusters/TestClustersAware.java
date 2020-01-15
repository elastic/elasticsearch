package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Task;
import org.gradle.api.tasks.Nested;

import java.util.Collection;

interface TestClustersAware extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    default void useCluster(ElasticsearchCluster cluster) {
        if (cluster.getPath().equals(getProject().getPath()) == false) {
            throw new TestClustersException("Task " + getPath() + " can't use test cluster from" + " another project " + cluster);
        }

        cluster.getNodes().stream().flatMap(node -> node.getDistributions().stream()).forEach(distro -> dependsOn(distro.getExtracted()));
        getClusters().add(cluster);
    }

    default void beforeStart() {}

}
