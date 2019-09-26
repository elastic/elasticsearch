package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.tasks.Nested;

import java.util.Collection;

interface TestClustersAware extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    default void useCluster(ElasticsearchCluster cluster) {
        prepareClusterForUse(getProject(), this, cluster);
        getClusters().add(cluster);
    }

    static void prepareClusterForUse(Project project, Task task, ElasticsearchCluster cluster) {
        if (cluster.getPath().equals(project.getPath()) == false) {
            throw new TestClustersException(
                "Task " + task.getPath() + " can't use test cluster from" +
                    " another project " + cluster
            );
        }

        cluster.getNodes().stream().flatMap(node -> node.getDistributions().stream()).forEach( distro ->
            task.dependsOn(distro.getExtracted())
        );
    }
}
