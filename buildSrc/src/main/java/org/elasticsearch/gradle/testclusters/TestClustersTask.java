package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Task;
import org.gradle.api.tasks.Nested;

import java.util.Collection;

public interface TestClustersTask extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    void testCluster(ElasticsearchCluster cluster);
}
