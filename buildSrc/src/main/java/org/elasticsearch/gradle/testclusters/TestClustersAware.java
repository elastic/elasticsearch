package org.elasticsearch.gradle.testclusters;

import org.gradle.api.Task;
import org.gradle.api.tasks.Nested;

import java.util.Collection;

interface TestClustersAware extends Task {

    @Nested
    Collection<ElasticsearchCluster> getClusters();

    void testCluster(ElasticsearchCluster cluster);
}
