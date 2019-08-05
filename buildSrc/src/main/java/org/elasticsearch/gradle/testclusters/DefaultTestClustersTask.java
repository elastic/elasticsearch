package org.elasticsearch.gradle.testclusters;

import org.gradle.api.DefaultTask;

import java.util.Collection;
import java.util.HashSet;

public class DefaultTestClustersTask extends DefaultTask implements TestClustersTask {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();

    @Override
    public Collection<ElasticsearchCluster> getClusters() {
        return clusters;
    }

    @Override
    public void testCluster(ElasticsearchCluster cluster) {
        this.clusters.add(cluster);
    }

}
