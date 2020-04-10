package org.elasticsearch.gradle.testclusters;

import org.gradle.api.DefaultTask;

import java.util.Collection;
import java.util.HashSet;

public class DefaultTestClustersTask extends DefaultTask implements TestClustersAware {

    private Collection<ElasticsearchCluster> clusters = new HashSet<>();

    @Override
    public Collection<ElasticsearchCluster> getClusters() {
        return clusters;
    }

}
