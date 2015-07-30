/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.collector.cluster;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.exporter.MarvelDoc;
import org.elasticsearch.test.ElasticsearchIntegrationTest;
import org.junit.Test;

import java.util.Collection;

import static org.hamcrest.Matchers.*;

public class ClusterStatsCollectorTests extends ElasticsearchIntegrationTest {

    @Test
    public void testClusterStatsCollector() throws Exception {
        Collection<MarvelDoc> results = newClusterStatsCollector().doCollect();
        assertThat(results, hasSize(1));

        MarvelDoc marvelDoc = results.iterator().next();
        assertNotNull(marvelDoc);
        assertThat(marvelDoc, instanceOf(ClusterStatsMarvelDoc.class));

        ClusterStatsMarvelDoc clusterStatsMarvelDoc = (ClusterStatsMarvelDoc) marvelDoc;
        assertThat(clusterStatsMarvelDoc.clusterName(), equalTo(client().admin().cluster().prepareHealth().get().getClusterName()));
        assertThat(clusterStatsMarvelDoc.timestamp(), greaterThan(0L));
        assertThat(clusterStatsMarvelDoc.type(), equalTo(ClusterStatsCollector.TYPE));

        ClusterStatsMarvelDoc.Payload payload = clusterStatsMarvelDoc.payload();
        assertNotNull(payload);
        assertNotNull(payload.getClusterStats());
        assertThat(payload.getClusterStats().getNodesStats().getCounts().getTotal(), equalTo(internalCluster().getNodeNames().length));
    }

    private ClusterStatsCollector newClusterStatsCollector() {
        return new ClusterStatsCollector(internalCluster().getInstance(Settings.class), internalCluster().getInstance(ClusterService.class),
                internalCluster().getInstance(ClusterName.class), client());
    }
}
