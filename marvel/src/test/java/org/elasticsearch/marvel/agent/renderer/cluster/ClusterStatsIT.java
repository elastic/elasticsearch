/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.renderer.AbstractRendererTestCase;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

public class ClusterStatsIT extends AbstractRendererTestCase {

    @Override
    protected Collection<String> collectors() {
        return Collections.singletonList(ClusterStatsCollector.NAME);
    }

    @Test
    public void testClusterStats() throws Exception {
        logger.debug("--> creating some indices so that cluster stats reports data about shards");
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            createIndex("test-" + i);
        }

        logger.debug("--> wait for cluster stats to report data about shards");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                ClusterStatsResponse response = client().admin().cluster().prepareClusterStats().get();
                assertNotNull(response.getIndicesStats().getShards());
                assertThat(response.getIndicesStats().getShards().getTotal(), greaterThan(0));
            }
        }, 30L, TimeUnit.SECONDS);

        logger.debug("--> delete all indices in case of cluster stats documents have been indexed with no shards data");
        deleteMarvelIndices();

        awaitMarvelDocsCount(greaterThan(0L), ClusterStatsCollector.TYPE);

        logger.debug("--> searching for marvel documents of type [{}]", ClusterStatsCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(ClusterStatsCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = ClusterStatsRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> cluster stats successfully collected");
    }
}
