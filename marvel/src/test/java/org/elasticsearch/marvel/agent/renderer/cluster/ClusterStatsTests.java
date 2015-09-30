/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.cluster;

import org.elasticsearch.action.admin.cluster.stats.ClusterStatsNodes;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.cluster.ClusterStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.junit.Test;

import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.ESIntegTestCase.Scope.SUITE;
import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = SUITE, numClientNodes = 0)
public class ClusterStatsTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "3s")
                .put(MarvelSettings.COLLECTORS, ClusterStatsCollector.NAME)
                .build();
    }

    @Test
    public void testClusterStats() throws Exception {

        // lets wait with the collection until all the shards started
        stopCollection();

        logger.debug("--> creating some indices so that every data nodes will at least a shard");
        ClusterStatsNodes.Counts counts = client().admin().cluster().prepareClusterStats().get().getNodesStats().getCounts();
        assertThat(counts.getTotal(), greaterThan(0));

        String indexNameBase = randomAsciiOfLength(5).toLowerCase(Locale.ROOT);
        int indicesCount = randomIntBetween(1, 5);
        String[] indices = new String[indicesCount];
        for (int i = 0; i < indicesCount; i++) {
            indices[i] = indexNameBase + "-" + i;
            index(indices[i], "foo", "1", jsonBuilder().startObject().field("dummy_field", 1).endObject());
        }

        securedFlush();
        securedRefresh();
        securedEnsureGreen();

        // ok.. we'll start collecting now...
        startCollection();

        awaitMarvelTemplateInstalled();

        assertBusy(new Runnable() {
            @Override
            public void run() {
                logger.debug("--> searching for marvel [{}] documents", ClusterStatsCollector.TYPE);
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
        });
    }
}
