/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.marvel.agent.collector.indices.IndexStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numClientNodes = 0)
public class IndexStatsTests extends MarvelIntegTestCase {
    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "-1")
                .put(MarvelSettings.COLLECTORS, IndexStatsCollector.NAME)
                .put("marvel.agent.exporters.default_local.type", "local")
                .put("marvel.agent.exporters.default_local.template.settings.index.number_of_replicas", 0)
                .build();
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testIndexStats() throws Exception {
        logger.debug("--> creating some indices for future index stats");
        final int nbIndices = randomIntBetween(1, 5);
        String[] indices = new String[nbIndices];
        for (int i = 0; i < nbIndices; i++) {
            indices[i] = "stat" + i;
            createIndex(indices[i]);
        }

        final long[] nbDocsPerIndex = new long[nbIndices];
        for (int i = 0; i < nbIndices; i++) {
            nbDocsPerIndex[i] = randomIntBetween(1, 50);
            for (int j = 0; j < nbDocsPerIndex[i]; j++) {
                client().prepareIndex("stat" + i, "type1").setSource("num", i).get();
            }
        }

        securedFlush();
        securedRefresh();

        updateMarvelInterval(3L, TimeUnit.SECONDS);
        waitForMarvelIndices();

        awaitMarvelDocsCount(greaterThan(0L), IndexStatsCollector.TYPE);

        logger.debug("--> wait for index stats collector to collect stat for each index");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                securedFlush(indices);
                securedRefresh();
                for (int i = 0; i < nbIndices; i++) {
                    SearchResponse count = client().prepareSearch()
                            .setSize(0)
                            .setTypes(IndexStatsCollector.TYPE)
                            .setQuery(QueryBuilders.termQuery("index_stats.index", indices[i]))
                            .get();
                    assertThat(count.getHits().totalHits(), greaterThan(0L));
                }
            }
        });

        logger.debug("--> searching for marvel documents of type [{}]", IndexStatsCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(IndexStatsCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = IndexStatsRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> index stats successfully collected");
    }
}
