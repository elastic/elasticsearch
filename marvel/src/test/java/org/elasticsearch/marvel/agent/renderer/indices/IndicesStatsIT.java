/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.renderer.indices;

import org.apache.lucene.util.LuceneTestCase.AwaitsFix;
import org.elasticsearch.action.count.CountResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsCollector;
import org.elasticsearch.marvel.agent.settings.MarvelSettings;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.junit.Test;

import java.util.Map;

import static org.hamcrest.Matchers.greaterThan;

@AwaitsFix(bugUrl="https://github.com/elastic/x-plugins/issues/729")
public class IndicesStatsIT extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MarvelSettings.INTERVAL, "3s")
                .put(MarvelSettings.COLLECTORS, IndicesStatsCollector.NAME)
                .build();
    }

    @Test
    public void testIndicesStats() throws Exception {
        logger.debug("--> creating some indices for future indices stats");
        final int nbIndices = randomIntBetween(1, 5);
        for (int i = 0; i < nbIndices; i++) {
            createIndex("stat" + i);
        }

        final long[] nbDocsPerIndex = new long[nbIndices];
        for (int i = 0; i < nbIndices; i++) {
            nbDocsPerIndex[i] = randomIntBetween(1, 50);
            for (int j = 0; j < nbDocsPerIndex[i]; j++) {
                client().prepareIndex("stat" + i, "type1").setSource("num", i).get();
            }
        }

        awaitMarvelDocsCount(greaterThan(0L), IndicesStatsCollector.TYPE);

        logger.debug("--> wait for indicesx stats collector to collect global stat");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                for (int i = 0; i < nbIndices; i++) {
                    CountResponse count = client().prepareCount()
                            .setTypes(IndicesStatsCollector.TYPE)
                            .get();
                    assertThat(count.getCount(), greaterThan(0L));
                }
            }
        });

        logger.debug("--> searching for marvel documents of type [{}]", IndicesStatsCollector.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(IndicesStatsCollector.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = IndicesStatsRenderer.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> indices stats successfully collected");
    }
}
