/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.marvel.agent.resolver.indices;

import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.marvel.MonitoringSettings;
import org.elasticsearch.marvel.agent.collector.indices.IndicesStatsCollector;
import org.elasticsearch.marvel.test.MarvelIntegTestCase;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numClientNodes = 0)
public class IndicesStatsTests extends MarvelIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put(MonitoringSettings.COLLECTORS.getKey(), IndicesStatsCollector.NAME)
                .put("xpack.monitoring.agent.exporters.default_local.type", "local")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        updateMarvelInterval(-1, TimeUnit.SECONDS);
        wipeMarvelIndices();
    }

    public void testIndicesStats() throws Exception {
        logger.debug("--> creating some indices for future indices stats");
        final int nbIndices = randomIntBetween(1, 5);
        for (int i = 0; i < nbIndices; i++) {
            createIndex("stat-" + i);
        }

        final long[] nbDocsPerIndex = new long[nbIndices];
        for (int i = 0; i < nbIndices; i++) {
            nbDocsPerIndex[i] = randomIntBetween(1, 50);
            for (int j = 0; j < nbDocsPerIndex[i]; j++) {
                client().prepareIndex("stat-" + i, "type1").setSource("num", i).get();
            }
        }

        logger.debug("--> wait for indices stats collector to collect stats for all primaries shards");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                securedFlush();
                securedRefresh();

                for (int i = 0; i < nbIndices; i++) {
                    IndicesStatsResponse indicesStats = client().admin().indices().prepareStats().get();
                    assertThat(indicesStats.getPrimaries().getDocs().getCount(), greaterThan(0L));
                }
            }
        });

        updateMarvelInterval(3L, TimeUnit.SECONDS);
        waitForMarvelIndices();

        logger.debug("--> wait for indices stats collector to collect global stat");
        awaitMarvelDocsCount(greaterThan(0L), IndicesStatsResolver.TYPE);

        logger.debug("--> searching for monitoring documents of type [{}]", IndicesStatsResolver.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(IndicesStatsResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        String[] filters = IndicesStatsResolver.FILTERS;
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.sourceAsMap();

            for (String filter : filters) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> indices stats successfully collected");
    }
}
