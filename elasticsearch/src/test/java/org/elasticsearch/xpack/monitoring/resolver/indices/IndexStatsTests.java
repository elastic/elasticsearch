/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.monitoring.resolver.indices;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.test.ESIntegTestCase.ClusterScope;
import org.elasticsearch.test.ESIntegTestCase.Scope;
import org.elasticsearch.xpack.monitoring.MonitoringSettings;
import org.elasticsearch.xpack.monitoring.collector.indices.IndexStatsCollector;
import org.elasticsearch.xpack.monitoring.test.MonitoringIntegTestCase;
import org.junit.After;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

@ClusterScope(scope = Scope.TEST, numClientNodes = 0)
public class IndexStatsTests extends MonitoringIntegTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
                .put(super.nodeSettings(nodeOrdinal))
                .put(MonitoringSettings.INTERVAL.getKey(), "-1")
                .put(MonitoringSettings.COLLECTORS.getKey(), IndexStatsCollector.NAME)
                .put("xpack.monitoring.exporters.default_local.type", "local")
                .build();
    }

    @After
    public void cleanup() throws Exception {
        disableMonitoringInterval();
        wipeMonitoringIndices();
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/x-plugins/issues/1396")
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

        flush();
        refresh();

        updateMonitoringInterval(3L, TimeUnit.SECONDS);
        waitForMonitoringIndices();

        awaitMonitoringDocsCount(greaterThan(0L), IndexStatsResolver.TYPE);

        logger.debug("--> wait for index stats collector to collect stat for each index");
        assertBusy(new Runnable() {
            @Override
            public void run() {
                flush(indices);
                refresh();
                for (int i = 0; i < nbIndices; i++) {
                    SearchResponse count = client().prepareSearch()
                            .setSize(0)
                            .setTypes(IndexStatsResolver.TYPE)
                            .setQuery(QueryBuilders.termQuery("index_stats.index", indices[i]))
                            .get();
                    assertThat(count.getHits().getTotalHits(), greaterThan(0L));
                }
            }
        });

        logger.debug("--> searching for monitoring documents of type [{}]", IndexStatsResolver.TYPE);
        SearchResponse response = client().prepareSearch().setTypes(IndexStatsResolver.TYPE).get();
        assertThat(response.getHits().getTotalHits(), greaterThan(0L));

        logger.debug("--> checking that every document contains the expected fields");
        for (SearchHit searchHit : response.getHits().getHits()) {
            Map<String, Object> fields = searchHit.getSourceAsMap();

            for (String filter : IndexStatsResolver.FILTERS) {
                assertContains(filter, fields);
            }
        }

        logger.debug("--> index stats successfully collected");
    }
}
