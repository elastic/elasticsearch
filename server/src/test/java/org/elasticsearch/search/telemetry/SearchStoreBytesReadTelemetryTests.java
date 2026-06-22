/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.telemetry;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.telemetry.Measurement;
import org.elasticsearch.telemetry.TestTelemetryPlugin;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.index.query.QueryBuilders.simpleQueryStringQuery;
import static org.elasticsearch.rest.action.search.SearchResponseMetrics.STORE_BYTES_READ_HISTOGRAM_NAME;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSearchHitsWithoutFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

public class SearchStoreBytesReadTelemetryTests extends ESSingleNodeTestCase {

    @After
    public void resetTelemetry() {
        getTestTelemetryPlugin().resetMeter();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(TestTelemetryPlugin.class);
    }

    public void testSimpleQueryRecordsStoreBytesRead() throws Exception {
        assumeTrue("directory metrics must be enabled", Store.DIRECTORY_METRICS_FEATURE_FLAG.isEnabled());

        var numPrimaries = randomIntBetween(1, 3);
        String indexName = randomIndexName();
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, numPrimaries)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .build()
        );
        ensureGreen(indexName);

        prepareIndex(indexName).setId("1").setSource("body", "red").get();
        prepareIndex(indexName).setId("2").setSource("body", "green").get();
        prepareIndex(indexName).setId("3").setSource("body", "green").get();
        prepareIndex(indexName).setId("4").setSource("body", "blue").get();

        admin().indices().prepareRefresh(indexName).get();

        assertSearchHitsWithoutFailures(client().prepareSearch(indexName).setQuery(simpleQueryStringQuery("green")), "2", "3");
        assertBusy(() -> {
            List<Measurement> measurements = getTestTelemetryPlugin().getLongHistogramMeasurement(STORE_BYTES_READ_HISTOGRAM_NAME);
            assertThat(measurements.size(), equalTo(1));
            // serving the query reads postings and stored fields from the store, so the recorded value must be positive
            assertThat(measurements.get(0).getLong(), greaterThan(0L));
        });
    }

    private TestTelemetryPlugin getTestTelemetryPlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestTelemetryPlugin.class).toList().get(0);
    }
}
