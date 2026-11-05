/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.apache.logging.log4j.Level;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.MockLog;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBlobCache;

import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;

/**
 * A cache is an optimization: a node that cannot create it, here because the data path has far less free space than the size that was
 * asked for, must start, say why in its log, and serve the mount by reading the repository directly.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetCacheFallbackIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        // with several data paths the plugin also falls back, but this test is about the free space
        return true;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(LocalStateParquetDocValues.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            // far more than any disk has free, and large regions to keep the number of regions small
            .put(ParquetBlobCache.CACHE_SIZE.getKey(), "100tb")
            .put(ParquetBlobCache.CACHE_REGION_SIZE.getKey(), "1gb")
            .build();
    }

    public void testTheNodeStartsAndServesTheMountWithoutTheCache() throws Exception {
        try (MockLog mockLog = MockLog.capture(ParquetDocValuesPlugin.class)) {
            mockLog.addExpectation(
                new MockLog.SeenEventExpectation(
                    "the node says that it has no cache",
                    ParquetDocValuesPlugin.class.getCanonicalName(),
                    Level.WARN,
                    "cannot create the Parquet cache of [100tb]: reads go straight to the repository"
                )
            );
            internalCluster().startNode();
            mockLog.assertAllExpectationsMatched();
        }
        ParquetMounts.writeFileAndRegisterRepository(this);
        assertAcked(
            prepareCreate("events").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put(ParquetMounts.mountSettings().build())
            ).setMapping(ParquetMounts.MAPPING)
        );
        ensureGreen("events");
        assertResponse(
            prepareSearch("events").setSize(0)
                .setTrackTotalHits(true)
                .addAggregation(AggregationBuilders.max("max_status").field("status")),
            response -> {
                assertEquals(ParquetMounts.ROWS, response.getHits().getTotalHits().value());
                assertEquals(ParquetMounts.ROWS - 1, ((Max) response.getAggregations().get("max_status")).value(), 0.0);
            }
        );
    }
}
