/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.parquet;

import org.elasticsearch.blobcache.shared.SharedBlobCacheService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.Sum;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.parquet.docvalues.ParquetBlobCache;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Does a mount that has a node-local cache read each byte range of its file once? The first search fills the cache from the file, and
 * a second search that reads the same column chunks must not write to the cache again.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0)
public class ParquetCacheIT extends ESIntegTestCase {

    @Override
    protected boolean addMockInternalEngine() {
        // only one engine factory may apply to an index, and ours must be the one
        return false;
    }

    @Override
    protected boolean forceSingleDataPath() {
        // the shared cache is not allowed on a node with several data paths
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
            .put(ParquetBlobCache.CACHE_SIZE.getKey(), "32mb")
            .put(ParquetBlobCache.CACHE_REGION_SIZE.getKey(), "1mb")
            .build();
    }

    public void testASecondSearchReadsFromTheCache() throws Exception {
        internalCluster().startNode();
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
        ParquetBlobCache cache = internalCluster().getInstance(ParquetBlobCache.class);

        assertServesTheFile();
        SharedBlobCacheService.Stats afterFirst = cache.stats();
        assertThat("the first search must fill the cache", afterFirst.writeBytes(), greaterThan(0L));
        // our file has its own name, so that it cannot clash with the cache of searchable snapshots (which deletes its file at a size of 0)
        Path dataPath = internalCluster().getInstance(NodeEnvironment.class).nodeDataPaths()[0];
        assertTrue(Files.exists(dataPath.resolve("parquet_cache")));
        assertFalse(Files.exists(dataPath.resolve("shared_snapshot_cache")));

        assertServesTheFile();
        SharedBlobCacheService.Stats afterSecond = cache.stats();
        assertEquals("the second search must not write to the cache", afterFirst.writeBytes(), afterSecond.writeBytes());
        assertEquals("the second search must not miss", afterFirst.missCount(), afterSecond.missCount());
    }

    private static void assertServesTheFile() {
        int rows = ParquetMounts.ROWS;
        assertResponse(
            prepareSearch("events").setSize(0)
                .setTrackTotalHits(true)
                .addAggregation(AggregationBuilders.max("max_status").field("status"))
                .addAggregation(AggregationBuilders.sum("sum_ts").field("ts")),
            response -> {
                assertEquals(rows, response.getHits().getTotalHits().value());
                assertEquals(rows - 1, ((Max) response.getAggregations().get("max_status")).value(), 0.0);
                assertEquals(1000.0 * rows * (rows - 1) / 2, ((Sum) response.getAggregations().get("sum_ts")).value(), 0.0);
            }
        );
    }
}
