/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.indices;

import org.elasticsearch.action.support.broadcast.BaseBroadcastResponse;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.refresh.RefreshStats;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.util.Collection;
import java.util.Optional;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class IndexingMemoryControllerIT extends ESSingleNodeTestCase {

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            // small indexing buffer so that
            // 1. We can trigger refresh after buffering 100 deletes
            // 2. Indexing memory Controller writes indexing buffers in sync with indexing on the indexing thread
            .put("indices.memory.index_buffer_size", "1kb")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return CollectionUtils.appendToCopy(super.getPlugins(), TestEnginePlugin.class);
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {

        EngineConfig engineConfigWithLargerIndexingMemory(EngineConfig config) {
            // We need to set a larger buffer for the IndexWriter; otherwise, it will flush before the IndexingMemoryController.
            Settings settings = Settings.builder()
                .put(config.getIndexSettings().getSettings())
                .put("indices.memory.index_buffer_size", "10mb")
                .build();
            IndexSettings indexSettings = new IndexSettings(config.getIndexSettings().getIndexMetadata(), settings);
            return new EngineConfig(
                config.getShardId(),
                config.getThreadPool(),
                config.getThreadPoolMergeExecutorService(),
                indexSettings,
                config.getWarmer(),
                config.getStore(),
                config.getMergePolicy(),
                config.getAnalyzer(),
                config.getSimilarity(),
                new CodecService(null, BigArrays.NON_RECYCLING_INSTANCE),
                config.getEventListener(),
                config.getQueryCache(),
                config.getQueryCachingPolicy(),
                config.getTranslogConfig(),
                config.getFlushMergesAfter(),
                config.getExternalRefreshListener(),
                config.getInternalRefreshListener(),
                config.getIndexSort(),
                config.getCircuitBreakerService(),
                config.getGlobalCheckpointSupplier(),
                config.retentionLeasesSupplier(),
                config.getPrimaryTermSupplier(),
                config.getSnapshotCommitSupplier(),
                config.getLeafSorter(),
                config.getRelativeTimeInNanosSupplier(),
                config.getIndexCommitListener(),
                config.isPromotableToPrimary(),
                config.getMapperService(),
                config.getEngineResetLock(),
                config.getMergeMetrics()
            );
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(engineConfigWithLargerIndexingMemory(config)));
        }
    }

    // #10312
    public void testDeletesAloneCanTriggerRefresh() throws Exception {
        IndexService indexService = createIndex("index", indexSettings(1, 0).put("index.refresh_interval", -1).build());
        IndexShard shard = indexService.getShard(0);
        for (int i = 0; i < 100; i++) {
            prepareIndex("index").setId(Integer.toString(i)).setSource("field", "value").get();
        }
        // Force merge so we know all merges are done before we start deleting:
        BaseBroadcastResponse r = client().admin().indices().prepareForceMerge().setMaxNumSegments(1).get();
        assertNoFailures(r);
        final RefreshStats refreshStats = shard.refreshStats();
        for (int i = 0; i < 100; i++) {
            client().prepareDelete("index", Integer.toString(i)).get();
        }
        assertThat(shard.getEngineOrNull().getIndexBufferRAMBytesUsed(), lessThanOrEqualTo(ByteSizeUnit.KB.toBytes(1)));
    }

    /* When there is memory pressure, we write indexing buffers to disk on the same thread as the indexing thread,
     * @see org.elasticsearch.indices.IndexingMemoryController.
     * This test verifies that we update the stats that capture the combined time for indexing + writing the
     * indexing buffers.
     * Note that the small indices.memory.index_buffer_size setting is required for this test to work.
     */
    public void testIndexingUpdatesRelevantStats() throws Exception {
        IndexService indexService = createIndex("index", indexSettings(1, 0).put("index.refresh_interval", -1).build());
        IndexShard shard = indexService.getShard(0);
        prepareIndex("index").setSource("field", randomUnicodeOfCodepointLengthBetween(10, 25)).get();
        // Check that
        assertThat(shard.indexingStats().getTotal().getTotalIndexingExecutionTimeInMillis(), greaterThan(0L));
        assertThat(
            shard.indexingStats().getTotal().getTotalIndexingExecutionTimeInMillis(),
            greaterThan(shard.indexingStats().getTotal().getIndexTime().getMillis())
        );
    }
}
