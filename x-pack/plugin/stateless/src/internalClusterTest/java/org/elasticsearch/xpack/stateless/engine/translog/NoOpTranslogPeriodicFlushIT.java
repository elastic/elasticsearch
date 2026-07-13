/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine.translog;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.cluster.routing.SplitShardCountSummary;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.UncategorizedExecutionException;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.CommitBCCResolver;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.ShardLocalReadersTracker;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;
import java.util.function.Predicate;

import static org.elasticsearch.action.support.WriteRequest.RefreshPolicy.IMMEDIATE;
import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertResponse;
import static org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator.FLUSH_INTERVAL_SETTING;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

// Regression test for the #3539 bug.
public class NoOpTranslogPeriodicFlushIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        return plugins;
    }

    public void testBufferedTranslogFlushesNoOpAfterSimulatedLuceneFailure() throws Exception {
        startMasterOnlyNode();

        final long flushIntervalMs = randomLongBetween(50, 100);
        final var indexNodeSettings = Settings.builder()
            .put(disableIndexingDiskAndMemoryControllersNodeSettings())
            .put(FLUSH_INTERVAL_SETTING.getKey(), TimeValue.timeValueMillis(flushIntervalMs))
            .build();
        final var indexNode = startIndexNode(indexNodeSettings);
        final var translogReplicator = getTranslogReplicator(indexNode);

        startSearchNode();
        ensureStableCluster(3);

        final String indexName = randomAlphaOfLength(10).toLowerCase(Locale.ROOT);
        createIndex(
            indexName,
            indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST)
                .build()
        );
        ensureGreen(indexName);

        client().prepareIndex(indexName).setSource("id", "warmup").setRefreshPolicy(IMMEDIATE).get();

        // Finish any Lucene writes for the first document before starting failure
        flush(indexName);
        assertBusy(() -> assertThat(translogReplicator.getTranslogBufferedDataSize(), equalTo(0L)), 30, TimeUnit.SECONDS);

        assertResponse(
            prepareSearch(indexName).setQuery(matchAllQuery()),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(1L))
        );

        TestStatelessPlugin.FAIL_NEXT_ASSERT_DOC.set(true);
        assertThrows(ElasticsearchException.class, () -> client().prepareIndex(indexName).setSource("id", "failed").get());

        final var objectStoreService = getObjectStoreService(indexNode);
        final var translogContainer = objectStoreService.getTranslogBlobContainer();
        final var activeFiles = translogReplicator.getActiveTranslogFiles();
        assertThat(activeFiles, not(empty()));
        assertTranslogBlobsExist(activeFiles, translogContainer);

        assertResponse(
            prepareSearch(indexName).setQuery(matchAllQuery()),
            r -> assertThat(r.getHits().getTotalHits().value(), equalTo(1L))
        );

        assertAcked(indicesAdmin().prepareDelete(indexName));
        assertBusy(() -> assertThat(translogReplicator.getTranslogBufferedDataSize(), equalTo(0L)), 30, TimeUnit.SECONDS);
    }

    private static void assertTranslogBlobsExist(Set<TranslogReplicator.BlobTranslogFile> shouldExist, BlobContainer container)
        throws IOException {
        final OperationPurpose purpose = operationPurpose;
        for (TranslogReplicator.BlobTranslogFile translogFile : shouldExist) {
            assertTrue(container.blobExists(purpose, translogFile.blobName()));
        }
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        static final AtomicBoolean FAIL_NEXT_ASSERT_DOC = new AtomicBoolean(false);

        public TestStatelessPlugin(Settings settings) {
            super(settings);
        }

        @Override
        protected IndexEngine newIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService sharedBlobCacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new FailingSearcherIndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                shardId -> {
                    final var indexShard = getIndicesService().getShardOrNull(shardId);
                    return indexShard == null || indexShard.routingEntry().relocating();
                },
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            );
        }
    }

    /// Test [IndexEngine] for [NoOpTranslogPeriodicFlushIT]: simulates a non-tragic indexing failure by throwing
    /// [UncategorizedExecutionException] from [Engine#acquireSearcher] when [InternalEngine] opens the internal
    /// searcher whose `source` is `assert doc doesn't exist` (the path used around document-existence checks).
    ///
    /// Use [TestStatelessPlugin#FAIL_NEXT_ASSERT_DOC] to only throw once when requested, after which the flag is cleared.
    static final class FailingSearcherIndexEngine extends IndexEngine {

        FailingSearcherIndexEngine(
            EngineConfig engineConfig,
            TranslogReplicator translogReplicator,
            Function<String, BlobContainer> translogBlobContainer,
            StatelessCommitService statelessCommitService,
            HollowShardsService hollowShardsService,
            SharedBlobCacheWarmingService cacheWarmingService,
            RefreshManagerService refreshManagerService,
            ReshardIndexService reshardIndexService,
            CommitBCCResolver commitBCCResolver,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics metrics,
            Predicate<ShardId> shouldSkipMerges,
            ShardLocalReadersTracker shardLocalReadersTracker
        ) {
            super(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                cacheWarmingService,
                refreshManagerService,
                reshardIndexService,
                commitBCCResolver,
                documentParsingProvider,
                metrics,
                shouldSkipMerges,
                shardLocalReadersTracker
            );
        }

        @Override
        public Engine.Searcher acquireSearcher(
            String source,
            Engine.SearcherScope scope,
            SplitShardCountSummary splitShardCountSummary,
            Function<Engine.Searcher, Engine.Searcher> wrapper
        ) throws EngineException {
            if (TestStatelessPlugin.FAIL_NEXT_ASSERT_DOC.get() && "assert doc doesn't exist".equals(source)) {
                TestStatelessPlugin.FAIL_NEXT_ASSERT_DOC.set(false);
                throw new UncategorizedExecutionException("simulated failure for assertDocDoesNotExist", null);
            }
            return super.acquireSearcher(source, scope, splitShardCountSummary, wrapper);
        }
    }
}
