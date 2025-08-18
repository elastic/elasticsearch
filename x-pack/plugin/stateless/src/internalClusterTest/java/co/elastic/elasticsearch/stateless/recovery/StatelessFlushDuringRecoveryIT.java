/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.recovery;

import co.elastic.elasticsearch.stateless.AbstractStatelessIntegTestCase;
import co.elastic.elasticsearch.stateless.Stateless;
import co.elastic.elasticsearch.stateless.cache.SharedBlobCacheWarmingService;
import co.elastic.elasticsearch.stateless.commits.BatchedCompoundCommit;
import co.elastic.elasticsearch.stateless.commits.HollowShardsService;
import co.elastic.elasticsearch.stateless.commits.StatelessCommitService;
import co.elastic.elasticsearch.stateless.engine.IndexEngine;
import co.elastic.elasticsearch.stateless.engine.RefreshThrottler;
import co.elastic.elasticsearch.stateless.engine.translog.TranslogReplicator;

import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.search.SearchResponseUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;
import java.util.function.Function;

public class StatelessFlushDuringRecoveryIT extends AbstractStatelessIntegTestCase {
    public void testFlushDuringTranslogReplay() throws IOException, InterruptedException {
        startMasterOnlyNode();
        String indexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        startSearchNode();

        String indexName = randomIdentifier();
        createIndex(indexName, indexSettings(1, 1).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), -1).build());
        ensureGreen(indexName);

        BatchedCompoundCommit initialCommit = internalCluster().getInstance(StatelessCommitService.class, indexNode)
            .getLatestUploadedBcc(new ShardId(resolveIndex(indexName), 0));

        int documentCount = between(10, 20);
        indexDocs(indexName, documentCount);

        String newIndexNode = startIndexNode(disableIndexingDiskAndMemoryControllersNodeSettings());
        TestStateless testStateless = findPlugin(newIndexNode, TestStateless.class);

        // Block translog replay during future recovery after some documents are applied.
        // And fail after the flush to observe recovery after the flush during translog replay.
        int writtenDocsBeforeFlush = between(1, 10);
        var translogBlocked = new CountDownLatch(1);
        var blockTranslog = new CountDownLatch(1);
        testStateless.translogNextCallback = new Consumer<>() {
            private int seenDocuments = 0;

            @Override
            public void accept(Void ignored) {
                if (blockTranslog.getCount() == 0) {
                    return;
                }
                // Make sure there is something to flush.
                if (seenDocuments >= writtenDocsBeforeFlush) {
                    try {
                        translogBlocked.countDown();
                        blockTranslog.await();
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                    throw new IllegalStateException("fail");
                }
                seenDocuments += 1;
            }
        };

        // Now recover
        internalCluster().stopNode(indexNode);

        translogBlocked.await();

        var shard = findIndexShard(indexName);
        try {
            // We are in the middle of translog replay, let's flush.
            // Needs to be forced to actually have effect.
            // This should not throw.
            shard.flush(new FlushRequest(indexName));
        } finally {
            blockTranslog.countDown();
        }

        // Recovery will be retried with new commit uploaded during flush and succeed.
        ensureGreen(indexName);

        // Documents that were flushed during the previous recovery were not applied.
        final RecoveryState recoveryState = indicesAdmin().prepareRecoveries(indexName)
            .get()
            .shardRecoveryStates()
            .get(indexName)
            .stream()
            .filter(RecoveryState::getPrimary)
            .findFirst()
            .get();
        assertEquals(RecoveryState.Stage.DONE, recoveryState.getStage());
        assertEquals(documentCount, recoveryState.getTranslog().totalOperations());
        assertEquals(documentCount - writtenDocsBeforeFlush, recoveryState.getTranslog().recoveredOperations());

        // We have performed a flush during translog replay and a flush after successful recovery.
        // So generation advanced by 2.
        BatchedCompoundCommit latestCommit = internalCluster().getInstance(StatelessCommitService.class, newIndexNode)
            .getLatestUploadedBcc(shard.shardId());
        assertEquals(initialCommit.primaryTermAndGeneration().generation() + 2, latestCommit.primaryTermAndGeneration().generation());

        assertEquals(documentCount, SearchResponseUtils.getTotalHitsValue(prepareSearch(indexName)));
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(Stateless.class);
        plugins.add(TestStateless.class);
        return plugins;
    }

    public static class TestStateless extends Stateless {
        public volatile Consumer<Void> translogNextCallback = null;

        public TestStateless(Settings settings) {
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
            RefreshThrottler.Factory refreshThrottlerFactory,
            DocumentParsingProvider documentParsingProvider,
            IndexEngine.EngineMetrics engineMetrics
        ) {
            return new IndexEngine(
                engineConfig,
                translogReplicator,
                translogBlobContainer,
                statelessCommitService,
                hollowShardsService,
                sharedBlobCacheWarmingService,
                refreshThrottlerFactory,
                statelessCommitService.getCommitBCCResolverForShard(engineConfig.getShardId()),
                documentParsingProvider,
                engineMetrics,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                protected Translog.Snapshot newTranslogSnapshot(long fromSeqNo, long toSeqNo) throws IOException {
                    return new TranslogSnapshotWithCallback(super.newTranslogSnapshot(fromSeqNo, toSeqNo), translogNextCallback);
                }
            };
        }

        public static class TranslogSnapshotWithCallback implements Translog.Snapshot {
            private final Translog.Snapshot decorated;
            private final Consumer<Void> onNext;

            public TranslogSnapshotWithCallback(Translog.Snapshot decorated, Consumer<Void> onNext) {
                this.decorated = decorated;
                this.onNext = onNext;
            }

            @Override
            public void close() throws IOException {
                decorated.close();
            }

            @Override
            public int totalOperations() {
                return decorated.totalOperations();
            }

            @Override
            public Translog.Operation next() throws IOException {
                try {
                    if (onNext != null) {
                        onNext.accept(null);
                    }
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                return decorated.next();
            }
        }
    }
}
