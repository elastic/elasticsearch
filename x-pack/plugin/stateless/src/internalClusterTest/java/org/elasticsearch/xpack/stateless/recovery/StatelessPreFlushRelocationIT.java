/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.recovery;

import org.apache.lucene.index.IndexWriter;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.blobstore.BlobContainer;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.EngineException;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.CompositeRecoverySchedulingListener;
import org.elasticsearch.indices.recovery.RecoverySchedulingListener;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.xpack.stateless.AbstractStatelessPluginIntegTestCase;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryPlugin;
import org.elasticsearch.xpack.stateless.StatelessMockRepositoryStrategy;
import org.elasticsearch.xpack.stateless.TestUtils;
import org.elasticsearch.xpack.stateless.cache.SharedBlobCacheWarmingService;
import org.elasticsearch.xpack.stateless.commits.HollowShardsService;
import org.elasticsearch.xpack.stateless.commits.StatelessCommitService;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.engine.IndexEngine;
import org.elasticsearch.xpack.stateless.engine.IndexEngineDynamicSettings;
import org.elasticsearch.xpack.stateless.engine.RefreshManagerService;
import org.elasticsearch.xpack.stateless.engine.translog.TranslogReplicator;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;
import org.elasticsearch.xpack.stateless.reshard.ReshardIndexService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.elasticsearch.xpack.stateless.recovery.StatelessPrimaryRelocationSourceService.PRE_FLUSH_SLOW_UPLOAD_QUEUE_THRESHOLD_SETTING;
import static org.hamcrest.Matchers.is;

public class StatelessPreFlushRelocationIT extends AbstractStatelessPluginIntegTestCase {

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(TestStatelessPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return List.copyOf(plugins);
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK);
    }

    /**
     * A BCC upload is in flight when the relocation starts. The pre-flush drains it via
     * {@code waitForCurrentCommitDurability}, then calls {@code flush(false, waitIfOngoing)}.
     * Because flushLock is free at that point, both threshold values commit and wait for the
     * new BCC upload (when there is uncommitted data).
     */
    public void testPreFlushRelocationQueueDrain() {
        // threshold=ZERO → waitIfOngoing=true; threshold=1h → waitIfOngoing=false.
        // When flushLock is free both values behave identically: the pre-flush commits and waits.
        final TimeValue threshold = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueHours(1);
        final var sourceNode = startMasterAndIndexNode(
            Settings.builder().put(PRE_FLUSH_SLOW_UPLOAD_QUEUE_THRESHOLD_SETTING.getKey(), threshold).build()
        );
        final var indexName = createTestIndex();
        final boolean hasUncommittedDataDuringPreFlush = randomBoolean();

        indexDocs(indexName, randomIntBetween(10, 20));

        var firstUploadStarted = new CountDownLatch(1);
        var unblockFirstUpload = new CountDownLatch(1);
        var secondUploadStarted = new CountDownLatch(1);
        var unblockSecondUpload = new CountDownLatch(1);
        var bccUploadCount = new AtomicInteger();
        setNodeRepositoryStrategy(sourceNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (blobName.startsWith(StatelessCompoundCommit.PREFIX)) {
                    int n = bccUploadCount.incrementAndGet();
                    if (n == 1) {
                        firstUploadStarted.countDown();
                        safeAwait(unblockFirstUpload);
                    } else if (n == 2) {
                        secondUploadStarted.countDown();
                        safeAwait(unblockSecondUpload);
                    }
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        // Trigger flush; block the BCC upload so the queue is non-empty when recovery starts.
        indicesAdmin().prepareFlush(indexName).execute();
        safeAwait(firstUploadStarted);

        var preRecoveryFlushDone = startRelocationAndAwaitUntilItStartsOnSource(sourceNode, indexName);

        if (hasUncommittedDataDuringPreFlush) {
            indexDocs(indexName, randomIntBetween(10, 20));
            assertThat(preRecoveryFlushDone.isDone(), is(false));
        }

        // Unblock the queued BCC upload; the pre-flush drains it and then flushes the uncommitted data.
        // flushLock is free so both threshold values commit and block on the second BCC upload.
        unblockFirstUpload.countDown();
        if (hasUncommittedDataDuringPreFlush) {
            safeAwait(secondUploadStarted);
            assertThat(preRecoveryFlushDone.isDone(), is(false));
        }
        unblockSecondUpload.countDown();

        safeGet(preRecoveryFlushDone);
        ensureGreen(indexName);
    }

    /**
     * A Lucene flush holds flushLock when the pre-flush calls {@code flush(false, waitIfOngoing)}.
     * With {@code threshold=ZERO} ({@code waitIfOngoing=true}) the pre-flush blocks until the lock
     * is released and then waits for the BCC upload. With {@code threshold=1h}
     * ({@code waitIfOngoing=false}) the pre-flush skips immediately (SKIPPED).
     *
     * There is no pending BCC upload when the pre-flush runs: {@code waitForCurrentCommitDurability}
     * resolves immediately because the blocking flush has not yet committed.
     */
    public void testPreFlushRelocationOngoingFlush() {
        final TimeValue threshold = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueHours(1);
        final var sourceNode = startMasterAndIndexNode(
            Settings.builder().put(PRE_FLUSH_SLOW_UPLOAD_QUEUE_THRESHOLD_SETTING.getKey(), threshold).build()
        );
        final var indexName = createTestIndex();

        indexDocs(indexName, randomIntBetween(10, 20));

        var commitStartedLatch = new CountDownLatch(1);
        var unblockCommitLatch = new CountDownLatch(1);

        var firstUploadStarted = new CountDownLatch(1);
        var unblockFirstUpload = new CountDownLatch(1);
        var bccUploadCount = new AtomicInteger();
        setNodeRepositoryStrategy(sourceNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (blobName.startsWith(StatelessCompoundCommit.PREFIX) && bccUploadCount.incrementAndGet() == 1) {
                    firstUploadStarted.countDown();
                    safeAwait(unblockFirstUpload);
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        // Block in commitIndexWriter before the Lucene commit so flushLock is held.
        // getCurrentGeneration() still returns the old gen, so waitForCurrentCommitDurability
        // in the pre-flush resolves immediately and hits the held flushLock directly.
        TestStatelessPlugin.commitStartedLatch = commitStartedLatch;
        TestStatelessPlugin.unblockCommitLatch = unblockCommitLatch;
        indicesAdmin().prepareFlush(indexName).execute();
        safeAwait(commitStartedLatch);
        // Disable the latches so the pre-flush and final flush are not intercepted.
        TestStatelessPlugin.commitStartedLatch = null;
        TestStatelessPlugin.unblockCommitLatch = null;

        PreFlushObserver preFlush = installPreFlushInterceptor();
        PlainActionFuture<Void> preRecoveryFlushDone = startRelocationAndAwaitUntilItStartsOnSource(sourceNode, indexName);

        if (threshold.equals(TimeValue.ZERO)) {
            assertThat(safeGet(preFlush.waitIfOngoing()), is(true));
            assertThat(preFlush.result().isDone(), is(false));
        } else {
            assertThat(safeGet(preFlush.waitIfOngoing()), is(false));
        }
        TestStatelessPlugin.flushInterceptor = null;

        // Unblock the flush: it commits, releases flushLock, and starts the BCC upload.
        unblockCommitLatch.countDown();
        if (threshold.equals(TimeValue.ZERO)) {
            // waitIfOngoing=true: the pre-flush waited for flushLock and now waits for the BCC upload.
            safeAwait(firstUploadStarted);
            assertThat(preRecoveryFlushDone.isDone(), is(false));
        }
        // waitIfOngoing=false: the pre-flush returned SKIPPED
        unblockFirstUpload.countDown();

        safeGet(preRecoveryFlushDone);
        if (threshold.equals(TimeValue.ZERO)) {
            assertThat(safeGet(preFlush.result()).skippedDueToCollision(), is(false));
        } else {
            assertThat(safeGet(preFlush.result()).skippedDueToCollision(), is(true));
        }
        ensureGreen(indexName);
    }

    /**
     * Both a BCC upload is in flight and a Lucene flush holds flushLock when the pre-flush runs.
     * {@code waitForCurrentCommitDurability} blocks on the first BCC upload; only after it resolves
     * does {@code flush(false, waitIfOngoing)} encounter the held flushLock.
     * With {@code threshold=ZERO} ({@code waitIfOngoing=true}) the pre-flush then waits for both
     * the lock and the second BCC upload. With {@code threshold=1h} ({@code waitIfOngoing=false})
     * it skips after draining the first upload.
     */
    public void testPreFlushRelocationCombined() {
        final TimeValue threshold = randomBoolean() ? TimeValue.ZERO : TimeValue.timeValueHours(1);
        final var sourceNode = startMasterAndIndexNode(
            Settings.builder().put(PRE_FLUSH_SLOW_UPLOAD_QUEUE_THRESHOLD_SETTING.getKey(), threshold).build()
        );
        final var indexName = createTestIndex();

        indexDocs(indexName, randomIntBetween(10, 20));

        var commitStartedLatch = new CountDownLatch(1);
        var unblockCommitLatch = new CountDownLatch(1);

        var firstUploadStarted = new CountDownLatch(1);
        var unblockFirstUpload = new CountDownLatch(1);
        var secondUploadStarted = new CountDownLatch(1);
        var unblockSecondUpload = new CountDownLatch(1);
        var bccUploadCount = new AtomicInteger();
        setNodeRepositoryStrategy(sourceNode, new StatelessMockRepositoryStrategy() {
            @Override
            public void blobContainerWriteBlobAtomic(
                CheckedRunnable<IOException> originalRunnable,
                OperationPurpose purpose,
                String blobName,
                InputStream inputStream,
                long blobSize,
                boolean failIfAlreadyExists
            ) throws IOException {
                if (blobName.startsWith(StatelessCompoundCommit.PREFIX)) {
                    int n = bccUploadCount.incrementAndGet();
                    if (n == 1) {
                        firstUploadStarted.countDown();
                        safeAwait(unblockFirstUpload);
                    } else if (n == 2) {
                        secondUploadStarted.countDown();
                        safeAwait(unblockSecondUpload);
                    }
                }
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });

        // First flush: commits gen1 and starts the BCC upload (blocked). Gen1 is now the current gen.
        indicesAdmin().prepareFlush(indexName).execute();
        safeAwait(firstUploadStarted);

        // Second flush: blocks in commitIndexWriter before committing gen2, so gen1 remains the current gen.
        // The pre-flush will call waitForCurrentCommitDurability(gen1) — blocked — and only after gen1
        // BCC completes will it call flush(false, waitIfOngoing), which then hits the held gen2 flushLock.
        indexDocs(indexName, randomIntBetween(10, 20));
        TestStatelessPlugin.commitStartedLatch = commitStartedLatch;
        TestStatelessPlugin.unblockCommitLatch = unblockCommitLatch;
        indicesAdmin().prepareFlush(indexName).execute();
        safeAwait(commitStartedLatch);
        TestStatelessPlugin.commitStartedLatch = null;
        TestStatelessPlugin.unblockCommitLatch = null;

        // Randomly release gen1 before or after the relocation starts, exercising both the case where
        // waitForCurrentCommitDurability blocks (gen1 still in flight) and where it resolves synchronously
        // (gen1 already complete). Gen2 holds flushLock either way, so the relocation cannot complete.
        boolean releaseGen1BeforeRelocation = randomBoolean();
        if (releaseGen1BeforeRelocation) {
            unblockFirstUpload.countDown();
        }

        PreFlushObserver preFlush = installPreFlushInterceptor();
        PlainActionFuture<Void> preRecoveryFlushDone = startRelocationAndAwaitUntilItStartsOnSource(sourceNode, indexName);

        if (releaseGen1BeforeRelocation == false) {
            unblockFirstUpload.countDown();
        }

        if (threshold.equals(TimeValue.ZERO)) {
            assertThat(safeGet(preFlush.waitIfOngoing()), is(true));
            assertThat(preFlush.result().isDone(), is(false));
            assertThat(preRecoveryFlushDone.isDone(), is(false));
        } else {
            assertThat(safeGet(preFlush.waitIfOngoing()), is(false));
        }
        TestStatelessPlugin.flushInterceptor = null;

        // Unblock gen2's commit: it commits, releases flushLock, and gen2 BCC upload begins.
        unblockCommitLatch.countDown();
        if (threshold.equals(TimeValue.ZERO)) {
            // waitIfOngoing=true: the pre-flush waited for the flushLock and now waits for gen2 BCC durability.
            safeAwait(secondUploadStarted);
            assertThat(preRecoveryFlushDone.isDone(), is(false));
        }
        // waitIfOngoing=false: the pre-flush returned SKIPPED
        unblockSecondUpload.countDown();

        safeGet(preRecoveryFlushDone);
        if (threshold.equals(TimeValue.ZERO)) {
            assertThat(safeGet(preFlush.result()).skippedDueToCollision(), is(false));
        } else {
            assertThat(safeGet(preFlush.result()).skippedDueToCollision(), is(true));
        }
        ensureGreen(indexName);
    }

    private String createTestIndex() {
        final var indexName = randomIndexName();
        createIndex(indexName, indexSettings(1, 0).put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE).build());
        ensureGreen(indexName);
        return indexName;
    }

    private record PreFlushObserver(PlainActionFuture<Boolean> waitIfOngoing, PlainActionFuture<Engine.FlushResult> result) {}

    private static PreFlushObserver installPreFlushInterceptor() {
        PreFlushObserver observer = new PreFlushObserver(new PlainActionFuture<>(), new PlainActionFuture<>());
        TestStatelessPlugin.flushInterceptor = (waitIfOngoing, listener) -> {
            observer.waitIfOngoing().onResponse(waitIfOngoing);
            return Engine.FlushResultListener.wrap(ActionListener.wrap(r -> {
                observer.result().onResponse(r);
                listener.onResponse(r);
            }, e -> {
                observer.result().onFailure(e);
                listener.onFailure(e);
            }), listener::afterFlushWithLock);
        };
        return observer;
    }

    private PlainActionFuture<Void> startRelocationAndAwaitUntilItStartsOnSource(String sourceNode, String indexName) {
        var peerRecoveryCompletedOnSource = new PlainActionFuture<Void>();
        var recoveryStarted = new CountDownLatch(1);
        internalCluster().getInstance(CompositeRecoverySchedulingListener.class, sourceNode).addListener(new RecoverySchedulingListener() {
            @Override
            public void onPeerRecoveryStartedOnSource() {
                recoveryStarted.countDown();
            }

            @Override
            public void onPeerRecoveryCompletedOnSource() {
                peerRecoveryCompletedOnSource.onResponse(null);
            }
        });
        startIndexNode();
        updateIndexSettings(Settings.builder().put(IndexMetadata.INDEX_ROUTING_EXCLUDE_GROUP_PREFIX + "._name", sourceNode), indexName);
        safeAwait(recoveryStarted);
        assertThat(peerRecoveryCompletedOnSource.isDone(), is(false));
        return peerRecoveryCompletedOnSource;
    }

    public static class TestStatelessPlugin extends TestUtils.StatelessPluginWithTrialLicense {
        static volatile CountDownLatch commitStartedLatch;
        static volatile CountDownLatch unblockCommitLatch;
        static volatile BiFunction<Boolean, Engine.FlushResultListener, Engine.FlushResultListener> flushInterceptor;

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
            IndexEngine.EngineMetrics engineMetrics,
            IndexEngineDynamicSettings indexEngineDynamicSettings
        ) {
            return new IndexEngine(
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
                indexEngineDynamicSettings,
                statelessCommitService.getShardLocalCommitsTracker(engineConfig.getShardId()).shardLocalReadersTracker()
            ) {
                @Override
                protected void commitIndexWriter(IndexWriter writer, Translog translog) throws IOException {
                    CountDownLatch started = commitStartedLatch;
                    if (started != null) {
                        started.countDown();
                    }
                    CountDownLatch block = unblockCommitLatch;
                    if (block != null) {
                        safeAwait(block);
                    }
                    super.commitIndexWriter(writer, translog);
                }

                @Override
                protected void flushHoldingLock(boolean force, boolean waitIfOngoing, Engine.FlushResultListener listener)
                    throws EngineException {
                    super.flushHoldingLock(
                        force,
                        waitIfOngoing,
                        flushInterceptor != null ? flushInterceptor.apply(waitIfOngoing, listener) : listener
                    );
                }
            };
        }
    }
}
