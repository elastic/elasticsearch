/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.FilterNumericDocValues;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.admin.cluster.reroute.ClusterRerouteUtils;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.cluster.routing.allocation.command.MoveAllocationCommand;
import org.elasticsearch.common.blobstore.OperationPurpose;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.index.engine.MergeEventListener;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.stateless.commits.StatelessCompoundCommit;
import org.elasticsearch.xpack.stateless.objectstore.ObjectStoreService;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.elasticsearch.index.engine.ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class StatelessAbortRunningMergesOnRelocationIT extends AbstractStatelessPluginIntegTestCase {

    private static final int MAX_CONCURRENT_RUNNING_MERGES = 2;

    @Override
    protected boolean addMockFsRepository() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final var plugins = new ArrayList<>(super.nodePlugins());
        plugins.remove(TestUtils.StatelessPluginWithTrialLicense.class);
        plugins.add(BlockingMergePolicyPlugin.class);
        plugins.add(StatelessMockRepositoryPlugin.class);
        return plugins;
    }

    @Override
    protected Settings.Builder nodeSettings() {
        return super.nodeSettings().put(USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(ObjectStoreService.TYPE_SETTING.getKey(), ObjectStoreService.ObjectStoreType.MOCK)
            .put(disableIndexingDiskAndMemoryControllersNodeSettings());
    }

    public void testRelocationAbortsRunningMerges() throws Exception {
        final var scenario = startBlockedRelocationWithParkedMerges(false);
        scenario.recorder().awaitAborted();
        scenario.uploadGate().release();
        setNodeRepositoryStrategy(scenario.sourceNode(), StatelessMockRepositoryStrategy.DEFAULT);
        ensureGreen(scenario.indexName());
    }

    public void testConcurrentForceMergeIsNotAborted() throws Exception {
        final var scenario = startBlockedRelocationWithParkedMerges(true);
        assertNoFailures(scenario.forceMergeFuture().get());
        scenario.recorder().awaitSucceeded();
        assertThat(scenario.recorder().abortedCount(), equalTo(0));
        scenario.uploadGate().release();
        setNodeRepositoryStrategy(scenario.sourceNode(), StatelessMockRepositoryStrategy.DEFAULT);
        ensureGreen(scenario.indexName());
    }

    /**
     * Parks some running (and optionally queued) merges on a source primary, then starts a relocation that is paused at the
     * pre-flush commit upload, so the relocation has begun (its merge-abort hook has run) but the engine is not yet
     * reset/closed. The parked merges are then released into that window and the returned {@link RelocationScenario} lets the
     * caller assert the outcome (aborted vs. exempt) and finish the relocation.
     */
    private RelocationScenario startBlockedRelocationWithParkedMerges(boolean withConcurrentForceMerge) throws Exception {
        final var sourceNode = startMasterAndIndexNode(
            nodeSettings().put("thread_pool." + ThreadPool.Names.MERGE + ".max", MAX_CONCURRENT_RUNNING_MERGES).build()
        );

        final var indexName = randomIdentifier();
        final var segmentsPerTier = 2;
        createIndex(
            indexName,
            indexSettings(1, 0).put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), segmentsPerTier)
                .put(IndexSettings.INDEX_REFRESH_INTERVAL_SETTING.getKey(), TimeValue.MINUS_ONE)
                .build()
        );
        ensureGreen(indexName);

        final var numQueuedMerges = randomIntBetween(0, 2);
        // If anything is queued, all running slots must be occupied; otherwise just run at least one merge.
        final var numRunningMerges = numQueuedMerges > 0
            ? MAX_CONCURRENT_RUNNING_MERGES
            : randomIntBetween(1, MAX_CONCURRENT_RUNNING_MERGES);
        final var totalMerges = numRunningMerges + numQueuedMerges;

        // A concurrent force merge exempts every merge (nothing aborts); otherwise every parked merge is aborted.
        final var recorder = new MergeEventRecorder(
            totalMerges,
            numRunningMerges,
            withConcurrentForceMerge ? 0 : totalMerges,
            withConcurrentForceMerge ? totalMerges : 0
        );
        final var executor = internalCluster().getInstance(IndicesService.class, sourceNode).getThreadPoolMergeExecutorService();
        assertThat(executor, notNullValue());
        executor.registerMergeEventListener(recorder.listener());

        final var sourcePlugin = internalCluster().getInstance(PluginsService.class, sourceNode)
            .filterPlugins(BlockingMergePolicyPlugin.class)
            .findFirst()
            .orElseThrow();
        sourcePlugin.blockRunningMerges(indexName);

        // Over-provision flushed segments so the tiered policy is guaranteed to schedule at least `totalMerges` merges.
        for (int i = 0; i < totalMerges * (segmentsPerTier + 1); i++) {
            indexDocs(indexName, randomIntBetween(20, 50));
            flush(indexName);
        }
        recorder.awaitQueued();
        recorder.awaitStarted();

        // Leave uncommitted docs so the relocation pre-flush produces a commit upload we can block.
        indexDocs(indexName, randomIntBetween(20, 50));

        ActionFuture<BroadcastResponse> forceMergeFuture = null;
        if (withConcurrentForceMerge) {
            // Don't flush: the force-merge flush would wait on the (blocked) relocation pre-flush and deadlock.
            forceMergeFuture = indicesAdmin().prepareForceMerge(indexName).setMaxNumSegments(1).setFlush(false).execute();
        }

        final var uploadGate = blockPreFlushUpload(sourceNode);
        final var targetNode = startIndexNode();
        ensureStableCluster(2);
        ClusterRerouteUtils.reroute(client(), new MoveAllocationCommand(indexName, 0, sourceNode, targetNode));

        uploadGate.awaitBlocked();
        final var primaryShard = client().admin()
            .cluster()
            .prepareState(TEST_REQUEST_TIMEOUT)
            .get()
            .getState()
            .routingTable()
            .index(indexName)
            .shard(0)
            .primaryShard();
        assertTrue(primaryShard.relocating());

        // Resume the parked merges; with the engine still open they can only be aborted by the relocation hook.
        sourcePlugin.releaseBlockedMerges();
        return new RelocationScenario(sourceNode, indexName, recorder, uploadGate, forceMergeFuture);
    }

    private PreFlushUploadGate blockPreFlushUpload(String sourceNode) {
        final var gate = new PreFlushUploadGate();
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
                gate.maybeBlock(blobName);
                super.blobContainerWriteBlobAtomic(originalRunnable, purpose, blobName, inputStream, blobSize, failIfAlreadyExists);
            }
        });
        return gate;
    }

    private record RelocationScenario(
        String sourceNode,
        String indexName,
        MergeEventRecorder recorder,
        PreFlushUploadGate uploadGate,
        ActionFuture<BroadcastResponse> forceMergeFuture
    ) {}

    /**
     * Records merge lifecycle events on a node via a {@link MergeEventListener}, exposing latches to await the expected
     * number of queued/started/aborted/successful merges and a count of how many merges were aborted.
     */
    private static final class MergeEventRecorder {
        private final CountDownLatch queued;
        private final CountDownLatch started;
        private final CountDownLatch aborted;
        private final CountDownLatch succeeded;
        private final AtomicInteger abortedCount = new AtomicInteger();

        MergeEventRecorder(int expectedQueued, int expectedStarted, int expectedAborted, int expectedSucceeded) {
            this.queued = new CountDownLatch(expectedQueued);
            this.started = new CountDownLatch(expectedStarted);
            this.aborted = new CountDownLatch(expectedAborted);
            this.succeeded = new CountDownLatch(expectedSucceeded);
        }

        MergeEventListener listener() {
            return new MergeEventListener() {
                @Override
                public void onMergeQueued(OnGoingMerge merge, long estimateMergeMemoryBytes) {
                    queued.countDown();
                }

                @Override
                public void onMergeStarted(OnGoingMerge merge) {
                    started.countDown();
                }

                @Override
                public void onMergeCompleted(OnGoingMerge merge) {
                    // A merge aborted while running still surfaces here, with isAborted() == true.
                    if (merge.getMerge().isAborted()) {
                        abortedCount.incrementAndGet();
                        aborted.countDown();
                    } else {
                        succeeded.countDown();
                    }
                }

                @Override
                public void onMergeAborted(OnGoingMerge merge) {
                    abortedCount.incrementAndGet();
                    aborted.countDown();
                }
            };
        }

        void awaitQueued() {
            safeAwait(queued);
        }

        void awaitStarted() {
            safeAwait(started);
        }

        void awaitAborted() {
            safeAwait(aborted);
        }

        void awaitSucceeded() {
            safeAwait(succeeded);
        }

        int abortedCount() {
            return abortedCount.get();
        }
    }

    /**
     * Blocks the first compound-commit (BCC) upload seen on a node — used to pause a relocation at its pre-flush so the
     * source engine stays open while the test inspects merge state.
     */
    private static final class PreFlushUploadGate {
        private final CountDownLatch blocked = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final AtomicBoolean blockNextCommitUpload = new AtomicBoolean(true);

        void maybeBlock(String blobName) {
            if (StatelessCompoundCommit.startsWithBlobPrefix(blobName) && blockNextCommitUpload.compareAndSet(true, false)) {
                blocked.countDown();
                safeAwait(proceed);
            }
        }

        void awaitBlocked() {
            safeAwait(blocked);
        }

        void release() {
            proceed.countDown();
        }
    }

    /**
     * Test plugin that wraps the index engine's merge policy so merges for a given index park mid-merge until released,
     * letting the test hold merges in the running state.
     */
    public static class BlockingMergePolicyPlugin extends TestUtils.StatelessPluginWithTrialLicense {

        private volatile String blockMergesForIndex;
        private volatile CountDownLatch proceedMerge;

        public BlockingMergePolicyPlugin(Settings settings) {
            super(settings);
        }

        void blockRunningMerges(String indexName) {
            this.proceedMerge = new CountDownLatch(1);
            this.blockMergesForIndex = indexName;
        }

        void releaseBlockedMerges() {
            proceedMerge.countDown();
        }

        private void maybeBlockMerge(String indexName) {
            if (indexName.equals(blockMergesForIndex)) {
                safeAwait(proceedMerge);
            }
        }

        @Override
        protected MergePolicy getMergePolicy(EngineConfig engineConfig) {
            final var indexName = engineConfig.getShardId().getIndexName();
            return new OneMergeWrappingMergePolicy(engineConfig.getMergePolicy(), toWrap -> new MergePolicy.OneMerge(toWrap) {
                @Override
                public CodecReader wrapForMerge(CodecReader reader) throws IOException {
                    return new FilterCodecReader(toWrap.wrapForMerge(reader)) {
                        @Override
                        public CacheHelper getCoreCacheHelper() {
                            return in.getCoreCacheHelper();
                        }

                        @Override
                        public CacheHelper getReaderCacheHelper() {
                            return in.getReaderCacheHelper();
                        }

                        @Override
                        public DocValuesProducer getDocValuesReader() {
                            return new FilterDocValuesProducer(super.getDocValuesReader()) {
                                @Override
                                public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                                    // A merge always reads numeric doc values (e.g. _primary_term), so this is hit mid-merge.
                                    return new FilterNumericDocValues(super.getNumeric(field)) {
                                        @Override
                                        public int nextDoc() throws IOException {
                                            maybeBlockMerge(indexName);
                                            return super.nextDoc();
                                        }
                                    };
                                }
                            };
                        }
                    };
                }
            });
        }
    }
}
