/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.codecs.DocValuesProducer;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.apache.lucene.index.SortedDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.SortedSetDocValues;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.env.ShardLockObtainFailedException;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

import static org.elasticsearch.core.Strings.format;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.notNullValue;

public class MergeWithFailureIT extends ESIntegTestCase {

    private static final String FAILING_MERGE_ON_PURPOSE = "Failing merge on purpose";

    public static class TestPlugin extends Plugin implements EnginePlugin {

        // Number of queued merges in the thread pool. Lucene considers those as "running" and blocks waiting on them to complete in case
        // of a merge failure.
        private final Set<MergePolicy.OneMerge> pendingMerges = ConcurrentCollections.newConcurrentSet();

        // Number of running merges in the thread pool
        private final AtomicInteger runningMergesCount = new AtomicInteger();

        // Latch to unblock merges
        private final CountDownLatch runMerges = new CountDownLatch(1);

        // Reference to the ThreadPoolMergeExecutorService
        private final AtomicReference<ThreadPoolMergeExecutorService> threadPoolMergeExecutorServiceReference = new AtomicReference<>();

        // This future is completed once the shard that is expected to fail has its store closed
        private final PlainActionFuture<Void> shardStoreClosedListener = new PlainActionFuture<>();

        private final boolean isDataNode;

        public TestPlugin(Settings settings) {
            this.isDataNode = DiscoveryNode.hasDataRole(settings);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (isDataNode == false) {
                return Optional.of(InternalEngine::new);
            }
            return Optional.of(
                config -> new TestEngine(
                    EngineTestCase.copy(
                        config,
                        new OneMergeWrappingMergePolicy(config.getMergePolicy(), toWrap -> new MergePolicy.OneMerge(toWrap) {
                            @Override
                            public CodecReader wrapForMerge(CodecReader reader) {
                                return new FilterCodecReader(reader) {
                                    final AtomicBoolean failOnce = new AtomicBoolean(false);

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
                                        final var in = super.getDocValuesReader();
                                        return new DocValuesProducer() {
                                            @Override
                                            public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                                                safeAwait(runMerges, TimeValue.ONE_MINUTE);
                                                if (failOnce.compareAndSet(false, true)) {
                                                    throw new IOException(FAILING_MERGE_ON_PURPOSE);
                                                }
                                                return in.getNumeric(field);
                                            }

                                            @Override
                                            public BinaryDocValues getBinary(FieldInfo field) throws IOException {
                                                return in.getBinary(field);
                                            }

                                            @Override
                                            public SortedDocValues getSorted(FieldInfo fieldInfo) throws IOException {
                                                return in.getSorted(fieldInfo);
                                            }

                                            @Override
                                            public SortedNumericDocValues getSortedNumeric(FieldInfo fieldInfo) throws IOException {
                                                return in.getSortedNumeric(fieldInfo);
                                            }

                                            @Override
                                            public SortedSetDocValues getSortedSet(FieldInfo field) throws IOException {
                                                return in.getSortedSet(field);
                                            }

                                            @Override
                                            public DocValuesSkipper getSkipper(FieldInfo fieldInfo) throws IOException {
                                                return in.getSkipper(fieldInfo);
                                            }

                                            @Override
                                            public void checkIntegrity() throws IOException {
                                                in.checkIntegrity();
                                            }

                                            @Override
                                            public void close() throws IOException {
                                                in.close();
                                            }
                                        };
                                    }
                                };
                            }
                        })
                    )
                )
            );
        }

        private class TestEngine extends InternalEngine {

            TestEngine(EngineConfig engineConfig) {
                super(engineConfig);
            }

            @Override
            protected ElasticsearchMergeScheduler createMergeScheduler(
                ShardId shardId,
                IndexSettings indexSettings,
                ThreadPoolMergeExecutorService executor
            ) {
                threadPoolMergeExecutorServiceReference.set(Objects.requireNonNull(executor));
                return new ThreadPoolMergeScheduler(shardId, indexSettings, executor, merge -> 0L) {

                    @Override
                    public void merge(MergeSource mergeSource, MergeTrigger trigger) {
                        var wrapped = wrapMergeSource(mergeSource);
                        super.merge(wrapped, trigger);
                    }

                    private MergeSource wrapMergeSource(MergeSource delegate) {
                        // Wraps the merge source to know which merges were pulled from Lucene by the IndexWriter
                        return new MergeSource() {
                            @Override
                            public MergePolicy.OneMerge getNextMerge() {
                                var merge = delegate.getNextMerge();
                                if (merge != null) {
                                    if (pendingMerges.add(merge) == false) {
                                        throw new AssertionError("Merge already pending " + merge);
                                    }
                                }
                                return merge;
                            }

                            @Override
                            public void onMergeFinished(MergePolicy.OneMerge merge) {
                                delegate.onMergeFinished(merge);
                            }

                            @Override
                            public boolean hasPendingMerges() {
                                return delegate.hasPendingMerges();
                            }

                            @Override
                            public void merge(MergePolicy.OneMerge merge) throws IOException {
                                runningMergesCount.incrementAndGet();
                                if (pendingMerges.remove(merge) == false) {
                                    throw new AssertionError("Pending merge not found " + merge);
                                }
                                delegate.merge(merge);
                            }
                        };
                    }

                    @Override
                    protected void handleMergeException(final Throwable exc) {
                        mergeException(exc);
                    }
                };
            }
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (isDataNode) {
                indexModule.addIndexEventListener(new IndexEventListener() {
                    @Override
                    public void onStoreClosed(ShardId shardId) {
                        shardStoreClosedListener.onResponse(null);
                    }
                });
            }
        }

        public void registerMergeEventListener(MergeEventListener listener) {
            var threadPoolMergeExecutorService = Objects.requireNonNull(threadPoolMergeExecutorServiceReference.get());
            threadPoolMergeExecutorService.registerMergeEventListener(listener);
        }
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestPlugin.class);
    }

    public void testFailedMergeDeadlock() throws Exception {
        internalCluster().startMasterOnlyNode();
        final int maxMergeThreads = randomIntBetween(1, 3);
        final int indexMaxThreadCount = randomBoolean() ? randomIntBetween(1, 10) : Integer.MAX_VALUE;

        final var dataNode = internalCluster().startDataOnlyNode(
            Settings.builder()
                .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
                .put("thread_pool." + ThreadPool.Names.MERGE + ".max", maxMergeThreads)
                .build()
        );

        final var plugin = getTestPlugin(dataNode);
        assertThat(plugin, notNullValue());

        final var indexName = randomIdentifier();
        createIndex(
            indexName,
            indexSettings(1, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".name", dataNode)
                .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                // when indexMaxThreadCount is small so merge tasks might be backlogged
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), indexMaxThreadCount)
                // no merge throttling
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), Integer.MAX_VALUE)
                .build()
        );

        final var mergesListener = new AssertingMergeEventListener();
        plugin.registerMergeEventListener(mergesListener);

        // Kick off enough merges to block the thread pool
        var maxRunningThreads = Math.min(maxMergeThreads, indexMaxThreadCount);
        indexDocsInManySegmentsUntil(indexName, () -> plugin.runningMergesCount.get() == maxRunningThreads);
        assertThat(plugin.runningMergesCount.get(), equalTo(maxRunningThreads));

        // Now pull more merges so they are queued in the merge thread pool, but Lucene thinks they are running
        final int pendingMerges = plugin.pendingMerges.size() + randomIntBetween(1, 5);
        indexDocsInManySegmentsUntil(indexName, () -> plugin.pendingMerges.size() > pendingMerges);

        var mergeThreadPool = asInstanceOf(
            ThreadPoolExecutor.class,
            internalCluster().clusterService(dataNode).threadPool().executor(ThreadPool.Names.MERGE)
        );
        assertThat(mergeThreadPool.getActiveCount(), greaterThanOrEqualTo(maxRunningThreads));

        // More merges in the hope to have backlogged merges
        if (indexMaxThreadCount != Integer.MAX_VALUE) {
            final int backloggedMerges = plugin.pendingMerges.size() + randomIntBetween(1, 5);
            indexDocsInManySegmentsUntil(indexName, () -> plugin.pendingMerges.size() > backloggedMerges);
        }

        // Sometime closes the shard concurrently with the tragic failure
        Thread closingThread = null;
        if (rarely()) {
            closingThread = new Thread(() -> {
                safeAwait(plugin.runMerges, TimeValue.ONE_MINUTE);
                client().admin().indices().prepareClose(indexName).get();
            });
            closingThread.start();
        }

        // unblock merges, one merge will fail the IndexWriter
        plugin.runMerges.countDown();

        // Deadlock sample:
        //
        // "elasticsearch[node_s5][merge][T#1]@16690" tid=0x8e nid=NA waiting
        // java.lang.Thread.State: WAITING
        // at java.lang.Object.wait0(Object.java:-1)
        // at java.lang.Object.wait(Object.java:389)
        // at org.apache.lucene.index.IndexWriter.doWait(IndexWriter.java:5531)
        // at org.apache.lucene.index.IndexWriter.abortMerges(IndexWriter.java:2733)
        // at org.apache.lucene.index.IndexWriter.rollbackInternalNoCommi(IndexWriter.java:2488)
        // at org.apache.lucene.index.IndexWriter.rollbackInternal(IndexWriter.java:2457)
        // - locked <0x429a> (a java.lang.Object)
        // at org.apache.lucene.index.IndexWriter.maybeCloseOnTragicEvent(IndexWriter.java:5765)
        // at org.apache.lucene.index.IndexWriter.tragicEvent(IndexWriter.java:5755)
        // at org.apache.lucene.index.IndexWriter.merge(IndexWriter.java:4780)
        // at org.apache.lucene.index.IndexWriter$IndexWriterMergeSource.merge(IndexWriter.java:6567)
        // at org.elasticsearch.index.engine.MergeWithFailureIT$TestPlugin$TestEngine$1$1.merge(MergeWithFailureIT.java:178)
        // at org.elasticsearch.index.engine.ThreadPoolMergeScheduler.doMerge(ThreadPoolMergeScheduler.java:347)
        // at org.elasticsearch.index.engine.ThreadPoolMergeScheduler$MergeTask.run(ThreadPoolMergeScheduler.java:459)
        // at org.elasticsearch.index.engine.ThreadPoolMergeExecutorService.runMergeTask(ThreadPoolMergeExecutorService.java:364)

        ensureRed(indexName);

        // verify that the shard store is effectively closed
        safeGet(plugin.shardStoreClosedListener);

        if (closingThread != null) {
            closingThread.join();
        }

        final var shardId = new ShardId(resolveIndex(indexName), 0);
        var nodeEnvironment = internalCluster().getInstance(NodeEnvironment.class, dataNode);
        try {
            var shardLock = nodeEnvironment.shardLock(shardId, getTestName(), 10_000L);
            shardLock.close();
        } catch (ShardLockObtainFailedException ex) {
            throw new AssertionError("Shard " + shardId + " is still locked after 10 seconds", ex);
        }

        // check the state of the shard
        var routingTable = internalCluster().clusterService(dataNode).state().routingTable();
        var indexRoutingTable = routingTable.index(shardId.getIndex());
        var primary = asInstanceOf(IndexShardRoutingTable.class, indexRoutingTable.shard(shardId.id())).primaryShard();
        assertThat(primary.state(), equalTo(ShardRoutingState.UNASSIGNED));
        assertThat(primary.unassignedInfo(), notNullValue());
        assertThat(primary.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
        var failure = ExceptionsHelper.unwrap(primary.unassignedInfo().failure(), IOException.class);
        assertThat(failure, notNullValue());
        assertThat(failure.getMessage(), containsString(FAILING_MERGE_ON_PURPOSE));

        // verify the number of queued, completed and aborted merges
        mergesListener.verify();

        assertAcked(indicesAdmin().prepareDelete(indexName));
    }

    private void indexDocsInManySegmentsUntil(String indexName, Supplier<Boolean> stopCondition) {
        indexDocsInManySegmentsUntil(indexName, stopCondition, TimeValue.THIRTY_SECONDS);
    }

    private void indexDocsInManySegmentsUntil(String indexName, Supplier<Boolean> stopCondition, TimeValue timeout) {
        long millisWaited = 0L;
        do {
            if (millisWaited >= timeout.millis()) {
                logger.warn(format("timed out after waiting for [%d]", millisWaited));
                return;
            }
            var client = client();
            for (int request = 0; request < 10; request++) {
                var bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                for (int doc = 0; doc < 10; doc++) {
                    bulkRequest.add(client.prepareIndex(indexName).setCreate(true).setSource("value", randomIntBetween(0, 1024)));
                }
                bulkRequest.get();
            }
            // Sleep a bit to wait for merges to kick in
            long sleepInMillis = randomLongBetween(50L, 200L);
            safeSleep(sleepInMillis);
            millisWaited += sleepInMillis;
        } while (stopCondition.get() == false);
    }

    private static TestPlugin getTestPlugin(String dataNode) {
        return internalCluster().getInstance(PluginsService.class, dataNode).filterPlugins(TestPlugin.class).findFirst().get();
    }

    private static void ensureRed(String indexName) throws Exception {
        assertBusy(() -> {
            var healthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForStatus(ClusterHealthStatus.RED)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        });
    }

    private static class AssertingMergeEventListener implements MergeEventListener {

        private final AtomicInteger mergesQueued = new AtomicInteger();
        private final AtomicInteger mergesCompleted = new AtomicInteger();
        private final AtomicInteger mergesAborted = new AtomicInteger();

        @Override
        public void onMergeQueued(OnGoingMerge merge, long estimateMergeMemoryBytes) {
            mergesQueued.incrementAndGet();
        }

        @Override
        public void onMergeCompleted(OnGoingMerge merge) {
            mergesCompleted.incrementAndGet();
        }

        @Override
        public void onMergeAborted(OnGoingMerge merge) {
            mergesAborted.incrementAndGet();
        }

        private void verify() {
            int queued = mergesQueued.get();
            int completed = mergesCompleted.get();
            int aborted = mergesAborted.get();
            var error = format("Queued merges mismatch (queued=%d, completed=%d, aborted=%d)", queued, completed, aborted);
            assertThat(error, queued, equalTo(completed + aborted));
        }
    }
}
