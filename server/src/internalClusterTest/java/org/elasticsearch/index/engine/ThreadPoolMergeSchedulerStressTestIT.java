/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import org.apache.lucene.index.MergePolicy.OneMerge;
import org.apache.lucene.index.MergeScheduler;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.store.Directory;
import org.elasticsearch.action.admin.indices.segments.IndexShardSegments;
import org.elasticsearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.elasticsearch.action.admin.indices.segments.ShardSegments;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.ConcurrentCollections;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergePolicyConfig;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.merge.MergeStats;
import org.elasticsearch.index.merge.OnGoingMerge;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAllSuccessful;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ThreadPoolMergeSchedulerStressTestIT extends ESSingleNodeTestCase {

    private static final int MERGE_SCHEDULER_MAX_CONCURRENCY = 3;

    @Override
    protected Settings nodeSettings() {
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            // when there are more threads than scheduler(s)' concurrency capacity, excess merges will be backlogged
            // alternatively, when scheduler(s)' concurrency capacity exceeds the executor's thread count, excess merges will be enqueued
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), MERGE_SCHEDULER_MAX_CONCURRENCY + randomFrom(-2, -1, 0, 1, 2))
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return CollectionUtils.appendToCopy(super.getPlugins(), ThreadPoolMergeSchedulerStressTestIT.TestEnginePlugin.class);
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {

        final AtomicReference<ThreadPoolMergeExecutorService> mergeExecutorServiceReference = new AtomicReference<>();
        final Set<OneMerge> enqueuedMergesSet = ConcurrentCollections.newConcurrentSet();
        final Set<OneMerge> runningMergesSet = ConcurrentCollections.newConcurrentSet();
        // maybe let a few merges run at the start
        final int initialRunMergesCount = randomIntBetween(0, 5);
        final Semaphore runMergeSemaphore = new Semaphore(initialRunMergesCount);
        final int waitMergesEnqueuedCount = randomIntBetween(50, 100);

        class TestInternalEngine extends org.elasticsearch.index.engine.InternalEngine {

            TestInternalEngine(EngineConfig engineConfig) {
                super(engineConfig);
            }

            protected ElasticsearchMergeScheduler createMergeScheduler(
                ShardId shardId,
                IndexSettings indexSettings,
                @Nullable ThreadPoolMergeExecutorService threadPoolMergeExecutorService,
                MergeMetrics mergeMetrics) {
                ElasticsearchMergeScheduler mergeScheduler = super.createMergeScheduler(
                    shardId,
                    indexSettings,
                    threadPoolMergeExecutorService,
                    mergeMetrics);
                assertThat(mergeScheduler, instanceOf(ThreadPoolMergeScheduler.class));
                // assert there is a single merge executor service for all shards
                mergeExecutorServiceReference.compareAndSet(null, threadPoolMergeExecutorService);
                assertThat(mergeExecutorServiceReference.get(), is(threadPoolMergeExecutorService));
                return new TestMergeScheduler((ThreadPoolMergeScheduler) mergeScheduler);
            }

            class TestMergeScheduler implements ElasticsearchMergeScheduler {

                ThreadPoolMergeScheduler delegateMergeScheduler;

                TestMergeScheduler(ThreadPoolMergeScheduler threadPoolMergeScheduler) {
                    this.delegateMergeScheduler = threadPoolMergeScheduler;
                }

                @Override
                public Set<OnGoingMerge> onGoingMerges() {
                    return delegateMergeScheduler.onGoingMerges();
                }

                @Override
                public MergeStats stats() {
                    return delegateMergeScheduler.stats();
                }

                @Override
                public void refreshConfig() {
                    delegateMergeScheduler.refreshConfig();
                }

                @Override
                public MergeScheduler getMergeScheduler() {
                    return new MergeScheduler() {
                        @Override
                        public void merge(MergeSource mergeSource, MergeTrigger trigger) {
                            delegateMergeScheduler.merge(new MergeSource() {
                                @Override
                                public OneMerge getNextMerge() {
                                    OneMerge nextMerge = mergeSource.getNextMerge();
                                    if (nextMerge != null) {
                                        assertTrue(TestEnginePlugin.this.enqueuedMergesSet.add(nextMerge));
                                        // avoid excess merges pilling up
                                        if (TestEnginePlugin.this.enqueuedMergesSet
                                            .size() > TestEnginePlugin.this.waitMergesEnqueuedCount) {
                                            runMergeSemaphore.release();
                                        }
                                    }
                                    return nextMerge;
                                }

                                @Override
                                public void onMergeFinished(OneMerge merge) {
                                    mergeSource.onMergeFinished(merge);
                                }

                                @Override
                                public boolean hasPendingMerges() {
                                    return mergeSource.hasPendingMerges();
                                }

                                @Override
                                public void merge(OneMerge merge) throws IOException {
                                    assertNotNull(merge);
                                    try {
                                        // most merges need to acquire the semaphore in order to run
                                        if (frequently()) {
                                            runMergeSemaphore.acquire();
                                        }
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                    // assert to-be-run merge was enqueued
                                    assertTrue(TestEnginePlugin.this.enqueuedMergesSet.remove(merge));
                                    TestEnginePlugin.this.runningMergesSet.add(merge);
                                    assertThat(
                                        TestEnginePlugin.this.runningMergesSet.size(),
                                        lessThanOrEqualTo(
                                            TestEnginePlugin.this.mergeExecutorServiceReference.get().getMaxConcurrentMerges()
                                        )
                                    );
                                    mergeSource.merge(merge);
                                    assertTrue(TestEnginePlugin.this.runningMergesSet.remove(merge));
                                }
                            }, trigger);
                        }

                        @Override
                        public Directory wrapForMerge(OneMerge merge, Directory in) {
                            return delegateMergeScheduler.wrapForMerge(merge, in);
                        }

                        @Override
                        public Executor getIntraMergeExecutor(OneMerge merge) {
                            return delegateMergeScheduler.getIntraMergeExecutor(merge);
                        }

                        @Override
                        public void close() throws IOException {
                            delegateMergeScheduler.close();
                        }
                    };
                }
            }
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(TestInternalEngine::new);
        }

    }

    public void testMergingFallsBehindAndThenCatchesUp() throws Exception {
        createIndex(
            "index",
            // stress test merging across multiple shards
            indexSettings(randomIntBetween(1, 10), 0)
                // few segments per merge ought to result in more merging activity
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_MAX_MERGE_AT_ONCE_SETTING.getKey(), randomIntBetween(2, 3))
                .put(MergePolicyConfig.INDEX_MERGE_POLICY_SEGMENTS_PER_TIER_SETTING.getKey(), randomIntBetween(2, 3))
                // few concurrent merges allowed per scheduler
                .put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), randomIntBetween(1, MERGE_SCHEDULER_MAX_CONCURRENCY))
                // many pending merges allowed, in order to disable indexing throttle
                .put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), randomIntBetween(1, Integer.MAX_VALUE))
                .build()
        );
        ensureGreen("index");
        // generate merging activity across many threads
        Thread[] indexingThreads = new Thread[randomIntBetween(20, 30)];
        AtomicBoolean indexingDone = new AtomicBoolean(false);
        for (int i = 0; i < indexingThreads.length; i++) {
            int finalI = i;
            indexingThreads[i] = new Thread(() -> {
                long termUpto = 0;
                while (indexingDone.get() == false) {
                    for (int j = 0; j < 100; j++) {
                        // Provoke slowish merging by making many unique terms:
                        StringBuilder sb = new StringBuilder();
                        for (int k = 0; k < 100; k++) {
                            sb.append(' ');
                            sb.append(termUpto++);
                        }
                        prepareIndex("index").setId("thread_" + finalI + "_term_" + termUpto)
                            .setSource("field" + (j % 10), sb.toString())
                            .get();
                        if (j % 2 == 0) {
                            indicesAdmin().prepareRefresh("index").get();
                        }
                    }
                    indicesAdmin().prepareRefresh("index").get();
                }
            });
            indexingThreads[i].start();
        }
        TestEnginePlugin testEnginePlugin = getTestEnginePlugin();
        assertBusy(() -> {
            // wait for merges to enqueue or backlog
            assertThat(testEnginePlugin.enqueuedMergesSet.size(), greaterThanOrEqualTo(testEnginePlugin.waitMergesEnqueuedCount));
        }, 1, TimeUnit.MINUTES);
        // finish up indexing
        indexingDone.set(true);
        for (Thread indexingThread : indexingThreads) {
            indexingThread.join();
        }
        // even when indexing is done, queued and backlogged merges can themselves trigger further merging
        // don't let this test be bothered by that, and simply unblock all merges
        // 100k is a fudge value, but there's no easy way to find a smartest one here
        testEnginePlugin.runMergeSemaphore.release(100_000);
        // await all merging to catch up
        assertBusy(() -> {
            assert testEnginePlugin.runMergeSemaphore.availablePermits() > 0 : "some merges are blocked, test is broken";
            assertThat(testEnginePlugin.runningMergesSet.size(), is(0));
            assertThat(testEnginePlugin.enqueuedMergesSet.size(), is(0));
            testEnginePlugin.mergeExecutorServiceReference.get().allDone();
        }, 1, TimeUnit.MINUTES);
        var segmentsCountAfterMergingCaughtUp = getSegmentsCountForAllShards("index");
        // force merge should be a noop after all available merging was done
        assertAllSuccessful(indicesAdmin().prepareForceMerge("index").get());
        var segmentsCountAfterForceMerge = getSegmentsCountForAllShards("index");
        assertThat(segmentsCountAfterForceMerge, is(segmentsCountAfterMergingCaughtUp));
        // let's also run a force-merge to 1 segment
        assertAllSuccessful(indicesAdmin().prepareForceMerge("index").setMaxNumSegments(1).get());
        assertAllSuccessful(indicesAdmin().prepareRefresh("index").get());
        // assert one segment per shard
        {
            IndicesSegmentResponse indicesSegmentResponse = indicesAdmin().prepareSegments("index").get();
            Iterator<IndexShardSegments> indexShardSegmentsIterator = indicesSegmentResponse.getIndices().get("index").iterator();
            while (indexShardSegmentsIterator.hasNext()) {
                for (ShardSegments segments : indexShardSegmentsIterator.next()) {
                    assertThat(segments.getSegments().size(), is(1));
                }
            }
        }
    }

    private int getSegmentsCountForAllShards(String indexName) {
        // refresh, otherwise we'd be still seeing the old merged-away segments
        assertAllSuccessful(indicesAdmin().prepareRefresh(indexName).get());
        int count = 0;
        IndicesSegmentResponse indicesSegmentResponse = indicesAdmin().prepareSegments(indexName).get();
        Iterator<IndexShardSegments> indexShardSegmentsIterator = indicesSegmentResponse.getIndices().get(indexName).iterator();
        while (indexShardSegmentsIterator.hasNext()) {
            for (ShardSegments segments : indexShardSegmentsIterator.next()) {
                count += segments.getSegments().size();
            }
        }
        return count;
    }

    private TestEnginePlugin getTestEnginePlugin() {
        return getInstanceFromNode(PluginsService.class).filterPlugins(TestEnginePlugin.class).toList().get(0);
    }
}
