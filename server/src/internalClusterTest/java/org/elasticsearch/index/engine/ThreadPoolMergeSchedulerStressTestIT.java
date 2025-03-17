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
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.junit.BeforeClass;

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
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ThreadPoolMergeSchedulerStressTestIT extends ESSingleNodeTestCase {

    private static final AtomicReference<ThreadPoolMergeExecutorService> MERGE_EXECUTOR_SERVICE_REFERENCE = new AtomicReference<>();
    private static final Set<OneMerge> ENQUEUED_MERGES_SET = ConcurrentCollections.newConcurrentSet();
    private static final Set<OneMerge> RUNNING_MERGES_SET = ConcurrentCollections.newConcurrentSet();
    private static int WAIT_MERGES_ENQUEUED_COUNT;
    private static Semaphore RUN_MERGE_SEMAPHORE;

    private static final int MERGE_SCHEDULER_MAX_CONCURRENCY = 3;
    private static int MERGE_EXECUTOR_THREAD_COUNT;

    @BeforeClass
    public static void beforeTests() {
        WAIT_MERGES_ENQUEUED_COUNT = randomIntBetween(50, 100);
        // maybe let a few merges run at the start
        RUN_MERGE_SEMAPHORE = new Semaphore(randomIntBetween(0, 5));
    }

    @Override
    protected Settings nodeSettings() {
        // when there are more threads than scheduler(s)' concurrency capacity, excess merges will be backlogged (by the merge schedulers)
        // alternatively, when scheduler(s)' concurrency capacity exceeds the executor's thread count, excess merges will be enqueued
        MERGE_EXECUTOR_THREAD_COUNT = MERGE_SCHEDULER_MAX_CONCURRENCY + randomFrom(-2, -1, 0, 1, 2);
        return Settings.builder()
            .put(super.nodeSettings())
            .put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), MERGE_EXECUTOR_THREAD_COUNT)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return CollectionUtils.appendToCopy(super.getPlugins(), ThreadPoolMergeSchedulerStressTestIT.TestEnginePlugin.class);
    }

    public static class TestEnginePlugin extends Plugin implements EnginePlugin {

        static class TestInternalEngine extends org.elasticsearch.index.engine.InternalEngine {
            TestInternalEngine(EngineConfig engineConfig) {
                super(engineConfig);
            }

            protected ElasticsearchMergeScheduler createMergeScheduler(
                ShardId shardId,
                IndexSettings indexSettings,
                @Nullable ThreadPoolMergeExecutorService threadPoolMergeExecutorService
            ) {
                ElasticsearchMergeScheduler mergeScheduler = super.createMergeScheduler(
                    shardId,
                    indexSettings,
                    threadPoolMergeExecutorService
                );
                assertThat(mergeScheduler, instanceOf(ThreadPoolMergeScheduler.class));
                // assert there is a single merge executor service for all shards
                MERGE_EXECUTOR_SERVICE_REFERENCE.compareAndSet(null, threadPoolMergeExecutorService);
                assertThat(MERGE_EXECUTOR_SERVICE_REFERENCE.get(), is(threadPoolMergeExecutorService));
                return new TestMergeScheduler((ThreadPoolMergeScheduler) mergeScheduler);
            }
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(TestInternalEngine::new);
        }

        static class TestMergeScheduler implements ElasticsearchMergeScheduler {

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
                    public void merge(MergeSource mergeSource, MergeTrigger trigger) throws IOException {
                        delegateMergeScheduler.merge(new MergeSource() {
                            @Override
                            public OneMerge getNextMerge() {
                                OneMerge nextMerge = mergeSource.getNextMerge();
                                if (nextMerge != null) {
                                    assertTrue(ENQUEUED_MERGES_SET.add(nextMerge));
                                    // avoid excess merges piling up
                                    if (ENQUEUED_MERGES_SET.size() > WAIT_MERGES_ENQUEUED_COUNT) {
                                        RUN_MERGE_SEMAPHORE.release();
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
                                        RUN_MERGE_SEMAPHORE.acquire();
                                    }
                                } catch (InterruptedException e) {
                                    throw new RuntimeException(e);
                                }
                                // assert to-be-run merge was enqueued
                                assertTrue(ENQUEUED_MERGES_SET.remove(merge));
                                RUNNING_MERGES_SET.add(merge);
                                assertThat(RUNNING_MERGES_SET.size(), lessThanOrEqualTo(MERGE_EXECUTOR_THREAD_COUNT));
                                mergeSource.merge(merge);
                                assertTrue(RUNNING_MERGES_SET.remove(merge));
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
        // wait for merges to enqueue or backlog
        assertBusy(() -> assertThat(ENQUEUED_MERGES_SET.size(), greaterThanOrEqualTo(WAIT_MERGES_ENQUEUED_COUNT)), 1, TimeUnit.MINUTES);
        // finish up indexing
        indexingDone.set(true);
        for (Thread indexingThread : indexingThreads) {
            indexingThread.join();
        }
        var segmentsBefore = getSegmentsCountForAllShards("index");
        // even though indexing is done, merging can itself trigger further merging
        // don't let this test be bothered by that, let any merging run un-hindered
        RUN_MERGE_SEMAPHORE.release(999999);
        // await all merging to catch up
        assertBusy(() -> {
            assertThat(RUNNING_MERGES_SET.size(), is(0));
            assertThat(ENQUEUED_MERGES_SET.size(), is(0));
            MERGE_EXECUTOR_SERVICE_REFERENCE.get().allDone();
        }, 1, TimeUnit.MINUTES);
        // refresh, otherwise we'd be still seeing the old merged-away segments
        assertAllSuccessful(indicesAdmin().prepareRefresh("index").get());
        var segmentsAfter = getSegmentsCountForAllShards("index");
        // there should be way fewer segments after merging completed
        assertThat(segmentsBefore, greaterThan(segmentsAfter));
        // let's also run a force-merge
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
}
