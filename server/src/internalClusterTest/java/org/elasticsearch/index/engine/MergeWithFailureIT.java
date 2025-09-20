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
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.OneMergeWrappingMergePolicy;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.RefCountingListener;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterStateListener;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.UnassignedInfo;
import org.elasticsearch.cluster.routing.allocation.decider.MaxRetryAllocationDecider;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.Priority;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexModule;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.MergeSchedulerConfig;
import org.elasticsearch.index.codec.FilterDocValuesProducer;
import org.elasticsearch.index.shard.AbstractIndexShardComponent;
import org.elasticsearch.index.shard.IndexEventListener;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Test a specific deadlock situation encountered when a merge throws an exception which closes the
 * {@link org.apache.lucene.index.IndexWriter}, which in turn closes the {@link ThreadPoolMergeScheduler} which would wait for all merges
 * to be aborted/completed in a manner that would not work if the current thread is one of the merges to be aborted. As a consequence, the
 *  {@link ThreadPoolMergeScheduler} is blocked indefinitely and the shard's store remains opened.
 *
 * This test creates one or more indices with one or more shards, then selects some of those shards and ensures each selected shard throws
 * an exception during a merge. It finally checks that the shard indeed failed and that their stores were closed correctly.
 */
public class MergeWithFailureIT extends ESIntegTestCase {

    private static final String FAILING_MERGE_ON_PURPOSE = "Failing merge on purpose";

    /**
     * Test plugin that allows to fail the merges of specific shard instances
     */
    public static class TestMergeFailurePlugin extends Plugin implements EnginePlugin {

        record TestMergeInstance(
            ActionListener<Void> readyToFailMergeListener,
            AtomicLong minMergesBeforeFailure,
            ActionListener<Void> onStoreClosedListener
        ) {
            private boolean shouldFailMerge() {
                if (minMergesBeforeFailure.getAndDecrement() == 0) {
                    readyToFailMergeListener.onResponse(null);
                    return true;
                }
                return false;
            }
        }

        // This future is completed once all the shards that are expected to fail are ready to execute the failing merge
        private final PlainActionFuture<Void> allMergesReadyToFailListener = new PlainActionFuture<>();

        // This future is completed once all the shards that are expected to fail have their store closed
        private final PlainActionFuture<Void> allShardsStoresClosedListener = new PlainActionFuture<>();

        // Map of shards that are expected to fail in the test
        private final Map<ShardId, TestMergeInstance> shardsToFail = new HashMap<>();

        // Latch to fail the merges
        private final CountDownLatch failMerges = new CountDownLatch(1);

        private final boolean isDataNode;

        public TestMergeFailurePlugin(Settings settings) {
            this.isDataNode = DiscoveryNode.hasDataRole(settings);
        }

        private synchronized TestMergeInstance getTestMergeInstance(ShardId shardId) {
            return shardsToFail.get(shardId);
        }

        private synchronized void failMergesForShards(Set<ShardId> shards, long minMergesBeforeFailure) {
            try (
                var ready = new RefCountingListener(allMergesReadyToFailListener);
                var close = new RefCountingListener(allShardsStoresClosedListener)
            ) {
                shards.forEach(shardId -> {
                    var instance = new TestMergeInstance(ready.acquire(), new AtomicLong(minMergesBeforeFailure), close.acquire());
                    if (shardsToFail.put(shardId, instance) != null) {
                        throw new AssertionError("Shard already registered for merge failure: " + shardId);
                    }
                });
            }
        }

        private void waitForMergesReadyToFail() {
            safeGet(this.allMergesReadyToFailListener);
        }

        private void failMerges() {
            assert this.allMergesReadyToFailListener.isDone();
            failMerges.countDown();
        }

        private void waitForClose() {
            safeGet(this.allShardsStoresClosedListener);
        }

        private void onShardStoreClosed(ShardId shardId) {
            var candidate = getTestMergeInstance(shardId);
            if (candidate != null) {
                candidate.onStoreClosedListener().onResponse(null);
            }
        }

        @Override
        public void onIndexModule(IndexModule indexModule) {
            if (isDataNode) {
                indexModule.addIndexEventListener(new IndexEventListener() {
                    @Override
                    public void onStoreClosed(ShardId shardId) {
                        onShardStoreClosed(shardId);
                    }
                });
            }
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (isDataNode == false) {
                return Optional.of(InternalEngine::new);
            }
            return Optional.of(
                config -> new InternalEngine(
                    EngineTestCase.copy(
                        config,
                        new OneMergeWrappingMergePolicy(config.getMergePolicy(), toWrap -> new MergePolicy.OneMerge(toWrap) {
                            @Override
                            public CodecReader wrapForMerge(CodecReader reader) {
                                var candidate = getTestMergeInstance(config.getShardId());
                                if (candidate == null || candidate.shouldFailMerge() == false) {
                                    return reader;
                                }
                                return new FilterCodecReader(reader) {
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

                                            final AtomicBoolean failOnce = new AtomicBoolean(false);

                                            @Override
                                            public NumericDocValues getNumeric(FieldInfo field) throws IOException {
                                                if (failOnce.compareAndSet(false, true)) {
                                                    safeAwait(failMerges);
                                                    throw new IOException(FAILING_MERGE_ON_PURPOSE);
                                                }
                                                return super.getNumeric(field);
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
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return CollectionUtils.appendToCopy(super.nodePlugins(), TestMergeFailurePlugin.class);
    }

    public void testFailedMergeDoesNotPreventShardFromClosing() throws Exception {
        internalCluster().startMasterOnlyNode();
        final var dataNode = internalCluster().startDataOnlyNode(
            // test works with concurrent and pooled merge scheduler
            Settings.builder().put(ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.getKey(), randomBoolean()).build()
        );

        final int nbPrimariesPerIndex = randomIntBetween(1, 3);

        final var indices = new String[randomIntBetween(1, 2)];
        for (int i = 0; i < indices.length; i++) {
            var indexName = randomIdentifier();
            createIndex(
                indexName,
                indexSettings(nbPrimariesPerIndex, 0).put(IndexMetadata.INDEX_ROUTING_INCLUDE_GROUP_PREFIX + ".name", dataNode)
                    .put(MaxRetryAllocationDecider.SETTING_ALLOCATION_MAX_RETRY.getKey(), 1)
                    .build()
            );
            indices[i] = indexName;
        }

        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        ensureGreen(indices);

        // randomly choose an index
        final var indexToFail = resolveIndex(randomFrom(indices));

        // randomly choose one or more of its shards where the plugin will fail merges
        final var randomShardsIdsToFail = randomSubsetOf(
            // expect failing merges to be executed concurrently as the test waits for them to be "ready to fail"
            randomIntBetween(1, Math.min(nbPrimariesPerIndex, maxConcurrentMerges(dataNode, indexToFail))),
            IntStream.range(0, nbPrimariesPerIndex).boxed().toList()
        );

        // capture the IndexShard instances, this will be useful to ensure the shards are closed
        var indexService = internalCluster().getInstance(IndicesService.class, dataNode).indexService(indexToFail);
        final var shardsToFail = randomShardsIdsToFail.stream().map(shardId -> {
            var indexShard = indexService.getShardOrNull(shardId);
            assertThat(indexShard, notNullValue());
            return indexShard;
        }).filter(Objects::nonNull).collect(Collectors.toUnmodifiableMap(AbstractIndexShardComponent::shardId, Function.identity()));

        logger.debug("--> merges of the following shards will fail: {}", shardsToFail.keySet());

        // sometimes allow some merges to run before triggering the merge failure
        final long minMergesPerShardBeforeFailure = randomBoolean() ? randomLongBetween(1, 3) : 0L;

        // a future completed when all expected shards are unassigned due to merge failures
        final var waitForShardsFailuresListener = new PlainActionFuture<Void>();

        // the cluster state listener that completes the previous future
        var allExpectedShardsAreUnassignedListener = new ClusterStateListener() {
            @Override
            public void clusterChanged(ClusterChangedEvent event) {
                if (event.routingTableChanged()) {
                    var allExpectedShardsAreUnassigned = shardsToFail.keySet().stream().allMatch(shardId -> {
                        var routingTable = event.state().routingTable(ProjectId.DEFAULT).index(shardId.getIndex());
                        return routingTable != null
                            && routingTable.shard(shardId.id()).primaryShard().unassigned()
                            && routingTable.shard(shardId.id()).primaryShard().unassignedInfo().failure() != null;
                    });
                    if (allExpectedShardsAreUnassigned) {
                        clusterService.removeListener(this);
                        waitForShardsFailuresListener.onResponse(null);
                    }
                }
            }
        };

        // now instruct on which shards the merges must fail
        var plugin = getTestMergeFailurePlugin(dataNode);
        plugin.failMergesForShards(shardsToFail.keySet(), minMergesPerShardBeforeFailure);

        // add the cluster state listener
        clusterService.addListener(allExpectedShardsAreUnassignedListener);

        // create many new segments on every index, enough to trigger merges on every shard
        final var createSegments = new AtomicBoolean(true);
        final var createSegmentsThread = new Thread(() -> {
            while (createSegments.get()) {
                var client = client();
                for (int request = 0; request < 10; request++) {
                    var bulkRequest = client.prepareBulk().setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
                    for (var index : indices) {
                        for (int doc = 0; doc < 10; doc++) {
                            bulkRequest.add(client.prepareIndex(index).setCreate(true).setSource("value", randomIntBetween(0, 1024)));
                        }
                    }
                    bulkRequest.get();
                }
                safeSleep(randomLongBetween(50, 200L));
            }
        });
        createSegmentsThread.start();

        // wait for the merges to start on the shards that are expected to fail
        plugin.waitForMergesReadyToFail();

        // no need to create more segments
        createSegments.set(false);
        createSegmentsThread.join();

        // allow merges to fail
        plugin.failMerges();

        // wait for the expected shards to be failed and unassigned
        safeGet(waitForShardsFailuresListener);
        ensureRed(indexToFail.getName());

        // waits for the expected shards stores to be closed
        plugin.waitForClose();

        // check the state of every shard
        var routingTable = clusterService.state().routingTable(ProjectId.DEFAULT);
        for (var index : indices) {
            var indexRoutingTable = routingTable.index(index);
            for (int shard = 0; shard < nbPrimariesPerIndex; shard++) {
                var primary = asInstanceOf(IndexShardRoutingTable.class, indexRoutingTable.shard(shard)).primaryShard();

                if (shardsToFail.containsKey(primary.shardId())) {
                    assertThat(primary.state(), equalTo(ShardRoutingState.UNASSIGNED));
                    assertThat(primary.unassignedInfo(), notNullValue());
                    assertThat(primary.unassignedInfo().reason(), equalTo(UnassignedInfo.Reason.ALLOCATION_FAILED));
                    var failure = ExceptionsHelper.unwrap(primary.unassignedInfo().failure(), IOException.class);
                    assertThat(failure, notNullValue());
                    assertThat(failure.getMessage(), containsString(FAILING_MERGE_ON_PURPOSE));
                    continue;
                }

                assertThat(primary.state(), equalTo(ShardRoutingState.STARTED));
            }
        }
    }

    private static TestMergeFailurePlugin getTestMergeFailurePlugin(String dataNode) {
        return internalCluster().getInstance(PluginsService.class, dataNode).filterPlugins(TestMergeFailurePlugin.class).findFirst().get();
    }

    private void ensureRed(String indexName) throws Exception {
        assertBusy(() -> {
            var healthResponse = clusterAdmin().prepareHealth(TEST_REQUEST_TIMEOUT, indexName)
                .setWaitForStatus(ClusterHealthStatus.RED)
                .setWaitForEvents(Priority.LANGUID)
                .get();
            assertThat(healthResponse.getStatus(), equalTo(ClusterHealthStatus.RED));
        });
    }

    private static int maxConcurrentMerges(String dataNode, Index index) {
        var clusterService = internalCluster().clusterService(dataNode);
        var indexMetadata = clusterService.state().metadata().indexMetadata(index);
        int maxMerges = MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.get(indexMetadata.getSettings());
        if (ThreadPoolMergeScheduler.USE_THREAD_POOL_MERGE_SCHEDULER_SETTING.get(clusterService.getSettings())) {
            return Math.min(maxMerges, clusterService.threadPool().info(ThreadPool.Names.MERGE).getMax());
        }
        return maxMerges;
    }
}
