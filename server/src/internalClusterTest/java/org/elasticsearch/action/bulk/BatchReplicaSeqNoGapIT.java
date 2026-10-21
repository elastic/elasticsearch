/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with one
 * of these licenses. You may obtain a copy of the licenses at
 * https://www.elastic.co/licensing/elastic-license
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.stats.IndicesStatsResponse;
import org.elasticsearch.action.admin.indices.stats.ShardStats;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.EngineFactory;
import org.elasticsearch.index.engine.InternalEngine;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.EnginePlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.PluginsService;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 2)
public class BatchReplicaSeqNoGapIT extends ESIntegTestCase {

    /**
     * Parks every seqNo reservation of a primary batch sub-batch until the test releases it, in arrival order.
     * The reservation happens inside {@link InternalEngine#indexBatch} with the sub-batch's uid locks held, so
     * a concurrent bulk that contains one of those ids is forced to split there, exactly like it would under
     * real contention.
     */
    public static class ParkingEnginePlugin extends Plugin implements EnginePlugin {

        private final BlockingQueue<CountDownLatch> parked = new LinkedBlockingQueue<>();
        private volatile boolean open = false;

        /** Waits for the next reservation to park and returns the latch that lets it continue. */
        CountDownLatch nextParked() throws InterruptedException {
            final CountDownLatch release = parked.poll(30, TimeUnit.SECONDS);
            assertNotNull("no seqNo reservation parked in time", release);
            return release;
        }

        /** Lets every parked and future reservation through; for cleanup, whatever state the test is in. */
        void releaseAll() {
            open = true;
            CountDownLatch release;
            while ((release = parked.poll()) != null) {
                release.countDown();
            }
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            return Optional.of(config -> new InternalEngine(config) {
                @Override
                protected long doGenerateSeqNos(int count) {
                    if (open == false) {
                        final CountDownLatch release = new CountDownLatch(1);
                        parked.add(release);
                        safeAwait(release, TimeValue.timeValueSeconds(60));
                    }
                    return super.doGenerateSeqNos(count);
                }
            });
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        // add the test plugin
        // InternalSettingsPlugin registers index.global_checkpoint_sync.interval
        return CollectionUtils.appendToCopy(
            CollectionUtils.appendToCopy(super.nodePlugins(), ParkingEnginePlugin.class),
            InternalSettingsPlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false; // only one EnginePlugin may provide an engine factory
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal, otherSettings))
            .put(BatchIndexingEnabled.BATCH_INDEXING.getKey(), true)
            .put(EsExecutors.NODE_PROCESSORS_SETTING.getKey(), Math.min(4, Runtime.getRuntime().availableProcessors()))
            .build();
    }

    public void testReplicaBatchSpanningSeqNoGap() throws Exception {
        assumeTrue(
            "the primary needs a write thread for each of the two concurrent bulks",
            Runtime.getRuntime().availableProcessors() >= 2
        );
        final String index = "columnar";
        assertAcked(
            indicesAdmin().prepareCreate(index)
                .setSettings(
                    indexSettings(1, 1).put(IndexSettings.MODE.getKey(), IndexMode.COLUMNAR.getName())
                        .put(IndexSettings.RECOVERY_USE_SYNTHETIC_SOURCE_SETTING.getKey(), true)
                        .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                )
                .setMapping("host", "type=keyword", "service", "type=keyword")
        );
        ensureGreen(index);

        final ClusterState state = clusterAdmin().prepareState(TEST_REQUEST_TIMEOUT).get().getState();
        final ShardRouting primaryRouting = state.routingTable().index(index).shard(0).primaryShard();
        final String primaryNode = state.nodes().get(primaryRouting.currentNodeId()).getName();
        final IndexShard primary = internalCluster().getInstance(IndicesService.class, primaryNode)
            .indexServiceSafe(resolveIndex(index))
            .getShard(0);
        final ParkingEnginePlugin plugin = internalCluster().getInstance(PluginsService.class, primaryNode)
            .filterPlugins(ParkingEnginePlugin.class)
            .findFirst()
            .orElseThrow();

        try {
            // 1. bulk X = [X] parks on the primary inside its sub-batch, holding X's uid lock, before reserving a seqNo
            final PlainActionFuture<BulkResponse> bulkX = new PlainActionFuture<>();
            client().bulk(new BulkRequest().add(doc(index, "x")), bulkX);

            // `doGenerateSeqNos` will put this latch and block the release of the uid lock.
            final CountDownLatch releaseX = plugin.nextParked();

            // 2. bulk A = [a, b, x, c, d]: the primary's first sub-batch stops at x, which X holds, so it is [a, b]; let it
            // reserve 0 and 1. The thread then blocks on X's lock.
            final PlainActionFuture<BulkResponse> bulkA = new PlainActionFuture<>();
            client().bulk(
                new BulkRequest().add(doc(index, "a"), doc(index, "b"), doc(index, "x"), doc(index, "c"), doc(index, "d")),
                bulkA
            );

            // Allow bulkMain to finish indexing
            plugin.nextParked().countDown();

            // Max seq no should be 1
            assertBusy(() -> assertThat(primary.seqNoStats().getMaxSeqNo(), equalTo(1L)));

            // 3. X takes seqNo 2 and completes on both copies; meanwhile A's second sub-batch [X, c, d] gets X's lock and parks
            releaseX.countDown();
            assertNoBulkFailures(bulkX.actionGet(TimeValue.timeValueSeconds(30)));

            // 4. A's second sub-batch takes 3, 4 and 5; the replica receives A as one chunk with seqNos [0, 1, 3, 4, 5]
            plugin.nextParked().countDown();
            assertNoBulkFailures(bulkA.actionGet(TimeValue.timeValueSeconds(30)));
        } finally {
            plugin.releaseAll(); // never leave a write thread parked in the engine, whatever failed above
        }

        // Check that both replica and primary are in sync
        assertBusy(() -> {
            for (ShardStats shard : shardStats(index)) {
                assertThat(shard.getShardRouting().toString(), shard.getSeqNoStats().getLocalCheckpoint(), equalTo(5L));
                assertThat(shard.getShardRouting().toString(), shard.getSeqNoStats().getGlobalCheckpoint(), equalTo(5L));
            }
        }, 60, TimeUnit.SECONDS);
        // the flush commits everything and the trim drops every generation the commit no longer needs, on both copies
        assertBusy(() -> {
            indicesAdmin().prepareFlush(index).setForce(true).get();
            for (ShardStats shard : shardStats(index)) {
                final String copy = shard.getShardRouting().toString();
                assertThat(copy, shard.getStats().getTranslog().getUncommittedOperations(), equalTo(0));
                assertThat(copy, shard.getStats().getTranslog().estimatedNumberOfOperations(), equalTo(0));
            }
        }, 60, TimeUnit.SECONDS);
    }

    private static ShardStats[] shardStats(String index) {
        final IndicesStatsResponse stats = indicesAdmin().prepareStats(index).clear().setTranslog(true).get();
        return stats.getShards();
    }

    private static IndexRequest doc(String index, String id) {
        return new IndexRequest(index).id(id)
            .opType(DocWriteRequest.OpType.INDEX)
            .source(XContentType.JSON, "host", "h-" + id, "service", "s-" + id);
    }

    private static void assertNoBulkFailures(BulkResponse response) {
        assertFalse(response.buildFailureMessage(), response.hasFailures());
    }
}
