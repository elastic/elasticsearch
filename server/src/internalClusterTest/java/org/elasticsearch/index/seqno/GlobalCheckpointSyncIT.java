/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.apache.lucene.store.AlreadyClosedException;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.IndicesService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalSettingsPlugin;
import org.elasticsearch.test.transport.MockTransportService;
import org.elasticsearch.transport.TransportService;
import org.elasticsearch.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;

public class GlobalCheckpointSyncIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Stream.concat(super.nodePlugins().stream(), Stream.of(InternalSettingsPlugin.class, MockTransportService.TestPlugin.class))
            .toList();
    }

    public void testGlobalCheckpointSyncWithAsyncDurability() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        prepareCreate(
            "test",
            Settings.builder()
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), "1s")
                .put("index.number_of_replicas", 1)
        ).get();

        for (int j = 0; j < 10; j++) {
            final String id = Integer.toString(j);
            client().prepareIndex("test").setId(id).setSource("{\"foo\": " + id + "}", XContentType.JSON).get();
        }

        assertBusy(() -> {
            SeqNoStats seqNoStats = indicesAdmin().prepareStats("test").get().getIndex("test").getShards()[0].getSeqNoStats();
            assertThat(seqNoStats.getGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
        });
    }

    public void testPostOperationGlobalCheckpointSync() throws Exception {
        // set the sync interval high so it does not execute during this test. This only allows the global checkpoint to catch up
        // on a post-operation background sync if translog durability is set to sync. Async durability relies on a scheduled global
        // checkpoint sync to allow the information about persisted local checkpoints to be transferred to the primary.
        runGlobalCheckpointSyncTest(
            TimeValue.timeValueHours(24),
            client -> client.admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST))
                .get(),
            client -> {}
        );
    }

    /*
     * This test swallows the post-operation global checkpoint syncs, and then restores the ability to send these requests at the end of the
     * test so that a background sync can fire and sync the global checkpoint.
     */
    public void testBackgroundGlobalCheckpointSync() throws Exception {
        runGlobalCheckpointSyncTest(TimeValue.timeValueSeconds(randomIntBetween(1, 3)), client -> {
            // prevent global checkpoint syncs between all nodes
            final DiscoveryNodes nodes = client.admin().cluster().prepareState().get().getState().getNodes();
            for (final DiscoveryNode node : nodes) {
                for (final DiscoveryNode other : nodes) {
                    if (node == other) {
                        continue;
                    }
                    final MockTransportService senderTransportService = (MockTransportService) internalCluster().getInstance(
                        TransportService.class,
                        node.getName()
                    );
                    final MockTransportService receiverTransportService = (MockTransportService) internalCluster().getInstance(
                        TransportService.class,
                        other.getName()
                    );
                    senderTransportService.addSendBehavior(receiverTransportService, (connection, requestId, action, request, options) -> {
                        if ("indices:admin/seq_no/global_checkpoint_sync[r]".equals(action)) {
                            throw new IllegalStateException("blocking indices:admin/seq_no/global_checkpoint_sync[r]");
                        } else {
                            connection.sendRequest(requestId, action, request, options);
                        }
                    });
                }
            }
        }, client -> {
            // restore global checkpoint syncs between all nodes
            final DiscoveryNodes nodes = client.admin().cluster().prepareState().get().getState().getNodes();
            for (final DiscoveryNode node : nodes) {
                for (final DiscoveryNode other : nodes) {
                    if (node == other) {
                        continue;
                    }
                    final MockTransportService senderTransportService = (MockTransportService) internalCluster().getInstance(
                        TransportService.class,
                        node.getName()
                    );
                    final MockTransportService receiverTransportService = (MockTransportService) internalCluster().getInstance(
                        TransportService.class,
                        other.getName()
                    );
                    senderTransportService.clearOutboundRules(receiverTransportService);
                }
            }
        });
    }

    private void runGlobalCheckpointSyncTest(
        final TimeValue globalCheckpointSyncInterval,
        final Consumer<Client> beforeIndexing,
        final Consumer<Client> afterIndexing
    ) throws Exception {
        final int numberOfReplicas = randomIntBetween(1, 4);
        internalCluster().ensureAtLeastNumDataNodes(1 + numberOfReplicas);
        prepareCreate(
            "test",
            Settings.builder()
                .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), globalCheckpointSyncInterval)
                .put("index.number_of_replicas", numberOfReplicas)
        ).get();
        if (randomBoolean()) {
            ensureGreen();
        }

        beforeIndexing.accept(client());

        final int numberOfDocuments = randomIntBetween(0, 256);

        final int numberOfThreads = randomIntBetween(1, 4);
        final CyclicBarrier barrier = new CyclicBarrier(1 + numberOfThreads);

        // start concurrent indexing threads
        final List<Thread> threads = new ArrayList<>(numberOfThreads);
        for (int i = 0; i < numberOfThreads; i++) {
            final int index = i;
            final Thread thread = new Thread(() -> {
                try {
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
                for (int j = 0; j < numberOfDocuments; j++) {
                    final String id = Integer.toString(index * numberOfDocuments + j);
                    client().prepareIndex("test").setId(id).setSource("{\"foo\": " + id + "}", XContentType.JSON).get();
                }
                try {
                    barrier.await();
                } catch (BrokenBarrierException | InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
            threads.add(thread);
            thread.start();
        }

        // synchronize the start of the threads
        barrier.await();

        // wait for the threads to finish
        barrier.await();

        afterIndexing.accept(client());

        assertBusy(() -> {
            for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
                for (IndexService indexService : indicesService) {
                    for (IndexShard shard : indexService) {
                        if (shard.routingEntry().primary()) {
                            try {
                                final SeqNoStats seqNoStats = shard.seqNoStats();
                                assertThat(
                                    "shard " + shard.routingEntry() + " seq_no [" + seqNoStats + "]",
                                    seqNoStats.getGlobalCheckpoint(),
                                    equalTo(seqNoStats.getMaxSeqNo())
                                );
                            } catch (AlreadyClosedException e) {
                                logger.error(
                                    "received unexpected AlreadyClosedException when fetching stats for shard: {}, shard state: {}",
                                    shard.shardId(),
                                    shard.state()
                                );
                                throw e;
                            }
                        }
                    }
                }
            }
        }, 60, TimeUnit.SECONDS);
        ensureGreen("test");
        for (final Thread thread : threads) {
            thread.join();
        }
    }

    public void testPersistGlobalCheckpoint() throws Exception {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Settings.Builder indexSettings = Settings.builder()
            .put(IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(100, 1000, "ms"))
            .put("index.number_of_replicas", randomIntBetween(0, 1));
        if (randomBoolean()) {
            indexSettings.put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.ASYNC)
                .put(IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(), randomTimeValue(100, 1000, "ms"));
        }
        prepareCreate("test", indexSettings).get();
        if (randomBoolean()) {
            ensureGreen("test");
        }
        int numDocs = randomIntBetween(1, 20);
        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get();
        }
        ensureGreen("test");
        assertBusy(() -> {
            for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
                for (IndexService indexService : indicesService) {
                    for (IndexShard shard : indexService) {
                        final SeqNoStats seqNoStats = shard.seqNoStats();
                        assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
                        assertThat(shard.getLastKnownGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
                        assertThat(shard.getLastSyncedGlobalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
                    }
                }
            }
        });
    }

    public void testPersistLocalCheckpoint() {
        internalCluster().ensureAtLeastNumDataNodes(2);
        Settings.Builder indexSettings = indexSettings(1, randomIntBetween(0, 1)).put(
            IndexService.GLOBAL_CHECKPOINT_SYNC_INTERVAL_SETTING.getKey(),
            "10m"
        ).put(IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(), Translog.Durability.REQUEST);
        prepareCreate("test", indexSettings).get();
        ensureGreen("test");
        int numDocs = randomIntBetween(1, 20);
        logger.info("numDocs {}", numDocs);
        long maxSeqNo = 0;
        for (int i = 0; i < numDocs; i++) {
            maxSeqNo = client().prepareIndex("test").setId(Integer.toString(i)).setSource("{}", XContentType.JSON).get().getSeqNo();
            logger.info("got {}", maxSeqNo);
        }
        for (IndicesService indicesService : internalCluster().getDataNodeInstances(IndicesService.class)) {
            for (IndexService indexService : indicesService) {
                for (IndexShard shard : indexService) {
                    final SeqNoStats seqNoStats = shard.seqNoStats();
                    assertThat(maxSeqNo, equalTo(seqNoStats.getMaxSeqNo()));
                    assertThat(seqNoStats.getLocalCheckpoint(), equalTo(seqNoStats.getMaxSeqNo()));
                    ;
                }
            }
        }
    }
}
