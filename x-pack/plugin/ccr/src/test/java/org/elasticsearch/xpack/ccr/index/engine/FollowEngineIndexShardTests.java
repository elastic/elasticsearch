/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.apache.lucene.store.IOContext;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.index.engine.EngineTestCase;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.shard.RestoreOnlyRepository;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.store.Store;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.elasticsearch.common.lucene.Lucene.cleanLuceneIndex;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class FollowEngineIndexShardTests extends IndexShardTestCase {

    public void testDoNotFillGaps() throws Exception {
        Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build();
        final IndexShard indexShard = newStartedShard(false, settings, new FollowingEngineFactory());

        long seqNo = -1;
        for (int i = 0; i < 8; i++) {
            final String id = Long.toString(i);
            SourceToParse sourceToParse = new SourceToParse(id, new BytesArray("{}"), XContentType.JSON);
            indexShard.applyIndexOperationOnReplica(
                ++seqNo,
                indexShard.getOperationPrimaryTerm(),
                1,
                IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
                false,
                sourceToParse
            );
        }
        long seqNoBeforeGap = seqNo;
        seqNo += 8;
        SourceToParse sourceToParse = new SourceToParse("9", new BytesArray("{}"), XContentType.JSON);
        indexShard.applyIndexOperationOnReplica(
            seqNo,
            indexShard.getOperationPrimaryTerm(),
            1,
            IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP,
            false,
            sourceToParse
        );

        // promote the replica to primary:
        final ShardRouting replicaRouting = indexShard.routingEntry();
        final ShardRouting primaryRouting = newShardRouting(
            replicaRouting.shardId(),
            replicaRouting.currentNodeId(),
            null,
            true,
            ShardRoutingState.STARTED,
            replicaRouting.allocationId()
        );
        indexShard.updateShardState(
            primaryRouting,
            indexShard.getOperationPrimaryTerm() + 1,
            (shard, listener) -> {},
            0L,
            Collections.singleton(primaryRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(primaryRouting.shardId()).addShard(primaryRouting).build()
        );

        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Releasable> actionListener = ActionListener.wrap(releasable -> {
            releasable.close();
            latch.countDown();
        }, e -> { assert false : "expected no exception, but got [" + e.getMessage() + "]"; });
        indexShard.acquirePrimaryOperationPermit(actionListener, ThreadPool.Names.GENERIC, "");
        latch.await();
        assertThat(indexShard.getLocalCheckpoint(), equalTo(seqNoBeforeGap));
        indexShard.refresh("test");
        assertThat(indexShard.docStats().getCount(), equalTo(9L));
        closeShards(indexShard);
    }

    public void testRestoreShard() throws IOException {
        final IndexShard source = newStartedShard(true, Settings.EMPTY);
        final Settings targetSettings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build();
        IndexShard target = newStartedShard(true, targetSettings, new FollowingEngineFactory());
        assertThat(IndexShardTestCase.getEngine(target), instanceOf(FollowingEngine.class));

        indexDoc(source, "_doc", "0");
        EngineTestCase.generateNewSeqNo(IndexShardTestCase.getEngine(source));
        indexDoc(source, "_doc", "2");
        if (randomBoolean()) {
            source.refresh("test");
        }
        flushShard(source); // only flush source
        ShardRouting routing = ShardRoutingHelper.initWithSameId(
            target.routingEntry(),
            RecoverySource.ExistingStoreRecoverySource.INSTANCE
        );
        final Snapshot snapshot = new Snapshot("foo", new SnapshotId("bar", UUIDs.randomBase64UUID()));
        routing = ShardRoutingHelper.newWithRestoreSource(
            routing,
            new RecoverySource.SnapshotRecoverySource(
                UUIDs.randomBase64UUID(),
                snapshot,
                Version.CURRENT,
                new IndexId("test", UUIDs.randomBase64UUID(random()))
            )
        );
        target = reinitShard(target, routing);
        Store sourceStore = source.store();
        Store targetStore = target.store();

        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        target.markAsRecovering("store", new RecoveryState(routing, localNode, null));
        final PlainActionFuture<Boolean> future = PlainActionFuture.newFuture();
        target.restoreFromRepository(new RestoreOnlyRepository("test") {
            @Override
            public void restoreShard(
                Store store,
                SnapshotId snapshotId,
                IndexId indexId,
                ShardId snapshotShardId,
                RecoveryState recoveryState,
                ActionListener<Void> listener
            ) {
                ActionListener.completeWith(listener, () -> {
                    cleanLuceneIndex(targetStore.directory());
                    for (String file : sourceStore.directory().listAll()) {
                        if (file.equals("write.lock") || file.startsWith("extra")) {
                            continue;
                        }
                        targetStore.directory().copyFrom(sourceStore.directory(), file, file, IOContext.DEFAULT);
                    }
                    recoveryState.getIndex().setFileDetailsComplete();
                    return null;
                });
            }
        }, future);
        assertTrue(future.actionGet());
        assertThat(target.getLocalCheckpoint(), equalTo(0L));
        assertThat(target.seqNoStats().getMaxSeqNo(), equalTo(2L));
        assertThat(target.seqNoStats().getGlobalCheckpoint(), equalTo(0L));
        IndexShardTestCase.updateRoutingEntry(target, routing.moveToStarted());
        assertThat(target.seqNoStats().getGlobalCheckpoint(), equalTo(0L));

        assertDocs(target, "0", "2");

        closeShard(source, false);
        closeShards(target);
    }

}
