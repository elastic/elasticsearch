/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.index.engine;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.lease.Releasable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.mapper.SourceToParse;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xpack.ccr.CcrSettings;

import java.util.Collections;
import java.util.concurrent.CountDownLatch;

import static org.elasticsearch.cluster.routing.TestShardRouting.newShardRouting;
import static org.hamcrest.Matchers.equalTo;

public class FollowEngineIndexShardTests extends IndexShardTestCase {

    public void testDoNotFillGaps() throws Exception {
        Settings settings = Settings.builder()
            .put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .build();
        final IndexShard indexShard = newStartedShard(false, settings, new FollowingEngineFactory());

        long seqNo = -1;
        for (int i = 0; i < 8; i++) {
            final String id = Long.toString(i);
            SourceToParse sourceToParse = new SourceToParse(indexShard.shardId().getIndexName(), "_doc", id,
                new BytesArray("{}"), XContentType.JSON);
            indexShard.applyIndexOperationOnReplica(++seqNo, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, sourceToParse);
        }
        long seqNoBeforeGap = seqNo;
        seqNo += 8;
        SourceToParse sourceToParse = new SourceToParse(indexShard.shardId().getIndexName(), "_doc", "9",
            new BytesArray("{}"), XContentType.JSON);
        indexShard.applyIndexOperationOnReplica(seqNo, 1, IndexRequest.UNSET_AUTO_GENERATED_TIMESTAMP, false, sourceToParse);

        // promote the replica to primary:
        final ShardRouting replicaRouting = indexShard.routingEntry();
        final ShardRouting primaryRouting =
            newShardRouting(
                replicaRouting.shardId(),
                replicaRouting.currentNodeId(),
                null,
                true,
                ShardRoutingState.STARTED,
                replicaRouting.allocationId());
        indexShard.updateShardState(primaryRouting, indexShard.getOperationPrimaryTerm() + 1, (shard, listener) -> {},
            0L, Collections.singleton(primaryRouting.allocationId().getId()),
            new IndexShardRoutingTable.Builder(primaryRouting.shardId()).addShard(primaryRouting).build(), Collections.emptySet());

        final CountDownLatch latch = new CountDownLatch(1);
        ActionListener<Releasable> actionListener = ActionListener.wrap(releasable -> {
            releasable.close();
            latch.countDown();
        }, e -> {assert false : "expected no exception, but got [" + e.getMessage() + "]";});
        indexShard.acquirePrimaryOperationPermit(actionListener, ThreadPool.Names.GENERIC, "");
        latch.await();
        assertThat(indexShard.getLocalCheckpoint(), equalTo(seqNoBeforeGap));
        indexShard.refresh("test");
        assertThat(indexShard.docStats().getCount(), equalTo(9L));
        closeShards(indexShard);
    }

}
