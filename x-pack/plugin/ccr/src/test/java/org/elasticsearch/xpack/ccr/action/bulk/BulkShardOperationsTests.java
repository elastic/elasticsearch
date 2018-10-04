/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.bulk;

import org.apache.lucene.index.Term;
import org.elasticsearch.ElasticsearchTimeoutException;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction.CcrWritePrimaryResult;

public class BulkShardOperationsTests extends IndexShardTestCase {

    private static final byte[] SOURCE = "{}".getBytes(StandardCharsets.UTF_8);

    // test that we use the primary term on the follower when applying operations from the leader
    public void testPrimaryTermFromFollower() throws IOException {
        final Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build();
        final IndexShard followerPrimary = newStartedShard(true, settings, new FollowingEngineFactory());

        // we use this primary on the operations yet we expect the applied operations to have the primary term of the follower
        final long primaryTerm = randomLongBetween(1, Integer.MAX_VALUE);

        int numOps = randomIntBetween(0, 127);
        final List<Translog.Operation> operations = new ArrayList<>(randomIntBetween(0, 127));
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(i);
            final long seqNo = i;
            final Translog.Operation.Type type =
                    randomValueOtherThan(Translog.Operation.Type.CREATE, () -> randomFrom(Translog.Operation.Type.values()));
            switch (type) {
                case INDEX:
                    operations.add(new Translog.Index("_doc", id, seqNo, primaryTerm, 0, SOURCE, null, -1));
                    break;
                case DELETE:
                    operations.add(
                        new Translog.Delete("_doc", id, new Term("_id", Uid.encodeId(id)), seqNo, primaryTerm, 0));
                    break;
                case NO_OP:
                    operations.add(new Translog.NoOp(seqNo, primaryTerm, "test"));
                    break;
                default:
                    throw new IllegalStateException("unexpected operation type [" + type + "]");
            }
        }

        final TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> result =
            TransportBulkShardOperationsAction.shardOperationOnPrimary(followerPrimary.shardId(), followerPrimary.getHistoryUUID(),
                    operations,
                numOps - 1, followerPrimary, logger);

        try (Translog.Snapshot snapshot = followerPrimary.getHistoryOperations("test", 0)) {
            assertThat(snapshot.totalOperations(), equalTo(operations.size()));
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                assertThat(operation.primaryTerm(), equalTo(followerPrimary.getOperationPrimaryTerm()));
            }
        }

        for (final Translog.Operation operation : result.replicaRequest().getOperations()) {
            assertThat(operation.primaryTerm(), equalTo(followerPrimary.getOperationPrimaryTerm()));
        }

        closeShards(followerPrimary);
    }

    public void testPrimaryResultWaitForGlobalCheckpoint() throws Exception {
        final Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build();
        final IndexShard shard = newStartedShard(false, settings, new FollowingEngineFactory());
        int numOps = between(1, 100);
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(i);
            final Translog.Operation op;
            if (randomBoolean()) {
                op = new Translog.Index("_doc", id, i, primaryTerm, 0, SOURCE, null, -1);
            } else if (randomBoolean()) {
                shard.advanceMaxSeqNoOfUpdatesOrDeletes(i);
                op = new Translog.Delete("_doc", id, new Term("_id", Uid.encodeId(id)), i, primaryTerm, 0);
            } else {
                op = new Translog.NoOp(i, primaryTerm, "test");
            }
            shard.applyTranslogOperation(op, Engine.Operation.Origin.REPLICA);
        }
        BulkShardOperationsRequest request = new BulkShardOperationsRequest();
        {
            PlainActionFuture<BulkShardOperationsResponse> listener = new PlainActionFuture<>();
            CcrWritePrimaryResult primaryResult = new CcrWritePrimaryResult(request, null, shard, -2, logger);
            primaryResult.respond(listener);
            assertThat("should return intermediately if waiting_global_checkpoint is not specified",
                listener.actionGet(TimeValue.ZERO).getMaxSeqNo(), equalTo(shard.seqNoStats().getMaxSeqNo()));
        }
        {
            PlainActionFuture<BulkShardOperationsResponse> listener = new PlainActionFuture<>();
            long waitingForGlobalCheckpoint = randomLongBetween(shard.getGlobalCheckpoint() + 1, shard.getLocalCheckpoint());
            CcrWritePrimaryResult primaryResult = new CcrWritePrimaryResult(request, null, shard, waitingForGlobalCheckpoint, logger);
            primaryResult.respond(listener);
            expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TimeValue.timeValueMillis(1)));

            shard.updateGlobalCheckpointOnReplica(randomLongBetween(shard.getGlobalCheckpoint(), waitingForGlobalCheckpoint - 1), "test");
            expectThrows(ElasticsearchTimeoutException.class, () -> listener.actionGet(TimeValue.timeValueMillis(1)));

            shard.updateGlobalCheckpointOnReplica(randomLongBetween(waitingForGlobalCheckpoint, shard.getLocalCheckpoint()), "test");
            assertThat(listener.actionGet(TimeValue.timeValueMillis(10)).getMaxSeqNo(), equalTo(shard.seqNoStats().getMaxSeqNo()));
        }
        {
            PlainActionFuture<BulkShardOperationsResponse> listener = new PlainActionFuture<>();
            long waitingForGlobalCheckpoint = randomLongBetween(-1, shard.getGlobalCheckpoint());
            CcrWritePrimaryResult primaryResult = new CcrWritePrimaryResult(request, null, shard, waitingForGlobalCheckpoint, logger);
            primaryResult.respond(listener);
            assertThat(listener.actionGet(TimeValue.timeValueMillis(10)).getMaxSeqNo(), equalTo(shard.seqNoStats().getMaxSeqNo()));
        }
        closeShards(shard);
    }

    public void testPrimaryResultIncludeOnlyAppliedOperations() throws Exception {
        final Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true).build();
        final IndexShard primary = newStartedShard(true, settings, new FollowingEngineFactory());
        long seqno = 0;
        int numOps = between(1, 100);
        List<Translog.Operation> ops = new ArrayList<>(numOps);
        for (int i = 0; i < numOps; i++) {
            final String id = Integer.toString(between(1, 100));
            if (randomBoolean()) {
                ops.add(new Translog.Index("_doc", id, seqno++, primaryTerm, 0, SOURCE, null, -1));
            } else {
                ops.add(new Translog.Delete("_doc", id, new Term("_id", Uid.encodeId(id)), seqno++, primaryTerm, 0));
            }
        }
        primary.advanceMaxSeqNoOfUpdatesOrDeletes(seqno);
        Randomness.shuffle(ops);

        List<Translog.Operation> firstBulk = randomSubsetOf(ops);
        ops.removeAll(firstBulk);
        final CcrWritePrimaryResult fullResult = TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(),
            primary.getHistoryUUID(), firstBulk, seqno, primary, logger);
        assertThat(fullResult.replicaRequest().getOperations(),
            equalTo(rewriteWithPrimaryTerm(firstBulk, primary.getOperationPrimaryTerm())));
        assertThat(fullResult.waitingForGlobalCheckpoint, equalTo(-2));

        List<Translog.Operation> subOfFirstBulk = randomSubsetOf(firstBulk);
        final CcrWritePrimaryResult emptyResult = TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(),
            primary.getHistoryUUID(), subOfFirstBulk, seqno, primary, logger);
        assertThat(emptyResult.replicaRequest().getOperations(), empty());
        assertThat(fullResult.waitingForGlobalCheckpoint, equalTo(subOfFirstBulk.stream().mapToLong(o -> o.seqNo()).max().orElse(-2)));

        final List<Translog.Operation> secondBulk = new ArrayList<>(ops);
        subOfFirstBulk = randomSubsetOf(firstBulk);
        secondBulk.addAll(subOfFirstBulk);
        final CcrWritePrimaryResult partialResult = TransportBulkShardOperationsAction.shardOperationOnPrimary(primary.shardId(),
            primary.getHistoryUUID(), secondBulk, seqno, primary, logger);
        assertThat(partialResult.replicaRequest().getOperations(), equalTo(rewriteWithPrimaryTerm(ops, primary.getOperationPrimaryTerm())));
        assertThat(partialResult.waitingForGlobalCheckpoint, equalTo(subOfFirstBulk.stream().mapToLong(o -> o.seqNo()).max().orElse(-2)));

        closeShards(primary);
    }

    private List<Translog.Operation> rewriteWithPrimaryTerm(List<Translog.Operation> sourceOperations, long primaryTerm) {
        return sourceOperations.stream().map(op -> {
            switch (op.opType()) {
                case INDEX:
                    final Translog.Index index = (Translog.Index) op;
                    return new Translog.Index(index.type(), index.id(), index.seqNo(), primaryTerm,
                        index.version(), BytesReference.toBytes(index.source()), index.routing(), index.getAutoGeneratedIdTimestamp());
                case DELETE:
                    final Translog.Delete delete = (Translog.Delete) op;
                    return new Translog.Delete(delete.type(), delete.id(), delete.uid(), delete.seqNo(), primaryTerm, delete.version());
                case NO_OP:
                    final Translog.NoOp noOp = (Translog.NoOp) op;
                    return new Translog.NoOp(noOp.seqNo(), primaryTerm, noOp.reason());
                default:
                    throw new IllegalStateException("unexpected operation type [" + op.opType() + "]");
            }
        }).collect(Collectors.toList());
    }
}
