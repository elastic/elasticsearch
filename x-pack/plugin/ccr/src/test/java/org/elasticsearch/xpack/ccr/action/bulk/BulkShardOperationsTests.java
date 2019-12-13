/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.bulk;

import org.apache.lucene.index.Term;
import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.flush.FlushRequest;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.engine.Engine;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.shard.IndexShard;
import org.elasticsearch.index.shard.IndexShardTestCase;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.indices.recovery.RecoveryState;
import org.elasticsearch.xpack.ccr.CcrSettings;
import org.elasticsearch.xpack.ccr.index.engine.FollowingEngineFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.elasticsearch.xpack.ccr.action.bulk.TransportBulkShardOperationsAction.rewriteOperationWithPrimaryTerm;
import static org.hamcrest.Matchers.equalTo;

public class BulkShardOperationsTests extends IndexShardTestCase {

    private static final byte[] SOURCE = "{}".getBytes(StandardCharsets.UTF_8);

    // test that we use the primary term on the follower when applying operations from the leader
    public void testPrimaryTermFromFollower() throws IOException {
        final Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
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
                    operations.add(new Translog.Index(id, seqNo, primaryTerm, 0, SOURCE, null, -1));
                    break;
                case DELETE:
                    operations.add(
                        new Translog.Delete(id, new Term("_id", Uid.encodeId(id)), seqNo, primaryTerm, 0));
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

        try (Translog.Snapshot snapshot = followerPrimary.getHistoryOperations("test", Engine.HistorySource.INDEX, 0)) {
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

    public void testPrimaryResultIncludeOnlyAppliedOperations() throws Exception {
        final Settings settings = Settings.builder().put(CcrSettings.CCR_FOLLOWING_INDEX_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build();
        final IndexShard oldPrimary = newStartedShard(true, settings, new FollowingEngineFactory());
        final long oldPrimaryTerm = oldPrimary.getOperationPrimaryTerm();
        long seqno = 0;
        List<Translog.Operation> firstBulk = new ArrayList<>();
        List<Translog.Operation> secondBulk = new ArrayList<>();
        for (int numOps = between(1, 100), i = 0; i < numOps; i++) {
            final String id = Integer.toString(between(1, 100));
            final Translog.Operation op;
            if (randomBoolean()) {
                op = new Translog.Index(id, seqno++, primaryTerm, 0, SOURCE, null, -1);
            } else if (randomBoolean()) {
                op = new Translog.Delete(id, new Term("_id", Uid.encodeId(id)), seqno++, primaryTerm, 0);
            } else {
                op = new Translog.NoOp(seqno++, primaryTerm, "test-" + i);
            }
            if (randomBoolean()) {
                firstBulk.add(op);
            } else {
                secondBulk.add(op);
            }
            if (rarely()) {
                oldPrimary.refresh("test");
            }
            if (rarely()) {
                oldPrimary.flush(new FlushRequest());
            }
        }
        Randomness.shuffle(firstBulk);
        Randomness.shuffle(secondBulk);
        oldPrimary.advanceMaxSeqNoOfUpdatesOrDeletes(seqno);
        final TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> fullResult =
            TransportBulkShardOperationsAction.shardOperationOnPrimary(oldPrimary.shardId(),
            oldPrimary.getHistoryUUID(), firstBulk, seqno, oldPrimary, logger);
        assertThat(fullResult.replicaRequest().getOperations(),
            equalTo(firstBulk.stream().map(op -> rewriteOperationWithPrimaryTerm(op, oldPrimaryTerm)).collect(Collectors.toList())));
        primaryTerm = randomLongBetween(primaryTerm, primaryTerm + 10);
        final IndexShard newPrimary = reinitShard(oldPrimary);
        DiscoveryNode localNode = new DiscoveryNode("foo", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        newPrimary.markAsRecovering("store", new RecoveryState(newPrimary.routingEntry(), localNode, null));
        assertTrue(recoverFromStore(newPrimary));
        IndexShardTestCase.updateRoutingEntry(newPrimary, newPrimary.routingEntry().moveToStarted());
        newPrimary.advanceMaxSeqNoOfUpdatesOrDeletes(seqno);
        // The second bulk includes some operations from the first bulk which were processed already;
        // only a subset of these operations will be included the result but with the old primary term.
        final List<Translog.Operation> existingOps = randomSubsetOf(firstBulk);
        final TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> partialResult =
            TransportBulkShardOperationsAction.shardOperationOnPrimary(newPrimary.shardId(),
            newPrimary.getHistoryUUID(), Stream.concat(secondBulk.stream(), existingOps.stream()).collect(Collectors.toList()),
            seqno, newPrimary, logger);
        final long newPrimaryTerm = newPrimary.getOperationPrimaryTerm();
        final long globalCheckpoint = newPrimary.getLastKnownGlobalCheckpoint();
        final List<Translog.Operation> appliedOperations = Stream.concat(
            secondBulk.stream().map(op -> rewriteOperationWithPrimaryTerm(op, newPrimaryTerm)),
            existingOps.stream().filter(op -> op.seqNo() > globalCheckpoint).map(op -> rewriteOperationWithPrimaryTerm(op, oldPrimaryTerm))
        ).collect(Collectors.toList());

        assertThat(partialResult.replicaRequest().getOperations(), equalTo(appliedOperations));
        closeShards(newPrimary);
    }
}
