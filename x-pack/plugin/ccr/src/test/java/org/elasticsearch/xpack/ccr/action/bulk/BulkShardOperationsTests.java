/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.ccr.action.bulk;

import org.apache.lucene.index.Term;
import org.elasticsearch.action.support.replication.TransportWriteAction;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.VersionType;
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

import static org.hamcrest.Matchers.equalTo;

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
                    operations.add(new Translog.Index("_doc", id, seqNo, primaryTerm, 0, VersionType.INTERNAL, SOURCE, null, -1));
                    break;
                case DELETE:
                    operations.add(
                        new Translog.Delete("_doc", id, new Term("_id", Uid.encodeId(id)), seqNo, primaryTerm, 0, VersionType.INTERNAL));
                    break;
                case NO_OP:
                    operations.add(new Translog.NoOp(seqNo, primaryTerm, "test"));
                    break;
                default:
                    throw new IllegalStateException("unexpected operation type [" + type + "]");
            }
        }

        final TransportWriteAction.WritePrimaryResult<BulkShardOperationsRequest, BulkShardOperationsResponse> result =
                TransportBulkShardOperationsAction.shardOperationOnPrimary(followerPrimary.shardId(), operations, followerPrimary, logger);

        try (Translog.Snapshot snapshot = followerPrimary.newTranslogSnapshotFromMinSeqNo(0)) {
            assertThat(snapshot.totalOperations(), equalTo(operations.size()));
            Translog.Operation operation;
            while ((operation = snapshot.next()) != null) {
                assertThat(operation.primaryTerm(), equalTo(followerPrimary.getPrimaryTerm()));
            }
        }

        for (final Translog.Operation operation : result.replicaRequest().getOperations()) {
            assertThat(operation.primaryTerm(), equalTo(followerPrimary.getPrimaryTerm()));
        }

        closeShards(followerPrimary);
    }

}
