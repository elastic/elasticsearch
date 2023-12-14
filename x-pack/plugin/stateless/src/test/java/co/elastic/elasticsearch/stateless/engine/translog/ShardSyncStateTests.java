/*
 * ELASTICSEARCH CONFIDENTIAL
 * __________________
 *
 * Copyright Elasticsearch B.V. All rights reserved.
 *
 * NOTICE:  All information contained herein is, and remains
 * the property of Elasticsearch B.V. and its suppliers, if any.
 * The intellectual and technical concepts contained herein
 * are proprietary to Elasticsearch B.V. and its suppliers and
 * may be covered by U.S. and Foreign Patents, patents in
 * process, and are protected by trade secret or copyright
 * law.  Dissemination of this information or reproduction of
 * this material is strictly forbidden unless prior written
 * permission is obtained from Elasticsearch B.V.
 */

package co.elastic.elasticsearch.stateless.engine.translog;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.index.translog.Translog;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongConsumer;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ShardSyncStateTests extends ESTestCase {

    public void testActiveTranslogFileIsReleasedAfterCommit() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);

        shardSyncState.markCommitUploaded(1L);
        shardSyncState.markCommitUploaded(2L);
        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.markCommitUploaded(3L);
        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileIsReleasedIfCommitAlreadyHappened() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markCommitUploaded(3L);

        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);

        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileIsNotReleasedAfterShardClose() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);

        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.close(false);

        assertTrue(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileIsNotReleasedWhenNodeShuttingDown() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);

        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.close(true);

        assertTrue(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileCannotBeQueuedAfterShardClose() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        boolean nodeClosing = randomBoolean();

        shardSyncState.close(nodeClosing);

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);

        assertTrue(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileCannotBeQueuedWithDifferentPrimaryTerm() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(1, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncFinished(
            activeTranslogFile,
            new ShardSyncState.SyncMarker(syncMarker.primaryTerm() - 1, syncMarker.location(), List.of(0L))
        );

        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testPersistedSeqNoConsumerCalledAfterSync() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ArrayList<Long> seqNos = new ArrayList<>();
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, seqNos::add);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 1, new Translog.Location(0, 10, 20));
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 2, new Translog.Location(0, 20, 30));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 3, new Translog.Location(0, 30, 40));

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 30)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        assertThat(seqNos, empty());

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);
        shardSyncState.notifyListeners();

        assertThat(seqNos, contains(0L, 1L, 2L));
        assertThat(seqNos, not(contains(3L)));
    }

    public void testPersistedSeqNoConsumerCalledFirst() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ArrayList<Long> seqNos = new ArrayList<>();
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, seqNos::add);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));

        Translog.Location manualSync = new Translog.Location(0, 10, 0);
        shardSyncState.ensureSynced(manualSync, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                assertThat(seqNos, contains(0L));
            }

            @Override
            public void onFailure(Exception e) {
                fail();
            }
        });

        ShardSyncState.SyncState syncState = shardSyncState.pollSync();
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();
        assertThat(syncMarker.location(), equalTo(manualSync));

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            2,
            "",
            Map.of(shardId, syncState.metadata(0, 30)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        assertThat(seqNos, empty());

        shardSyncState.markSyncFinished(activeTranslogFile, syncMarker);
        shardSyncState.notifyListeners();

        assertThat(seqNos, contains(0L));
    }

    private static ShardSyncState getShardSyncState(ShardId shardId, long primaryTerm) {
        return getShardSyncState(shardId, primaryTerm, seqNo -> {});
    }

    private static ShardSyncState getShardSyncState(ShardId shardId, long primaryTerm, LongConsumer persistedSeqNoConsumer) {
        ShardSyncState shardSyncState = new ShardSyncState(
            shardId,
            primaryTerm,
            () -> primaryTerm,
            persistedSeqNoConsumer,
            new ThreadContext(Settings.EMPTY),
            BigArrays.NON_RECYCLING_INSTANCE
        );
        return shardSyncState;
    }
}
