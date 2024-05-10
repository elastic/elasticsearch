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
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ShardSyncStateTests extends ESTestCase {

    public void testActiveTranslogFileIsReleasedAfterCommit() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long generation = 2;
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);

        shardSyncState.markCommitUploaded(1L);
        shardSyncState.markCommitUploaded(2L);
        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.markCommitUploaded(3L);
        assertFalse(activeTranslogFile.hasReferences());

        // Advance again to ensure that an assertion is not thrown from decrementing generation 3 file again
        shardSyncState.markCommitUploaded(4L);
        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileIsReferencedInNextSync() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);

        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(
                1,
                "",
                Map.of(shardId, shardSyncState.pollSync(1).metadata(0, 10)),
                Collections.singleton(shardId)
            ) {
                @Override
                protected void closeInternal() {}
            }
        );

        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 10, 10));
        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(
                2,
                "",
                Map.of(shardId, shardSyncState.pollSync(2).metadata(0, 10)),
                Collections.singleton(shardId)
            ) {
                @Override
                protected void closeInternal() {}
            }
        );

        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 20, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(4);
        assertThat(syncState.estimatedOps(), equalTo(3L));
        assertThat(syncState.referencedTranslogFileOffsets(), equalTo(new int[] { 3, 2 }));
        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(4, "", Map.of(shardId, syncState.metadata(0, 10)), Collections.singleton(shardId)) {
                @Override
                protected void closeInternal() {}
            }
        );

        shardSyncState.markCommitUploaded(2);

        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 30, 10));
        ShardSyncState.SyncState syncState2 = shardSyncState.pollSync(5);
        assertThat(syncState2.estimatedOps(), equalTo(4L));
        assertThat(syncState2.referencedTranslogFileOffsets(), equalTo(new int[] { 4, 3, 1 }));
        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(5, "", Map.of(shardId, syncState2.metadata(0, 10)), Collections.singleton(shardId)) {
                @Override
                protected void closeInternal() {}
            }
        );

        // Now that 1 is fully marked as deleted, it will not be referenced in the next directory
        shardSyncState.markTranslogDeleted(1);

        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 40, 10));
        ShardSyncState.SyncState syncState3 = shardSyncState.pollSync(6);
        assertThat(syncState3.estimatedOps(), equalTo(4L));
        assertThat(syncState3.referencedTranslogFileOffsets(), equalTo(new int[] { 4, 2, 1 }));
    }

    public void testActiveTranslogFileIsNotReleasedAfterShardClose() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);

        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.close();

        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.markCommitUploaded(generation + 1);

        // Even if a commit comes in telling us to advance the start file ignore since the shard is closed.
        assertTrue(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileCannotBeQueuedAfterShardClose() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.close();

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);

        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.markCommitUploaded(generation + 1);

        // Even if a commit comes in telling us to advance the start file ignore since the shard is closed.
        assertTrue(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileReleasedIfDifferentPrimaryTerm() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(1, 20);
        long generation = randomLongBetween(1, 5);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 10)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncStarting(primaryTerm - 1, activeTranslogFile);

        // References are released because the advance of the primary term tells us we no longer need this sync
        // TODO: Is this true? Maybe safer to just drop
        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testPersistedSeqNoConsumerCalledAfterSync() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
        ArrayList<Long> seqNos = new ArrayList<>();
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, seqNos::add);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 1, new Translog.Location(0, 10, 20));
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 2, new Translog.Location(0, 20, 30));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 3, new Translog.Location(0, 30, 40));

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 30)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        assertThat(seqNos, empty());

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);
        assertTrue(shardSyncState.markSyncFinished(syncMarker));
        shardSyncState.notifyListeners();

        assertThat(seqNos, contains(0L, 1L, 2L));
        assertThat(seqNos, not(contains(3L)));
    }

    public void testSyncDoesNotAdvanceIfPrimaryTermChange() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
        ArrayList<Long> seqNos = new ArrayList<>();
        AtomicLong currentPrimaryTerm = new AtomicLong(primaryTerm);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, currentPrimaryTerm::get, seqNos::add);
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 0, new Translog.Location(0, 0, 10));
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 1, new Translog.Location(0, 10, 20));
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 2, new Translog.Location(0, 20, 30));
        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();
        shardSyncState.writeToBuffer(new BytesArray(new byte[10]), 3, new Translog.Location(0, 30, 40));

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 30)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        assertThat(seqNos, empty());

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);
        currentPrimaryTerm.incrementAndGet();
        assertFalse(shardSyncState.markSyncFinished(syncMarker));
        shardSyncState.notifyListeners();

        assertThat(seqNos, empty());
    }

    public void testPersistedSeqNoConsumerCalledFirst() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
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

        ShardSyncState.SyncState syncState = shardSyncState.pollSync(generation);
        ShardSyncState.SyncMarker syncMarker = syncState.buffer().syncMarker();
        assertThat(syncMarker.location(), equalTo(manualSync));

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, syncState.metadata(0, 30)),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        assertThat(seqNos, empty());

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);
        assertTrue(shardSyncState.markSyncFinished(syncMarker));
        shardSyncState.notifyListeners();

        assertThat(seqNos, contains(0L));
    }

    private static ShardSyncState getShardSyncState(ShardId shardId, long primaryTerm) {
        return getShardSyncState(shardId, primaryTerm, seqNo -> {});
    }

    private static ShardSyncState getShardSyncState(ShardId shardId, long primaryTerm, LongConsumer persistedSeqNoConsumer) {
        return getShardSyncState(shardId, primaryTerm, () -> primaryTerm, persistedSeqNoConsumer);
    }

    private static ShardSyncState getShardSyncState(
        ShardId shardId,
        long primaryTerm,
        LongSupplier primaryTermSupplier,
        LongConsumer persistedSeqNoConsumer
    ) {
        ShardSyncState shardSyncState = new ShardSyncState(
            shardId,
            primaryTerm,
            primaryTermSupplier,
            persistedSeqNoConsumer,
            new ThreadContext(Settings.EMPTY),
            BigArrays.NON_RECYCLING_INSTANCE
        );
        return shardSyncState;
    }
}
