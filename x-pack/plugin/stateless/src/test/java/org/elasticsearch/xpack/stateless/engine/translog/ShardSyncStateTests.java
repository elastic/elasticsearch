/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.stateless.engine.translog;

import org.apache.lucene.internal.hppc.LongArrayList;
import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.util.LongsRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.settings.Settings;
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
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.function.LongSupplier;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class ShardSyncStateTests extends ESTestCase {

    public void testActiveTranslogFileIsReleasedAfterCommit() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long generation = 2;
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 1L),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);

        shardSyncState.markCommitUploaded(1L);
        shardSyncState.markCommitUploaded(2L);
        assertTrue(activeTranslogFile.isSafeForDelete());
        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.markCommitUploaded(3L);
        assertTrue(activeTranslogFile.isSafeForDelete());
        assertFalse(activeTranslogFile.hasReferences());

        // Advance again to ensure that an assertion is not thrown from decrementing generation 3 file again
        shardSyncState.markCommitUploaded(4L);
        assertTrue(activeTranslogFile.isSafeForDelete());
        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileIsReferencedInNextSync() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);

        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(1, "", Map.of(shardId, 1L), Collections.singleton(shardId)) {
                @Override
                protected void closeInternal() {}
            }
        );

        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(2, "", Map.of(shardId, 2L), Collections.singleton(shardId)) {
                @Override
                protected void closeInternal() {}
            }
        );

        TranslogMetadata.Directory directory = shardSyncState.createDirectory(4, 0);
        assertThat(directory.estimatedOperationsToRecover(), equalTo(3L));
        assertThat(directory.referencedTranslogFileOffsets(), equalTo(new int[] { 3, 2 }));
        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(4, "", Map.of(shardId, 1L), Collections.singleton(shardId)) {
                @Override
                protected void closeInternal() {}
            }
        );

        shardSyncState.markCommitUploaded(2);

        TranslogMetadata.Directory directory2 = shardSyncState.createDirectory(5, 0);
        assertThat(directory2.estimatedOperationsToRecover(), equalTo(4L));
        assertThat(directory2.referencedTranslogFileOffsets(), equalTo(new int[] { 4, 3, 1 }));
        shardSyncState.markSyncStarting(
            primaryTerm,
            new TranslogReplicator.BlobTranslogFile(5, "", Map.of(shardId, 1L), Collections.singleton(shardId)) {
                @Override
                protected void closeInternal() {}
            }
        );

        // Now that 1 is fully marked as deleted, it will not be referenced in the next directory
        shardSyncState.markTranslogDeleted(1);

        TranslogMetadata.Directory directory3 = shardSyncState.createDirectory(6, 0);
        assertThat(directory3.estimatedOperationsToRecover(), equalTo(4L));
        assertThat(directory3.referencedTranslogFileOffsets(), equalTo(new int[] { 4, 2, 1 }));
    }

    public void testActiveTranslogFileIsMarkedUnsafeForDeleteAfterShardClose() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 1L),
            Collections.singleton(shardId)
        ) {
            @Override
            protected void closeInternal() {}
        };

        shardSyncState.markSyncStarting(primaryTerm, activeTranslogFile);

        assertTrue(activeTranslogFile.isSafeForDelete());
        assertTrue(activeTranslogFile.hasReferences());

        shardSyncState.close();

        assertFalse(activeTranslogFile.isSafeForDelete());
        assertFalse(activeTranslogFile.hasReferences());

        shardSyncState.markCommitUploaded(generation + 1);

        // Even if a commit comes in telling us to advance the start file ignore since the shard is closed.
        assertFalse(activeTranslogFile.isSafeForDelete());
        assertFalse(activeTranslogFile.hasReferences());
    }

    public void testActiveTranslogFileCannotBeQueuedAfterShardClose() throws IOException {
        ShardId shardId = new ShardId(new Index("name", "uuid"), 0);
        long primaryTerm = randomLongBetween(0, 20);
        long generation = randomLongBetween(1, 5);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm);

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 1L),
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

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 1L),
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
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, longsRefConsumer(seqNos::add));
        TranslogMetadata.Directory directory = shardSyncState.createDirectory(generation, 0);
        ShardSyncState.SyncMarker syncMarker = new ShardSyncState.SyncMarker(
            primaryTerm,
            new Translog.Location(0, 30, 40),
            longArrayListOf(0L, 1L, 2L)
        );

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 4L),
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
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, currentPrimaryTerm::get, longsRefConsumer(seqNos::add));
        ShardSyncState.SyncMarker syncMarker = new ShardSyncState.SyncMarker(
            primaryTerm,
            new Translog.Location(0, 30, 0),
            longArrayListOf(0L, 1L, 2L)
        );

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 3L),
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
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, longsRefConsumer(seqNos::add));

        Translog.Location manualSync = new Translog.Location(0, 10, 0);
        shardSyncState.updateProcessedLocation(manualSync);
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

        ShardSyncState.SyncMarker syncMarker = new ShardSyncState.SyncMarker(primaryTerm, manualSync, longArrayListOf(0L));
        assertThat(syncMarker.location(), equalTo(manualSync));

        TranslogReplicator.BlobTranslogFile activeTranslogFile = new TranslogReplicator.BlobTranslogFile(
            generation,
            "",
            Map.of(shardId, 3L),
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

    public void testQueuedSyncRemainsPendingWhenPrimaryTermBecomesStale() {
        final var shardId = new ShardId(new Index(randomIndexName(), UUIDs.randomBase64UUID()), between(0, 5));
        final var primaryTerm = randomLongBetween(1, 20);
        AtomicLong currentPrimaryTerm = new AtomicLong(primaryTerm);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, currentPrimaryTerm::get, longsRef -> {});

        Translog.Location queuedLocation = new Translog.Location(0, between(1, 100), 0);
        shardSyncState.updateProcessedLocation(queuedLocation);
        PlainActionFuture<Void> queuedListener = new PlainActionFuture<>();
        assertFalse(shardSyncState.ensureSynced(queuedLocation, queuedListener));

        // The live term has moved, but no upload has been rejected yet, so this sync is still queued.
        currentPrimaryTerm.addAndGet(between(1, 100));
        final var locationBeforeRejection = new Translog.Location(0, queuedLocation.translogLocation() + between(1, 100), 0);
        shardSyncState.updateProcessedLocation(locationBeforeRejection);
        PlainActionFuture<Void> listenerBeforeRejection = new PlainActionFuture<>();
        assertFalse(shardSyncState.ensureSynced(locationBeforeRejection, listenerBeforeRejection));

        assertFalse(
            shardSyncState.markSyncFinished(new ShardSyncState.SyncMarker(primaryTerm, locationBeforeRejection, longArrayListOf(1L)))
        );
        shardSyncState.notifyListeners();
        assertFalse(queuedListener.isDone());
        assertFalse(listenerBeforeRejection.isDone());

        // A sync registered after the rejected upload fails on its own and leaves the queued listeners pending.
        final var locationAfterRejection = new Translog.Location(0, locationBeforeRejection.translogLocation() + between(1, 100), 0);
        shardSyncState.updateProcessedLocation(locationAfterRejection);
        PlainActionFuture<Void> listenerAfterRejection = new PlainActionFuture<>();
        assertTrue(shardSyncState.ensureSynced(locationAfterRejection, listenerAfterRejection));
        assertStalePrimaryTerm(listenerAfterRejection);
        assertFalse(queuedListener.isDone());
        assertFalse(listenerBeforeRejection.isDone());

        shardSyncState.failPendingListeners(ShardSyncState.stalePrimaryTermException(shardId));
        assertStalePrimaryTerm(queuedListener);
        assertStalePrimaryTerm(listenerBeforeRejection);
    }

    public void testMarkerFromEarlierRegistrationDoesNotMakeCurrentTermStale() {
        final var shardId = new ShardId(new Index(randomIndexName(), UUIDs.randomBase64UUID()), between(0, 5));
        final var currentTerm = randomLongBetween(2, 20);
        ShardSyncState shardSyncState = getShardSyncState(shardId, currentTerm, () -> currentTerm, longsRef -> {});

        final var queuedLocation = new Translog.Location(0, between(1, 100), 0);
        shardSyncState.updateProcessedLocation(queuedLocation);
        PlainActionFuture<Void> queuedListener = new PlainActionFuture<>();
        assertFalse(shardSyncState.ensureSynced(queuedLocation, queuedListener));

        final var earlierTerm = randomLongBetween(0, currentTerm - 1);
        assertFalse(shardSyncState.markSyncFinished(new ShardSyncState.SyncMarker(earlierTerm, queuedLocation, longArrayListOf(0L))));
        shardSyncState.notifyListeners();
        assertFalse(queuedListener.isDone());
        assertFalse(shardSyncState.isPrimaryTermStale());

        final var laterLocation = new Translog.Location(0, queuedLocation.translogLocation() + between(1, 100), 0);
        shardSyncState.updateProcessedLocation(laterLocation);
        PlainActionFuture<Void> laterListener = new PlainActionFuture<>();
        assertFalse(shardSyncState.ensureSynced(laterLocation, laterListener));

        assertTrue(shardSyncState.markSyncFinished(new ShardSyncState.SyncMarker(currentTerm, laterLocation, longArrayListOf(1L))));
        shardSyncState.notifyListeners();
        assertNull(queuedListener.actionGet());
        assertNull(laterListener.actionGet());
    }

    public void testCoveredListenersCompleteAfterPrimaryTermBecomesStale() {
        final var shardId = new ShardId(new Index(randomIndexName(), UUIDs.randomBase64UUID()), between(0, 5));
        final var primaryTerm = randomLongBetween(1, 20);
        AtomicLong currentPrimaryTerm = new AtomicLong(primaryTerm);
        ShardSyncState shardSyncState = getShardSyncState(shardId, primaryTerm, currentPrimaryTerm::get, longsRef -> {});

        final var coveredLocation = new Translog.Location(0, between(1, 100), 0);
        final var beyondLocation = new Translog.Location(0, coveredLocation.translogLocation() + between(1, 100), 0);
        shardSyncState.updateProcessedLocation(beyondLocation);
        PlainActionFuture<Void> coveredListener = new PlainActionFuture<>();
        PlainActionFuture<Void> beyondListener = new PlainActionFuture<>();
        assertFalse(shardSyncState.ensureSynced(coveredLocation, coveredListener));
        assertFalse(shardSyncState.ensureSynced(beyondLocation, beyondListener));

        assertTrue(shardSyncState.markSyncFinished(new ShardSyncState.SyncMarker(primaryTerm, coveredLocation, longArrayListOf(0L))));
        currentPrimaryTerm.addAndGet(between(1, 100));
        assertFalse(shardSyncState.markSyncFinished(new ShardSyncState.SyncMarker(primaryTerm, beyondLocation, longArrayListOf(1L))));
        shardSyncState.notifyListeners();

        assertTrue(coveredListener.isDone());
        assertNull(coveredListener.actionGet());
        assertFalse(beyondListener.isDone());

        // The location already reached is still synced. A later location is rejected without failing the listener left queued.
        PlainActionFuture<Void> alreadySyncedListener = new PlainActionFuture<>();
        assertTrue(shardSyncState.ensureSynced(coveredLocation, alreadySyncedListener));
        assertNull(alreadySyncedListener.actionGet());
        final var laterLocation = new Translog.Location(0, beyondLocation.translogLocation() + between(1, 100), 0);
        shardSyncState.updateProcessedLocation(laterLocation);
        PlainActionFuture<Void> laterListener = new PlainActionFuture<>();
        assertTrue(shardSyncState.ensureSynced(laterLocation, laterListener));
        assertStalePrimaryTerm(laterListener);
        assertFalse(beyondListener.isDone());

        shardSyncState.failPendingListeners(ShardSyncState.stalePrimaryTermException(shardId));
        assertStalePrimaryTerm(beyondListener);
    }

    private static void assertStalePrimaryTerm(PlainActionFuture<Void> listener) {
        assertTrue(listener.isDone());
        assertThat(expectThrows(AlreadyClosedException.class, listener::actionGet).getMessage(), containsString("primary term is stale"));
    }

    private static ShardSyncState getShardSyncState(ShardId shardId, long primaryTerm) {
        return getShardSyncState(shardId, primaryTerm, seqNo -> {});
    }

    private static ShardSyncState getShardSyncState(ShardId shardId, long primaryTerm, Consumer<LongsRef> persistedSeqNosConsumer) {
        return getShardSyncState(shardId, primaryTerm, () -> primaryTerm, persistedSeqNosConsumer);
    }

    private static ShardSyncState getShardSyncState(
        ShardId shardId,
        long primaryTerm,
        LongSupplier primaryTermSupplier,
        Consumer<LongsRef> persistedSeqNosConsumer
    ) {
        ShardSyncState shardSyncState = new ShardSyncState(
            shardId,
            primaryTerm,
            primaryTermSupplier,
            persistedSeqNosConsumer,
            new ThreadContext(Settings.EMPTY)
        );
        return shardSyncState;
    }

    /**
     * Wraps a {@link LongConsumer} (convenient for tests) in some ceremony that allows it to be used as
     * a {@link Consumer} for {@link LongsRef}. The wrapped consumer will be invoked once for each long
     * referenced by the LongsRef.
     */
    private static Consumer<LongsRef> longsRefConsumer(LongConsumer consumer) {
        return longsRef -> {
            for (int i = longsRef.offset; i < longsRef.offset + longsRef.length; i++) {
                consumer.accept(longsRef.longs[i]);
            }
        };
    }

    private static LongArrayList longArrayListOf(long... longs) {
        var result = new LongArrayList();
        result.add(longs);
        return result;
    }
}
