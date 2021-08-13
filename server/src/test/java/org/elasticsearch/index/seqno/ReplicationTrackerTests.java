/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.replication.ReplicationResponse;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongConsumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

public class ReplicationTrackerTests extends ReplicationTrackerTestCase {

    public void testEmptyShards() {
        final ReplicationTracker tracker = newTracker(AllocationId.newInitializing());
        assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
    }

    private Map<AllocationId, Long> randomAllocationsWithLocalCheckpoints(int min, int max) {
        Map<AllocationId, Long> allocations = new HashMap<>();
        for (int i = randomIntBetween(min, max); i > 0; i--) {
            allocations.put(AllocationId.newInitializing(), (long) randomInt(1000));
        }
        return allocations;
    }

    private static Set<String> ids(Set<AllocationId> allocationIds) {
        return allocationIds.stream().map(AllocationId::getId).collect(Collectors.toSet());
    }

    private void updateLocalCheckpoint(final ReplicationTracker tracker, final String allocationId, final long localCheckpoint) {
        tracker.updateLocalCheckpoint(allocationId, localCheckpoint);
        assertThat(updatedGlobalCheckpoint.get(), equalTo(tracker.getGlobalCheckpoint()));
    }

    public void testGlobalCheckpointUpdate() {
        final long initialClusterStateVersion = randomNonNegativeLong();
        Map<AllocationId, Long> allocations = new HashMap<>();
        Map<AllocationId, Long> activeWithCheckpoints = randomAllocationsWithLocalCheckpoints(1, 5);
        Set<AllocationId> active = new HashSet<>(activeWithCheckpoints.keySet());
        allocations.putAll(activeWithCheckpoints);
        Map<AllocationId, Long> initializingWithCheckpoints = randomAllocationsWithLocalCheckpoints(0, 5);
        Set<AllocationId> initializing = new HashSet<>(initializingWithCheckpoints.keySet());
        allocations.putAll(initializingWithCheckpoints);
        assertThat(allocations.size(), equalTo(active.size() + initializing.size()));

        // note: allocations can never be empty in practice as we always have at least one primary shard active/in sync
        // it is however nice not to assume this on this level and check we do the right thing.
        final long minLocalCheckpoint = allocations.values().stream().min(Long::compare).orElse(UNASSIGNED_SEQ_NO);


        final AllocationId primaryId = active.iterator().next();
        final ReplicationTracker tracker = newTracker(primaryId);
        assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

        logger.info("--> using allocations");
        allocations.keySet().forEach(aId -> {
            final String type;
            if (active.contains(aId)) {
                type = "active";
            } else if (initializing.contains(aId)) {
                type = "init";
            } else {
                throw new IllegalStateException(aId + " not found in any map");
            }
            logger.info("  - [{}], local checkpoint [{}], [{}]", aId, allocations.get(aId), type);
        });

        tracker.updateFromMaster(initialClusterStateVersion, ids(active), routingTable(initializing, primaryId));
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        assertThat(tracker.getReplicationGroup().getReplicationTargets().size(), equalTo(1));
        initializing.forEach(aId -> markAsTrackingAndInSyncQuietly(tracker, aId.getId(), NO_OPS_PERFORMED));
        assertThat(tracker.getReplicationGroup().getReplicationTargets().size(), equalTo(1 + initializing.size()));
        allocations.keySet().forEach(aId -> updateLocalCheckpoint(tracker, aId.getId(), allocations.get(aId)));

        assertThat(tracker.getGlobalCheckpoint(), equalTo(minLocalCheckpoint));

        // increment checkpoints
        active.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        initializing.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        allocations.keySet().forEach(aId -> updateLocalCheckpoint(tracker, aId.getId(), allocations.get(aId)));

        final long minLocalCheckpointAfterUpdates =
                allocations.entrySet().stream().map(Map.Entry::getValue).min(Long::compareTo).orElse(UNASSIGNED_SEQ_NO);

        // now insert an unknown active/insync id , the checkpoint shouldn't change but a refresh should be requested.
        final AllocationId extraId = AllocationId.newInitializing();

        // first check that adding it without the master blessing doesn't change anything.
        updateLocalCheckpoint(tracker, extraId.getId(), minLocalCheckpointAfterUpdates + 1 + randomInt(4));
        assertNull(tracker.checkpoints.get(extraId.getId()));
        expectThrows(IllegalStateException.class, () -> tracker.initiateTracking(extraId.getId()));

        Set<AllocationId> newInitializing = new HashSet<>(initializing);
        newInitializing.add(extraId);
        tracker.updateFromMaster(initialClusterStateVersion + 1, ids(active), routingTable(newInitializing, primaryId));

        addPeerRecoveryRetentionLease(tracker, extraId);
        tracker.initiateTracking(extraId.getId());

        // now notify for the new id
        if (randomBoolean()) {
            updateLocalCheckpoint(tracker, extraId.getId(), minLocalCheckpointAfterUpdates + 1 + randomInt(4));
            markAsTrackingAndInSyncQuietly(tracker, extraId.getId(), randomInt((int) minLocalCheckpointAfterUpdates));
        } else {
            markAsTrackingAndInSyncQuietly(tracker, extraId.getId(), minLocalCheckpointAfterUpdates + 1 + randomInt(4));
        }

        // now it should be incremented
        assertThat(tracker.getGlobalCheckpoint(), greaterThan(minLocalCheckpoint));
    }

    public void testUpdateGlobalCheckpointOnReplica() {
        final AllocationId active = AllocationId.newInitializing();
        final ReplicationTracker tracker = newTracker(active);
        final long globalCheckpoint = randomLongBetween(NO_OPS_PERFORMED, Long.MAX_VALUE - 1);
        tracker.updateGlobalCheckpointOnReplica(globalCheckpoint, "test");
        assertThat(updatedGlobalCheckpoint.get(), equalTo(globalCheckpoint));
        final long nonUpdate = randomLongBetween(NO_OPS_PERFORMED, globalCheckpoint);
        updatedGlobalCheckpoint.set(UNASSIGNED_SEQ_NO);
        tracker.updateGlobalCheckpointOnReplica(nonUpdate, "test");
        assertThat(updatedGlobalCheckpoint.get(), equalTo(UNASSIGNED_SEQ_NO));
        final long update = randomLongBetween(globalCheckpoint, Long.MAX_VALUE);
        tracker.updateGlobalCheckpointOnReplica(update, "test");
        assertThat(updatedGlobalCheckpoint.get(), equalTo(update));
    }

    public void testMarkAllocationIdAsInSync() throws Exception {
        final long initialClusterStateVersion = randomNonNegativeLong();
        Map<AllocationId, Long> activeWithCheckpoints = randomAllocationsWithLocalCheckpoints(1, 1);
        Set<AllocationId> active = new HashSet<>(activeWithCheckpoints.keySet());
        Map<AllocationId, Long> initializingWithCheckpoints = randomAllocationsWithLocalCheckpoints(1, 1);
        Set<AllocationId> initializing = new HashSet<>(initializingWithCheckpoints.keySet());
        final AllocationId primaryId = active.iterator().next();
        final AllocationId replicaId = initializing.iterator().next();
        final ReplicationTracker tracker = newTracker(primaryId);
        tracker.updateFromMaster(initialClusterStateVersion, ids(active), routingTable(initializing, primaryId));
        final long localCheckpoint = randomLongBetween(0, Long.MAX_VALUE - 1);
        tracker.activatePrimaryMode(localCheckpoint);
        addPeerRecoveryRetentionLease(tracker, replicaId);
        tracker.initiateTracking(replicaId.getId());
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
                tracker.markAllocationIdAsInSync(
                        replicaId.getId(),
                        randomLongBetween(NO_OPS_PERFORMED, localCheckpoint - 1));
                barrier.await();
            } catch (BrokenBarrierException | InterruptedException e) {
                throw new AssertionError(e);
            }
        });
        thread.start();
        barrier.await();
        assertBusy(() -> assertTrue(tracker.pendingInSync()));
        final long updatedLocalCheckpoint = randomLongBetween(1 + localCheckpoint, Long.MAX_VALUE);
        // there is a shard copy pending in sync, the global checkpoint can not advance
        updatedGlobalCheckpoint.set(UNASSIGNED_SEQ_NO);
        tracker.updateLocalCheckpoint(primaryId.getId(), updatedLocalCheckpoint);
        assertThat(updatedGlobalCheckpoint.get(), equalTo(UNASSIGNED_SEQ_NO));
        // we are implicitly marking the pending in sync copy as in sync with the current global checkpoint, no advancement should occur
        tracker.updateLocalCheckpoint(replicaId.getId(), localCheckpoint);
        assertThat(updatedGlobalCheckpoint.get(), equalTo(UNASSIGNED_SEQ_NO));
        barrier.await();
        thread.join();
        // now we expect that the global checkpoint would advance
        tracker.markAllocationIdAsInSync(replicaId.getId(), updatedLocalCheckpoint);
        assertThat(updatedGlobalCheckpoint.get(), equalTo(updatedLocalCheckpoint));
    }

    public void testMissingActiveIdsPreventAdvance() {
        final Map<AllocationId, Long> active = randomAllocationsWithLocalCheckpoints(2, 5);
        final Map<AllocationId, Long> initializing = randomAllocationsWithLocalCheckpoints(0, 5);
        final Map<AllocationId, Long> assigned = new HashMap<>();
        assigned.putAll(active);
        assigned.putAll(initializing);
        AllocationId primaryId = active.keySet().iterator().next();
        final ReplicationTracker tracker = newTracker(primaryId);
        tracker.updateFromMaster(randomNonNegativeLong(), ids(active.keySet()), routingTable(initializing.keySet(), primaryId));
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        randomSubsetOf(initializing.keySet()).forEach(k -> markAsTrackingAndInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        final AllocationId missingActiveID = randomFrom(active.keySet());
        assigned
                .entrySet()
                .stream()
                .filter(e -> e.getKey().equals(missingActiveID) == false)
                .forEach(e -> updateLocalCheckpoint(tracker, e.getKey().getId(), e.getValue()));

        if (missingActiveID.equals(primaryId) == false) {
            assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
            assertThat(updatedGlobalCheckpoint.get(), equalTo(UNASSIGNED_SEQ_NO));
        }
        // now update all knowledge of all shards
        assigned.forEach((aid, localCP) -> updateLocalCheckpoint(tracker, aid.getId(), localCP));
        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
        assertThat(updatedGlobalCheckpoint.get(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testMissingInSyncIdsPreventAdvance() {
        final Map<AllocationId, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> initializing = randomAllocationsWithLocalCheckpoints(2, 5);
        logger.info("active: {}, initializing: {}", active, initializing);

        AllocationId primaryId = active.keySet().iterator().next();
        final ReplicationTracker tracker = newTracker(primaryId);
        tracker.updateFromMaster(randomNonNegativeLong(), ids(active.keySet()), routingTable(initializing.keySet(), primaryId));
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        randomSubsetOf(randomIntBetween(1, initializing.size() - 1),
            initializing.keySet()).forEach(aId -> markAsTrackingAndInSyncQuietly(tracker, aId.getId(), NO_OPS_PERFORMED));

        active.forEach((aid, localCP) -> updateLocalCheckpoint(tracker, aid.getId(), localCP));

        assertThat(tracker.getGlobalCheckpoint(), equalTo(NO_OPS_PERFORMED));
        assertThat(updatedGlobalCheckpoint.get(), equalTo(NO_OPS_PERFORMED));

        // update again
        initializing.forEach((aid, localCP) -> updateLocalCheckpoint(tracker, aid.getId(), localCP));
        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
        assertThat(updatedGlobalCheckpoint.get(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreIgnoredIfNotValidatedByMaster() {
        final Map<AllocationId, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> initializing = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> nonApproved = randomAllocationsWithLocalCheckpoints(1, 5);
        final AllocationId primaryId = active.keySet().iterator().next();
        final ReplicationTracker tracker = newTracker(primaryId);
        tracker.updateFromMaster(randomNonNegativeLong(), ids(active.keySet()), routingTable(initializing.keySet(), primaryId));
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        initializing.keySet().forEach(k -> markAsTrackingAndInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        nonApproved.keySet().forEach(k ->
            expectThrows(IllegalStateException.class, () -> markAsTrackingAndInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED)));

        List<Map<AllocationId, Long>> allocations = Arrays.asList(active, initializing, nonApproved);
        Collections.shuffle(allocations, random());
        allocations.forEach(a -> a.forEach((aid, localCP) -> updateLocalCheckpoint(tracker, aid.getId(), localCP)));

        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreRemovedIfNotValidatedByMaster() {
        final long initialClusterStateVersion = randomNonNegativeLong();
        final Map<AllocationId, Long> activeToStay = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> initializingToStay = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> activeToBeRemoved = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> initializingToBeRemoved = randomAllocationsWithLocalCheckpoints(1, 5);
        final Set<AllocationId> active = Sets.union(activeToStay.keySet(), activeToBeRemoved.keySet());
        final Set<AllocationId> initializing = Sets.union(initializingToStay.keySet(), initializingToBeRemoved.keySet());
        final Map<AllocationId, Long> allocations = new HashMap<>();
        final AllocationId primaryId = active.iterator().next();
        if (activeToBeRemoved.containsKey(primaryId)) {
            activeToStay.put(primaryId, activeToBeRemoved.remove(primaryId));
        }
        allocations.putAll(activeToStay);
        if (randomBoolean()) {
            allocations.putAll(activeToBeRemoved);
        }
        allocations.putAll(initializingToStay);
        if (randomBoolean()) {
            allocations.putAll(initializingToBeRemoved);
        }
        final ReplicationTracker tracker = newTracker(primaryId);
        tracker.updateFromMaster(initialClusterStateVersion, ids(active), routingTable(initializing, primaryId));
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        if (randomBoolean()) {
            initializingToStay.keySet().forEach(k -> markAsTrackingAndInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        } else {
            initializing.forEach(k -> markAsTrackingAndInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        }
        if (randomBoolean()) {
            allocations.forEach((aid, localCP) -> updateLocalCheckpoint(tracker, aid.getId(), localCP));
        }

        // now remove shards
        if (randomBoolean()) {
            tracker.updateFromMaster(
                    initialClusterStateVersion + 1,
                    ids(activeToStay.keySet()),
                    routingTable(initializingToStay.keySet(), primaryId));
            allocations.forEach((aid, ckp) -> updateLocalCheckpoint(tracker, aid.getId(), ckp + 10L));
        } else {
            allocations.forEach((aid, ckp) -> updateLocalCheckpoint(tracker, aid.getId(), ckp + 10L));
            tracker.updateFromMaster(
                    initialClusterStateVersion + 2,
                    ids(activeToStay.keySet()),
                    routingTable(initializingToStay.keySet(), primaryId));
        }

        final long checkpoint = Stream.concat(activeToStay.values().stream(), initializingToStay.values().stream())
            .min(Long::compare).get() + 10; // we added 10 to make sure it's advanced in the second time

        assertThat(tracker.getGlobalCheckpoint(), equalTo(checkpoint));
    }

    public void testWaitForAllocationIdToBeInSync() throws Exception {
        final int localCheckpoint = randomIntBetween(1, 32);
        final int globalCheckpoint = randomIntBetween(localCheckpoint + 1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean complete = new AtomicBoolean();
        final AllocationId inSyncAllocationId = AllocationId.newInitializing();
        final AllocationId trackingAllocationId = AllocationId.newInitializing();
        final ReplicationTracker tracker = newTracker(inSyncAllocationId);
        final long clusterStateVersion = randomNonNegativeLong();
        tracker.updateFromMaster(clusterStateVersion, Collections.singleton(inSyncAllocationId.getId()),
            routingTable(Collections.singleton(trackingAllocationId), inSyncAllocationId));
        tracker.activatePrimaryMode(globalCheckpoint);
        addPeerRecoveryRetentionLease(tracker, trackingAllocationId);
        final Thread thread = new Thread(() -> {
            try {
                // synchronize starting with the test thread
                barrier.await();
                tracker.initiateTracking(trackingAllocationId.getId());
                tracker.markAllocationIdAsInSync(trackingAllocationId.getId(), localCheckpoint);
                complete.set(true);
                // synchronize with the test thread checking if we are no longer waiting
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();

        // synchronize starting with the waiting thread
        barrier.await();

        final List<Integer> elements = IntStream.rangeClosed(0, globalCheckpoint - 1).boxed().collect(Collectors.toList());
        Randomness.shuffle(elements);
        for (int i = 0; i < elements.size(); i++) {
            updateLocalCheckpoint(tracker, trackingAllocationId.getId(), elements.get(i));
            assertFalse(complete.get());
            assertFalse(tracker.getTrackedLocalCheckpointForShard(trackingAllocationId.getId()).inSync);
            assertBusy(() -> assertTrue(tracker.pendingInSync.contains(trackingAllocationId.getId())));
        }

        if (randomBoolean()) {
            // normal path, shard catches up
            updateLocalCheckpoint(tracker, trackingAllocationId.getId(), randomIntBetween(globalCheckpoint, 64));
            // synchronize with the waiting thread to mark that it is complete
            barrier.await();
            assertTrue(complete.get());
            assertTrue(tracker.getTrackedLocalCheckpointForShard(trackingAllocationId.getId()).inSync);
        } else {
            // master changes its mind and cancels the allocation
            tracker.updateFromMaster(clusterStateVersion + 1, Collections.singleton(inSyncAllocationId.getId()),
                routingTable(emptySet(), inSyncAllocationId));
            barrier.await();
            assertTrue(complete.get());
            assertNull(tracker.getTrackedLocalCheckpointForShard(trackingAllocationId.getId()));
        }
        assertFalse(tracker.pendingInSync.contains(trackingAllocationId.getId()));
        thread.join();
    }

    private AtomicLong updatedGlobalCheckpoint = new AtomicLong(UNASSIGNED_SEQ_NO);

    private ReplicationTracker newTracker(final AllocationId allocationId) {
        return newTracker(allocationId, updatedGlobalCheckpoint::set, () -> 0L);
    }

    public void testWaitForAllocationIdToBeInSyncCanBeInterrupted() throws BrokenBarrierException, InterruptedException {
        final int localCheckpoint = randomIntBetween(1, 32);
        final int globalCheckpoint = randomIntBetween(localCheckpoint + 1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final AllocationId inSyncAllocationId = AllocationId.newInitializing();
        final AllocationId trackingAllocationId = AllocationId.newInitializing();
        final ReplicationTracker tracker = newTracker(inSyncAllocationId);
        tracker.updateFromMaster(randomNonNegativeLong(), Collections.singleton(inSyncAllocationId.getId()),
            routingTable(Collections.singleton(trackingAllocationId), inSyncAllocationId));
        tracker.activatePrimaryMode(globalCheckpoint);
        addPeerRecoveryRetentionLease(tracker, trackingAllocationId);
        final Thread thread = new Thread(() -> {
            try {
                // synchronize starting with the test thread
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                tracker.initiateTracking(trackingAllocationId.getId());
                tracker.markAllocationIdAsInSync(trackingAllocationId.getId(), localCheckpoint);
            } catch (final InterruptedException e) {
                interrupted.set(true);
                // synchronize with the test thread checking if we are interrupted
            }
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();

        // synchronize starting with the waiting thread
        barrier.await();

        thread.interrupt();

        // synchronize with the waiting thread to mark that it is complete
        barrier.await();

        assertTrue(interrupted.get());

        thread.join();
    }

    public void testUpdateAllocationIdsFromMaster() throws Exception {
        final long initialClusterStateVersion = randomNonNegativeLong();
        final int numberOfActiveAllocationsIds = randomIntBetween(2, 16);
        final int numberOfInitializingIds = randomIntBetween(2, 16);
        final Tuple<Set<AllocationId>, Set<AllocationId>> activeAndInitializingAllocationIds =
                randomActiveAndInitializingAllocationIds(numberOfActiveAllocationsIds, numberOfInitializingIds);
        final Set<AllocationId> activeAllocationIds = activeAndInitializingAllocationIds.v1();
        final Set<AllocationId> initializingIds = activeAndInitializingAllocationIds.v2();
        AllocationId primaryId = activeAllocationIds.iterator().next();
        IndexShardRoutingTable routingTable = routingTable(initializingIds, primaryId);
        final ReplicationTracker tracker = newTracker(primaryId);
        tracker.updateFromMaster(initialClusterStateVersion, ids(activeAllocationIds), routingTable);
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        assertThat(tracker.getReplicationGroup().getInSyncAllocationIds(), equalTo(ids(activeAllocationIds)));
        assertThat(tracker.getReplicationGroup().getRoutingTable(), equalTo(routingTable));

        // first we assert that the in-sync and tracking sets are set up correctly
        assertTrue(activeAllocationIds.stream().allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).inSync));
        assertTrue(
                activeAllocationIds
                        .stream()
                        .filter(a -> a.equals(primaryId) == false)
                        .allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).getLocalCheckpoint()
                            == SequenceNumbers.UNASSIGNED_SEQ_NO));
        assertTrue(initializingIds.stream().noneMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).inSync));
        assertTrue(
                initializingIds
                        .stream()
                        .filter(a -> a.equals(primaryId) == false)
                        .allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).getLocalCheckpoint()
                            == SequenceNumbers.UNASSIGNED_SEQ_NO));

        // now we will remove some allocation IDs from these and ensure that they propagate through
        final Set<AllocationId> removingActiveAllocationIds = new HashSet<>(randomSubsetOf(activeAllocationIds));
        removingActiveAllocationIds.remove(primaryId);
        final Set<AllocationId> newActiveAllocationIds =
                activeAllocationIds.stream().filter(a -> removingActiveAllocationIds.contains(a) == false).collect(Collectors.toSet());
        final List<AllocationId> removingInitializingAllocationIds = randomSubsetOf(initializingIds);
        final Set<AllocationId> newInitializingAllocationIds =
                initializingIds.stream().filter(a -> removingInitializingAllocationIds.contains(a) == false).collect(Collectors.toSet());
        routingTable = routingTable(newInitializingAllocationIds, primaryId);
        tracker.updateFromMaster(initialClusterStateVersion + 1, ids(newActiveAllocationIds), routingTable);
        assertTrue(newActiveAllocationIds.stream().allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).inSync));
        assertTrue(removingActiveAllocationIds.stream().allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()) == null));
        assertTrue(newInitializingAllocationIds.stream().noneMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).inSync));
        assertTrue(removingInitializingAllocationIds.stream().allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()) == null));
        assertThat(tracker.getReplicationGroup().getInSyncAllocationIds(), equalTo(
            ids(Sets.difference(Sets.union(activeAllocationIds, newActiveAllocationIds), removingActiveAllocationIds))));
        assertThat(tracker.getReplicationGroup().getRoutingTable(), equalTo(routingTable));

        /*
         * Now we will add an allocation ID to each of active and initializing and ensure they propagate through. Using different lengths
         * than we have been using above ensures that we can not collide with a previous allocation ID
         */
        newInitializingAllocationIds.add(AllocationId.newInitializing());
        tracker.updateFromMaster(
                initialClusterStateVersion + 2,
                ids(newActiveAllocationIds),
                routingTable(newInitializingAllocationIds, primaryId));
        assertTrue(newActiveAllocationIds.stream().allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).inSync));
        assertTrue(
                newActiveAllocationIds
                        .stream()
                        .filter(a -> a.equals(primaryId) == false)
                        .allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).getLocalCheckpoint()
                            == SequenceNumbers.UNASSIGNED_SEQ_NO));
        assertTrue(newInitializingAllocationIds.stream().noneMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).inSync));
        assertTrue(
                newInitializingAllocationIds
                        .stream()
                        .allMatch(a -> tracker.getTrackedLocalCheckpointForShard(a.getId()).getLocalCheckpoint()
                            == SequenceNumbers.UNASSIGNED_SEQ_NO));

        // the tracking allocation IDs should play no role in determining the global checkpoint
        final Map<AllocationId, Integer> activeLocalCheckpoints =
                newActiveAllocationIds.stream().collect(Collectors.toMap(Function.identity(), a -> randomIntBetween(1, 1024)));
        activeLocalCheckpoints.forEach((a, l) -> updateLocalCheckpoint(tracker, a.getId(), l));
        final Map<AllocationId, Integer> initializingLocalCheckpoints =
                newInitializingAllocationIds.stream().collect(Collectors.toMap(Function.identity(), a -> randomIntBetween(1, 1024)));
        initializingLocalCheckpoints.forEach((a, l) -> updateLocalCheckpoint(tracker, a.getId(), l));
        assertTrue(
                activeLocalCheckpoints
                        .entrySet()
                        .stream()
                        .allMatch(e -> tracker.getTrackedLocalCheckpointForShard(e.getKey().getId()).getLocalCheckpoint() == e.getValue()));
        assertTrue(
                initializingLocalCheckpoints
                        .entrySet()
                        .stream()
                        .allMatch(e -> tracker.getTrackedLocalCheckpointForShard(e.getKey().getId()).getLocalCheckpoint() == e.getValue()));
        final long minimumActiveLocalCheckpoint = activeLocalCheckpoints.values().stream().min(Integer::compareTo).get();
        assertThat(tracker.getGlobalCheckpoint(), equalTo(minimumActiveLocalCheckpoint));
        assertThat(updatedGlobalCheckpoint.get(), equalTo(minimumActiveLocalCheckpoint));
        final long minimumInitailizingLocalCheckpoint = initializingLocalCheckpoints.values().stream().min(Integer::compareTo).get();

        // now we are going to add a new allocation ID and bring it in sync which should move it to the in-sync allocation IDs
        final long localCheckpoint =
                randomIntBetween(0, Math.toIntExact(Math.min(minimumActiveLocalCheckpoint, minimumInitailizingLocalCheckpoint) - 1));

        // using a different length than we have been using above ensures that we can not collide with a previous allocation ID
        final AllocationId newSyncingAllocationId = AllocationId.newInitializing();
        newInitializingAllocationIds.add(newSyncingAllocationId);
        tracker.updateFromMaster(
                initialClusterStateVersion + 3,
                ids(newActiveAllocationIds),
                routingTable(newInitializingAllocationIds, primaryId));
        addPeerRecoveryRetentionLease(tracker, newSyncingAllocationId);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
                tracker.initiateTracking(newSyncingAllocationId.getId());
                tracker.markAllocationIdAsInSync(newSyncingAllocationId.getId(), localCheckpoint);
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();

        barrier.await();

        assertBusy(() -> {
            assertTrue(tracker.pendingInSync.contains(newSyncingAllocationId.getId()));
            assertFalse(tracker.getTrackedLocalCheckpointForShard(newSyncingAllocationId.getId()).inSync);
        });

        tracker.updateLocalCheckpoint(newSyncingAllocationId.getId(),
            randomIntBetween(Math.toIntExact(minimumActiveLocalCheckpoint), 1024));

        barrier.await();

        assertFalse(tracker.pendingInSync.contains(newSyncingAllocationId.getId()));
        assertTrue(tracker.getTrackedLocalCheckpointForShard(newSyncingAllocationId.getId()).inSync);

        /*
         * The new in-sync allocation ID is in the in-sync set now yet the master does not know this; the allocation ID should still be in
         * the in-sync set even if we receive a cluster state update that does not reflect this.
         *
         */
        tracker.updateFromMaster(
                initialClusterStateVersion + 4,
                ids(newActiveAllocationIds),
                routingTable(newInitializingAllocationIds, primaryId));
        assertTrue(tracker.getTrackedLocalCheckpointForShard(newSyncingAllocationId.getId()).inSync);
        assertFalse(tracker.pendingInSync.contains(newSyncingAllocationId.getId()));
    }

    /**
     * If we do not update the global checkpoint in {@link ReplicationTracker#markAllocationIdAsInSync(String, long)} after adding the
     * allocation ID to the in-sync set and removing it from pending, the local checkpoint update that freed the thread waiting for the
     * local checkpoint to advance could miss updating the global checkpoint in a race if the waiting thread did not add the allocation
     * ID to the in-sync set and remove it from the pending set before the local checkpoint updating thread executed the global checkpoint
     * update. This test fails without an additional call to {@code ReplicationTracker#updateGlobalCheckpointOnPrimary()} after
     * removing the allocation ID from the pending set in {@link ReplicationTracker#markAllocationIdAsInSync(String, long)} (even if a
     * call is added after notifying all waiters in {@link ReplicationTracker#updateLocalCheckpoint(String, long)}).
     *
     * @throws InterruptedException   if the main test thread was interrupted while waiting
     * @throws BrokenBarrierException if the barrier was broken while the main test thread was waiting
     */
    public void testRaceUpdatingGlobalCheckpoint() throws InterruptedException, BrokenBarrierException {

        final AllocationId active = AllocationId.newInitializing();
        final AllocationId initializing = AllocationId.newInitializing();
        final CyclicBarrier barrier = new CyclicBarrier(4);

        final int activeLocalCheckpoint = randomIntBetween(0, Integer.MAX_VALUE - 1);
        final ReplicationTracker tracker = newTracker(active);
        tracker.updateFromMaster(
                randomNonNegativeLong(),
                Collections.singleton(active.getId()),
                routingTable(Collections.singleton(initializing), active));
        tracker.activatePrimaryMode(activeLocalCheckpoint);
        addPeerRecoveryRetentionLease(tracker, initializing);
        final int nextActiveLocalCheckpoint = randomIntBetween(activeLocalCheckpoint + 1, Integer.MAX_VALUE);
        final Thread activeThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            tracker.updateLocalCheckpoint(active.getId(), nextActiveLocalCheckpoint);
        });

        final int initializingLocalCheckpoint = randomIntBetween(0, nextActiveLocalCheckpoint - 1);
        final Thread initializingThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            tracker.updateLocalCheckpoint(initializing.getId(), nextActiveLocalCheckpoint);
        });

        final Thread markingThread = new Thread(() -> {
            try {
                barrier.await();
                tracker.initiateTracking(initializing.getId());
                tracker.markAllocationIdAsInSync(initializing.getId(), initializingLocalCheckpoint - 1);
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        activeThread.start();
        initializingThread.start();
        markingThread.start();
        barrier.await();

        activeThread.join();
        initializingThread.join();
        markingThread.join();

        assertThat(tracker.getGlobalCheckpoint(), equalTo((long) nextActiveLocalCheckpoint));
    }

    public void testPrimaryContextHandoff() throws IOException {
        final IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("test", Settings.EMPTY);
        final ShardId shardId = new ShardId("test", "_na_", 0);

        FakeClusterState clusterState = initialState();
        final AllocationId aId = clusterState.routingTable.primaryShard().allocationId();
        final LongConsumer onUpdate = updatedGlobalCheckpoint -> {};
        final long primaryTerm = randomNonNegativeLong();
        final long globalCheckpoint = UNASSIGNED_SEQ_NO;
        final BiConsumer<RetentionLeases, ActionListener<ReplicationResponse>> onNewRetentionLease =
                (leases, listener) -> {};
        ReplicationTracker oldPrimary = new ReplicationTracker(shardId, aId.getId(), indexSettings, primaryTerm, globalCheckpoint,
            onUpdate, () -> 0L, onNewRetentionLease, OPS_BASED_RECOVERY_ALWAYS_REASONABLE);
        ReplicationTracker newPrimary = new ReplicationTracker(shardId, aId.getRelocationId(), indexSettings, primaryTerm, globalCheckpoint,
            onUpdate, () -> 0L, onNewRetentionLease, OPS_BASED_RECOVERY_ALWAYS_REASONABLE);

        Set<String> allocationIds = new HashSet<>(Arrays.asList(oldPrimary.shardAllocationId, newPrimary.shardAllocationId));

        clusterState.apply(oldPrimary);
        clusterState.apply(newPrimary);

        oldPrimary.activatePrimaryMode(randomIntBetween(Math.toIntExact(NO_OPS_PERFORMED), 10));
        addPeerRecoveryRetentionLease(oldPrimary, newPrimary.shardAllocationId);
        newPrimary.updateRetentionLeasesOnReplica(oldPrimary.getRetentionLeases());

        final int numUpdates = randomInt(10);
        for (int i = 0; i < numUpdates; i++) {
            if (rarely()) {
                clusterState = randomUpdateClusterState(allocationIds, clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
            }
            if (randomBoolean()) {
                randomLocalCheckpointUpdate(oldPrimary);
            }
            if (randomBoolean()) {
                randomMarkInSync(oldPrimary, newPrimary);
            }
        }

        // simulate transferring the global checkpoint to the new primary after finalizing recovery before the handoff
        markAsTrackingAndInSyncQuietly(
                oldPrimary,
                newPrimary.shardAllocationId,
                Math.max(SequenceNumbers.NO_OPS_PERFORMED, oldPrimary.getGlobalCheckpoint() + randomInt(5)));
        oldPrimary.updateGlobalCheckpointForShard(newPrimary.shardAllocationId, oldPrimary.getGlobalCheckpoint());
        ReplicationTracker.PrimaryContext primaryContext = oldPrimary.startRelocationHandoff(newPrimary.shardAllocationId);

        if (randomBoolean()) {
            // cluster state update after primary context handoff
            if (randomBoolean()) {
                clusterState = randomUpdateClusterState(allocationIds, clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
            }

            // abort handoff, check that we can continue updates and retry handoff
            oldPrimary.abortRelocationHandoff();

            if (rarely()) {
                clusterState = randomUpdateClusterState(allocationIds, clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
            }
            if (randomBoolean()) {
                randomLocalCheckpointUpdate(oldPrimary);
            }
            if (randomBoolean()) {
                randomMarkInSync(oldPrimary, newPrimary);
            }

            // do another handoff
            primaryContext = oldPrimary.startRelocationHandoff(newPrimary.shardAllocationId);
        }

        // send primary context through the wire
        BytesStreamOutput output = new BytesStreamOutput();
        primaryContext.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        primaryContext = new ReplicationTracker.PrimaryContext(streamInput);
        switch (randomInt(3)) {
            case 0: {
                // apply cluster state update on old primary while primary context is being transferred
                clusterState = randomUpdateClusterState(allocationIds, clusterState);
                clusterState.apply(oldPrimary);
                // activate new primary
                newPrimary.activateWithPrimaryContext(primaryContext);
                // apply cluster state update on new primary so that the states on old and new primary are comparable
                clusterState.apply(newPrimary);
                break;
            }
            case 1: {
                // apply cluster state update on new primary while primary context is being transferred
                clusterState = randomUpdateClusterState(allocationIds, clusterState);
                clusterState.apply(newPrimary);
                // activate new primary
                newPrimary.activateWithPrimaryContext(primaryContext);
                // apply cluster state update on old primary so that the states on old and new primary are comparable
                clusterState.apply(oldPrimary);
                break;
            }
            case 2: {
                // apply cluster state update on both copies while primary context is being transferred
                clusterState = randomUpdateClusterState(allocationIds, clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
                newPrimary.activateWithPrimaryContext(primaryContext);
                break;
            }
            case 3: {
                // no cluster state update
                newPrimary.activateWithPrimaryContext(primaryContext);
                break;
            }
        }

        assertTrue(oldPrimary.primaryMode);
        assertTrue(newPrimary.primaryMode);
        assertThat(newPrimary.appliedClusterStateVersion, equalTo(oldPrimary.appliedClusterStateVersion));
        /*
         * We can not assert on shared knowledge of the global checkpoint between the old primary and the new primary as the new primary
         * will update its global checkpoint state without the old primary learning of it, and the old primary could have updated its
         * global checkpoint state after the primary context was transferred.
         */
        Map<String, ReplicationTracker.CheckpointState> oldPrimaryCheckpointsCopy = new HashMap<>(oldPrimary.checkpoints);
        oldPrimaryCheckpointsCopy.remove(oldPrimary.shardAllocationId);
        oldPrimaryCheckpointsCopy.remove(newPrimary.shardAllocationId);
        Map<String, ReplicationTracker.CheckpointState> newPrimaryCheckpointsCopy = new HashMap<>(newPrimary.checkpoints);
        newPrimaryCheckpointsCopy.remove(oldPrimary.shardAllocationId);
        newPrimaryCheckpointsCopy.remove(newPrimary.shardAllocationId);
        assertThat(newPrimaryCheckpointsCopy, equalTo(oldPrimaryCheckpointsCopy));
        // we can however assert that shared knowledge of the local checkpoint and in-sync status is equal
        assertThat(
                oldPrimary.checkpoints.get(oldPrimary.shardAllocationId).localCheckpoint,
                equalTo(newPrimary.checkpoints.get(oldPrimary.shardAllocationId).localCheckpoint));
        assertThat(
                oldPrimary.checkpoints.get(newPrimary.shardAllocationId).localCheckpoint,
                equalTo(newPrimary.checkpoints.get(newPrimary.shardAllocationId).localCheckpoint));
        assertThat(
                oldPrimary.checkpoints.get(oldPrimary.shardAllocationId).inSync,
                equalTo(newPrimary.checkpoints.get(oldPrimary.shardAllocationId).inSync));
        assertThat(
                oldPrimary.checkpoints.get(newPrimary.shardAllocationId).inSync,
                equalTo(newPrimary.checkpoints.get(newPrimary.shardAllocationId).inSync));
        assertThat(newPrimary.getGlobalCheckpoint(), equalTo(oldPrimary.getGlobalCheckpoint()));
        assertThat(newPrimary.routingTable, equalTo(oldPrimary.routingTable));
        assertThat(newPrimary.replicationGroup, equalTo(oldPrimary.replicationGroup));

        assertFalse(oldPrimary.relocated);
        oldPrimary.completeRelocationHandoff();
        assertFalse(oldPrimary.primaryMode);
        assertTrue(oldPrimary.relocated);
    }

    public void testIllegalStateExceptionIfUnknownAllocationId() {
        final AllocationId active = AllocationId.newInitializing();
        final AllocationId initializing = AllocationId.newInitializing();
        final ReplicationTracker tracker = newTracker(active);
        tracker.updateFromMaster(randomNonNegativeLong(), Collections.singleton(active.getId()),
            routingTable(Collections.singleton(initializing), active));
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);

        expectThrows(IllegalStateException.class, () -> tracker.initiateTracking(randomAlphaOfLength(10)));
        expectThrows(IllegalStateException.class, () -> tracker.markAllocationIdAsInSync(randomAlphaOfLength(10), randomNonNegativeLong()));
    }

    private static class FakeClusterState {
        final long version;
        final Set<AllocationId> inSyncIds;
        final IndexShardRoutingTable routingTable;

        private FakeClusterState(long version, Set<AllocationId> inSyncIds, IndexShardRoutingTable routingTable) {
            this.version = version;
            this.inSyncIds = Collections.unmodifiableSet(inSyncIds);
            this.routingTable = routingTable;
        }

        public Set<AllocationId> allIds() {
            return Sets.union(initializingIds(), inSyncIds);
        }

        public Set<AllocationId> initializingIds() {
            return routingTable.getAllInitializingShards().stream()
                .map(ShardRouting::allocationId).collect(Collectors.toSet());
        }

        public void apply(ReplicationTracker gcp) {
            gcp.updateFromMaster(version, ids(inSyncIds), routingTable);
        }
    }

    private static FakeClusterState initialState() {
        final long initialClusterStateVersion = randomIntBetween(1, Integer.MAX_VALUE);
        final int numberOfActiveAllocationsIds = randomIntBetween(1, 8);
        final int numberOfInitializingIds = randomIntBetween(0, 8);
        final Tuple<Set<AllocationId>, Set<AllocationId>> activeAndInitializingAllocationIds =
                randomActiveAndInitializingAllocationIds(numberOfActiveAllocationsIds, numberOfInitializingIds);
        final Set<AllocationId> activeAllocationIds = activeAndInitializingAllocationIds.v1();
        final Set<AllocationId> initializingAllocationIds = activeAndInitializingAllocationIds.v2();
        final AllocationId primaryId = randomFrom(activeAllocationIds);
        final AllocationId relocatingId = AllocationId.newRelocation(primaryId);
        activeAllocationIds.remove(primaryId);
        activeAllocationIds.add(relocatingId);
        final ShardId shardId = new ShardId("test", "_na_", 0);
        final ShardRouting primaryShard =
                TestShardRouting.newShardRouting(
                    shardId,
                    nodeIdFromAllocationId(relocatingId),
                    nodeIdFromAllocationId(AllocationId.newInitializing(relocatingId.getRelocationId())),
                    true, ShardRoutingState.RELOCATING, relocatingId);

        return new FakeClusterState(
                initialClusterStateVersion,
                activeAllocationIds,
                routingTable(initializingAllocationIds, primaryShard));
    }

    private static void randomLocalCheckpointUpdate(ReplicationTracker gcp) {
        String allocationId = randomFrom(gcp.checkpoints.keySet());
        long currentLocalCheckpoint = gcp.checkpoints.get(allocationId).getLocalCheckpoint();
        gcp.updateLocalCheckpoint(allocationId, Math.max(SequenceNumbers.NO_OPS_PERFORMED, currentLocalCheckpoint + randomInt(5)));
    }

    private static void randomMarkInSync(ReplicationTracker oldPrimary, ReplicationTracker newPrimary) {
        final String allocationId = randomFrom(oldPrimary.checkpoints.keySet());
        final long newLocalCheckpoint = Math.max(NO_OPS_PERFORMED, oldPrimary.getGlobalCheckpoint() + randomInt(5));
        markAsTrackingAndInSyncQuietly(oldPrimary, allocationId, newLocalCheckpoint);
        newPrimary.updateRetentionLeasesOnReplica(oldPrimary.getRetentionLeases());
    }

    private static FakeClusterState randomUpdateClusterState(Set<String> allocationIds, FakeClusterState clusterState) {
        final Set<AllocationId> initializingIdsToAdd =
                randomAllocationIdsExcludingExistingIds(exclude(clusterState.allIds(), allocationIds), randomInt(2));
        final Set<AllocationId> initializingIdsToRemove = new HashSet<>(
            exclude(randomSubsetOf(randomInt(clusterState.initializingIds().size()), clusterState.initializingIds()), allocationIds));
        final Set<AllocationId> inSyncIdsToRemove = new HashSet<>(
            exclude(randomSubsetOf(randomInt(clusterState.inSyncIds.size()), clusterState.inSyncIds), allocationIds));
        final Set<AllocationId> remainingInSyncIds = Sets.difference(clusterState.inSyncIds, inSyncIdsToRemove);
        final Set<AllocationId> initializingIdsExceptRelocationTargets = exclude(clusterState.initializingIds(),
            clusterState.routingTable.activeShards().stream().filter(ShardRouting::relocating)
                .map(s -> s.allocationId().getRelocationId()).collect(Collectors.toSet()));
        return new FakeClusterState(
                clusterState.version + randomIntBetween(1, 5),
                remainingInSyncIds.isEmpty() ? clusterState.inSyncIds : remainingInSyncIds,
                routingTable(
                        Sets.difference(Sets.union(initializingIdsExceptRelocationTargets, initializingIdsToAdd), initializingIdsToRemove),
                        clusterState.routingTable.primaryShard()));
    }

    private static Set<AllocationId> exclude(Collection<AllocationId> allocationIds, Set<String> excludeIds) {
        return allocationIds.stream().filter(aId -> excludeIds.contains(aId.getId()) == false).collect(Collectors.toSet());
    }

    private static Tuple<Set<AllocationId>, Set<AllocationId>> randomActiveAndInitializingAllocationIds(
            final int numberOfActiveAllocationsIds,
            final int numberOfInitializingIds) {
        final Set<AllocationId> activeAllocationIds =
            IntStream.range(0, numberOfActiveAllocationsIds).mapToObj(i -> AllocationId.newInitializing()).collect(Collectors.toSet());
        final Set<AllocationId> initializingIds = randomAllocationIdsExcludingExistingIds(activeAllocationIds, numberOfInitializingIds);
        return Tuple.tuple(activeAllocationIds, initializingIds);
    }

    private static Set<AllocationId> randomAllocationIdsExcludingExistingIds(final Set<AllocationId> existingAllocationIds,
                                                                             final int numberOfAllocationIds) {
        return IntStream.range(0, numberOfAllocationIds).mapToObj(i -> {
            do {
                final AllocationId newAllocationId = AllocationId.newInitializing();
                // ensure we do not duplicate an allocation ID
                if (existingAllocationIds.contains(newAllocationId) == false) {
                    return newAllocationId;
                }
            } while (true);
        }).collect(Collectors.toSet());
    }

    private static void markAsTrackingAndInSyncQuietly(
        final ReplicationTracker tracker, final String allocationId, final long localCheckpoint) {
        try {
            addPeerRecoveryRetentionLease(tracker, allocationId);
            tracker.initiateTracking(allocationId);
            tracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void addPeerRecoveryRetentionLease(final ReplicationTracker tracker, final AllocationId allocationId) {
        final String nodeId = nodeIdFromAllocationId(allocationId);
        if (tracker.getRetentionLeases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(nodeId)) == false) {
            tracker.addPeerRecoveryRetentionLease(nodeId, NO_OPS_PERFORMED, ActionListener.wrap(() -> { }));
        }
    }

    private static void addPeerRecoveryRetentionLease(final ReplicationTracker tracker, final String allocationId) {
        addPeerRecoveryRetentionLease(tracker, AllocationId.newInitializing(allocationId));
    }

    public void testPeerRecoveryRetentionLeaseCreationAndRenewal() {

        final int numberOfActiveAllocationsIds = randomIntBetween(1, 8);
        final int numberOfInitializingIds = randomIntBetween(0, 8);
        final Tuple<Set<AllocationId>, Set<AllocationId>> activeAndInitializingAllocationIds =
            randomActiveAndInitializingAllocationIds(numberOfActiveAllocationsIds, numberOfInitializingIds);
        final Set<AllocationId> activeAllocationIds = activeAndInitializingAllocationIds.v1();
        final Set<AllocationId> initializingAllocationIds = activeAndInitializingAllocationIds.v2();

        final AllocationId primaryId = activeAllocationIds.iterator().next();

        final long initialClusterStateVersion = randomNonNegativeLong();

        final AtomicLong currentTimeMillis = new AtomicLong(0L);
        final ReplicationTracker tracker = newTracker(primaryId, updatedGlobalCheckpoint::set, currentTimeMillis::get);

        final long retentionLeaseExpiryTimeMillis = tracker.indexSettings().getRetentionLeaseMillis();
        final long peerRecoveryRetentionLeaseRenewalTimeMillis = retentionLeaseExpiryTimeMillis / 2;

        final long maximumTestTimeMillis = 13 * retentionLeaseExpiryTimeMillis;
        final long testStartTimeMillis = randomLongBetween(0L, Long.MAX_VALUE - maximumTestTimeMillis);
        currentTimeMillis.set(testStartTimeMillis);

        final Function<AllocationId, RetentionLease> retentionLeaseFromAllocationId = allocationId
            -> new RetentionLease(ReplicationTracker.getPeerRecoveryRetentionLeaseId(nodeIdFromAllocationId(allocationId)),
            0L, currentTimeMillis.get(), ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE);

        final List<RetentionLease> initialLeases = new ArrayList<>();
        if (randomBoolean()) {
            initialLeases.add(retentionLeaseFromAllocationId.apply(primaryId));
        }
        for (final AllocationId replicaId : initializingAllocationIds) {
            if (randomBoolean()) {
                initialLeases.add(retentionLeaseFromAllocationId.apply(replicaId));
            }
        }
        for (int i = randomIntBetween(0, 5); i > 0; i--) {
            initialLeases.add(retentionLeaseFromAllocationId.apply(AllocationId.newInitializing()));
        }
        tracker.updateRetentionLeasesOnReplica(new RetentionLeases(randomNonNegativeLong(), randomNonNegativeLong(), initialLeases));

        IndexShardRoutingTable routingTable = routingTable(initializingAllocationIds, primaryId);
        tracker.updateFromMaster(initialClusterStateVersion, ids(activeAllocationIds), routingTable);
        tracker.activatePrimaryMode(NO_OPS_PERFORMED);
        assertTrue("primary's retention lease should exist",
            tracker.getRetentionLeases().contains(ReplicationTracker.getPeerRecoveryRetentionLeaseId(routingTable.primaryShard())));

        final Consumer<Runnable> assertAsTimePasses = assertion -> {
            final long startTime = currentTimeMillis.get();
            while (currentTimeMillis.get() < startTime + retentionLeaseExpiryTimeMillis * 2) {
                currentTimeMillis.addAndGet(randomLongBetween(0L, retentionLeaseExpiryTimeMillis * 2));
                tracker.renewPeerRecoveryRetentionLeases();
                tracker.getRetentionLeases(true);
                assertion.run();
            }
        };

        assertAsTimePasses.accept(() -> {
            // Leases for assigned replicas do not expire
            final RetentionLeases retentionLeases = tracker.getRetentionLeases();
            for (final AllocationId replicaId : initializingAllocationIds) {
                final String leaseId = retentionLeaseFromAllocationId.apply(replicaId).id();
                assertTrue("should not have removed lease for " + replicaId + " in " + retentionLeases,
                    initialLeases.stream().noneMatch(l -> l.id().equals(leaseId)) || retentionLeases.contains(leaseId));
            }
        });

        // Leases that don't correspond to assigned replicas, however, are expired by this time.
        final Set<String> expectedLeaseIds = Stream.concat(Stream.of(primaryId), initializingAllocationIds.stream())
            .map(allocationId -> retentionLeaseFromAllocationId.apply(allocationId).id()).collect(Collectors.toSet());
        for (final RetentionLease retentionLease : tracker.getRetentionLeases().leases()) {
            assertThat(expectedLeaseIds, hasItem(retentionLease.id()));
        }

        for (AllocationId replicaId : initializingAllocationIds) {
            markAsTrackingAndInSyncQuietly(tracker, replicaId.getId(), NO_OPS_PERFORMED);
        }

        assertThat(tracker.getRetentionLeases().leases().stream().map(RetentionLease::id).collect(Collectors.toSet()),
            equalTo(expectedLeaseIds));

        assertAsTimePasses.accept(() -> {
            // Leases still don't expire
            assertThat(tracker.getRetentionLeases().leases().stream().map(RetentionLease::id).collect(Collectors.toSet()),
                equalTo(expectedLeaseIds));

            // Also leases are renewed before reaching half the expiry time
            //noinspection OptionalGetWithoutIsPresent
            assertThat(tracker.getRetentionLeases() + " renewed before too long",
                tracker.getRetentionLeases().leases().stream().mapToLong(RetentionLease::timestamp).min().getAsLong(),
                greaterThanOrEqualTo(currentTimeMillis.get() - peerRecoveryRetentionLeaseRenewalTimeMillis));
        });

        IndexShardRoutingTable.Builder routingTableBuilder = new IndexShardRoutingTable.Builder(routingTable);
        for (ShardRouting replicaShard : routingTable.replicaShards()) {
            routingTableBuilder.removeShard(replicaShard);
            routingTableBuilder.addShard(replicaShard.moveToStarted());
        }
        routingTable = routingTableBuilder.build();
        activeAllocationIds.addAll(initializingAllocationIds);

        tracker.updateFromMaster(initialClusterStateVersion + randomLongBetween(1, 10), ids(activeAllocationIds), routingTable);

        assertAsTimePasses.accept(() -> {
            // Leases still don't expire
            assertThat(tracker.getRetentionLeases().leases().stream().map(RetentionLease::id).collect(Collectors.toSet()),
                equalTo(expectedLeaseIds));
            // ... and any extra peer recovery retention leases are expired immediately since the shard is fully active
            tracker.addPeerRecoveryRetentionLease(randomAlphaOfLength(10), randomNonNegativeLong(), ActionListener.wrap(() -> {}));
        });

        tracker.renewPeerRecoveryRetentionLeases();
        assertThat(tracker.getRetentionLeases().leases().stream().map(RetentionLease::id).collect(Collectors.toSet()),
            not(equalTo(expectedLeaseIds)));
        assertThat("expired extra lease",
            tracker.getRetentionLeases(true).leases().stream().map(RetentionLease::id).collect(Collectors.toSet()),
            equalTo(expectedLeaseIds));

        final AllocationId advancingAllocationId
            = initializingAllocationIds.isEmpty() || rarely() ? primaryId : randomFrom(initializingAllocationIds);
        final String advancingLeaseId = retentionLeaseFromAllocationId.apply(advancingAllocationId).id();

        final long initialGlobalCheckpoint
            = Math.max(NO_OPS_PERFORMED, tracker.getTrackedLocalCheckpointForShard(advancingAllocationId.getId()).globalCheckpoint);
        assertThat(tracker.getRetentionLeases().get(advancingLeaseId).retainingSequenceNumber(), equalTo(initialGlobalCheckpoint + 1));
        final long newGlobalCheckpoint = initialGlobalCheckpoint + randomLongBetween(1, 1000);
        tracker.updateGlobalCheckpointForShard(advancingAllocationId.getId(), newGlobalCheckpoint);
        tracker.renewPeerRecoveryRetentionLeases();
        assertThat("lease was renewed because the shard advanced its global checkpoint",
            tracker.getRetentionLeases().get(advancingLeaseId).retainingSequenceNumber(), equalTo(newGlobalCheckpoint + 1));

        final long initialVersion = tracker.getRetentionLeases().version();
        tracker.renewPeerRecoveryRetentionLeases();
        assertThat("immediate renewal is a no-op", tracker.getRetentionLeases().version(), equalTo(initialVersion));

        //noinspection OptionalGetWithoutIsPresent
        final long millisUntilFirstRenewal
            = tracker.getRetentionLeases().leases().stream().mapToLong(RetentionLease::timestamp).min().getAsLong()
            + peerRecoveryRetentionLeaseRenewalTimeMillis
            - currentTimeMillis.get();

        if (millisUntilFirstRenewal != 0) {
            final long shorterThanRenewalTime = randomLongBetween(0L, millisUntilFirstRenewal - 1);
            currentTimeMillis.addAndGet(shorterThanRenewalTime);
            tracker.renewPeerRecoveryRetentionLeases();
            assertThat("renewal is a no-op after a short time", tracker.getRetentionLeases().version(), equalTo(initialVersion));
            currentTimeMillis.addAndGet(millisUntilFirstRenewal - shorterThanRenewalTime);
        }

        tracker.renewPeerRecoveryRetentionLeases();
        assertThat("renewal happens after a sufficiently long time", tracker.getRetentionLeases().version(), greaterThan(initialVersion));
        assertTrue("all leases were renewed",
            tracker.getRetentionLeases().leases().stream().allMatch(l -> l.timestamp() == currentTimeMillis.get()));

        assertThat("test ran for too long, potentially leading to overflow",
            currentTimeMillis.get(), lessThanOrEqualTo(testStartTimeMillis + maximumTestTimeMillis));
    }

}
