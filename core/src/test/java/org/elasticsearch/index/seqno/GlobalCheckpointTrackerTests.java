/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.index.seqno;

import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.cluster.routing.ShardRouting;
import org.elasticsearch.cluster.routing.ShardRoutingState;
import org.elasticsearch.cluster.routing.TestShardRouting;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static java.util.Collections.emptySet;
import static org.elasticsearch.index.seqno.SequenceNumbers.NO_OPS_PERFORMED;
import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;

public class GlobalCheckpointTrackerTests extends ESTestCase {

    GlobalCheckpointTracker tracker;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        tracker =
            new GlobalCheckpointTracker(
                new ShardId("test", "_na_", 0),
                IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
                UNASSIGNED_SEQ_NO);
    }

    public void testEmptyShards() {
        assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
    }

    private final AtomicInteger aIdGenerator = new AtomicInteger();

    private Map<AllocationId, Long> randomAllocationsWithLocalCheckpoints(int min, int max) {
        Map<AllocationId, Long> allocations = new HashMap<>();
        for (int i = randomIntBetween(min, max); i > 0; i--) {
            allocations.put(AllocationId.newInitializing(), (long) randomInt(1000));
        }
        return allocations;
    }

    private static IndexShardRoutingTable routingTable(Set<AllocationId> initializingIds) {
        ShardId shardId = new ShardId("test", "_na_", 0);
        IndexShardRoutingTable.Builder builder = new IndexShardRoutingTable.Builder(shardId);
        for (AllocationId initializingId : initializingIds) {
            builder.addShard(TestShardRouting.newShardRouting(shardId, randomAlphaOfLength(10), null, false, ShardRoutingState.INITIALIZING,
                initializingId));
        }
        return builder.build();
    }

    private static Set<String> ids(Set<AllocationId> allocationIds) {
        return allocationIds.stream().map(AllocationId::getId).collect(Collectors.toSet());
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

        tracker.updateFromMaster(initialClusterStateVersion, ids(active), routingTable(initializing), emptySet());
        tracker.activatePrimaryMode(active.iterator().next().getId(), NO_OPS_PERFORMED);
        initializing.forEach(aId -> markAllocationIdAsInSyncQuietly(tracker, aId.getId(), NO_OPS_PERFORMED));
        allocations.keySet().forEach(aId -> tracker.updateLocalCheckpoint(aId.getId(), allocations.get(aId)));

        assertThat(tracker.getGlobalCheckpoint(), equalTo(minLocalCheckpoint));

        // increment checkpoints
        active.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        initializing.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        allocations.keySet().forEach(aId -> tracker.updateLocalCheckpoint(aId.getId(), allocations.get(aId)));

        final long minLocalCheckpointAfterUpdates =
                allocations.entrySet().stream().map(Map.Entry::getValue).min(Long::compareTo).orElse(UNASSIGNED_SEQ_NO);

        // now insert an unknown active/insync id , the checkpoint shouldn't change but a refresh should be requested.
        final AllocationId extraId = AllocationId.newInitializing();

        // first check that adding it without the master blessing doesn't change anything.
        tracker.updateLocalCheckpoint(extraId.getId(), minLocalCheckpointAfterUpdates + 1 + randomInt(4));
        assertNull(tracker.localCheckpoints.get(extraId));
        expectThrows(IllegalStateException.class, () -> tracker.initiateTracking(extraId.getId()));

        Set<AllocationId> newInitializing = new HashSet<>(initializing);
        newInitializing.add(extraId);
        tracker.updateFromMaster(initialClusterStateVersion + 1, ids(active), routingTable(newInitializing), emptySet());

        tracker.initiateTracking(extraId.getId());

        // now notify for the new id
        if (randomBoolean()) {
            tracker.updateLocalCheckpoint(extraId.getId(), minLocalCheckpointAfterUpdates + 1 + randomInt(4));
            markAllocationIdAsInSyncQuietly(tracker, extraId.getId(), randomInt((int) minLocalCheckpointAfterUpdates));
        } else {
            markAllocationIdAsInSyncQuietly(tracker, extraId.getId(), minLocalCheckpointAfterUpdates + 1 + randomInt(4));
        }

        // now it should be incremented
        assertThat(tracker.getGlobalCheckpoint(), greaterThan(minLocalCheckpoint));
    }

    public void testMissingActiveIdsPreventAdvance() {
        final Map<AllocationId, Long> active = randomAllocationsWithLocalCheckpoints(2, 5);
        final Map<AllocationId, Long> initializing = randomAllocationsWithLocalCheckpoints(0, 5);
        final Map<AllocationId, Long> assigned = new HashMap<>();
        assigned.putAll(active);
        assigned.putAll(initializing);
        tracker.updateFromMaster(randomNonNegativeLong(), ids(active.keySet()), routingTable(initializing.keySet()), emptySet());
        AllocationId primary = active.keySet().iterator().next();
        tracker.activatePrimaryMode(primary.getId(), NO_OPS_PERFORMED);
        randomSubsetOf(initializing.keySet()).forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        final AllocationId missingActiveID = randomFrom(active.keySet());
        assigned
                .entrySet()
                .stream()
                .filter(e -> !e.getKey().equals(missingActiveID))
                .forEach(e -> tracker.updateLocalCheckpoint(e.getKey().getId(), e.getValue()));

        if (missingActiveID.equals(primary) == false) {
            assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
        }
        // now update all knowledge of all shards
        assigned.forEach((aid, localCP) -> tracker.updateLocalCheckpoint(aid.getId(), localCP));
        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testMissingInSyncIdsPreventAdvance() {
        final Map<AllocationId, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> initializing = randomAllocationsWithLocalCheckpoints(2, 5);
        logger.info("active: {}, initializing: {}", active, initializing);
        tracker.updateFromMaster(randomNonNegativeLong(), ids(active.keySet()), routingTable(initializing.keySet()), emptySet());
        AllocationId primary = active.keySet().iterator().next();
        tracker.activatePrimaryMode(primary.getId(), NO_OPS_PERFORMED);
        randomSubsetOf(randomIntBetween(1, initializing.size() - 1),
            initializing.keySet()).forEach(aId -> markAllocationIdAsInSyncQuietly(tracker, aId.getId(), NO_OPS_PERFORMED));

        active.forEach((aid, localCP) -> tracker.updateLocalCheckpoint(aid.getId(), localCP));

        assertThat(tracker.getGlobalCheckpoint(), equalTo(NO_OPS_PERFORMED));

        // update again
        initializing.forEach((aid, localCP) -> tracker.updateLocalCheckpoint(aid.getId(), localCP));
        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreIgnoredIfNotValidatedByMaster() {
        final Map<AllocationId, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> initializing = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<AllocationId, Long> nonApproved = randomAllocationsWithLocalCheckpoints(1, 5);
        tracker.updateFromMaster(randomNonNegativeLong(), ids(active.keySet()), routingTable(initializing.keySet()), emptySet());
        tracker.activatePrimaryMode(active.keySet().iterator().next().getId(), NO_OPS_PERFORMED);
        initializing.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        nonApproved.keySet().forEach(k ->
            expectThrows(IllegalStateException.class, () -> markAllocationIdAsInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED)));

        List<Map<AllocationId, Long>> allocations = Arrays.asList(active, initializing, nonApproved);
        Collections.shuffle(allocations, random());
        allocations.forEach(a -> a.forEach((aid, localCP) -> tracker.updateLocalCheckpoint(aid.getId(), localCP)));

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
        allocations.putAll(activeToStay);
        if (randomBoolean()) {
            allocations.putAll(activeToBeRemoved);
        }
        allocations.putAll(initializingToStay);
        if (randomBoolean()) {
            allocations.putAll(initializingToBeRemoved);
        }
        tracker.updateFromMaster(initialClusterStateVersion, ids(active), routingTable(initializing), emptySet());
        tracker.activatePrimaryMode(active.iterator().next().getId(), NO_OPS_PERFORMED);
        if (randomBoolean()) {
            initializingToStay.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        } else {
            initializing.forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k.getId(), NO_OPS_PERFORMED));
        }
        if (randomBoolean()) {
            allocations.forEach((aid, localCP) -> tracker.updateLocalCheckpoint(aid.getId(), localCP));
        }

        // now remove shards
        if (randomBoolean()) {
            tracker.updateFromMaster(initialClusterStateVersion + 1, ids(activeToStay.keySet()), routingTable(initializingToStay.keySet()),
                emptySet());
            allocations.forEach((aid, ckp) -> tracker.updateLocalCheckpoint(aid.getId(), ckp + 10L));
        } else {
            allocations.forEach((aid, ckp) -> tracker.updateLocalCheckpoint(aid.getId(), ckp + 10L));
            tracker.updateFromMaster(initialClusterStateVersion + 2, ids(activeToStay.keySet()), routingTable(initializingToStay.keySet()),
                emptySet());
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
        tracker.updateFromMaster(randomNonNegativeLong(), Collections.singleton(inSyncAllocationId.getId()),
            routingTable(Collections.singleton(trackingAllocationId)), emptySet());
        tracker.activatePrimaryMode(inSyncAllocationId.getId(), globalCheckpoint);
        final Thread thread = new Thread(() -> {
            try {
                // synchronize starting with the test thread
                barrier.await();
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
            tracker.updateLocalCheckpoint(trackingAllocationId.getId(), elements.get(i));
            assertFalse(complete.get());
            assertFalse(tracker.getTrackedLocalCheckpointForShard(trackingAllocationId.getId()).inSync);
            assertBusy(() -> assertTrue(tracker.pendingInSync.contains(trackingAllocationId.getId())));
        }

        tracker.updateLocalCheckpoint(trackingAllocationId.getId(), randomIntBetween(globalCheckpoint, 64));
        // synchronize with the waiting thread to mark that it is complete
        barrier.await();
        assertTrue(complete.get());
        assertTrue(tracker.getTrackedLocalCheckpointForShard(trackingAllocationId.getId()).inSync);
        assertFalse(tracker.pendingInSync.contains(trackingAllocationId.getId()));

        thread.join();
    }

    public void testWaitForAllocationIdToBeInSyncCanBeInterrupted() throws BrokenBarrierException, InterruptedException {
        final int localCheckpoint = randomIntBetween(1, 32);
        final int globalCheckpoint = randomIntBetween(localCheckpoint + 1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final AllocationId inSyncAllocationId = AllocationId.newInitializing();
        final AllocationId trackingAllocationId = AllocationId.newInitializing();
        tracker.updateFromMaster(randomNonNegativeLong(), Collections.singleton(inSyncAllocationId.getId()),
            routingTable(Collections.singleton(trackingAllocationId)), emptySet());
        tracker.activatePrimaryMode(inSyncAllocationId.getId(), globalCheckpoint);
        final Thread thread = new Thread(() -> {
            try {
                // synchronize starting with the test thread
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
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
        IndexShardRoutingTable routingTable = routingTable(initializingIds);
        tracker.updateFromMaster(initialClusterStateVersion, ids(activeAllocationIds), routingTable, emptySet());
        AllocationId primaryId = activeAllocationIds.iterator().next();
        tracker.activatePrimaryMode(primaryId.getId(), NO_OPS_PERFORMED);
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
        final Set<AllocationId> newActiveAllocationIds =
                activeAllocationIds.stream().filter(a -> !removingActiveAllocationIds.contains(a)).collect(Collectors.toSet());
        final List<AllocationId> removingInitializingAllocationIds = randomSubsetOf(initializingIds);
        final Set<AllocationId> newInitializingAllocationIds =
                initializingIds.stream().filter(a -> !removingInitializingAllocationIds.contains(a)).collect(Collectors.toSet());
        routingTable = routingTable(newInitializingAllocationIds);
        tracker.updateFromMaster(initialClusterStateVersion + 1, ids(newActiveAllocationIds), routingTable,
            emptySet());
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
        tracker.updateFromMaster(initialClusterStateVersion + 2, ids(newActiveAllocationIds), routingTable(newInitializingAllocationIds),
            emptySet());
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
        activeLocalCheckpoints.forEach((a, l) -> tracker.updateLocalCheckpoint(a.getId(), l));
        final Map<AllocationId, Integer> initializingLocalCheckpoints =
                newInitializingAllocationIds.stream().collect(Collectors.toMap(Function.identity(), a -> randomIntBetween(1, 1024)));
        initializingLocalCheckpoints.forEach((a, l) -> tracker.updateLocalCheckpoint(a.getId(), l));
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
        final long minimumActiveLocalCheckpoint = (long) activeLocalCheckpoints.values().stream().min(Integer::compareTo).get();
        assertThat(tracker.getGlobalCheckpoint(), equalTo(minimumActiveLocalCheckpoint));
        final long minimumInitailizingLocalCheckpoint = (long) initializingLocalCheckpoints.values().stream().min(Integer::compareTo).get();

        // now we are going to add a new allocation ID and bring it in sync which should move it to the in-sync allocation IDs
        final long localCheckpoint =
                randomIntBetween(0, Math.toIntExact(Math.min(minimumActiveLocalCheckpoint, minimumInitailizingLocalCheckpoint) - 1));

        // using a different length than we have been using above ensures that we can not collide with a previous allocation ID
        final AllocationId newSyncingAllocationId = AllocationId.newInitializing();
        newInitializingAllocationIds.add(newSyncingAllocationId);
        tracker.updateFromMaster(initialClusterStateVersion + 3, ids(newActiveAllocationIds), routingTable(newInitializingAllocationIds),
            emptySet());
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
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
        tracker.updateFromMaster(initialClusterStateVersion + 4, ids(newActiveAllocationIds), routingTable(newInitializingAllocationIds),
            emptySet());
        assertTrue(tracker.getTrackedLocalCheckpointForShard(newSyncingAllocationId.getId()).inSync);
        assertFalse(tracker.pendingInSync.contains(newSyncingAllocationId.getId()));
    }

    /**
     * If we do not update the global checkpoint in {@link GlobalCheckpointTracker#markAllocationIdAsInSync(String, long)} after adding the
     * allocation ID to the in-sync set and removing it from pending, the local checkpoint update that freed the thread waiting for the
     * local checkpoint to advance could miss updating the global checkpoint in a race if the the waiting thread did not add the allocation
     * ID to the in-sync set and remove it from the pending set before the local checkpoint updating thread executed the global checkpoint
     * update. This test fails without an additional call to {@link GlobalCheckpointTracker#updateGlobalCheckpointOnPrimary()} after
     * removing the allocation ID from the pending set in {@link GlobalCheckpointTracker#markAllocationIdAsInSync(String, long)} (even if a
     * call is added after notifying all waiters in {@link GlobalCheckpointTracker#updateLocalCheckpoint(String, long)}).
     *
     * @throws InterruptedException   if the main test thread was interrupted while waiting
     * @throws BrokenBarrierException if the barrier was broken while the main test thread was waiting
     */
    public void testRaceUpdatingGlobalCheckpoint() throws InterruptedException, BrokenBarrierException {

        final AllocationId active = AllocationId.newInitializing();
        final AllocationId initializing = AllocationId.newInitializing();
        final CyclicBarrier barrier = new CyclicBarrier(4);

        final int activeLocalCheckpoint = randomIntBetween(0, Integer.MAX_VALUE - 1);
        tracker.updateFromMaster(randomNonNegativeLong(), Collections.singleton(active.getId()),
            routingTable(Collections.singleton(initializing)), emptySet());
        tracker.activatePrimaryMode(active.getId(), activeLocalCheckpoint);
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
        GlobalCheckpointTracker oldPrimary = new GlobalCheckpointTracker(new ShardId("test", "_na_", 0),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), UNASSIGNED_SEQ_NO);
        GlobalCheckpointTracker newPrimary = new GlobalCheckpointTracker(new ShardId("test", "_na_", 0),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY), UNASSIGNED_SEQ_NO);

        FakeClusterState clusterState = initialState();
        clusterState.apply(oldPrimary);
        clusterState.apply(newPrimary);

        activatePrimary(clusterState, oldPrimary);

        final int numUpdates = randomInt(10);
        for (int i = 0; i < numUpdates; i++) {
            if (rarely()) {
                clusterState = randomUpdateClusterState(clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
            }
            if (randomBoolean()) {
                randomLocalCheckpointUpdate(oldPrimary);
            }
            if (randomBoolean()) {
                randomMarkInSync(oldPrimary);
            }
        }

        GlobalCheckpointTracker.PrimaryContext primaryContext = oldPrimary.startRelocationHandoff();

        if (randomBoolean()) {
            // cluster state update after primary context handoff
            if (randomBoolean()) {
                clusterState = randomUpdateClusterState(clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
            }

            // abort handoff, check that we can continue updates and retry handoff
            oldPrimary.abortRelocationHandoff();

            if (rarely()) {
                clusterState = randomUpdateClusterState(clusterState);
                clusterState.apply(oldPrimary);
                clusterState.apply(newPrimary);
            }
            if (randomBoolean()) {
                randomLocalCheckpointUpdate(oldPrimary);
            }
            if (randomBoolean()) {
                randomMarkInSync(oldPrimary);
            }

            // do another handoff
            primaryContext = oldPrimary.startRelocationHandoff();
        }

        // send primary context through the wire
        BytesStreamOutput output = new BytesStreamOutput();
        primaryContext.writeTo(output);
        StreamInput streamInput = output.bytes().streamInput();
        primaryContext = new GlobalCheckpointTracker.PrimaryContext(streamInput);

        switch (randomInt(3)) {
            case 0: {
                // apply cluster state update on old primary while primary context is being transferred
                clusterState = randomUpdateClusterState(clusterState);
                clusterState.apply(oldPrimary);
                // activate new primary
                newPrimary.activateWithPrimaryContext(primaryContext);
                // apply cluster state update on new primary so that the states on old and new primary are comparable
                clusterState.apply(newPrimary);
                break;
            }
            case 1: {
                // apply cluster state update on new primary while primary context is being transferred
                clusterState = randomUpdateClusterState(clusterState);
                clusterState.apply(newPrimary);
                // activate new primary
                newPrimary.activateWithPrimaryContext(primaryContext);
                // apply cluster state update on old primary so that the states on old and new primary are comparable
                clusterState.apply(oldPrimary);
                break;
            }
            case 2: {
                // apply cluster state update on both copies while primary context is being transferred
                clusterState = randomUpdateClusterState(clusterState);
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
        assertThat(newPrimary.localCheckpoints, equalTo(oldPrimary.localCheckpoints));
        assertThat(newPrimary.globalCheckpoint, equalTo(oldPrimary.globalCheckpoint));
        assertThat(newPrimary.routingTable, equalTo(oldPrimary.routingTable));
        assertThat(newPrimary.replicationGroup, equalTo(oldPrimary.replicationGroup));

        oldPrimary.completeRelocationHandoff();
        assertFalse(oldPrimary.primaryMode);
    }

    public void testIllegalStateExceptionIfUnknownAllocationId() {
        final AllocationId active = AllocationId.newInitializing();
        final AllocationId initializing = AllocationId.newInitializing();
        tracker.updateFromMaster(randomNonNegativeLong(), Collections.singleton(active.getId()),
            routingTable(Collections.singleton(initializing)), emptySet());
        tracker.activatePrimaryMode(active.getId(), NO_OPS_PERFORMED);

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

        public void apply(GlobalCheckpointTracker gcp) {
            gcp.updateFromMaster(version, ids(inSyncIds), routingTable, Collections.emptySet());
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
        return new FakeClusterState(initialClusterStateVersion, activeAllocationIds, routingTable(initializingAllocationIds));
    }

    private static void activatePrimary(FakeClusterState clusterState, GlobalCheckpointTracker gcp) {
        gcp.activatePrimaryMode(randomFrom(ids(clusterState.inSyncIds)), randomIntBetween(Math.toIntExact(NO_OPS_PERFORMED), 10));
    }

    private static void randomLocalCheckpointUpdate(GlobalCheckpointTracker gcp) {
        String allocationId = randomFrom(gcp.localCheckpoints.keySet());
        long currentLocalCheckpoint = gcp.localCheckpoints.get(allocationId).getLocalCheckpoint();
        gcp.updateLocalCheckpoint(allocationId, Math.max(SequenceNumbers.NO_OPS_PERFORMED, currentLocalCheckpoint + randomInt(5)));
    }

    private static void randomMarkInSync(GlobalCheckpointTracker gcp) {
        String allocationId = randomFrom(gcp.localCheckpoints.keySet());
        long newLocalCheckpoint = Math.max(NO_OPS_PERFORMED, gcp.getGlobalCheckpoint() + randomInt(5));
        markAllocationIdAsInSyncQuietly(gcp, allocationId, newLocalCheckpoint);
    }

    private static FakeClusterState randomUpdateClusterState(FakeClusterState clusterState) {
        final Set<AllocationId> initializingIdsToAdd = randomAllocationIdsExcludingExistingIds(clusterState.allIds(), randomInt(2));
        final Set<AllocationId> initializingIdsToRemove = new HashSet<>(
            randomSubsetOf(randomInt(clusterState.initializingIds().size()), clusterState.initializingIds()));
        final Set<AllocationId> inSyncIdsToRemove = new HashSet<>(
            randomSubsetOf(randomInt(clusterState.inSyncIds.size()), clusterState.inSyncIds));
        final Set<AllocationId> remainingInSyncIds = Sets.difference(clusterState.inSyncIds, inSyncIdsToRemove);
        return new FakeClusterState(clusterState.version + randomIntBetween(1, 5),
            remainingInSyncIds.isEmpty() ? clusterState.inSyncIds : remainingInSyncIds,
            routingTable(Sets.difference(Sets.union(clusterState.initializingIds(), initializingIdsToAdd), initializingIdsToRemove)));
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
                if (!existingAllocationIds.contains(newAllocationId)) {
                    return newAllocationId;
                }
            } while (true);
        }).collect(Collectors.toSet());
    }

    private static void markAllocationIdAsInSyncQuietly(
            final GlobalCheckpointTracker tracker, final String allocationId, final long localCheckpoint) {
        try {
            tracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
