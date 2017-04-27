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

import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.IndexSettingsModule;
import org.junit.Before;

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

import static org.elasticsearch.index.seqno.SequenceNumbersService.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.comparesEqualTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
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
        assertFalse("checkpoint shouldn't be updated when the are no active shards", tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));
    }

    private final AtomicInteger aIdGenerator = new AtomicInteger();

    private Map<String, Long> randomAllocationsWithLocalCheckpoints(int min, int max) {
        Map<String, Long> allocations = new HashMap<>();
        for (int i = randomIntBetween(min, max); i > 0; i--) {
            allocations.put("id_" + aIdGenerator.incrementAndGet(), (long) randomInt(1000));
        }
        return allocations;
    }

    public void testGlobalCheckpointUpdate() {
        Map<String, Long> allocations = new HashMap<>();
        Map<String, Long> activeWithCheckpoints = randomAllocationsWithLocalCheckpoints(0, 5);
        Set<String> active = new HashSet<>(activeWithCheckpoints.keySet());
        allocations.putAll(activeWithCheckpoints);
        Map<String, Long> initializingWithCheckpoints = randomAllocationsWithLocalCheckpoints(0, 5);
        Set<String> initializing = new HashSet<>(initializingWithCheckpoints.keySet());
        allocations.putAll(initializingWithCheckpoints);
        assertThat(allocations.size(), equalTo(active.size() + initializing.size()));

        // note: allocations can never be empty in practice as we always have at least one primary shard active/in sync
        // it is however nice not to assume this on this level and check we do the right thing.
        final long maxLocalCheckpoint = allocations.values().stream().min(Long::compare).orElse(UNASSIGNED_SEQ_NO);

        assertThat(tracker.getCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

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

        tracker.updateAllocationIdsFromMaster(active, initializing);
        initializing.forEach(aId -> markAllocationIdAsInSyncQuietly(tracker, aId, tracker.getCheckpoint()));
        allocations.keySet().forEach(aId -> tracker.updateLocalCheckpoint(aId, allocations.get(aId)));


        assertThat(tracker.getCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

        assertThat(tracker.updateCheckpointOnPrimary(), equalTo(maxLocalCheckpoint != UNASSIGNED_SEQ_NO));
        assertThat(tracker.getCheckpoint(), equalTo(maxLocalCheckpoint));

        // increment checkpoints
        active.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        initializing.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        allocations.keySet().forEach(aId -> tracker.updateLocalCheckpoint(aId, allocations.get(aId)));

        // now insert an unknown active/insync id , the checkpoint shouldn't change but a refresh should be requested.
        final String extraId = "extra_" + randomAlphaOfLength(5);

        // first check that adding it without the master blessing doesn't change anything.
        tracker.updateLocalCheckpoint(extraId, maxLocalCheckpoint + 1 + randomInt(4));
        assertThat(tracker.getLocalCheckpointForAllocationId(extraId), equalTo(UNASSIGNED_SEQ_NO));

        Set<String> newActive = new HashSet<>(active);
        newActive.add(extraId);
        tracker.updateAllocationIdsFromMaster(newActive, initializing);

        // we should ask for a refresh , but not update the checkpoint
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), equalTo(maxLocalCheckpoint));

        // now notify for the new id
        tracker.updateLocalCheckpoint(extraId, maxLocalCheckpoint + 1 + randomInt(4));

        // now it should be incremented
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), greaterThan(maxLocalCheckpoint));
    }

    public void testMissingActiveIdsPreventAdvance() {
        final Map<String, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> initializing = randomAllocationsWithLocalCheckpoints(0, 5);
        final Map<String, Long> assigned = new HashMap<>();
        assigned.putAll(active);
        assigned.putAll(initializing);
        tracker.updateAllocationIdsFromMaster(
            new HashSet<>(randomSubsetOf(randomInt(active.size() - 1), active.keySet())),
            initializing.keySet());
        randomSubsetOf(initializing.keySet()).forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getCheckpoint()));
        assigned.forEach(tracker::updateLocalCheckpoint);

        // now mark all active shards
        tracker.updateAllocationIdsFromMaster(active.keySet(), initializing.keySet());

        // global checkpoint can't be advanced, but we need a sync
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

        // update again
        assigned.forEach(tracker::updateLocalCheckpoint);
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testMissingInSyncIdsPreventAdvance() {
        final Map<String, Long> active = randomAllocationsWithLocalCheckpoints(0, 5);
        final Map<String, Long> initializing = randomAllocationsWithLocalCheckpoints(1, 5);
        tracker.updateAllocationIdsFromMaster(active.keySet(), initializing.keySet());
        initializing.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getCheckpoint()));
        randomSubsetOf(randomInt(initializing.size() - 1),
            initializing.keySet()).forEach(aId -> tracker.updateLocalCheckpoint(aId, initializing.get(aId)));

        active.forEach(tracker::updateLocalCheckpoint);

        // global checkpoint can't be advanced, but we need a sync
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

        // update again
        initializing.forEach(tracker::updateLocalCheckpoint);
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreIgnoredIfNotValidatedByMaster() {
        final Map<String, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> initializing = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> nonApproved = randomAllocationsWithLocalCheckpoints(1, 5);
        tracker.updateAllocationIdsFromMaster(active.keySet(), initializing.keySet());
        initializing.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getCheckpoint()));
        nonApproved.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getCheckpoint()));

        List<Map<String, Long>> allocations = Arrays.asList(active, initializing, nonApproved);
        Collections.shuffle(allocations, random());
        allocations.forEach(a -> a.forEach(tracker::updateLocalCheckpoint));

        // global checkpoint can be advanced, but we need a sync
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreRemovedIfNotValidatedByMaster() {
        final Map<String, Long> activeToStay = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> initializingToStay = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> activeToBeRemoved = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> initializingToBeRemoved = randomAllocationsWithLocalCheckpoints(1, 5);
        final Set<String> active = Sets.union(activeToStay.keySet(), activeToBeRemoved.keySet());
        final Set<String> initializing = Sets.union(initializingToStay.keySet(), initializingToBeRemoved.keySet());
        final Map<String, Long> allocations = new HashMap<>();
        allocations.putAll(activeToStay);
        if (randomBoolean()) {
            allocations.putAll(activeToBeRemoved);
        }
        allocations.putAll(initializingToStay);
        if (randomBoolean()) {
            allocations.putAll(initializingToBeRemoved);
        }
        tracker.updateAllocationIdsFromMaster(active, initializing);
        if (randomBoolean()) {
            initializingToStay.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getCheckpoint()));
        } else {
            initializing.forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getCheckpoint()));
        }
        if (randomBoolean()) {
            allocations.forEach(tracker::updateLocalCheckpoint);
        }

        // global checkpoint may be advanced, but we need a sync in any case
        assertTrue(tracker.updateCheckpointOnPrimary());

        // now remove shards
        if (randomBoolean()) {
            tracker.updateAllocationIdsFromMaster(activeToStay.keySet(), initializingToStay.keySet());
            allocations.forEach((aid, ckp) -> tracker.updateLocalCheckpoint(aid, ckp + 10L));
        } else {
            allocations.forEach((aid, ckp) -> tracker.updateLocalCheckpoint(aid, ckp + 10L));
            tracker.updateAllocationIdsFromMaster(activeToStay.keySet(), initializingToStay.keySet());
        }

        final long checkpoint = Stream.concat(activeToStay.values().stream(), initializingToStay.values().stream())
            .min(Long::compare).get() + 10; // we added 10 to make sure it's advanced in the second time

        // global checkpoint is advanced and we need a sync
        assertTrue(tracker.updateCheckpointOnPrimary());
        assertThat(tracker.getCheckpoint(), equalTo(checkpoint));
    }

    public void testWaitForAllocationIdToBeInSync() throws BrokenBarrierException, InterruptedException {
        final int localCheckpoint = randomIntBetween(1, 32);
        final int globalCheckpoint = randomIntBetween(localCheckpoint + 1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean complete = new AtomicBoolean();
        final String inSyncAllocationId =randomAlphaOfLength(16);
        final String trackingAllocationId = randomAlphaOfLength(16);
        tracker.updateAllocationIdsFromMaster(Collections.singleton(inSyncAllocationId), Collections.singleton(trackingAllocationId));
        tracker.updateLocalCheckpoint(inSyncAllocationId, globalCheckpoint);
        tracker.updateCheckpointOnPrimary();
        final Thread thread = new Thread(() -> {
            try {
                // synchronize starting with the test thread
                barrier.await();
                tracker.markAllocationIdAsInSync(trackingAllocationId, localCheckpoint);
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
            tracker.updateLocalCheckpoint(trackingAllocationId, elements.get(i));
            assertFalse(complete.get());
            assertTrue(awaitBusy(() -> tracker.trackingLocalCheckpoints.containsKey(trackingAllocationId)));
            assertTrue(awaitBusy(() -> tracker.pendingInSync.contains(trackingAllocationId)));
            assertFalse(tracker.inSyncLocalCheckpoints.containsKey(trackingAllocationId));
        }

        tracker.updateLocalCheckpoint(trackingAllocationId, randomIntBetween(globalCheckpoint, 64));
        // synchronize with the waiting thread to mark that it is complete
        barrier.await();
        assertTrue(complete.get());
        assertTrue(tracker.trackingLocalCheckpoints.isEmpty());
        assertTrue(tracker.pendingInSync.isEmpty());
        assertTrue(tracker.inSyncLocalCheckpoints.containsKey(trackingAllocationId));

        thread.join();
    }

    public void testWaitForAllocationIdToBeInSyncCanBeInterrupted() throws BrokenBarrierException, InterruptedException {
        final int localCheckpoint = randomIntBetween(1, 32);
        final int globalCheckpoint = randomIntBetween(localCheckpoint + 1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean interrupted = new AtomicBoolean();
        final String inSyncAllocationId = randomAlphaOfLength(16);
        final String trackingAllocationId = randomAlphaOfLength(32);
        tracker.updateAllocationIdsFromMaster(Collections.singleton(inSyncAllocationId), Collections.singleton(trackingAllocationId));
        tracker.updateLocalCheckpoint(inSyncAllocationId, globalCheckpoint);
        tracker.updateCheckpointOnPrimary();
        final Thread thread = new Thread(() -> {
            try {
                // synchronize starting with the test thread
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            try {
                tracker.markAllocationIdAsInSync(trackingAllocationId, localCheckpoint);
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
        final int numberOfActiveAllocationsIds = randomIntBetween(2, 16);
        final Set<String> activeAllocationIds =
                IntStream.range(0, numberOfActiveAllocationsIds).mapToObj(i -> randomAlphaOfLength(16)).collect(Collectors.toSet());
        final int numberOfInitializingIds = randomIntBetween(2, 16);
        final Set<String> initializingIds =
                IntStream.range(0, numberOfInitializingIds).mapToObj(i -> {
                    do {
                        final String initializingId = randomAlphaOfLength(16);
                        // ensure we do not duplicate an allocation ID in active and initializing sets
                        if (!activeAllocationIds.contains(initializingId)) {
                            return initializingId;
                        }
                    } while (true);
                }).collect(Collectors.toSet());
        tracker.updateAllocationIdsFromMaster(activeAllocationIds, initializingIds);

        // first we assert that the in-sync and tracking sets are set up correctly
        assertTrue(activeAllocationIds.stream().allMatch(a -> tracker.inSyncLocalCheckpoints.containsKey(a)));
        assertTrue(
                activeAllocationIds
                        .stream()
                        .allMatch(a -> tracker.inSyncLocalCheckpoints.get(a) == SequenceNumbersService.UNASSIGNED_SEQ_NO));
        assertTrue(initializingIds.stream().allMatch(a -> tracker.trackingLocalCheckpoints.containsKey(a)));
        assertTrue(
                initializingIds
                        .stream()
                        .allMatch(a -> tracker.trackingLocalCheckpoints.get(a) == SequenceNumbersService.UNASSIGNED_SEQ_NO));

        // now we will remove some allocation IDs from these and ensure that they propagate through
        final List<String> removingActiveAllocationIds = randomSubsetOf(activeAllocationIds);
        final Set<String> newActiveAllocationIds =
                activeAllocationIds.stream().filter(a -> !removingActiveAllocationIds.contains(a)).collect(Collectors.toSet());
        final List<String> removingInitializingAllocationIds = randomSubsetOf(initializingIds);
        final Set<String> newInitializingAllocationIds =
                initializingIds.stream().filter(a -> !removingInitializingAllocationIds.contains(a)).collect(Collectors.toSet());
        tracker.updateAllocationIdsFromMaster(newActiveAllocationIds, newInitializingAllocationIds);
        assertTrue(newActiveAllocationIds.stream().allMatch(a -> tracker.inSyncLocalCheckpoints.containsKey(a)));
        assertTrue(removingActiveAllocationIds.stream().noneMatch(a -> tracker.inSyncLocalCheckpoints.containsKey(a)));
        assertTrue(newInitializingAllocationIds.stream().allMatch(a -> tracker.trackingLocalCheckpoints.containsKey(a)));
        assertTrue(removingInitializingAllocationIds.stream().noneMatch(a -> tracker.trackingLocalCheckpoints.containsKey(a)));

        /*
         * Now we will add an allocation ID to each of active and initializing and ensure they propagate through. Using different lengths
         * than we have been using above ensures that we can not collide with a previous allocation ID
         */
        newActiveAllocationIds.add(randomAlphaOfLength(32));
        newInitializingAllocationIds.add(randomAlphaOfLength(64));
        tracker.updateAllocationIdsFromMaster(newActiveAllocationIds, newInitializingAllocationIds);
        assertTrue(newActiveAllocationIds.stream().allMatch(a -> tracker.inSyncLocalCheckpoints.containsKey(a)));
        assertTrue(
                newActiveAllocationIds
                        .stream()
                        .allMatch(a -> tracker.inSyncLocalCheckpoints.get(a) == SequenceNumbersService.UNASSIGNED_SEQ_NO));
        assertTrue(newInitializingAllocationIds.stream().allMatch(a -> tracker.trackingLocalCheckpoints.containsKey(a)));
        assertTrue(
                newInitializingAllocationIds
                        .stream()
                        .allMatch(a -> tracker.trackingLocalCheckpoints.get(a) == SequenceNumbersService.UNASSIGNED_SEQ_NO));

        // the tracking allocation IDs should play no role in determining the global checkpoint
        final Map<String, Integer> activeLocalCheckpoints =
                newActiveAllocationIds.stream().collect(Collectors.toMap(Function.identity(), a -> randomIntBetween(1, 1024)));
        activeLocalCheckpoints.forEach((a, l) -> tracker.updateLocalCheckpoint(a, l));
        final Map<String, Integer> initializingLocalCheckpoints =
                newInitializingAllocationIds.stream().collect(Collectors.toMap(Function.identity(), a -> randomIntBetween(1, 1024)));
        initializingLocalCheckpoints.forEach((a, l) -> tracker.updateLocalCheckpoint(a, l));
        assertTrue(
                activeLocalCheckpoints
                        .entrySet()
                        .stream()
                        .allMatch(e -> tracker.getLocalCheckpointForAllocationId(e.getKey()) == e.getValue()));
        assertTrue(
                initializingLocalCheckpoints
                        .entrySet()
                        .stream()
                        .allMatch(e -> tracker.trackingLocalCheckpoints.get(e.getKey()) == e.getValue()));
        assertTrue(tracker.updateCheckpointOnPrimary());
        final long minimumActiveLocalCheckpoint = (long) activeLocalCheckpoints.values().stream().min(Integer::compareTo).get();
        assertThat(tracker.getCheckpoint(), equalTo(minimumActiveLocalCheckpoint));
        final long minimumInitailizingLocalCheckpoint = (long) initializingLocalCheckpoints.values().stream().min(Integer::compareTo).get();

        // now we are going to add a new allocation ID and bring it in sync which should move it to the in-sync allocation IDs
        final long localCheckpoint =
                randomIntBetween(0, Math.toIntExact(Math.min(minimumActiveLocalCheckpoint, minimumInitailizingLocalCheckpoint) - 1));

        // using a different length than we have been using above ensures that we can not collide with a previous allocation ID
        final String newSyncingAllocationId = randomAlphaOfLength(128);
        newInitializingAllocationIds.add(newSyncingAllocationId);
        tracker.updateAllocationIdsFromMaster(newActiveAllocationIds, newInitializingAllocationIds);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final Thread thread = new Thread(() -> {
            try {
                barrier.await();
                tracker.markAllocationIdAsInSync(newSyncingAllocationId, localCheckpoint);
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        thread.start();

        barrier.await();

        assertBusy(() -> {
            assertTrue(tracker.pendingInSync.contains(newSyncingAllocationId));
            assertTrue(tracker.trackingLocalCheckpoints.containsKey(newSyncingAllocationId));
        });

        tracker.updateLocalCheckpoint(newSyncingAllocationId, randomIntBetween(Math.toIntExact(minimumActiveLocalCheckpoint), 1024));

        barrier.await();

        assertFalse(tracker.pendingInSync.contains(newSyncingAllocationId));
        assertFalse(tracker.trackingLocalCheckpoints.containsKey(newSyncingAllocationId));
        assertTrue(tracker.inSyncLocalCheckpoints.containsKey(newSyncingAllocationId));

        /*
         * The new in-sync allocation ID is in the in-sync set now yet the master does not know this; the allocation ID should still be in
         * the in-sync set even if we receive a cluster state update that does not reflect this.
         *
         */
        tracker.updateAllocationIdsFromMaster(newActiveAllocationIds, newInitializingAllocationIds);
        assertFalse(tracker.trackingLocalCheckpoints.containsKey(newSyncingAllocationId));
        assertTrue(tracker.inSyncLocalCheckpoints.containsKey(newSyncingAllocationId));
    }


    private void markAllocationIdAsInSyncQuietly(
            final GlobalCheckpointTracker tracker, final String allocationId, final long localCheckpoint) {
        try {
            tracker.markAllocationIdAsInSync(allocationId, localCheckpoint);
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

}
