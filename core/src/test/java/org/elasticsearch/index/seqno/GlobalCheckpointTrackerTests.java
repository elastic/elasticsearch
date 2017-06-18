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

import com.carrotsearch.hppc.ObjectLongHashMap;
import com.carrotsearch.hppc.ObjectLongMap;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.shard.PrimaryContext;
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
import java.util.stream.StreamSupport;

import static org.elasticsearch.index.seqno.SequenceNumbersService.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.not;
import static org.mockito.Mockito.mock;

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

    private Map<String, Long> randomAllocationsWithLocalCheckpoints(int min, int max) {
        Map<String, Long> allocations = new HashMap<>();
        for (int i = randomIntBetween(min, max); i > 0; i--) {
            allocations.put("id_" + aIdGenerator.incrementAndGet(), (long) randomInt(1000));
        }
        return allocations;
    }

    public void testGlobalCheckpointUpdate() {
        final long initialClusterStateVersion = randomNonNegativeLong();
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

        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion, active, initializing);
        initializing.forEach(aId -> markAllocationIdAsInSyncQuietly(tracker, aId, tracker.getGlobalCheckpoint()));
        allocations.keySet().forEach(aId -> tracker.updateLocalCheckpoint(aId, allocations.get(aId)));

        assertThat(tracker.getGlobalCheckpoint(), equalTo(minLocalCheckpoint));

        // increment checkpoints
        active.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        initializing.forEach(aId -> allocations.put(aId, allocations.get(aId) + 1 + randomInt(4)));
        allocations.keySet().forEach(aId -> tracker.updateLocalCheckpoint(aId, allocations.get(aId)));

        final long minLocalCheckpointAfterUpdates =
                allocations.entrySet().stream().map(Map.Entry::getValue).min(Long::compareTo).orElse(UNASSIGNED_SEQ_NO);

        // now insert an unknown active/insync id , the checkpoint shouldn't change but a refresh should be requested.
        final String extraId = "extra_" + randomAlphaOfLength(5);

        // first check that adding it without the master blessing doesn't change anything.
        tracker.updateLocalCheckpoint(extraId, minLocalCheckpointAfterUpdates + 1 + randomInt(4));
        assertThat(tracker.getLocalCheckpointForAllocationId(extraId), equalTo(UNASSIGNED_SEQ_NO));

        Set<String> newActive = new HashSet<>(active);
        newActive.add(extraId);
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 1, newActive, initializing);

        // now notify for the new id
        tracker.updateLocalCheckpoint(extraId, minLocalCheckpointAfterUpdates + 1 + randomInt(4));

        // now it should be incremented
        assertThat(tracker.getGlobalCheckpoint(), greaterThan(minLocalCheckpoint));
    }

    public void testMissingActiveIdsPreventAdvance() {
        final Map<String, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> initializing = randomAllocationsWithLocalCheckpoints(0, 5);
        final Map<String, Long> assigned = new HashMap<>();
        assigned.putAll(active);
        assigned.putAll(initializing);
        tracker.updateAllocationIdsFromMaster(
                randomNonNegativeLong(),
                active.keySet(),
                initializing.keySet());
        randomSubsetOf(initializing.keySet()).forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getGlobalCheckpoint()));
        final String missingActiveID = randomFrom(active.keySet());
        assigned
                .entrySet()
                .stream()
                .filter(e -> !e.getKey().equals(missingActiveID))
                .forEach(e -> tracker.updateLocalCheckpoint(e.getKey(), e.getValue()));

        assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

        // now update all knowledge of all shards
        assigned.forEach(tracker::updateLocalCheckpoint);
        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testMissingInSyncIdsPreventAdvance() {
        final Map<String, Long> active = randomAllocationsWithLocalCheckpoints(0, 5);
        final Map<String, Long> initializing = randomAllocationsWithLocalCheckpoints(1, 5);
        tracker.updateAllocationIdsFromMaster(randomNonNegativeLong(), active.keySet(), initializing.keySet());
        initializing.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getGlobalCheckpoint()));
        randomSubsetOf(randomInt(initializing.size() - 1),
            initializing.keySet()).forEach(aId -> tracker.updateLocalCheckpoint(aId, initializing.get(aId)));

        active.forEach(tracker::updateLocalCheckpoint);

        assertThat(tracker.getGlobalCheckpoint(), equalTo(UNASSIGNED_SEQ_NO));

        // update again
        initializing.forEach(tracker::updateLocalCheckpoint);
        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreIgnoredIfNotValidatedByMaster() {
        final Map<String, Long> active = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> initializing = randomAllocationsWithLocalCheckpoints(1, 5);
        final Map<String, Long> nonApproved = randomAllocationsWithLocalCheckpoints(1, 5);
        tracker.updateAllocationIdsFromMaster(randomNonNegativeLong(), active.keySet(), initializing.keySet());
        initializing.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getGlobalCheckpoint()));
        nonApproved.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getGlobalCheckpoint()));

        List<Map<String, Long>> allocations = Arrays.asList(active, initializing, nonApproved);
        Collections.shuffle(allocations, random());
        allocations.forEach(a -> a.forEach(tracker::updateLocalCheckpoint));

        assertThat(tracker.getGlobalCheckpoint(), not(equalTo(UNASSIGNED_SEQ_NO)));
    }

    public void testInSyncIdsAreRemovedIfNotValidatedByMaster() {
        final long initialClusterStateVersion = randomNonNegativeLong();
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
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion, active, initializing);
        if (randomBoolean()) {
            initializingToStay.keySet().forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getGlobalCheckpoint()));
        } else {
            initializing.forEach(k -> markAllocationIdAsInSyncQuietly(tracker, k, tracker.getGlobalCheckpoint()));
        }
        if (randomBoolean()) {
            allocations.forEach(tracker::updateLocalCheckpoint);
        }

        // now remove shards
        if (randomBoolean()) {
            tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 1, activeToStay.keySet(), initializingToStay.keySet());
            allocations.forEach((aid, ckp) -> tracker.updateLocalCheckpoint(aid, ckp + 10L));
        } else {
            allocations.forEach((aid, ckp) -> tracker.updateLocalCheckpoint(aid, ckp + 10L));
            tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 2, activeToStay.keySet(), initializingToStay.keySet());
        }

        final long checkpoint = Stream.concat(activeToStay.values().stream(), initializingToStay.values().stream())
            .min(Long::compare).get() + 10; // we added 10 to make sure it's advanced in the second time

        assertThat(tracker.getGlobalCheckpoint(), equalTo(checkpoint));
    }

    public void testWaitForAllocationIdToBeInSync() throws BrokenBarrierException, InterruptedException {
        final int localCheckpoint = randomIntBetween(1, 32);
        final int globalCheckpoint = randomIntBetween(localCheckpoint + 1, 64);
        final CyclicBarrier barrier = new CyclicBarrier(2);
        final AtomicBoolean complete = new AtomicBoolean();
        final String inSyncAllocationId =randomAlphaOfLength(16);
        final String trackingAllocationId = randomAlphaOfLength(16);
        tracker.updateAllocationIdsFromMaster(
                randomNonNegativeLong(), Collections.singleton(inSyncAllocationId), Collections.singleton(trackingAllocationId));
        tracker.updateLocalCheckpoint(inSyncAllocationId, globalCheckpoint);
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
        tracker.updateAllocationIdsFromMaster(
                randomNonNegativeLong(), Collections.singleton(inSyncAllocationId), Collections.singleton(trackingAllocationId));
        tracker.updateLocalCheckpoint(inSyncAllocationId, globalCheckpoint);
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
        final long initialClusterStateVersion = randomNonNegativeLong();
        final int numberOfActiveAllocationsIds = randomIntBetween(2, 16);
        final int numberOfInitializingIds = randomIntBetween(2, 16);
        final Tuple<Set<String>, Set<String>> activeAndInitializingAllocationIds =
                randomActiveAndInitializingAllocationIds(numberOfActiveAllocationsIds, numberOfInitializingIds);
        final Set<String> activeAllocationIds = activeAndInitializingAllocationIds.v1();
        final Set<String> initializingIds = activeAndInitializingAllocationIds.v2();
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion, activeAllocationIds, initializingIds);

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
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 1, newActiveAllocationIds, newInitializingAllocationIds);
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
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 2, newActiveAllocationIds, newInitializingAllocationIds);
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
        final long minimumActiveLocalCheckpoint = (long) activeLocalCheckpoints.values().stream().min(Integer::compareTo).get();
        assertThat(tracker.getGlobalCheckpoint(), equalTo(minimumActiveLocalCheckpoint));
        final long minimumInitailizingLocalCheckpoint = (long) initializingLocalCheckpoints.values().stream().min(Integer::compareTo).get();

        // now we are going to add a new allocation ID and bring it in sync which should move it to the in-sync allocation IDs
        final long localCheckpoint =
                randomIntBetween(0, Math.toIntExact(Math.min(minimumActiveLocalCheckpoint, minimumInitailizingLocalCheckpoint) - 1));

        // using a different length than we have been using above ensures that we can not collide with a previous allocation ID
        final String newSyncingAllocationId = randomAlphaOfLength(128);
        newInitializingAllocationIds.add(newSyncingAllocationId);
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 3, newActiveAllocationIds, newInitializingAllocationIds);
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
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion + 4, newActiveAllocationIds, newInitializingAllocationIds);
        assertFalse(tracker.trackingLocalCheckpoints.containsKey(newSyncingAllocationId));
        assertTrue(tracker.inSyncLocalCheckpoints.containsKey(newSyncingAllocationId));
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

        final String active = randomAlphaOfLength(16);
        final String initializing = randomAlphaOfLength(32);
        tracker.updateAllocationIdsFromMaster(randomNonNegativeLong(), Collections.singleton(active), Collections.singleton(initializing));

        final CyclicBarrier barrier = new CyclicBarrier(4);

        final int activeLocalCheckpoint = randomIntBetween(0, Integer.MAX_VALUE - 1);
        tracker.updateLocalCheckpoint(active, activeLocalCheckpoint);
        final int nextActiveLocalCheckpoint = randomIntBetween(activeLocalCheckpoint + 1, Integer.MAX_VALUE);
        final Thread activeThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            tracker.updateLocalCheckpoint(active, nextActiveLocalCheckpoint);
        });

        final int initializingLocalCheckpoint = randomIntBetween(0, nextActiveLocalCheckpoint - 1);
        final Thread initializingThread = new Thread(() -> {
            try {
                barrier.await();
            } catch (final BrokenBarrierException | InterruptedException e) {
                throw new RuntimeException(e);
            }
            tracker.updateLocalCheckpoint(initializing, nextActiveLocalCheckpoint);
        });

        final Thread markingThread = new Thread(() -> {
            try {
                barrier.await();
                tracker.markAllocationIdAsInSync(initializing, initializingLocalCheckpoint - 1);
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

    public void testPrimaryContextOlderThanAppliedClusterState() {
        final long initialClusterStateVersion = randomIntBetween(0, Integer.MAX_VALUE - 1) + 1;
        final int numberOfActiveAllocationsIds = randomIntBetween(0, 8);
        final int numberOfInitializingIds = randomIntBetween(1, 8);
        final Tuple<Set<String>, Set<String>> activeAndInitializingAllocationIds =
                randomActiveAndInitializingAllocationIds(numberOfActiveAllocationsIds, numberOfInitializingIds);
        final Set<String> activeAllocationIds = activeAndInitializingAllocationIds.v1();
        final Set<String> initializingAllocationIds = activeAndInitializingAllocationIds.v2();
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion, activeAllocationIds, initializingAllocationIds);

        /*
         * We are going to establish a primary context from a cluster state version older than the applied cluster state version on the
         * tracker. Because of recovery barriers established during relocation handoff, we know that the set of active allocation IDs in the
         * newer cluster state is a superset of the allocation IDs in the applied cluster state with the caveat that an existing
         * initializing allocation ID could have moved to an in-sync allocation ID within the tracker due to recovery finalization, and the
         * set of initializing allocation IDs is otherwise arbitrary.
         */
        final int numberOfAdditionalInitializingAllocationIds = randomIntBetween(0, 8);
        final Set<String> initializedAllocationIds =
                new HashSet<>(randomSubsetOf(randomIntBetween(0, initializingAllocationIds.size() - 1), initializingAllocationIds));
        final Set<String> newInitializingAllocationIds =
                randomAllocationIdsExcludingExistingIds(
                        Sets.union(activeAllocationIds, initializingAllocationIds), numberOfAdditionalInitializingAllocationIds);
        final Set<String> difference = Sets.difference(initializingAllocationIds, initializedAllocationIds);
        final Set<String> contextInitializingIds = Sets.union(
                new HashSet<>(randomSubsetOf(randomIntBetween(1, difference.size()), difference)),
                newInitializingAllocationIds);

        final int numberOfAdditionalActiveAllocationIds = randomIntBetween(0, 8);
        final Set<String> contextActiveAllocationIds = Sets.union(
                Sets.union(
                        activeAllocationIds,
                        randomAllocationIdsExcludingExistingIds(activeAllocationIds, numberOfAdditionalActiveAllocationIds)),
                initializedAllocationIds);

        final ObjectLongMap<String> activeAllocationIdsLocalCheckpoints = new ObjectLongHashMap<>();
        for (final String allocationId : contextActiveAllocationIds) {
            activeAllocationIdsLocalCheckpoints.put(allocationId, randomNonNegativeLong());
        }
        final ObjectLongMap<String> initializingAllocationIdsLocalCheckpoints = new ObjectLongHashMap<>();
        for (final String allocationId : contextInitializingIds) {
            initializingAllocationIdsLocalCheckpoints.put(allocationId, randomNonNegativeLong());
        }

        final PrimaryContext primaryContext = new PrimaryContext(
                initialClusterStateVersion - randomIntBetween(0, Math.toIntExact(initialClusterStateVersion) - 1),
                activeAllocationIdsLocalCheckpoints,
                initializingAllocationIdsLocalCheckpoints);

        final String initializingAllocationId = randomFrom(Sets.difference(contextInitializingIds, newInitializingAllocationIds));
        tracker.updateLocalCheckpoint(initializingAllocationId, initializingAllocationIdsLocalCheckpoints.get(initializingAllocationId));
        tracker.updateAllocationIdsFromPrimaryContext(initializingAllocationId, primaryContext);

        // the primary context carries an older cluster state version
        assertThat(tracker.appliedClusterStateVersion, equalTo(initialClusterStateVersion));

        // only existing active allocation IDs and initializing allocation IDs that moved to initialized should be in-sync
        assertThat(
                Sets.union(activeAllocationIds, initializedAllocationIds),
                equalTo(
                        StreamSupport
                                .stream(tracker.inSyncLocalCheckpoints.keys().spliterator(), false)
                                .map(e -> e.value)
                                .collect(Collectors.toSet())));

        // the local checkpoints known to the tracker for in-sync shards should match what is known in the primary context
        for (final String allocationId : Sets.union(activeAllocationIds, initializedAllocationIds)) {
            assertThat(
                    tracker.inSyncLocalCheckpoints.get(allocationId), equalTo(primaryContext.inSyncLocalCheckpoints().get(allocationId)));
        }

        // only existing initializing allocation IDs that did not moved to initialized should be tracked
        assertThat(
                Sets.difference(initializingAllocationIds, initializedAllocationIds),
                equalTo(
                        StreamSupport
                        .stream(tracker.trackingLocalCheckpoints.keys().spliterator(), false)
                        .map(e -> e.value)
                        .collect(Collectors.toSet())));

        // the local checkpoints known to the tracker for initializing shards should match what is known in the primary context
        for (final String allocationId : Sets.difference(initializingAllocationIds, initializedAllocationIds)) {
            if (primaryContext.trackingLocalCheckpoints().containsKey(allocationId)) {
                assertThat(
                        tracker.trackingLocalCheckpoints.get(allocationId),
                        equalTo(primaryContext.trackingLocalCheckpoints().get(allocationId)));
            } else {
                assertThat(allocationId, tracker.trackingLocalCheckpoints.get(allocationId), equalTo(SequenceNumbersService.UNASSIGNED_SEQ_NO));
            }
        }

        // the global checkpoint can only be computed from active allocation IDs and initializing allocation IDs that moved to initializing
        final long globalCheckpoint =
                StreamSupport
                        .stream(activeAllocationIdsLocalCheckpoints.spliterator(), false)
                        .filter(e -> tracker.inSyncLocalCheckpoints.containsKey(e.key) || initializedAllocationIds.contains(e.key))
                        .mapToLong(e -> e.value)
                        .min()
                        .orElse(SequenceNumbersService.UNASSIGNED_SEQ_NO);
        assertThat(tracker.getGlobalCheckpoint(), equalTo(globalCheckpoint));
    }

    public void testPrimaryContextNewerThanAppliedClusterState() {
        final long initialClusterStateVersion = randomIntBetween(0, Integer.MAX_VALUE);
        final int numberOfActiveAllocationsIds = randomIntBetween(0, 8);
        final int numberOfInitializingIds = randomIntBetween(1, 8);
        final Tuple<Set<String>, Set<String>> activeAndInitializingAllocationIds =
                randomActiveAndInitializingAllocationIds(numberOfActiveAllocationsIds, numberOfInitializingIds);
        final Set<String> activeAllocationIds = activeAndInitializingAllocationIds.v1();
        final Set<String> initializingAllocationIds = activeAndInitializingAllocationIds.v2();
        tracker.updateAllocationIdsFromMaster(initialClusterStateVersion, activeAllocationIds, initializingAllocationIds);

        /*
         * We are going to establish a primary context from a cluster state version older than the applied cluster state version on the
         * tracker. Because of recovery barriers established during relocation handoff, we know that the set of active allocation IDs in the
         * newer cluster state is a subset of the allocation IDs in the applied cluster state with the caveat that an existing initializing
         * allocation ID could have moved to an in-sync allocation ID within the tracker due to recovery finalization, and the set of
         * initializing allocation IDs is otherwise arbitrary.
         */
        final int numberOfNewInitializingAllocationIds = randomIntBetween(1, 8);
        final Set<String> initializedAllocationIds =
                new HashSet<>(randomSubsetOf(randomIntBetween(0, initializingAllocationIds.size() - 1), initializingAllocationIds));
        final Set<String> newInitializingAllocationIds =
                randomAllocationIdsExcludingExistingIds(
                        Sets.union(activeAllocationIds, initializingAllocationIds), numberOfNewInitializingAllocationIds);

        final ObjectLongMap<String> activeAllocationIdsLocalCheckpoints = new ObjectLongHashMap<>();
        final Set<String> contextActiveAllocationIds =
                Sets.union(new HashSet<>(randomSubsetOf(activeAllocationIds)), initializedAllocationIds);
        for (final String allocationId : contextActiveAllocationIds) {
            activeAllocationIdsLocalCheckpoints.put(allocationId, randomNonNegativeLong());
        }
        final ObjectLongMap<String> initializingIdsLocalCheckpoints = new ObjectLongHashMap<>();
        final Set<String> difference = Sets.difference(initializingAllocationIds, initializedAllocationIds);
        final Set<String> contextInitializingAllocationIds = Sets.union(
                new HashSet<>(randomSubsetOf(randomIntBetween(1, difference.size()), difference)),
                newInitializingAllocationIds);
        for (final String allocationId : contextInitializingAllocationIds) {
            initializingIdsLocalCheckpoints.put(allocationId, randomNonNegativeLong());
        }

        final PrimaryContext primaryContext =
                new PrimaryContext(
                        initialClusterStateVersion + randomIntBetween(0, Integer.MAX_VALUE) + 1,
                        activeAllocationIdsLocalCheckpoints,
                        initializingIdsLocalCheckpoints);

        final String initializingAllocationId = randomFrom(Sets.difference(contextInitializingAllocationIds, newInitializingAllocationIds));
        tracker.updateLocalCheckpoint(initializingAllocationId, initializingIdsLocalCheckpoints.get(initializingAllocationId));
        tracker.updateAllocationIdsFromPrimaryContext(initializingAllocationId, primaryContext);

        final PrimaryContext trackerPrimaryContext = tracker.primaryContext();
        try {
            assertTrue(tracker.sealed());
            final long globalCheckpoint =
                    StreamSupport
                            .stream(activeAllocationIdsLocalCheckpoints.values().spliterator(), false)
                            .mapToLong(e -> e.value)
                            .min()
                            .orElse(SequenceNumbersService.UNASSIGNED_SEQ_NO);

            // the primary context contains knowledge of the state of the entire universe
            assertThat(primaryContext.clusterStateVersion(), equalTo(trackerPrimaryContext.clusterStateVersion()));
            assertThat(primaryContext.inSyncLocalCheckpoints(), equalTo(trackerPrimaryContext.inSyncLocalCheckpoints()));
            assertThat(primaryContext.trackingLocalCheckpoints(), equalTo(trackerPrimaryContext.trackingLocalCheckpoints()));
            assertThat(tracker.getGlobalCheckpoint(), equalTo(globalCheckpoint));
        } finally {
            tracker.releasePrimaryContext();
            assertFalse(tracker.sealed());
        }
    }

    public void testPrimaryContextSealing() {
        // the tracker should start in the state of not being sealed
        assertFalse(tracker.sealed());

        // sampling the primary context should seal the tracker
        tracker.primaryContext();
        assertTrue(tracker.sealed());

        /*
         * Invoking methods that mutates the state of the tracker should fail (with the exception of updating allocation IDs and updating
         * global checkpoint on replica which can happen on the relocation source).
         */
        assertIllegalStateExceptionWhenSealed(() -> tracker.updateLocalCheckpoint(randomAlphaOfLength(16), randomNonNegativeLong()));
        assertIllegalStateExceptionWhenSealed(
                () -> tracker.updateAllocationIdsFromPrimaryContext(randomAlphaOfLength(16), mock(PrimaryContext.class)));
        assertIllegalStateExceptionWhenSealed(() -> tracker.primaryContext());
        assertIllegalStateExceptionWhenSealed(() -> tracker.markAllocationIdAsInSync(randomAlphaOfLength(16), randomNonNegativeLong()));

        // closing the releasable should unseal the tracker
        tracker.releasePrimaryContext();
        assertFalse(tracker.sealed());
    }

    private void assertIllegalStateExceptionWhenSealed(final ThrowingRunnable runnable) {
        final IllegalStateException e = expectThrows(IllegalStateException.class, runnable);
        assertThat(e, hasToString(containsString("global checkpoint tracker is sealed")));
    }

    private Tuple<Set<String>, Set<String>> randomActiveAndInitializingAllocationIds(
            final int numberOfActiveAllocationsIds,
            final int numberOfInitializingIds) {
        final Set<String> activeAllocationIds =
                IntStream.range(0, numberOfActiveAllocationsIds).mapToObj(i -> randomAlphaOfLength(16) + i).collect(Collectors.toSet());
        final Set<String> initializingIds = randomAllocationIdsExcludingExistingIds(activeAllocationIds, numberOfInitializingIds);
        return Tuple.tuple(activeAllocationIds, initializingIds);
    }

    private Set<String> randomAllocationIdsExcludingExistingIds(final Set<String> existingAllocationIds, final int numberOfAllocationIds) {
        return IntStream.range(0, numberOfAllocationIds).mapToObj(i -> {
            do {
                final String newAllocationId = randomAlphaOfLength(16);
                // ensure we do not duplicate an allocation ID
                if (!existingAllocationIds.contains(newAllocationId)) {
                    return newAllocationId + i;
                }
            } while (true);
        }).collect(Collectors.toSet());
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
