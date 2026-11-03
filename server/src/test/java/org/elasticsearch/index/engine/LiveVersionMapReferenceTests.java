/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.engine;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.core.Releasable;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Checks {@link LiveVersionMap} and {@link LiveVersionMap2} against a {@link ReferenceVersionMap}: a model of the version map
 * contract whose every operation runs under a single lock. The model does not mirror the implementation. It only records what
 * happened (which version was put or deleted for a uid, in which refresh generation, whether a put was skipped, when safe access
 * was enforced, which prunes ran) and derives from that the answers a correct map may and must give.
 * <p>
 * The contract, in terms of refresh generations ({@code beforeRefresh} starts generation {@code g+1}, {@code afterRefresh}
 * completes it; an operation made in generation {@code g} is "refreshed" once generation {@code g+1} completed, i.e. once a
 * refresh cycle that started after the operation has finished):
 * <ul>
 *     <li>A get returns the latest version put or deleted for the uid, never an older one.</li>
 *     <li>An index version may be forgotten (get returns null) only once it is refreshed; a delete only once it is refreshed and a
 *     prune that covers its timestamp and seqNo ran after that. Both may also be retained longer.</li>
 *     <li>A skipped put (unsafe mode) must leave the map unsafe until the skipped put is refreshed, and must drop any tombstone
 *     for the uid. A get for that uid may return null or the index version the map still had for it.</li>
 *     <li>Safe access enforced in generation {@code g} must be required at least until generation {@code g+1} completed, and a
 *     {@code maybePutIndexUnderLock} made while it is required must be reflected in the map. This is the property that the
 *     races in https://github.com/elastic/elasticsearch/issues/149677 break.</li>
 * </ul>
 * The sequential test checks the model after every operation. The concurrent tests check the uid of each operation under its
 * lock while other uids are modified concurrently, either with refreshes serialized against keyed operations (the model is then
 * exact) or with refreshes racing them (the model is then conservative, see {@link ReferenceVersionMap} for the ordering rules).
 */
public class LiveVersionMapReferenceTests extends ESTestCase {

    /** The subset of the version map API the engine relies on, so both implementations can be driven the same way. */
    interface VersionMapUnderTest {
        Releasable acquireLock(BytesRef uid);

        VersionValue getUnderLock(BytesRef uid);

        void putIndexUnderLock(BytesRef uid, IndexVersionValue version);

        void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version);

        void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version);

        void enforceSafeAccess();

        boolean isSafeAccessRequired();

        boolean isUnsafe();

        void beforeRefresh() throws IOException;

        void afterRefresh(boolean didRefresh) throws IOException;

        void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune);
    }

    public enum Implementation {
        LIVE_VERSION_MAP {
            @Override
            VersionMapUnderTest create() {
                LiveVersionMap map = new LiveVersionMap();
                return new VersionMapUnderTest() {
                    @Override
                    public Releasable acquireLock(BytesRef uid) {
                        return map.acquireLock(uid);
                    }

                    @Override
                    public VersionValue getUnderLock(BytesRef uid) {
                        return map.getUnderLock(uid);
                    }

                    @Override
                    public void putIndexUnderLock(BytesRef uid, IndexVersionValue version) {
                        map.putIndexUnderLock(uid, version);
                    }

                    @Override
                    public void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {
                        map.maybePutIndexUnderLock(uid, version);
                    }

                    @Override
                    public void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version) {
                        map.putDeleteUnderLock(uid, version);
                    }

                    @Override
                    public void enforceSafeAccess() {
                        map.enforceSafeAccess();
                    }

                    @Override
                    public boolean isSafeAccessRequired() {
                        return map.isSafeAccessRequired();
                    }

                    @Override
                    public boolean isUnsafe() {
                        return map.isUnsafe();
                    }

                    @Override
                    public void beforeRefresh() throws IOException {
                        map.beforeRefresh();
                    }

                    @Override
                    public void afterRefresh(boolean didRefresh) throws IOException {
                        map.afterRefresh(didRefresh);
                    }

                    @Override
                    public void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune) {
                        map.pruneTombstones(maxTimestampToPrune, maxSeqNoToPrune);
                    }
                };
            }
        },
        LIVE_VERSION_MAP_2 {
            @Override
            VersionMapUnderTest create() {
                LiveVersionMap2 map = new LiveVersionMap2();
                return new VersionMapUnderTest() {
                    @Override
                    public Releasable acquireLock(BytesRef uid) {
                        return map.acquireLock(uid);
                    }

                    @Override
                    public VersionValue getUnderLock(BytesRef uid) {
                        return map.getUnderLock(uid);
                    }

                    @Override
                    public void putIndexUnderLock(BytesRef uid, IndexVersionValue version) {
                        map.putIndexUnderLock(uid, version);
                    }

                    @Override
                    public void maybePutIndexUnderLock(BytesRef uid, IndexVersionValue version) {
                        map.maybePutIndexUnderLock(uid, version);
                    }

                    @Override
                    public void putDeleteUnderLock(BytesRef uid, DeleteVersionValue version) {
                        map.putDeleteUnderLock(uid, version);
                    }

                    @Override
                    public void enforceSafeAccess() {
                        map.enforceSafeAccess();
                    }

                    @Override
                    public boolean isSafeAccessRequired() {
                        return map.isSafeAccessRequired();
                    }

                    @Override
                    public boolean isUnsafe() {
                        return map.isUnsafe();
                    }

                    @Override
                    public void beforeRefresh() throws IOException {
                        map.beforeRefresh();
                    }

                    @Override
                    public void afterRefresh(boolean didRefresh) throws IOException {
                        map.afterRefresh(didRefresh);
                    }

                    @Override
                    public void pruneTombstones(long maxTimestampToPrune, long maxSeqNoToPrune) {
                        map.pruneTombstones(maxTimestampToPrune, maxSeqNoToPrune);
                    }
                };
            }
        };

        abstract VersionMapUnderTest create();
    }

    private final Implementation implementation;

    public LiveVersionMapReferenceTests(@Name("implementation") Implementation implementation) {
        this.implementation = implementation;
    }

    @ParametersFactory(shuffle = false)
    public static Iterable<Object[]> parameters() {
        return Arrays.stream(Implementation.values()).map(implementation -> new Object[] { implementation }).toList();
    }

    public void testRandomOperationsSequentially() {
        Harness harness = new Harness(Mode.SEQUENTIAL, between(1, 5));
        int operations = between(100, 1000);
        for (int i = 0; i < operations; i++) {
            BytesRef uid = randomFrom(harness.uids);
            switch (between(0, 7)) {
                case 0, 1 -> harness.putIndex(uid);
                case 2 -> harness.maybePutIndex(uid, randomBoolean());
                case 3 -> harness.putDelete(uid);
                case 4 -> harness.enforceSafeAccess();
                case 5 -> {
                    // refreshes are split in two so that operations (in particular enforceSafeAccess) land in between the two steps
                    if (harness.reference.isRefreshInProgress()) {
                        harness.afterRefresh(randomBoolean());
                    } else {
                        harness.beforeRefresh();
                    }
                }
                case 6 -> harness.randomPrune(random());
                case 7 -> harness.verify(uid);
                default -> throw new AssertionError("unexpected operation");
            }
            harness.verifyAll();
        }
    }

    public void testConcurrentOperationsWithSerializedRefreshes() {
        runConcurrently(Mode.SERIALIZED_REFRESHES);
    }

    public void testConcurrentOperationsWithRacingRefreshes() {
        runConcurrently(Mode.RACING_REFRESHES);
    }

    /**
     * Runs writer threads doing random keyed operations, one thread refreshing (and occasionally pruning) in a loop, and one
     * thread verifying random uids, all concurrently. Each thread has its own seeded {@link Random} so the sequence of
     * operations per thread is reproducible even though their interleaving is not.
     */
    private void runConcurrently(Mode mode) {
        Harness harness = new Harness(mode, between(1, 20));
        int writers = between(2, 4);
        int operationsPerWriter = between(1000, 5000);
        int tasks = writers + 2;
        long[] seeds = new long[tasks];
        for (int i = 0; i < tasks; i++) {
            seeds[i] = randomLong();
        }
        AtomicInteger activeWriters = new AtomicInteger(writers);
        startInParallel(tasks, i -> {
            Random random = new Random(seeds[i]);
            if (i < writers) {
                try {
                    for (int op = 0; op < operationsPerWriter; op++) {
                        harness.randomKeyedOperation(random);
                    }
                } finally {
                    activeWriters.decrementAndGet();
                }
            } else if (i == writers) {
                while (activeWriters.get() > 0) {
                    harness.beforeRefresh();
                    if (random.nextBoolean()) {
                        Thread.yield();
                    }
                    harness.afterRefresh(random.nextBoolean());
                    if (random.nextInt(10) == 0) {
                        harness.randomPrune(random);
                    }
                    // vary the refresh rate so that writers also get windows during which several operations see no refresh
                    if (random.nextInt(4) == 0) {
                        LockSupport.parkNanos(TimeUnit.MICROSECONDS.toNanos(random.nextInt(100)));
                    }
                }
            } else {
                while (activeWriters.get() > 0) {
                    harness.verify(RandomPicks.randomFrom(random, harness.uids));
                }
            }
        });
        // once everything is quiescent, a full refresh cycle and an unbounded prune must still leave the map consistent
        harness.verifyAll();
        harness.beforeRefresh();
        harness.verifyAll();
        harness.afterRefresh(true);
        harness.verifyAll();
        harness.prune(Long.MAX_VALUE, Long.MAX_VALUE);
        harness.verifyAll();
    }

    private enum Mode {
        /** A single thread, refreshes may be split around other operations. The model is exact. */
        SEQUENTIAL,
        /** Keyed operations run concurrently but each refresh step is serialized against them. The model is exact. */
        SERIALIZED_REFRESHES,
        /** Refreshes race keyed operations. The model is conservative. */
        RACING_REFRESHES
    }

    /**
     * Drives the real map and the reference model together. Every keyed operation runs under the uid lock of the real map,
     * takes a generation snapshot from the model before touching the real map, then records the operation in the model after
     * the real map has performed it, and finally verifies the uid (see {@link ReferenceVersionMap} for why this order matters).
     */
    private class Harness {
        final VersionMapUnderTest map = implementation.create();
        final ReferenceVersionMap reference = new ReferenceVersionMap();
        final List<BytesRef> uids;
        final Mode mode;
        /** Serializes refresh steps (write lock) against keyed operations and prunes (read lock) in {@link Mode#SERIALIZED_REFRESHES}. */
        private final ReentrantReadWriteLock refreshLock = new ReentrantReadWriteLock();
        /** Versions are unique per operation so that a stale value can never be mistaken for the latest one. */
        private final AtomicLong versions = new AtomicLong();
        private final AtomicLong seqNos = new AtomicLong();
        private final AtomicLong clock = new AtomicLong();

        Harness(Mode mode, int numUids) {
            this.mode = mode;
            this.uids = new ArrayList<>(numUids);
            for (int i = 0; i < numUids; i++) {
                // the array length must match the ref length, LiveVersionMap asserts on it
                uids.add(BytesRef.deepCopyOf(new BytesRef("uid-" + i)));
            }
        }

        void randomKeyedOperation(Random random) {
            BytesRef uid = RandomPicks.randomFrom(random, uids);
            switch (random.nextInt(6)) {
                case 0, 1 -> putIndex(uid);
                case 2 -> maybePutIndex(uid, random.nextBoolean());
                case 3 -> putDelete(uid);
                case 4 -> enforceSafeAccess();
                case 5 -> verify(uid);
                default -> throw new AssertionError("unexpected operation");
            }
        }

        void putIndex(BytesRef uid) {
            try (Releasable ignored = keyedOperationScope(); Releasable lock = map.acquireLock(uid)) {
                long generation = reference.generation();
                IndexVersionValue version = newIndexVersionValue();
                map.putIndexUnderLock(uid, version);
                reference.putIndex(uid, version, generation);
                verifyUnderLock(uid);
            }
        }

        void maybePutIndex(BytesRef uid, boolean enforceSafeAccessFirst) {
            try (Releasable ignored = keyedOperationScope(); Releasable lock = map.acquireLock(uid)) {
                long generation = reference.generation();
                if (enforceSafeAccessFirst) {
                    map.enforceSafeAccess();
                    reference.enforceSafeAccess(generation);
                }
                long enforcedGeneration = reference.enforcedGenerationIfSafeAccessRequired();
                IndexVersionValue version = newIndexVersionValue();
                map.maybePutIndexUnderLock(uid, version);
                VersionValue actual = map.getUnderLock(uid);
                if (version.equals(actual)) {
                    reference.putIndex(uid, version, generation);
                } else if (reference.isUnrefreshed(generation) == false) {
                    // a refresh cycle overtook the operation: the version may have been put and already legitimately forgotten,
                    // or it may have been skipped. Nothing can be asserted here, and later gets must allow for both.
                    reference.putOrSkipIndex(uid, version, generation);
                } else {
                    assertFalse(
                        "put of "
                            + version
                            + " for "
                            + uid.utf8ToString()
                            + " was skipped although safe access is required since generation "
                            + enforcedGeneration
                            + " "
                            + reference,
                        reference.isUnrefreshed(enforcedGeneration)
                    );
                    reference.skipIndex(uid, version, generation);
                }
                verifyUnderLock(uid);
            }
        }

        void putDelete(BytesRef uid) {
            try (Releasable ignored = keyedOperationScope(); Releasable lock = map.acquireLock(uid)) {
                long generation = reference.generation();
                DeleteVersionValue version = new DeleteVersionValue(
                    versions.incrementAndGet(),
                    seqNos.incrementAndGet(),
                    1,
                    clock.incrementAndGet()
                );
                map.putDeleteUnderLock(uid, version);
                reference.putDelete(uid, version, generation);
                verifyUnderLock(uid);
            }
        }

        void enforceSafeAccess() {
            try (Releasable ignored = keyedOperationScope()) {
                long generation = reference.generation();
                map.enforceSafeAccess();
                reference.enforceSafeAccess(generation);
                verifyFlags();
            }
        }

        void beforeRefresh() {
            try (Releasable ignored = refreshScope()) {
                map.beforeRefresh();
                reference.beforeRefresh();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void afterRefresh(boolean didRefresh) {
            try (Releasable ignored = refreshScope()) {
                reference.afterRefresh();
                map.afterRefresh(didRefresh);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        void randomPrune(Random random) {
            long maxTimestampToPrune = random.nextBoolean() ? Long.MAX_VALUE : RandomNumbers.randomLongBetween(random, 0, clock.get());
            long maxSeqNoToPrune = random.nextBoolean() ? Long.MAX_VALUE : RandomNumbers.randomLongBetween(random, 0, seqNos.get());
            prune(maxTimestampToPrune, maxSeqNoToPrune);
        }

        void prune(long maxTimestampToPrune, long maxSeqNoToPrune) {
            try (Releasable ignored = keyedOperationScope()) {
                int pruneId = reference.pruneStarted(maxTimestampToPrune, maxSeqNoToPrune);
                map.pruneTombstones(maxTimestampToPrune, maxSeqNoToPrune);
                reference.pruneCompleted(pruneId);
            }
        }

        void verify(BytesRef uid) {
            try (Releasable ignored = keyedOperationScope(); Releasable lock = map.acquireLock(uid)) {
                verifyUnderLock(uid);
            }
        }

        void verifyAll() {
            for (BytesRef uid : uids) {
                verify(uid);
            }
        }

        private void verifyUnderLock(BytesRef uid) {
            VersionValue actual = map.getUnderLock(uid);
            List<VersionValue> allowed = reference.allowedValues(uid);
            assertTrue(
                "get for " + uid.utf8ToString() + " returned " + actual + " but expected one of " + allowed + " " + reference,
                allowed.contains(actual)
            );
            verifyFlags();
        }

        /**
         * The generation that gives rise to a requirement (a skipped put, an enforced safe access) is read from the model before
         * the real map is queried, and the requirement is enforced only if that same generation is still unrefreshed afterwards.
         * In {@link Mode#RACING_REFRESHES} the model is updated after the real map, so a requirement that appeared in between
         * (e.g. a skipped put on another uid) may not have been visible to the real map at the time of the query, and one that
         * disappeared in between may have legitimately stopped applying.
         */
        private void verifyFlags() {
            long skippedGeneration = reference.latestUnrefreshedSkippedGeneration();
            long enforcedGeneration = reference.enforcedGenerationIfSafeAccessRequired();
            boolean unsafe = map.isUnsafe();
            boolean safeAccessRequired = map.isSafeAccessRequired();
            if (reference.isUnrefreshed(skippedGeneration)) {
                assertTrue(
                    "map is not unsafe although the put skipped in generation " + skippedGeneration + " is not refreshed " + reference,
                    unsafe
                );
            }
            if (mode == Mode.SEQUENTIAL && reference.latestUnrefreshedSkippedGeneration() < 0) {
                assertFalse("map is unsafe although every skipped put has been refreshed " + reference, unsafe);
            }
            if (reference.isUnrefreshed(enforcedGeneration)) {
                assertTrue(
                    "safe access is not required although it was enforced in generation "
                        + enforcedGeneration
                        + " which is not refreshed "
                        + reference,
                    safeAccessRequired
                );
            }
        }

        private IndexVersionValue newIndexVersionValue() {
            return new IndexVersionValue(null, versions.incrementAndGet(), seqNos.incrementAndGet(), 1);
        }

        private Releasable keyedOperationScope() {
            if (mode == Mode.SERIALIZED_REFRESHES) {
                refreshLock.readLock().lock();
                return () -> refreshLock.readLock().unlock();
            }
            return () -> {};
        }

        private Releasable refreshScope() {
            if (mode == Mode.SERIALIZED_REFRESHES) {
                refreshLock.writeLock().lock();
                return () -> refreshLock.writeLock().unlock();
            }
            return () -> {};
        }
    }

    /**
     * The reference model. All state is guarded by the instance lock, so it is trivially consistent. The model never answers
     * "the value is X", it answers "the value must be one of these" and "the map must (not) be unsafe / require safe access",
     * which is what lets it stay valid while the real map may retain versions longer than required, prune best-effort, etc.
     * <p>
     * When used concurrently with refreshes racing keyed operations, the callers keep the model conservative by ordering
     * their calls so that the model's refresh generation is never ahead of the real map's and the completed generation is
     * never behind it:
     * <ul>
     *     <li>{@link #beforeRefresh()} is called after the real map's {@code beforeRefresh}, {@link #afterRefresh()} before the
     *     real map's {@code afterRefresh}.</li>
     *     <li>A keyed operation takes its generation via {@link #generation()} before touching the real map and records
     *     itself after the real map performed it.</li>
     *     <li>A prune is registered via {@link #pruneStarted} before the real map prunes.</li>
     * </ul>
     * Under these rules whatever the real map is allowed to forget, the model already allows it to forget, and whatever the
     * model requires the real map to still know, it has not yet been allowed to forget.
     */
    static final class ReferenceVersionMap {

        private enum Kind {
            INDEX,
            DELETE,
            /** A put made in unsafe mode that the map did not reflect. */
            SKIPPED_INDEX,
            /** A put made in unsafe mode that the map either did not reflect or had already forgotten when it was checked. */
            UNCERTAIN_INDEX
        }

        /**
         * The latest operation for a uid.
         *
         * @param residue              for a skipped or uncertain put, the index versions the map may still hold for the uid
         * @param firstApplicablePrune for a delete, the id of the earliest prune that could have observed it
         */
        private record KeyState(Kind kind, VersionValue value, long generation, List<VersionValue> residue, int firstApplicablePrune) {}

        private record Prune(int id, long maxTimestampToPrune, long maxSeqNoToPrune) {
            boolean covers(DeleteVersionValue delete) {
                return delete.time < maxTimestampToPrune && delete.seqNo <= maxSeqNoToPrune;
            }
        }

        private final Map<BytesRef, KeyState> states = new HashMap<>();
        private final List<Long> skippedGenerations = new ArrayList<>();
        private final List<Prune> prunes = new ArrayList<>();
        private final TreeSet<Integer> prunesInFlight = new TreeSet<>();
        /** Number of {@code beforeRefresh} calls, i.e. the generation new operations land in. */
        private long generation;
        /** Number of {@code afterRefresh} calls. Generation {@code g} is refreshed once {@code completedGeneration > g}. */
        private long completedGeneration;
        private long enforcedAtGeneration = -1;
        private int nextPruneId;

        synchronized long generation() {
            return generation;
        }

        synchronized boolean isRefreshInProgress() {
            return generation > completedGeneration;
        }

        synchronized void beforeRefresh() {
            assert generation == completedGeneration : "refresh already in progress";
            generation++;
        }

        synchronized void afterRefresh() {
            assert generation == completedGeneration + 1 : "no refresh in progress";
            completedGeneration++;
        }

        /** Whether no refresh cycle that started after the given generation has completed yet. False for a negative generation. */
        synchronized boolean isUnrefreshed(long operationGeneration) {
            return operationGeneration >= 0 && completedGeneration <= operationGeneration;
        }

        synchronized void putIndex(BytesRef uid, IndexVersionValue version, long generation) {
            states.put(uid, new KeyState(Kind.INDEX, version, generation, List.of(), 0));
        }

        synchronized void putDelete(BytesRef uid, DeleteVersionValue version, long generation) {
            // a prune that is still running might observe this delete, one that completed before cannot
            int firstApplicablePrune = prunesInFlight.isEmpty() ? nextPruneId : prunesInFlight.first();
            states.put(uid, new KeyState(Kind.DELETE, version, generation, List.of(), firstApplicablePrune));
        }

        synchronized void skipIndex(BytesRef uid, IndexVersionValue version, long generation) {
            states.put(uid, new KeyState(Kind.SKIPPED_INDEX, version, generation, residue(uid), 0));
            skippedGenerations.add(generation);
        }

        synchronized void putOrSkipIndex(BytesRef uid, IndexVersionValue version, long generation) {
            assert isUnrefreshed(generation) == false : "only a refreshed operation can be uncertain";
            states.put(uid, new KeyState(Kind.UNCERTAIN_INDEX, version, generation, residue(uid), 0));
        }

        /** The index versions the map may still hold for the uid if a put for it is skipped now. */
        private List<VersionValue> residue(BytesRef uid) {
            KeyState previous = states.get(uid);
            if (previous == null) {
                return List.of();
            }
            return switch (previous.kind) {
                case INDEX -> List.of(previous.value);
                case DELETE -> List.of();
                case SKIPPED_INDEX -> previous.residue;
                case UNCERTAIN_INDEX -> {
                    List<VersionValue> residue = new ArrayList<>(previous.residue);
                    residue.add(previous.value);
                    yield residue;
                }
            };
        }

        synchronized void enforceSafeAccess(long generation) {
            enforcedAtGeneration = Math.max(enforcedAtGeneration, generation);
        }

        /**
         * Safe access enforced in generation {@code g} must be required until generation {@code g+1} completed. Returns the
         * generation safe access was last enforced in if that still holds, -1 otherwise.
         */
        synchronized long enforcedGenerationIfSafeAccessRequired() {
            return isUnrefreshed(enforcedAtGeneration) ? enforcedAtGeneration : -1;
        }

        /**
         * The map must be unsafe while a skipped put is not refreshed. Returns the latest generation of such a put, or -1.
         * Evaluated lazily: a skipped put is recorded after the real map performed it, by which time the refresher may
         * already have moved past its generation.
         */
        synchronized long latestUnrefreshedSkippedGeneration() {
            skippedGenerations.removeIf(skipped -> isUnrefreshed(skipped) == false);
            return skippedGenerations.stream().mapToLong(Long::longValue).max().orElse(-1);
        }

        synchronized int pruneStarted(long maxTimestampToPrune, long maxSeqNoToPrune) {
            int id = nextPruneId++;
            prunes.add(new Prune(id, maxTimestampToPrune, maxSeqNoToPrune));
            prunesInFlight.add(id);
            return id;
        }

        synchronized void pruneCompleted(int id) {
            prunesInFlight.remove(id);
        }

        /** The values a correct map may return from a get for the uid. Contains null where the map may have forgotten the uid. */
        synchronized List<VersionValue> allowedValues(BytesRef uid) {
            KeyState state = states.get(uid);
            if (state == null) {
                return Arrays.asList((VersionValue) null);
            }
            List<VersionValue> allowed = new ArrayList<>();
            switch (state.kind) {
                case INDEX -> {
                    allowed.add(state.value);
                    if (isUnrefreshed(state.generation) == false) {
                        allowed.add(null);
                    }
                }
                case DELETE -> {
                    allowed.add(state.value);
                    if (mayBePruned(state)) {
                        allowed.add(null);
                    }
                }
                case SKIPPED_INDEX -> {
                    allowed.add(null);
                    allowed.addAll(state.residue);
                }
                case UNCERTAIN_INDEX -> {
                    allowed.add(state.value);
                    allowed.add(null);
                    allowed.addAll(state.residue);
                }
            }
            return allowed;
        }

        private boolean mayBePruned(KeyState state) {
            DeleteVersionValue delete = (DeleteVersionValue) state.value;
            return isUnrefreshed(state.generation) == false
                && prunes.stream().anyMatch(prune -> prune.id >= state.firstApplicablePrune && prune.covers(delete));
        }

        @Override
        public synchronized String toString() {
            return "[generation="
                + generation
                + ", completedGeneration="
                + completedGeneration
                + ", enforcedAtGeneration="
                + enforcedAtGeneration
                + ", skippedGenerations="
                + skippedGenerations
                + ", states="
                + states.entrySet().stream().map(e -> e.getKey().utf8ToString() + "=" + e.getValue()).toList()
                + "]";
        }
    }
}
