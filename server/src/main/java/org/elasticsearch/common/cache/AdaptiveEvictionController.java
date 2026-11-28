/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.common.cache;

import java.util.Arrays;
import java.util.SplittableRandom;

/**
 * The state machine behind {@link AdaptiveEviction}. Every method must be called with the owning cache's LRU lock held.
 * <p>
 * Shadow caches ("ghosts") of the same maximum weight replay every access of the real cache; each ghost's miss indicator drives a
 * Hedge weight vector with fixed share. One ghost is "followed". The real cache keeps its normal LRU list, but when it is over
 * weight it evicts, in this order: a just-inserted entry that the followed ghost would not have admitted, the oldest real entry
 * the followed ghost does not hold, and finally the least recently used entry. Changing the followed ghost uses the
 * minimal-switching coupling: stay unless the followed ghost's weight fell, then move with the probability of the fall to a
 * ghost chosen in proportion to the weight increases.
 * <p>
 * This runs on every cache access under the cache's single LRU lock, so its cost is added to every lookup. The LRU and LFU ghosts and
 * the table of real entries therefore keep their per-key state in primitive arrays indexed by a {@link KeyDirectory} id: an access
 * hashes its key once, allocates nothing and boxes nothing.
 */
final class AdaptiveEvictionController<K, V> {

    /**
     * Told when the followed ghost starts or stops holding a key. {@code id} is the key's {@link KeyDirectory} id, or ABSENT if the
     * ghost does not know it.
     */
    interface Membership {
        void inserted(long fingerprint, int id);

        void evicted(long fingerprint, int id);
    }

    abstract static class Ghost {
        /** The key was not held and the ghost did not admit it. */
        static final int REJECTED = 0;
        /** The key was not held; the ghost admitted it (possibly evicting others). */
        static final int ADMITTED = 1;
        /** The key was held. */
        static final int HIT = 2;

        final long maxWeight;

        Ghost(long maxWeight) {
            this.maxWeight = maxWeight;
        }

        /** Makes room for the per-id state of ids below {@code capacity}. */
        void ensureCapacity(int capacity) {}

        /**
         * Replays one access of the key with the given {@link KeyDirectory} id; returns {@link #REJECTED}, {@link #ADMITTED} or
         * {@link #HIT}.
         */
        abstract int access(long fingerprint, int id, long weight, Membership listener);

        abstract boolean contains(long fingerprint, int id);

        /** Verifies internal consistency; for tests. */
        void checkInvariants() {}

    }

    /** A plain LRU: a doubly linked list threaded through per-id arrays. */
    static final class GhostLru extends Ghost {
        private static final int NONE = -1;
        private static final int NOT_HELD = -2;

        private final KeyDirectory directory;
        private int[] weights = new int[0];   // clamped to the int range: no entry weighs that much
        private int[] prev = new int[0];
        private int[] next = new int[0];   // NOT_HELD for a key that is not in the ghost
        private int head = NONE;           // most recently used
        private int tail = NONE;           // least recently used
        private long total;

        GhostLru(long maxWeight, KeyDirectory directory) {
            super(maxWeight);
            this.directory = directory;
        }

        @Override
        void ensureCapacity(int capacity) {
            if (capacity > next.length) {
                final int old = next.length;
                weights = Arrays.copyOf(weights, capacity);
                prev = Arrays.copyOf(prev, capacity);
                next = Arrays.copyOf(next, capacity);
                Arrays.fill(next, old, capacity, NOT_HELD);
            }
        }

        @Override
        boolean contains(long fingerprint, int id) {
            return next[id] != NOT_HELD;
        }

        @Override
        int access(long fp, int id, long weight, Membership listener) {
            if (next[id] != NOT_HELD) {
                if (id != head) {
                    unlink(id);
                    linkAtHead(id);
                }
                return HIT;
            }
            if (weight > maxWeight) {
                return REJECTED;
            }
            final int clamped = (int) Math.min(weight, Integer.MAX_VALUE);
            weights[id] = clamped;
            linkAtHead(id);
            directory.hold(id);
            total += clamped;
            if (listener != null) {
                listener.inserted(fp, id);
            }
            while (total > maxWeight && tail != NONE) {
                final int eldest = tail;
                unlink(eldest);
                next[eldest] = NOT_HELD;
                total -= weights[eldest];
                if (listener != null) {
                    listener.evicted(directory.fingerprint(eldest), eldest);
                }
                directory.drop(eldest);
            }
            return ADMITTED;
        }

        @Override
        void checkInvariants() {
            long sum = 0;
            int count = 0;
            int previous = NONE;
            for (int id = head; id != NONE; id = next[id]) {
                if (prev[id] != previous) {
                    throw new AssertionError("lru ghost: broken prev link at id " + id);
                }
                previous = id;
                sum += weights[id];
                if (++count > next.length) {
                    throw new AssertionError("lru ghost: cycle");
                }
            }
            if (previous != tail) {
                throw new AssertionError("lru ghost: wrong tail");
            }
            int members = 0;
            for (int id = 0; id < next.length; id++) {
                if (next[id] != NOT_HELD) {
                    members++;
                }
            }
            if (members != count) {
                throw new AssertionError("lru ghost: " + members + " members but " + count + " listed");
            }
            if (sum != total || total > maxWeight) {
                throw new AssertionError("lru ghost: total " + total + " but weights sum to " + sum + ", max " + maxWeight);
            }
        }

        private void linkAtHead(int id) {
            prev[id] = NONE;
            next[id] = head;
            if (head != NONE) {
                prev[head] = id;
            } else {
                tail = id;
            }
            head = id;
        }

        private void unlink(int id) {
            final int p = prev[id];
            final int n = next[id];
            if (p != NONE) {
                next[p] = n;
            } else {
                head = n;
            }
            if (n != NONE) {
                prev[n] = p;
            } else {
                tail = p;
            }
        }
    }

    /**
     * LFU with history: frequencies come from a count-min sketch, so a key that was popular before it was evicted is remembered. Held
     * keys are kept in one LRU list per frequency level, so the victim (the least frequent key, the least recently used among those)
     * and every update are O(1) array operations. Levels are exact below 16 and powers of two above, where telling a key seen 40 times
     * from one seen 50 times does not matter. A held key's level is the sketch's estimate at its last access, refreshed for all held
     * keys whenever the sketch halves; it is deliberately not re-read at eviction time, which would cost a sketch lookup per victim for
     * counts that only differ because other keys share the key's counters.
     */
    static final class GhostLfu extends Ghost {
        private static final int NONE = -1;
        private static final int NOT_HELD = -2;
        private static final int LEVELS = 64;

        private final KeyDirectory directory;
        private final FrequencySketch sketch;
        private int[] weights = new int[0];   // clamped to the int range: no entry weighs that much
        private int[] level = new int[0];
        private int[] prev = new int[0];
        private int[] next = new int[0];       // NOT_HELD for a key that is not in the ghost
        private final int[] mru = new int[LEVELS];
        private final int[] lru = new int[LEVELS];
        private long nonEmpty;                 // bit i is set when level i holds a key
        private int held;
        private long total;

        GhostLfu(long maxWeight, int sketchWidth, long sketchSample, KeyDirectory directory) {
            super(maxWeight);
            this.directory = directory;
            this.sketch = new FrequencySketch(sketchWidth, sketchSample);
            Arrays.fill(mru, NONE);
            Arrays.fill(lru, NONE);
        }

        private static int levelOf(int frequency) {
            return frequency < 16 ? frequency : Math.min(LEVELS - 1, 12 + 31 - Integer.numberOfLeadingZeros(frequency));
        }

        @Override
        void ensureCapacity(int capacity) {
            if (capacity > next.length) {
                final int old = next.length;
                weights = Arrays.copyOf(weights, capacity);
                level = Arrays.copyOf(level, capacity);
                prev = Arrays.copyOf(prev, capacity);
                next = Arrays.copyOf(next, capacity);
                Arrays.fill(next, old, capacity, NOT_HELD);
            }
        }

        @Override
        boolean contains(long fingerprint, int id) {
            return next[id] != NOT_HELD;
        }

        @Override
        int access(long fp, int id, long weight, Membership listener) {
            final int estimate = sketch.incrementAndEstimate(fp);
            if (sketch.wasHalved()) {
                rebuild();
            }
            if (next[id] != NOT_HELD) {
                final int target = levelOf(estimate);
                if (target != level[id] || id != mru[target]) {
                    unlink(id);
                    linkAtMru(id, target);
                }
                return HIT;
            }
            if (weight > maxWeight) {
                return REJECTED;
            }
            while (total + weight > maxWeight && held > 0) {
                evictLeastFrequent(listener);
            }
            final int clamped = (int) Math.min(weight, Integer.MAX_VALUE);
            weights[id] = clamped;
            directory.hold(id);
            total += clamped;
            linkAtMru(id, levelOf(estimate));
            if (listener != null) {
                listener.inserted(fp, id);
            }
            return ADMITTED;
        }

        private void evictLeastFrequent(Membership listener) {
            final int id = lru[Long.numberOfTrailingZeros(nonEmpty)];
            unlink(id);
            next[id] = NOT_HELD;
            total -= weights[id];
            if (listener != null) {
                listener.evicted(directory.fingerprint(id), id);
            }
            directory.drop(id);
        }

        /** After the sketch was halved every level is stale: put every held key on the level of its new estimate, keeping the order. */
        private void rebuild() {
            int count = 0;
            final int[] order = new int[held];   // transient: a rebuild happens once per ten sketch-widths of accesses
            long bits = nonEmpty;
            while (bits != 0) {
                final int l = Long.numberOfTrailingZeros(bits);
                bits &= bits - 1;
                for (int id = lru[l]; id != NONE; id = prev[id]) {   // from the least to the most recently used
                    order[count++] = id;
                }
            }
            Arrays.fill(mru, NONE);
            Arrays.fill(lru, NONE);
            nonEmpty = 0;
            held = 0;
            for (int i = 0; i < count; i++) {
                final int id = order[i];
                linkAtMru(id, levelOf(sketch.estimate(directory.fingerprint(id))));
            }
        }

        @Override
        void checkInvariants() {
            long sum = 0;
            int count = 0;
            for (int l = 0; l < LEVELS; l++) {
                final boolean flagged = (nonEmpty & (1L << l)) != 0;
                if (flagged != (mru[l] != NONE) || (mru[l] != NONE) != (lru[l] != NONE)) {
                    throw new AssertionError("lfu ghost: level " + l + " bookkeeping is inconsistent");
                }
                int previous = NONE;
                for (int id = mru[l]; id != NONE; id = next[id]) {
                    if (level[id] != l || prev[id] != previous) {
                        throw new AssertionError("lfu ghost: broken link at id " + id + " on level " + l);
                    }
                    previous = id;
                    sum += weights[id];
                    if (++count > next.length) {
                        throw new AssertionError("lfu ghost: cycle");
                    }
                }
                if (previous != lru[l]) {
                    throw new AssertionError("lfu ghost: wrong tail on level " + l);
                }
            }
            int members = 0;
            for (int id = 0; id < next.length; id++) {
                if (next[id] != NOT_HELD) {
                    members++;
                }
            }
            if (members != count || count != held) {
                throw new AssertionError("lfu ghost: " + members + " members, " + count + " listed, held=" + held);
            }
            if (sum != total || total > maxWeight) {
                throw new AssertionError("lfu ghost: total " + total + " but weights sum to " + sum + ", max " + maxWeight);
            }
        }

        private void linkAtMru(int id, int target) {
            level[id] = target;
            prev[id] = NONE;
            next[id] = mru[target];
            if (mru[target] != NONE) {
                prev[mru[target]] = id;
            } else {
                lru[target] = id;
                nonEmpty |= 1L << target;
            }
            mru[target] = id;
            held++;
        }

        private void unlink(int id) {
            final int l = level[id];
            final int p = prev[id];
            final int n = next[id];
            if (p != NONE) {
                next[p] = n;
            } else {
                mru[l] = n;
            }
            if (n != NONE) {
                prev[n] = p;
            } else {
                lru[l] = p;
                if (p == NONE) {
                    nonEmpty &= ~(1L << l);
                }
            }
            held--;
        }
    }

    /**
     * The Hedge weights are updated once per this many accesses, with the summed losses of the block and the fixed share of the block
     * compounded. That is the standard mini-batch (delayed feedback) form of Hedge; it makes the learner react up to
     * {@code UPDATE_BLOCK - 1} requests later, which does not matter for caches that see thousands of requests between traffic changes,
     * and it takes the weight update off most accesses.
     */
    private static final int UPDATE_BLOCK = 4;

    private final double eta;
    private final KeyDirectory directory = new KeyDirectory();
    private final Ghost[] experts;
    private final String[] names;
    private final double[] weights;
    private final double[] previousWeights;
    private final double[] lossScratch;
    private final double[] pendingLoss;
    private final double[] missPowers;
    private int pending;
    private final double blockAlpha;
    private final SplittableRandom rng;
    private int followed;

    private final RealEntryTable<Cache.Entry<K, V>> real = new RealEntryTable<>(directory);
    private int idCapacity;

    /** Keeps the outside marks of the real entries in step with what the followed ghost admits and evicts. */
    private final Membership membership = new Membership() {
        @Override
        public void inserted(long fingerprint, int id) {
            final int known = id != KeyDirectory.ABSENT ? id : directory.get((int) fingerprint);
            if (known != KeyDirectory.ABSENT) {
                real.markInside(known);
            }
        }

        @Override
        public void evicted(long fingerprint, int id) {
            final int known = id != KeyDirectory.ABSENT ? id : directory.get((int) fingerprint);
            if (known != KeyDirectory.ABSENT) {
                real.markOutside(known);
            }
        }
    };

    // the id of the entry handled last, so that the eviction and the removal that follow an insertion do not hash the key again
    private Cache.Entry<K, V> lastEntry;
    private int lastId;

    private long accesses;
    private long switches;
    private final long[] followSteps;
    private final long[] expertHits;

    AdaptiveEvictionController(AdaptiveEviction<K> config, long maximumWeight) {
        this.eta = config.eta();
        this.experts = new Ghost[] {
            new GhostLru(maximumWeight, directory),
            new GhostLfu(maximumWeight, config.sketchWidth(), config.sketchSample(), directory) };
        this.names = new String[] { "lru", "lfu" };
        this.weights = new double[] { 0.5, 0.5 };
        this.previousWeights = new double[experts.length];
        this.lossScratch = new double[experts.length];
        this.pendingLoss = new double[experts.length];
        this.followSteps = new long[experts.length];
        this.expertHits = new long[experts.length];
        this.missPowers = new double[UPDATE_BLOCK + 1];
        for (int k = 0; k <= UPDATE_BLOCK; k++) {
            missPowers[k] = Math.exp(-config.eta() * k);
        }
        // the fixed share of UPDATE_BLOCK consecutive per-request updates, applied once
        this.blockAlpha = 1 - Math.pow(1 - config.alpha(), UPDATE_BLOCK);
        this.rng = new SplittableRandom(config.seed());
        this.followed = 0;
    }

    /**
     * Replays an access. Called after the real cache linked {@code entry} at the head of its LRU list.
     *
     * @param realInsert true if the entry was just inserted (the access is a miss of the real cache), false for a hit
     */
    void onAccess(Cache.Entry<K, V> entry, long weight, boolean realInsert) {
        final int hash = entry.key.hashCode();
        final long fp = hash & 0xFFFFFFFFL;
        final int id = directory.getOrCreate(hash);
        if (id >= idCapacity) {
            idCapacity = directory.capacity();
            real.ensureCapacity(idCapacity);
            for (Ghost g : experts) {
                g.ensureCapacity(idCapacity);
            }
        }
        lastEntry = entry;
        lastId = id;
        accesses++;
        followSteps[followed]++;
        final int n = experts.length;
        int followedStatus = Ghost.REJECTED;
        for (int i = 0; i < n; i++) {
            final int status = experts[i].access(fp, id, weight, i == followed ? membership : null);
            if (i == followed) {
                followedStatus = status;
            }
            if (status == Ghost.HIT) {
                lossScratch[i] = 0;
                expertHits[i]++;
            } else {
                lossScratch[i] = 1.0;
            }
        }
        // An entry is outside exactly when the followed ghost does not hold its key. That is kept up to date by the ghost's
        // callbacks, so only an insertion, or a hit on a key the followed ghost rejected, has to say anything here.
        if (realInsert) {
            real.put(id, entry, followedStatus == Ghost.REJECTED);
        } else if (followedStatus == Ghost.REJECTED) {
            real.markOutside(id, entry);
        }
        if (directory.lastCreated()) {
            directory.releaseIfUnheld(id);
        }
        for (int i = 0; i < n; i++) {
            pendingLoss[i] += lossScratch[i];
        }
        if (++pending == UPDATE_BLOCK) {
            pending = 0;
            updateWeightsAndMaybeSwitch(n);
        }
    }

    private void updateWeightsAndMaybeSwitch(int n) {
        final double[] old = previousWeights;
        System.arraycopy(weights, 0, old, 0, n);
        double sum = 0;
        for (int i = 0; i < n; i++) {
            final double loss = pendingLoss[i];
            pendingLoss[i] = 0;
            final int misses = (int) loss;
            weights[i] *= misses == loss ? missPowers[misses] : Math.exp(-eta * loss);
            sum += weights[i];
        }
        final double scale = (1 - blockAlpha) / sum;
        final double share = blockAlpha / n;
        for (int i = 0; i < n; i++) {
            weights[i] = weights[i] * scale + share;
        }
        final int e = followed;
        if (weights[e] < old[e] && rng.nextDouble() < 1 - weights[e] / old[e]) {
            double total = 0;
            for (int i = 0; i < n; i++) {
                total += Math.max(0.0, weights[i] - old[i]);
            }
            if (total > 0) {
                double r = rng.nextDouble() * total;
                double acc = 0;
                for (int i = 0; i < n; i++) {
                    acc += Math.max(0.0, weights[i] - old[i]);
                    if (r <= acc) {
                        followed = i;
                        break;
                    }
                }
                if (followed != e) {
                    switches++;
                    final Ghost g = experts[followed];
                    real.rebuildOutside(id -> g.contains(directory.fingerprint(id), id));
                }
            }
        }
    }

    void onUnlink(Cache.Entry<K, V> entry) {
        final int id = entry == lastEntry ? lastId : directory.get(entry.key.hashCode());
        if (id != KeyDirectory.ABSENT) {
            real.remove(id, entry);
        }
        if (entry == lastEntry) {
            lastEntry = null;   // its id may now be recycled
        }
    }

    /** Forgets the real entries (the cache was emptied); the shadow caches keep modelling the workload. */
    void onClear() {
        real.clear();
        lastEntry = null;
    }

    /**
     * Chooses the entry to evict while the cache is over weight.
     *
     * @param justInserted the entry that triggered this eviction round (or null)
     * @param lruTail      the least recently used entry
     */
    Cache.Entry<K, V> chooseVictim(Cache.Entry<K, V> justInserted, Cache.Entry<K, V> lruTail) {
        if (justInserted != null) {
            final int id = justInserted == lastEntry ? lastId : directory.get(justInserted.key.hashCode());
            if (id != KeyDirectory.ABSENT && real.isOutside(id, justInserted)) {
                lastEntry = justInserted;
                lastId = id;
                return justInserted;
            }
        }
        final int id = real.firstOutside();
        if (id != RealEntryTable.NONE) {
            final Cache.Entry<K, V> victim = real.entry(id);
            lastEntry = victim;
            lastId = id;
            return victim;
        }
        return lruTail;
    }

    /** Verifies that all the bookkeeping agrees with itself; for tests. Must be called with the cache's LRU lock held. */
    void checkInvariants() {
        directory.checkInvariants();
        real.checkInvariants();
        for (Ghost g : experts) {
            g.checkInvariants();
        }
        final Ghost followedGhost = experts[followed];
        for (int id = 0; id < directory.used(); id++) {
            if (directory.isAllocated(id) == false) {
                continue;
            }
            int expectedHolders = real.tracks(id) ? 1 : 0;
            for (Ghost g : experts) {
                if (g.contains(directory.fingerprint(id), id)) {
                    expectedHolders++;
                }
            }
            if (directory.holders(id) != expectedHolders || expectedHolders == 0) {
                throw new AssertionError("id " + id + " has " + directory.holders(id) + " holders but " + expectedHolders + " structures");
            }
            if (real.tracks(id)) {
                final boolean held = followedGhost.contains(directory.fingerprint(id), id);
                if (real.isOutside(id) == held) {
                    throw new AssertionError("id " + id + ": outside=" + real.isOutside(id) + " but followed ghost holds it=" + held);
                }
            }
        }
    }

    AdaptiveEviction.Stats stats() {
        final int n = experts.length;
        double[] share = new double[n];
        double[] hr = new double[n];
        for (int i = 0; i < n; i++) {
            share[i] = accesses == 0 ? 0 : (double) followSteps[i] / accesses;
            hr[i] = accesses == 0 ? 0 : (double) expertHits[i] / accesses;
        }
        return new AdaptiveEviction.Stats(names.clone(), share, hr, switches, accesses);
    }
}
