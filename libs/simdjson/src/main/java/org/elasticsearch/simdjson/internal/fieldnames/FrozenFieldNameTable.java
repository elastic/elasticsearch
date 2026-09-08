/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static java.nio.ByteOrder.LITTLE_ENDIAN;

/**
 * Optimized field name table that freezes after the first document into a compact
 * power-of-two hash table sized to ~2x the field count. Uses:
 * <ul>
 *   <li>Same wyhash as {@link FieldNameHash} for compatibility with
 *       {@link FieldNameHash#scanAndHash}.</li>
 *   <li>Inline first-8-bytes prefix for fast rejection (avoids full comparison
 *       for hash collisions when prefixes differ).</li>
 *   <li>Power-of-two table — for 90 fields this gives a 256-slot table
 *       improving cache locality.</li>
 * </ul>
 *
 * <p>Thread-safety follows a parent/child model: a single root instance is shared
 * across all threads. Each parsing thread obtains a {@link Child} via {@link #makeChild()}.
 *
 * <h2>Learning beyond the first document</h2>
 * A child's hash table is immutable once frozen, so names the first document did not contain
 * would otherwise miss forever — a real cost when documents are sparse or when one child sees
 * several mappings. Instead, a frozen child records misses in a small bounded overflow buffer and
 * offers them to the shared table on {@link Child#release()}.
 *
 * <p>Publication swaps a superset table into {@link #shared} by CAS. Existing children are
 * unaffected because each holds its own immutable {@link Frozen} reference and never re-reads;
 * only children created afterwards see the richer table. The scheme converges: once the shared
 * table covers the field names a workload actually uses, nothing is new any more and merging
 * stops.
 */
public final class FrozenFieldNameTable {

    /**
     * Ceiling on names in the shared table. Merges that would exceed it are declined, which bounds
     * both memory and the cost of the rebuild a merge performs. Without a cap, workloads that put
     * high-cardinality data in field names (for example {@code {"user.9f3a1.count": 1}}) would grow
     * the table without limit.
     */
    static final int MAX_SHARED_NAMES = 4096;

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final AtomicReference<Frozen> shared = new AtomicReference<>();

    private final AtomicLong publications = new AtomicLong();
    private final AtomicLong tablesBuilt = new AtomicLong();
    private final AtomicLong entriesWritten = new AtomicLong();
    private final AtomicLong casRetries = new AtomicLong();
    private final AtomicLong declinedAtCap = new AtomicLong();

    public FrozenFieldNameTable() {}

    public Child makeChild() {
        Frozen f = shared.get();
        return new Child(this, f);
    }

    /**
     * Publishes a newly frozen child's table. The first publisher donates its table wholesale,
     * which costs nothing; later publishers have to merge their names into whatever is already
     * shared.
     */
    void mergeChild(Frozen childFrozen, String[] names, byte[][] keys, int[] lens, int count) {
        if (shared.compareAndSet(null, childFrozen)) {
            publications.incrementAndGet();
            return;
        }
        mergeNames(names, keys, lens, 0, count);
    }

    /**
     * Merges {@code names[from..to)} into the shared table, retrying until it wins the CAS or finds
     * it has nothing to add. Names already present keep their existing {@link String} instance, so
     * publication never changes the identity of a name callers may already hold.
     */
    void mergeNames(String[] names, byte[][] keys, int[] lens, int from, int to) {
        if (to <= from) {
            return;
        }
        for (;;) {
            Frozen current = shared.get();
            Frozen merged;
            if (current == null) {
                merged = build(names, keys, lens, from, to);
                recordBuild(merged.count());
            } else {
                merged = union(current, names, keys, lens, from, to);
            }
            if (merged == current) {
                // Every candidate is already shared, or the merge would exceed MAX_SHARED_NAMES.
                return;
            }
            if (shared.compareAndSet(current, merged)) {
                publications.incrementAndGet();
                return;
            }
            casRetries.incrementAndGet();
        }
    }

    Frozen getShared() {
        return shared.get();
    }

    /** Number of names in the shared table, or {@code 0} if nothing has been published. Primarily for testing. */
    public int sharedNameCount() {
        Frozen f = shared.get();
        return f == null ? 0 : f.count();
    }

    private void recordBuild(int entries) {
        tablesBuilt.incrementAndGet();
        entriesWritten.addAndGet(entries);
    }

    /** Snapshot of shared-table merging activity. Not atomic across fields; for reporting only. */
    public MergeStats mergeStats() {
        return new MergeStats(
            publications.get(),
            tablesBuilt.get(),
            entriesWritten.get(),
            casRetries.get(),
            declinedAtCap.get(),
            sharedNameCount()
        );
    }

    /**
     * Counts describing how much work shared-table merging has cost, so the convergence burst can
     * be measured deterministically rather than inferred from timings. Covers merging only: a
     * child freezing its own first-document table is not counted.
     *
     * @param publications  successful swaps of the shared table, including the first child's
     *                      wholesale hand-off
     * @param tablesBuilt   tables constructed while merging, including any thrown away after
     *                      losing a CAS
     * @param entriesWritten entries written across those constructions; the proxy for merge cost,
     *                      since each rebuild rewrites the whole table
     * @param casRetries    lost CAS races, indicating contention between concurrent publishers
     * @param declinedAtCap merges dropped because they would exceed {@link #MAX_SHARED_NAMES}
     * @param sharedNames   names currently in the shared table
     */
    public record MergeStats(
        long publications,
        long tablesBuilt,
        long entriesWritten,
        long casRetries,
        long declinedAtCap,
        int sharedNames
    ) {}

    /** Builds a frozen table from the {@code [from, to)} slice of parallel name/key/length arrays. */
    private static Frozen build(String[] names, byte[][] keys, int[] lens, int from, int to) {
        int count = to - from;
        int tableSize = Integer.highestOneBit(Math.max(16, count * 2 - 1)) << 1;
        int mask = tableSize - 1;

        int[] hashes = new int[tableSize];
        int[] tableLens = new int[tableSize];
        long[] prefix8 = new long[tableSize];
        byte[][] tableKeys = new byte[tableSize][];
        String[] tableNames = new String[tableSize];

        for (int i = from; i < to; i++) {
            int h = FieldNameHash.hashName(keys[i], 0, lens[i]);
            long pfx = readPrefix8(keys[i], 0, lens[i]);
            int slot = h & mask;
            while (hashes[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            hashes[slot] = h;
            tableLens[slot] = lens[i];
            prefix8[slot] = pfx;
            tableKeys[slot] = keys[i];
            tableNames[slot] = names[i];
        }

        return new Frozen(mask, hashes, tableLens, prefix8, tableKeys, tableNames, count);
    }

    /**
     * Returns a table holding {@code current} plus whichever of {@code names[from..to)} it lacks,
     * or {@code current} itself when there is nothing to add or the result would be too large.
     *
     * <p>The table is exact-sized and open-addressed with no spare capacity, so growing it means
     * rebuilding it. That is affordable only because merges stop once the shared table covers the
     * workload's names.
     */
    private Frozen union(Frozen current, String[] names, byte[][] keys, int[] lens, int from, int to) {
        boolean[] isNew = new boolean[to - from];
        int newCount = 0;
        for (int i = from; i < to; i++) {
            int len = lens[i];
            int h = FieldNameHash.hashName(keys[i], 0, len);
            if (current.lookup(keys[i], 0, len, h) == null) {
                isNew[i - from] = true;
                newCount++;
            }
        }
        if (newCount == 0) {
            return current;
        }
        if (current.count() + newCount > MAX_SHARED_NAMES) {
            declinedAtCap.incrementAndGet();
            return current;
        }

        String[] mergedNames = new String[current.count() + newCount];
        byte[][] mergedKeys = new byte[mergedNames.length][];
        int[] mergedLens = new int[mergedNames.length];
        int n = 0;

        int[] currentHashes = current.hashes();
        for (int slot = 0; slot < currentHashes.length; slot++) {
            if (currentHashes[slot] != 0) {
                mergedNames[n] = current.names()[slot];
                mergedKeys[n] = current.keys()[slot];
                mergedLens[n] = current.lens()[slot];
                n++;
            }
        }
        for (int i = from; i < to; i++) {
            if (isNew[i - from]) {
                mergedNames[n] = names[i];
                mergedKeys[n] = keys[i];
                mergedLens[n] = lens[i];
                n++;
            }
        }

        recordBuild(n);
        return build(mergedNames, mergedKeys, mergedLens, 0, n);
    }

    /**
     * Immutable frozen hash table state.
     */
    record Frozen(int mask, int[] hashes, int[] lens, long[] prefix8, byte[][] keys, String[] names, int count) {

        String lookup(byte[] buf, int off, int len, int h) {
            return lookup(buf, off, len, h, readPrefix8(buf, off, len));
        }

        /**
         * Looks up a field name using a pre-computed prefix8 value, avoiding a re-read
         * of the field name bytes for the prefix comparison.
         */
        String lookup(byte[] buf, int off, int len, int h, long pfx) {
            for (int i = h & mask;; i = (i + 1) & mask) {
                int sh = hashes[i];
                if (sh == 0) return null;
                if (sh == h && lens[i] == len && prefix8[i] == pfx) {
                    if (len <= 8 || Arrays.equals(keys[i], 0, len, buf, off, off + len)) {
                        return names[i];
                    }
                }
            }
        }
    }

    /**
     * Thread-confined child that implements {@link FieldNameLookup}. Obtained via
     * {@link FrozenFieldNameTable#makeChild()}.
     */
    public static final class Child implements FieldNameLookup {

        /**
         * Cap on post-freeze misses recorded per child, chosen from measurement rather than taste.
         * Every frozen-table miss scans this buffer linearly, and the scan is not free relative to
         * what it saves: {@code FieldNameCacheBenchmark} puts it at roughly 0.25ns per entry
         * against about 19.5ns and 64 bytes to allocate a name, so the buffer stops paying for
         * itself somewhere near 56 entries. At 32 an average hit costs about 10ns and a name the
         * buffer does not hold costs about 7ns more to reject.
         *
         * <p>The cost of keeping it small is only how long convergence takes, since a child learns
         * at most this many new names per batch. {@code FieldNameConvergenceReport} shows a
         * 300-name sparse mapping converging in single-digit batches either way, which is not worth
         * a slower miss path.
         */
        static final int MAX_OVERFLOW = 32;

        private final FrozenFieldNameTable parent;
        private Frozen frozen;

        private String[] learnNames;
        private byte[][] learnKeys;
        private int[] learnLens;
        private int learnCount;
        private boolean dirty;

        private String[] overflowNames;
        private byte[][] overflowKeys;
        private int[] overflowLens;
        private int[] overflowHashes;
        private int overflowCount;

        /** How much of the overflow buffer {@link #release()} has already offered to the parent. */
        private int overflowPublished;

        Child(FrozenFieldNameTable parent, Frozen frozen) {
            this.parent = parent;
            this.frozen = frozen;
            if (frozen == null) {
                learnNames = new String[128];
                learnKeys = new byte[128][];
                learnLens = new int[128];
                learnCount = 0;
            }
        }

        @Override
        public String lookup(byte[] buf, int off, int len, int hash) {
            if (frozen != null) {
                String hit = frozen.lookup(buf, off, len, hash);
                return hit != null ? hit : lookupOverflow(buf, off, len, hash);
            }
            for (int i = 0; i < learnCount; i++) {
                if (learnLens[i] == len && Arrays.equals(learnKeys[i], 0, len, buf, off, off + len)) {
                    return learnNames[i];
                }
            }
            return null;
        }

        @Override
        public String lookup(byte[] buf, int off, int len, int hash, long prefix8) {
            if (frozen != null) {
                String hit = frozen.lookup(buf, off, len, hash, prefix8);
                return hit != null ? hit : lookupOverflow(buf, off, len, hash);
            }
            for (int i = 0; i < learnCount; i++) {
                if (learnLens[i] == len && Arrays.equals(learnKeys[i], 0, len, buf, off, off + len)) {
                    return learnNames[i];
                }
            }
            return null;
        }

        /**
         * Scans the overflow buffer, so a name the frozen table lacks is still canonicalized once
         * per child rather than reallocated for every document that contains it.
         */
        private String lookupOverflow(byte[] buf, int off, int len, int hash) {
            for (int i = 0; i < overflowCount; i++) {
                if (overflowHashes[i] == hash && overflowLens[i] == len) {
                    if (Arrays.equals(overflowKeys[i], 0, len, buf, off, off + len)) {
                        return overflowNames[i];
                    }
                }
            }
            return null;
        }

        @Override
        public String insert(byte[] buf, int off, int len, int hash) {
            String s = new String(buf, off, len, StandardCharsets.UTF_8);
            if (frozen != null) {
                recordOverflow(s, buf, off, len, hash);
                return s;
            }
            if (learnCount >= learnNames.length) {
                int nc = learnNames.length * 2;
                learnNames = Arrays.copyOf(learnNames, nc);
                learnKeys = Arrays.copyOf(learnKeys, nc);
                learnLens = Arrays.copyOf(learnLens, nc);
            }
            byte[] key = Arrays.copyOfRange(buf, off, off + len);
            learnNames[learnCount] = s;
            learnKeys[learnCount] = key;
            learnLens[learnCount] = len;
            learnCount++;
            dirty = true;
            return s;
        }

        /**
         * Records a name the frozen table did not hold, for {@link #release()} to offer to the
         * shared table. Callers reach this only after {@link #lookup} missed both the frozen table
         * and the overflow buffer, so entries are inherently distinct.
         *
         * <p>Once the buffer is full further names are still returned to the caller, just neither
         * canonicalized nor published.
         */
        private void recordOverflow(String name, byte[] buf, int off, int len, int hash) {
            if (overflowCount == MAX_OVERFLOW) {
                return;
            }
            if (overflowNames == null) {
                overflowNames = new String[MAX_OVERFLOW];
                overflowKeys = new byte[MAX_OVERFLOW][];
                overflowLens = new int[MAX_OVERFLOW];
                overflowHashes = new int[MAX_OVERFLOW];
            }
            overflowNames[overflowCount] = name;
            // Copied because buf may be the walker's reusable string buffer.
            overflowKeys[overflowCount] = Arrays.copyOfRange(buf, off, off + len);
            overflowLens[overflowCount] = len;
            overflowHashes[overflowCount] = hash;
            overflowCount++;
        }

        @Override
        public void freeze() {
            if (frozen != null || learnCount == 0) return;

            frozen = build(learnNames, learnKeys, learnLens, 0, learnCount);
            parent.mergeChild(frozen, learnNames, learnKeys, learnLens, learnCount);

            learnNames = null;
            learnKeys = null;
            learnLens = null;
            dirty = false;
        }

        @Override
        public void release() {
            if (frozen == null && dirty) {
                freeze();
            } else if (frozen == null) {
                Frozen parentFrozen = parent.getShared();
                if (parentFrozen != null) {
                    frozen = parentFrozen;
                    learnNames = null;
                    learnKeys = null;
                    learnLens = null;
                }
            } else if (overflowCount > overflowPublished) {
                parent.mergeNames(overflowNames, overflowKeys, overflowLens, overflowPublished, overflowCount);
                overflowPublished = overflowCount;
                // The buffer stays live: this child keeps canonicalizing these names from it, since
                // its own frozen table is immutable and will never contain them.
            }
        }

        /** Returns {@code true} if this child has been frozen into a hash table. Primarily for testing. */
        public boolean isFrozen() {
            return frozen != null;
        }

        /** Number of post-freeze misses this child has recorded. Primarily for testing. */
        public int overflowCount() {
            return overflowCount;
        }
    }

    static long readPrefix8(byte[] buf, int off, int len) {
        if (len >= 8) return (long) LONG_LE.get(buf, off);
        long v = 0;
        for (int i = 0; i < len; i++) {
            v |= (long) (buf[off + i] & 0xFF) << (i * 8);
        }
        return v;
    }
}
