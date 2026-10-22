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
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntPredicate;
import java.util.function.LongSupplier;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Optimized field name table that freezes on a child's first {@link Child#freeze()} into
 * a compact power-of-two hash table sized to ~2x however many names the child had learned
 * by then. Uses:
 * <ul>
 *   <li>Same wyhash as {@link FieldNameHash} for compatibility with
 *       {@link FieldNameHash#scanAndHash}.</li>
 *   <li>Inline first-8-bytes prefix for fast rejection (avoids full comparison
 *       for hash collisions when prefixes differ).</li>
 *   <li>Power-of-two table; e.g. for 90 fields this gives a 256-slot table
 *       improving cache locality.</li>
 * </ul>
 *
 * <h2>Learning beyond the freeze point</h2>
 * A child's hash table is immutable once frozen, so names not yet learned at that point would
 * otherwise miss forever — a real cost when documents are sparse or when one child sees several
 * mappings. Instead, a frozen child records misses in a small bounded overflow buffer and offers
 * them to the shared table on {@link Child#release()}.
 *
 * <p>Publication swaps a superset table into {@link #shared} by CAS. A child does not eagerly
 * observe this — it keeps using the {@link Frozen} snapshot it already has, so a lookup never
 * pays for a shared read — but {@link Child#release()} re-syncs it against {@link #shared} before
 * returning. This matters because a child is typically thread-confined and long-lived (e.g. one
 * per parsing thread, kept for the thread's lifetime), while {@code release()} is called once per
 * batch; without the re-sync, a child that froze early would never benefit from names other
 * children taught the shared table later; with it, every batch boundary gives a chance to adopt
 * the richer table and drop the now-redundant entries from its own overflow buffer. The scheme
 * converges: once the shared table covers the field names a workload actually uses, nothing is
 * new any more and both publishing and re-syncing become no-ops.
 *
 * <h2>Long-running processes: the cap and its reset valve</h2>
 * A single instance is shared for the process's entire lifetime (see {@code SimdJsonParserPool}),
 * across every index and mapping that instance ever sees. Left unchecked, {@link #MAX_SHARED_NAMES}
 * guards against two different failure modes that pull in opposite directions:
 * <ul>
 *   <li>Genuinely unbounded cardinality (field names with embedded data) — here the right answer
 *       is to converge once on whichever names won the early race and stop trying; every declined
 *       merge is a handful of lookups with no allocation, so being stuck costs nothing beyond the
 *       lost canonicalization for names that never made it in.</li>
 *   <li>Cumulative staleness over a long enough process life — old, now-irrelevant mappings from
 *       long-deleted indices can fill the cap first and then never get evicted, permanently
 *       blocking a currently-active mapping's names from ever joining the shared table even
 *       though the live working set easily fits.</li>
 * </ul>
 * Declining forever handles the first well and the second badly. Resetting unconditionally, the
 * way Jackson's {@code ByteQuadsCanonicalizer} does, handles the second well but makes the first
 * actively worse: a table that can never fit unbounded names would be wiped and relearned
 * forever, paying the relearning cost indefinitely for names that were never going to converge
 * anyway. {@link #union} resets instead of declining once {@link #RESET_COOLDOWN_NANOS} has
 * passed since the last reset, which bounds the cost of the first case to at most one relearn per
 * cooldown window while still letting the second case self-heal, eventually, without a human
 * noticing.
 *
 * <p>Thread-safety follows a parent/child model: a single root instance is shared
 * across all threads. Each parsing thread obtains a {@link Child} via {@link #makeChild()}.
 */
public final class FrozenFieldNameTable {

    /**
     * Ceiling on names in the shared table. A merge that would exceed it is declined - or, once
     * {@link #RESET_COOLDOWN_NANOS} has passed since the last reset, replaces the table outright
     * with one holding only the merge's own candidate names (see {@link #union}) - which bounds
     * both memory and the cost of the rebuild a merge performs. Without a cap, workloads that put
     * high-cardinality data in field names (for example {@code {"user.9f3a1.count": 1}}) would
     * grow the table without limit.
     *
     * <p>Sized generously relative to a typical mapping (tens to a few hundred fields): even a
     * large multi-tenant cluster sharing many distinct schemas across one process's lifetime
     * should rarely approach this before the reset valve gets a chance to matter. The rebuild
     * {@link #union} performs when merging is cheap enough (an array copy over at most this many
     * entries, done on {@link Child#release()} rather than per document) that raising this
     * further costs little.
     */
    static final int MAX_SHARED_NAMES = 16384;

    /**
     * Minimum time between resets triggered by {@link #MAX_SHARED_NAMES}. Large and deliberately
     * conservative: a reset is a safety valve for staleness accumulated over a process's entire
     * lifetime, not a routine event, so there is no benefit to reacting quickly and real cost -
     * repeated relearning - to reacting too often. Best-effort rather than exact: concurrent
     * callers may occasionally race past this check together (see {@link #union}), which only
     * risks an extra reset right at the boundary, not a correctness problem.
     */
    static final long RESET_COOLDOWN_NANOS = TimeUnit.MINUTES.toNanos(30);

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, LITTLE_ENDIAN);

    private final AtomicReference<Frozen> shared = new AtomicReference<>();
    private final LongSupplier nanoTime;
    private final AtomicLong lastResetNanos;

    public FrozenFieldNameTable() {
        this(System::nanoTime);
    }

    /** Package-private so tests can drive the reset cooldown deterministically instead of via wall-clock sleeps. */
    FrozenFieldNameTable(LongSupplier nanoTime) {
        this.nanoTime = nanoTime;
        this.lastResetNanos = new AtomicLong(nanoTime.getAsLong());
    }

    public Child makeChild() {
        Frozen f = shared.get();
        return new Child(this, f);
    }

    /**
     * Publishes a newly frozen child's table. The first publisher donates its table wholesale,
     * which costs nothing; later publishers have to merge their names into whatever is already
     * shared.
     *
     * <p>Guarded by {@link #MAX_SHARED_NAMES} too: a child whose own table already exceeds the
     * cap is declined outright, without even attempting {@link #mergeNames}.
     */
    void mergeChild(Frozen childFrozen, String[] names, byte[][] keys, int[] lens, int count) {
        if (childFrozen.count() > MAX_SHARED_NAMES) {
            return;
        }
        if (shared.compareAndSet(null, childFrozen)) {
            return;
        }
        mergeNames(names, keys, lens, 0, count);
    }

    /**
     * Merges {@code names[from..to)} into the shared table, retrying until it wins the CAS or finds
     * it has nothing to add. Names already present keep their existing {@link String} instance, so
     * publication never changes the identity of a name callers may already hold.
     *
     * <p>Callers must keep {@code to - from} within {@link #MAX_SHARED_NAMES}: {@link #mergeChild}
     * guarantees this for a first-ever publish, and {@link Child#publishOverflow} never offers more
     * than {@link Child#MAX_OVERFLOW}, comfortably under it. This lets both this method and
     * {@link #resetOrDecline} treat a batch as always small enough to fit a fresh table alone,
     * without re-deriving that from {@code current}, which may be {@code null}.
     */
    void mergeNames(String[] names, byte[][] keys, int[] lens, int from, int to) {
        if (to <= from) {
            return;
        }
        assert to - from <= MAX_SHARED_NAMES : "merge batch of " + (to - from) + " exceeds MAX_SHARED_NAMES on its own";
        for (;;) {
            Frozen current = shared.get();
            Frozen merged = current == null ? build(names, keys, lens, from, to) : union(current, names, keys, lens, from, to);
            if (merged == current) {
                // Every candidate is already shared, or the merge would exceed MAX_SHARED_NAMES.
                return;
            }
            if (shared.compareAndSet(current, merged)) {
                return;
            }
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
     * Returns a table holding {@code current} plus whichever of {@code names[from..to)} it lacks;
     * {@code current} itself when there is nothing to add; or, once the merge would exceed
     * {@link #MAX_SHARED_NAMES}, either {@code current} unchanged or a fresh table built only
     * from {@code names[from..to)} - see {@link #resetOrDecline}.
     *
     * <p>The table is exact-sized and open-addressed with no spare capacity, so growing it means
     * rebuilding it. That is affordable only because merges stop growing it once the shared table
     * covers the workload's names.
     */
    Frozen union(Frozen current, String[] names, byte[][] keys, int[] lens, int from, int to) {
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
            return resetOrDecline(current, names, keys, lens, from, to);
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

        return build(mergedNames, mergedKeys, mergedLens, 0, n);
    }

    /**
     * Called once a merge would push {@code current} past {@link #MAX_SHARED_NAMES}. Declines
     * (returns {@code current} unchanged) unless {@link #RESET_COOLDOWN_NANOS} has passed since
     * the last reset, in which case it discards {@code current} - stale winners and all - and
     * returns a fresh table holding only this merge's own {@code names[from..to)}, giving the
     * table a clean start to reconverge on whatever is actually still active. That fresh table
     * cannot itself exceed the cap: it is exactly {@code to - from} entries, and {@link #mergeNames}
     * guarantees callers never pass more than {@link #MAX_SHARED_NAMES} of those.
     *
     * <p>Discarding {@code current} rather than trying to keep its "best" entries is deliberate:
     * nothing here can tell a name that is still relevant from one that is not, so picking
     * survivors would be guessing. A fresh start is simple, and if the live working set is small
     * relative to {@link #MAX_SHARED_NAMES} - the case this exists for - it reconverges quickly.
     *
     * <p>The cooldown check and the reset it may trigger are best-effort: two callers can race
     * between reading {@link #lastResetNanos} and updating it, so a burst of concurrent over-cap
     * merges could occasionally produce more than one reset right at the boundary. That merely
     * costs an extra relearn; {@link #mergeNames}'s CAS retry loop still ensures only one result
     * is ever published.
     */
    private Frozen resetOrDecline(Frozen current, String[] names, byte[][] keys, int[] lens, int from, int to) {
        long now = nanoTime.getAsLong();
        long last = lastResetNanos.get();
        if (now - last < RESET_COOLDOWN_NANOS || lastResetNanos.compareAndSet(last, now) == false) {
            return current;
        }
        return build(names, keys, lens, from, to);
    }

    /**
     * Immutable open-addressed hash table mapping field name bytes to their canonical
     * {@link String}. Built once by {@link #build} and never mutated afterwards; both a
     * {@link Child}'s own table and the one shared across children are instances of this record.
     *
     * <p>Storage is a set of parallel arrays of length {@code hashes.length}, a power of two. Slot
     * {@code i} across all arrays describes the same table entry: {@code hashes[i]} is that entry's
     * {@link FieldNameHash#hashName hash} (guaranteed non-zero); {@code keys[i]} holds the field
     * name's raw bytes and {@code lens[i]} their length; {@code prefix8[i]} is the first 8 of those
     * same bytes read as a little-endian long; and {@code names[i]} is the canonical {@code String}.
     * An entry is looked up by probing from {@code hash & mask} linearly forward, wrapping with
     * {@code (i + 1) & mask}, where {@code mask} is {@code hashes.length - 1}; {@code hashes[i] == 0}
     * marks a slot that was never written, i.e. the probe missed. There is no deletion and so no
     * tombstones.
     *
     * <p>{@code prefix8} exists to reject a hash collision cheaply: two names with different bytes
     * but the same hash almost always differ in their first 8 bytes too, so comparing longs weeds
     * out most false hash matches before falling back to a full {@code byte[]} comparison. For
     * names of 8 bytes or fewer the prefix comparison already covers every byte, so the full
     * comparison is skipped entirely.
     *
     * <p>{@code count} is the number of occupied slots, always at most half of {@code hashes.length}
     * (see {@link #build}), which keeps probe sequences short.
     *
     * <p>{@link #build} is the only place that constructs this record and is relied on to keep
     * {@code hashes.length} a power of two, {@code mask}/the parallel arrays consistent with it, and
     * {@code count} equal to the number of non-zero entries in {@code hashes}; the compact
     * constructor asserts all of this rather than re-deriving it, since re-deriving would mask a
     * bug in {@code build} instead of catching it.
     */
    record Frozen(int mask, int[] hashes, int[] lens, long[] prefix8, byte[][] keys, String[] names, int count) {

        Frozen {
            assert invariant(mask, hashes, lens, prefix8, keys, names, count);
        }

        /** Looks up a field name, computing its prefix8 from the bytes; see {@link #lookup(byte[], int, int, int, long)}. */
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

        private static boolean invariant(int mask, int[] hashes, int[] lens, long[] prefix8, byte[][] keys, String[] names, int count) {
            assert Integer.bitCount(hashes.length) == 1 : "table size must be a power of two, got " + hashes.length;
            assert mask == hashes.length - 1 : "mask must be table size - 1, got mask=" + mask + " size=" + hashes.length;
            assert lens.length == hashes.length
                && prefix8.length == hashes.length
                && keys.length == hashes.length
                && names.length == hashes.length : "parallel arrays must share the table size";
            assert count >= 0 && count <= hashes.length / 2
                : "count must be within [0, hashes.length / 2], got count=" + count + " size=" + hashes.length;
            int occupiedSlots = (int) Arrays.stream(hashes).filter(h -> h != 0).count();
            assert occupiedSlots == count
                : "count must match the number of occupied slots, got count=" + count + " occupied=" + occupiedSlots;
            return true;
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
         * at most this many new names per batch.
         */
        static final int MAX_OVERFLOW = 32;

        private final FrozenFieldNameTable parent;
        private Frozen frozen;

        /** Non-null exactly while {@code frozen == null}: names seen before this child ever froze. */
        private NameSet learned;

        /** Lazily created on the first post-freeze miss; names the frozen table above does not hold. */
        private NameSet overflow;

        /** How much of the overflow buffer {@link #release()} has already offered to the parent. */
        private int overflowPublished;

        Child(FrozenFieldNameTable parent, Frozen frozen) {
            this.parent = parent;
            this.frozen = frozen;
            this.learned = frozen == null ? NameSet.growable() : null;
        }

        @Override
        public String lookup(byte[] buf, int off, int len, int hash) {
            if (frozen != null) {
                String hit = frozen.lookup(buf, off, len, hash);
                return hit != null ? hit : lookupOverflow(buf, off, len, hash);
            }
            return learned.lookup(buf, off, len, hash);
        }

        @Override
        public String lookup(byte[] buf, int off, int len, int hash, long prefix8) {
            if (frozen != null) {
                String hit = frozen.lookup(buf, off, len, hash, prefix8);
                return hit != null ? hit : lookupOverflow(buf, off, len, hash);
            }
            return learned.lookup(buf, off, len, hash);
        }

        /**
         * Scans the overflow buffer, so a name the frozen table lacks is still canonicalized once
         * per child rather than reallocated for every document that contains it.
         */
        private String lookupOverflow(byte[] buf, int off, int len, int hash) {
            return overflow == null ? null : overflow.lookup(buf, off, len, hash);
        }

        @Override
        public String insert(byte[] buf, int off, int len, int hash) {
            String s = new String(buf, off, len, UTF_8);
            if (frozen != null) {
                recordOverflow(s, buf, off, len, hash);
                return s;
            }
            learned.add(s, buf, off, len, hash);
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
            if (overflow == null) {
                overflow = NameSet.capped(MAX_OVERFLOW);
            }
            overflow.add(name, buf, off, len, hash);
        }

        @Override
        public void freeze() {
            if (frozen != null || learned.count == 0) return;

            frozen = build(learned.names, learned.keys, learned.lens, 0, learned.count);
            parent.mergeChild(frozen, learned.names, learned.keys, learned.lens, learned.count);

            learned = null;
        }

        @Override
        public void release() {
            if (frozen == null && learned.count > 0) {
                freeze();
            } else if (frozen == null) {
                adoptShared();
            } else {
                publishOverflow();
                refreshFromShared();
            }
        }

        /** Adopts the parent's shared table if one has been published since this child was created. */
        private void adoptShared() {
            Frozen parentFrozen = parent.getShared();
            if (parentFrozen != null) {
                frozen = parentFrozen;
                learned = null;
            }
        }

        /** Offers any overflow entries not yet offered to the parent. */
        private void publishOverflow() {
            if (overflow != null && overflow.count > overflowPublished) {
                parent.mergeNames(overflow.names, overflow.keys, overflow.lens, overflowPublished, overflow.count);
                overflowPublished = overflow.count;
            }
        }

        /**
         * Re-syncs {@code frozen} against the parent's current shared table, so this child - which
         * may live for a long time relative to how often names change - eventually sees names other
         * children published, not only what it already had or learned itself.
         *
         * <p>Adopting a newer table is always safe for correctness, since callers compare resolved
         * names with {@code equals}, not identity. Most of the time it is also identity-preserving:
         * {@link FrozenFieldNameTable#union} copies existing entries by reference when it grows the
         * table, so a name already resolved through {@code frozen} keeps its instance, and a name
         * previously resolved through this child's own overflow buffer may simply start resolving to
         * a different, but {@code equals}, instance if another child published it first.
         *
         * <p>The exception is {@link FrozenFieldNameTable#resetOrDecline}: once the shared table has
         * been reset rather than grown, the adopted table holds only the resetting merge's own
         * candidate names, not the previous table's contents. A name this child's own {@code frozen}
         * already resolved - even one actively in use - can therefore stop resolving here entirely
         * until it is looked up as a miss and inserted again, exactly as if this child had never
         * learned it.
         */
        private void refreshFromShared() {
            Frozen latest = parent.getShared();
            if (latest == frozen) {
                return;
            }
            frozen = latest;
            compactOverflow();
        }

        /**
         * Drops overflow entries the freshly adopted table now covers, freeing bounded capacity for
         * names that are genuinely still missing rather than leaving it permanently occupied by
         * entries that have become redundant.
         *
         * <p>Entries kept are treated as unpublished again: re-offering an already-published name is
         * a cheap no-op (see {@link FrozenFieldNameTable#union}), and compaction can reorder entries,
         * so tracking exactly which survivors were already published is not worth the bookkeeping.
         */
        private void compactOverflow() {
            if (overflow == null) {
                return;
            }
            overflow.retainIf(i -> frozen.lookup(overflow.keys[i], 0, overflow.lens[i], overflow.hashes[i]) == null);
            overflowPublished = 0;
        }

        /** Returns {@code true} if this child has been frozen into a hash table. Primarily for testing. */
        public boolean isFrozen() {
            return frozen != null;
        }

        /** Number of post-freeze misses this child has recorded. Primarily for testing. */
        public int overflowCount() {
            return overflow == null ? 0 : overflow.count;
        }

        /**
         * Flat, parallel-array storage for field names not yet resolvable from the frozen table,
         * shared by two phases of {@link Child}'s lifecycle that need the same scan-then-append
         * shape but different growth rules: pre-freeze learning ({@link #growable()}, discarded once
         * frozen) and post-freeze overflow ({@link #capped}, kept for the rest of the child's life).
         *
         * <p>Every entry stores its hash for cheap rejection before the full byte comparison. That
         * costs the learning phase a few discarded bytes per entry for no benefit of its own (it is
         * scanned only while processing the first document, then thrown away), but sharing one
         * implementation is simpler than hand-rolling the same scan and grow-on-demand logic twice.
         *
         * <p>Package-private rather than {@code private} so {@code NameSetTests} can exercise its
         * array bookkeeping directly; everything below is otherwise an internal implementation
         * detail of {@link Child}.
         */
        static final class NameSet {

            /** {@link #capacity} sentinel meaning "grow as needed, never reject an add". */
            private static final int UNBOUNDED = -1;

            private final int capacity;
            private String[] names;
            private byte[][] keys;
            private int[] lens;
            private int[] hashes;
            private int count;

            /** A set capped at {@code capacity} entries; further adds beyond that are silently dropped. */
            static NameSet capped(int capacity) {
                if (capacity <= 0) {
                    throw new IllegalArgumentException("non-positive capacity:" + capacity);
                }
                return new NameSet(capacity);
            }

            /** An unbounded, doubling-on-demand set, used for the pre-freeze learning phase. */
            static NameSet growable() {
                return new NameSet(UNBOUNDED);
            }

            private NameSet(int capacity) {
                this.capacity = capacity;
            }

            /** Number of names currently recorded. Primarily for testing. */
            int count() {
                return count;
            }

            String lookup(byte[] buf, int off, int len, int hash) {
                for (int i = 0; i < count; i++) {
                    if (hashes[i] == hash && lens[i] == len && Arrays.equals(keys[i], 0, len, buf, off, off + len)) {
                        return names[i];
                    }
                }
                return null;
            }

            /** Records {@code name}; a capped set that is already full silently drops it. */
            void add(String name, byte[] buf, int off, int len, int hash) {
                assert lookup(buf, off, len, hash) == null : "caller must not add a name already present: " + name;
                if (capacity >= 0 && count == capacity) {
                    return;
                }
                if (names == null) {
                    int initial = capacity >= 0 ? capacity : 128;
                    names = new String[initial];
                    keys = new byte[initial][];
                    lens = new int[initial];
                    hashes = new int[initial];
                } else if (count == names.length) {
                    int nc = names.length * 2;
                    names = Arrays.copyOf(names, nc);
                    keys = Arrays.copyOf(keys, nc);
                    lens = Arrays.copyOf(lens, nc);
                    hashes = Arrays.copyOf(hashes, nc);
                }
                // Copied because buf may be the walker's reusable string buffer.
                keys[count] = Arrays.copyOfRange(buf, off, off + len);
                names[count] = name;
                lens[count] = len;
                hashes[count] = hash;
                count++;
            }

            /**
             * Compacts the set down to the entries for which {@code keep} returns {@code true},
             * dropping the rest. Used after adopting a fresher frozen table, to free capacity
             * previously spent on names that table now resolves directly.
             */
            void retainIf(IntPredicate keep) {
                int kept = 0;
                for (int i = 0; i < count; i++) {
                    if (keep.test(i) == false) {
                        continue;
                    }
                    if (kept != i) {
                        names[kept] = names[i];
                        keys[kept] = keys[i];
                        lens[kept] = lens[i];
                        hashes[kept] = hashes[i];
                    }
                    kept++;
                }
                count = kept;
            }
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
