/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.internal.fieldnames;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Optimized field name table that freezes once the field-name set stabilizes into a
 * compact power-of-two hash table sized to ~2x the field count. Uses:
 * <ul>
 *   <li>Same wyhash as {@link FieldNameHash} for compatibility with
 *       {@link FieldNameHash#scanFieldName}.</li>
 *   <li>Inline first-8-bytes prefix for fast rejection (avoids full comparison
 *       for hash collisions when prefixes differ).</li>
 *   <li>Power-of-two table — for 90 fields this gives a 256-slot table
 *       improving cache locality.</li>
 *   <li>Direct-mapped ordinal cache keyed by {@code (prefix8, len)} for O(1) hits
 *       when the schema has at least {@link #DIRECT_MAP_MIN_FIELDS} names.</li>
 *   <li>Dense ordinals ({@code 0..count-1}) for fast ESCF column indexing after freeze.</li>
 * </ul>
 *
 * <p>Thread-safety follows a parent/child model: a single root instance is shared
 * across all threads. Each parsing thread obtains a {@link Child} via {@link #makeChild()}.
 */
public final class FrozenFieldNameTable {

    /**
     * Schemas with fewer unique names use open-addressing probe only; the direct map
     * and its verification arrays cost more than they save on tiny tables.
     */
    static final int DIRECT_MAP_MIN_FIELDS = 32;

    /** Sentinel in {@link Frozen#directOrdinals} marking a colliding direct-map bucket. */
    private static final int DIRECT_COLLISION = -2;

    private final AtomicReference<Frozen> shared = new AtomicReference<>();

    public FrozenFieldNameTable() {}

    public Child makeChild() {
        Frozen f = shared.get();
        return new Child(this, f);
    }

    void mergeChild(Frozen childFrozen) {
        shared.compareAndSet(null, childFrozen);
    }

    Frozen getShared() {
        return shared.get();
    }

    /**
     * Immutable frozen hash table state.
     */
    record Frozen(
        int mask,
        int[] hashes,
        int[] lens,
        long[] prefix8,
        byte[][] keys,
        String[] names,
        int count,
        String[] namesByOrdinal,
        int[] ordinalHashes,
        int[] ordinalLens,
        long[] ordinalPrefix8,
        int[] slotOrdinals,
        int[] directOrdinals,
        boolean[] prefixLenUnique
    ) {

        String lookup(byte[] buf, int off, int len, int h) {
            return lookup(buf, off, len, h, FieldNameHash.readPrefix8(buf, off, len));
        }

        /**
         * Looks up a field name using a pre-computed prefix8 value, avoiding a re-read
         * of the field name bytes for the prefix comparison.
         */
        String lookup(byte[] buf, int off, int len, int h, long pfx) {
            ResolvedFieldName resolved = lookupField(buf, off, len, h, pfx);
            return resolved == null ? null : resolved.name();
        }

        ResolvedFieldName lookupField(byte[] buf, int off, int len, int h, long pfx) {
            if (directOrdinals != null) {
                int directIdx = directIndex(pfx, len, directOrdinals.length);
                int ordinal = directOrdinals[directIdx];
                if (ordinal >= 0 && ordinalHashes[ordinal] == h && ordinalLens[ordinal] == len && ordinalPrefix8[ordinal] == pfx) {
                    return new ResolvedFieldName(namesByOrdinal[ordinal], ordinal);
                }
            }

            for (int i = h & mask;; i = (i + 1) & mask) {
                int sh = hashes[i];
                if (sh == 0) {
                    return null;
                }
                if (sh == h && lens[i] == len && prefix8[i] == pfx) {
                    if (len <= 8 || prefixLenUnique[i] || Arrays.equals(keys[i], 0, len, buf, off, off + len)) {
                        return new ResolvedFieldName(names[i], slotOrdinals[i]);
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
        private final FrozenFieldNameTable parent;
        private Frozen frozen;

        private String[] learnNames;
        private byte[][] learnKeys;
        private int[] learnLens;
        private int learnCount;
        private boolean dirty;
        private boolean documentLearnedNew;

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
            ResolvedFieldName resolved = lookupField(buf, off, len, hash, FieldNameHash.readPrefix8(buf, off, len));
            return resolved == null ? null : resolved.name();
        }

        @Override
        public String lookup(byte[] buf, int off, int len, int hash, long prefix8) {
            ResolvedFieldName resolved = lookupField(buf, off, len, hash, prefix8);
            return resolved == null ? null : resolved.name();
        }

        @Override
        public ResolvedFieldName lookupField(byte[] buf, int off, int len, int hash, long prefix8) {
            if (frozen != null) {
                return frozen.lookupField(buf, off, len, hash, prefix8);
            }
            for (int i = 0; i < learnCount; i++) {
                if (learnLens[i] == len && Arrays.equals(learnKeys[i], 0, len, buf, off, off + len)) {
                    return new ResolvedFieldName(learnNames[i], -1);
                }
            }
            return null;
        }

        @Override
        public String insert(byte[] buf, int off, int len, int hash) {
            return insertField(buf, off, len, hash).name();
        }

        @Override
        public void beginDocument() {
            documentLearnedNew = false;
        }

        @Override
        public void maybeFreezeAfterDocument() {
            if (frozen == null && learnCount > 0 && documentLearnedNew == false) {
                freeze();
            }
        }

        @Override
        public ResolvedFieldName insertField(byte[] buf, int off, int len, int hash) {
            String s = new String(buf, off, len, StandardCharsets.UTF_8);
            if (frozen != null) {
                return new ResolvedFieldName(s, -1);
            }
            documentLearnedNew = true;
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
            return new ResolvedFieldName(s, -1);
        }

        @Override
        public void freeze() {
            if (frozen != null || learnCount == 0) {
                return;
            }

            int tableSize = Integer.highestOneBit(Math.max(16, learnCount * 2 - 1)) << 1;
            int mask = tableSize - 1;

            int[] hashes = new int[tableSize];
            int[] lens = new int[tableSize];
            long[] prefix8 = new long[tableSize];
            byte[][] keys = new byte[tableSize][];
            String[] names = new String[tableSize];
            int[] slotOrdinals = new int[tableSize];
            boolean[] prefixLenUnique = new boolean[tableSize];
            String[] namesByOrdinal = Arrays.copyOf(learnNames, learnCount);
            int[] ordinalHashes = new int[learnCount];
            int[] ordinalLens = new int[learnCount];
            long[] ordinalPrefix8 = new long[learnCount];

            HashMap<Long, Integer> prefixLenCounts = new HashMap<>();
            for (int i = 0; i < learnCount; i++) {
                long pfx = FieldNameHash.readPrefix8(learnKeys[i], 0, learnLens[i]);
                long key = prefixLenKey(pfx, learnLens[i]);
                prefixLenCounts.merge(key, 1, Integer::sum);
            }

            int[] directOrdinals = null;
            if (learnCount >= DIRECT_MAP_MIN_FIELDS) {
                int directSize = Integer.highestOneBit(Math.max(16, learnCount * 2 - 1)) << 1;
                directOrdinals = new int[directSize];
                Arrays.fill(directOrdinals, -1);
            }

            for (int i = 0; i < learnCount; i++) {
                int h = FieldNameHash.hashName(learnKeys[i], 0, learnLens[i]);
                long pfx = FieldNameHash.readPrefix8(learnKeys[i], 0, learnLens[i]);
                int slot = h & mask;
                while (hashes[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                hashes[slot] = h;
                lens[slot] = learnLens[i];
                prefix8[slot] = pfx;
                keys[slot] = learnKeys[i];
                names[slot] = learnNames[i];
                slotOrdinals[slot] = i;
                ordinalHashes[i] = h;
                ordinalLens[i] = learnLens[i];
                ordinalPrefix8[i] = pfx;
                prefixLenUnique[slot] = prefixLenCounts.get(prefixLenKey(pfx, learnLens[i])) == 1;

                if (directOrdinals != null) {
                    int directIdx = directIndex(pfx, learnLens[i], directOrdinals.length);
                    if (directOrdinals[directIdx] == -1) {
                        directOrdinals[directIdx] = i;
                    } else {
                        directOrdinals[directIdx] = DIRECT_COLLISION;
                    }
                }
            }

            frozen = new Frozen(
                mask,
                hashes,
                lens,
                prefix8,
                keys,
                names,
                learnCount,
                namesByOrdinal,
                ordinalHashes,
                ordinalLens,
                ordinalPrefix8,
                slotOrdinals,
                directOrdinals,
                prefixLenUnique
            );
            parent.mergeChild(frozen);

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
            }
        }

        /** Returns {@code true} if this child has been frozen into a hash table. Primarily for testing. */
        public boolean isFrozen() {
            return frozen != null;
        }

        /** Returns the direct-map array when frozen, or {@code null} for small schemas. Primarily for testing. */
        int[] frozenDirectOrdinals() {
            return frozen == null ? null : frozen.directOrdinals();
        }
    }

    static int directIndex(long pfx, int len, int directSize) {
        return ((int) (pfx ^ (pfx >>> 32) ^ len)) & (directSize - 1);
    }

    private static long prefixLenKey(long prefix8, int len) {
        return prefix8 ^ ((long) len << 56);
    }
}
