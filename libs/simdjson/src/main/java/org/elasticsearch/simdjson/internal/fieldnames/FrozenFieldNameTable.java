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
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicReference;

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
 */
public final class FrozenFieldNameTable {

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

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
        private final FrozenFieldNameTable parent;
        private Frozen frozen;

        private String[] learnNames;
        private byte[][] learnKeys;
        private int[] learnLens;
        private int learnCount;
        private boolean dirty;

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
                return frozen.lookup(buf, off, len, hash);
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
                return frozen.lookup(buf, off, len, hash, prefix8);
            }
            for (int i = 0; i < learnCount; i++) {
                if (learnLens[i] == len && Arrays.equals(learnKeys[i], 0, len, buf, off, off + len)) {
                    return learnNames[i];
                }
            }
            return null;
        }

        @Override
        public String insert(byte[] buf, int off, int len, int hash) {
            String s = new String(buf, off, len, StandardCharsets.UTF_8);
            if (frozen != null) {
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

        @Override
        public void freeze() {
            if (frozen != null || learnCount == 0) return;

            int tableSize = Integer.highestOneBit(Math.max(16, learnCount * 2 - 1)) << 1;
            int mask = tableSize - 1;

            int[] hashes = new int[tableSize];
            int[] lens = new int[tableSize];
            long[] prefix8 = new long[tableSize];
            byte[][] keys = new byte[tableSize][];
            String[] names = new String[tableSize];

            for (int i = 0; i < learnCount; i++) {
                int h = FieldNameHash.hashName(learnKeys[i], 0, learnLens[i]);
                long pfx = readPrefix8(learnKeys[i], 0, learnLens[i]);
                int slot = h & mask;
                while (hashes[slot] != 0) {
                    slot = (slot + 1) & mask;
                }
                hashes[slot] = h;
                lens[slot] = learnLens[i];
                prefix8[slot] = pfx;
                keys[slot] = learnKeys[i];
                names[slot] = learnNames[i];
            }

            frozen = new Frozen(mask, hashes, lens, prefix8, keys, names, learnCount);
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
