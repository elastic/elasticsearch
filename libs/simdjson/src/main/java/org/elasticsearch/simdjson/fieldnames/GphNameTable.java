/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.fieldnames;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Field name table using a minimal perfect hash based on the FKS/GPH two-level scheme.
 * After {@link #freeze()}, lookup is O(1): one hash to find the bucket, one to find
 * the final slot, one length+bytes comparison to verify.
 */
public final class GphNameTable {

    private static final long GOLDEN = 0x9E3779B97F4A7C15L;
    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private boolean frozen;

    private final List<Entry> entries = new ArrayList<>();

    private int n;
    private int shift;
    private int m;
    private int[] seeds;
    private String[] names;
    private byte[][] keys;
    private int[] lens;
    private long[] prefix8;

    private String[] ovNames;
    private byte[][] ovKeys;
    private int[] ovLens;
    private int ovCount;

    public GphNameTable() {}

    private record Entry(String name, byte[] key, int len, long hash) {}

    public String lookupOrInsert(byte[] buf, int off, int len) {
        if (frozen) {
            String s = frozenLookup(buf, off, len);
            if (s != null) return s;
            s = new String(buf, off, len, StandardCharsets.UTF_8);
            addOverflow(buf, off, len, s);
            return s;
        }
        return learningLookup(buf, off, len);
    }

    private String learningLookup(byte[] buf, int off, int len) {
        for (Entry e : entries) {
            if (e.len == len && Arrays.equals(e.key, 0, len, buf, off, off + len)) {
                return e.name;
            }
        }
        String s = new String(buf, off, len, StandardCharsets.UTF_8);
        byte[] key = Arrays.copyOfRange(buf, off, off + len);
        entries.add(new Entry(s, key, len, keyHash(buf, off, len)));
        return s;
    }

    private String frozenLookup(byte[] buf, int off, int len) {
        long h = keyHash(buf, off, len);
        int bucket = (int) ((h * GOLDEN) >>> shift) % m;
        if (bucket < 0) bucket += m;
        int seed = seeds[bucket];
        int slot = (int) (((h ^ seed) * GOLDEN) >>> shift);
        if (slot >= 0 && slot < n && lens[slot] == len) {
            long p = readPrefix8(buf, off, len);
            if (prefix8[slot] == p) {
                if (len <= 8 || Arrays.equals(keys[slot], 0, len, buf, off, off + len)) {
                    return names[slot];
                }
            }
        }
        for (int i = 0; i < ovCount; i++) {
            if (ovLens[i] == len && Arrays.equals(ovKeys[i], 0, len, buf, off, off + len)) {
                return ovNames[i];
            }
        }
        return null;
    }

    public void freeze() {
        if (frozen || entries.isEmpty()) {
            frozen = true;
            return;
        }

        int count = entries.size();
        n = Integer.highestOneBit(Math.max(16, (int) (count * 1.3)));
        if (n < count * 1.25) n <<= 1;
        shift = Long.numberOfLeadingZeros(n - 1);
        m = Math.max(1, count / 4 + 1);

        seeds = new int[m];
        names = new String[n];
        keys = new byte[n][];
        lens = new int[n];
        prefix8 = new long[n];
        ovNames = new String[4];
        ovKeys = new byte[4][];
        ovLens = new int[4];
        ovCount = 0;

        buildHash(count);
        frozen = true;
    }

    @SuppressWarnings("unchecked")
    private void buildHash(int count) {
        List<Entry>[] buckets = new List[m];
        for (int i = 0; i < m; i++) {
            buckets[i] = new ArrayList<>();
        }
        for (Entry e : entries) {
            int b = (int) ((e.hash * GOLDEN) >>> shift) % m;
            if (b < 0) b += m;
            buckets[b].add(e);
        }

        Integer[] order = new Integer[m];
        for (int i = 0; i < m; i++) {
            order[i] = i;
        }
        Arrays.sort(order, (a, b) -> Integer.compare(buckets[b].size(), buckets[a].size()));

        boolean[] occupied = new boolean[n];

        for (int bi : order) {
            List<Entry> bucket = buckets[bi];
            if (bucket.isEmpty()) continue;

            boolean placed = false;
            for (int seed = 0; seed < n * 8; seed++) {
                int[] slots = new int[bucket.size()];
                boolean ok = true;
                for (int k = 0; k < bucket.size(); k++) {
                    Entry e = bucket.get(k);
                    int slot = (int) (((e.hash ^ seed) * GOLDEN) >>> shift);
                    if (slot < 0 || slot >= n || occupied[slot]) {
                        ok = false;
                        break;
                    }
                    for (int prev = 0; prev < k; prev++) {
                        if (slots[prev] == slot) {
                            ok = false;
                            break;
                        }
                    }
                    if (!ok) break;
                    slots[k] = slot;
                }
                if (ok) {
                    seeds[bi] = seed;
                    for (int k = 0; k < bucket.size(); k++) {
                        Entry e = bucket.get(k);
                        int slot = slots[k];
                        occupied[slot] = true;
                        names[slot] = e.name;
                        keys[slot] = e.key;
                        lens[slot] = e.len;
                        prefix8[slot] = readPrefix8(e.key, 0, e.len);
                    }
                    placed = true;
                    break;
                }
            }
            if (!placed) {
                for (Entry e : bucket) {
                    addOverflow(e.key, 0, e.len, e.name);
                }
            }
        }
    }

    public boolean isFrozen() {
        return frozen;
    }

    private static long keyHash(byte[] buf, int off, int len) {
        return Integer.toUnsignedLong(FieldNameHash.hashName(buf, off, len));
    }

    private static long readPrefix8(byte[] buf, int off, int len) {
        if (len >= 8) return (long) LONG_LE.get(buf, off);
        long v = 0;
        for (int i = 0; i < len; i++) {
            v |= (long) (buf[off + i] & 0xFF) << (i * 8);
        }
        return v;
    }

    private void addOverflow(byte[] buf, int off, int len, String name) {
        if (ovNames == null) {
            ovNames = new String[4];
            ovKeys = new byte[4][];
            ovLens = new int[4];
        }
        if (ovCount >= ovNames.length) {
            int nc = ovNames.length * 2;
            ovNames = Arrays.copyOf(ovNames, nc);
            ovKeys = Arrays.copyOf(ovKeys, nc);
            ovLens = Arrays.copyOf(ovLens, nc);
        }
        ovNames[ovCount] = name;
        ovKeys[ovCount] = Arrays.copyOfRange(buf, off, off + len);
        ovLens[ovCount] = len;
        ovCount++;
    }
}
