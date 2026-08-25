/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdjson.fieldnames;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

/**
 * Field name table using a CHD (Compress, Hash, Displace) minimal perfect hash function.
 * After {@link #freeze()}, lookups are O(1) with no collision probing.
 */
public final class PerfectHashNameTable {

    private boolean frozen;

    private String[] learnNames;
    private byte[][] learnKeys;
    private int[] learnLens;
    private int learnCount;

    private int tableSize;
    private int bucketCount;
    private int[] displacements;
    private String[] frozenNames;
    private byte[][] frozenKeys;
    private int[] frozenLens;

    private String[] overflowNames;
    private byte[][] overflowKeys;
    private int[] overflowLens;
    private int overflowCount;

    public PerfectHashNameTable() {
        learnNames = new String[128];
        learnKeys = new byte[128][];
        learnLens = new int[128];
        learnCount = 0;
        frozen = false;
    }

    public void insert(byte[] buf, int off, int len, String name) {
        if (frozen) {
            addOverflow(buf, off, len, name);
            return;
        }
        if (learnCount >= learnNames.length) {
            int newCap = learnNames.length * 2;
            learnNames = Arrays.copyOf(learnNames, newCap);
            learnKeys = Arrays.copyOf(learnKeys, newCap);
            learnLens = Arrays.copyOf(learnLens, newCap);
        }
        learnNames[learnCount] = name;
        learnKeys[learnCount] = Arrays.copyOfRange(buf, off, off + len);
        learnLens[learnCount] = len;
        learnCount++;
    }

    public String lookup(byte[] buf, int off, int len) {
        if (frozen) {
            return frozenLookup(buf, off, len);
        }
        return learningLookup(buf, off, len);
    }

    public String lookupOrInsert(byte[] buf, int off, int len) {
        String s = lookup(buf, off, len);
        if (s != null) return s;
        s = new String(buf, off, len, StandardCharsets.UTF_8);
        insert(buf, off, len, s);
        return s;
    }

    private String learningLookup(byte[] buf, int off, int len) {
        for (int i = 0; i < learnCount; i++) {
            if (learnLens[i] == len && Arrays.equals(learnKeys[i], 0, len, buf, off, off + len)) {
                return learnNames[i];
            }
        }
        return null;
    }

    private String frozenLookup(byte[] buf, int off, int len) {
        int idx = perfectHashIndex(buf, off, len);
        if (idx >= 0 && idx < tableSize && frozenLens[idx] == len) {
            byte[] key = frozenKeys[idx];
            if (Arrays.equals(key, 0, len, buf, off, off + len)) {
                return frozenNames[idx];
            }
        }
        for (int i = 0; i < overflowCount; i++) {
            if (overflowLens[i] == len && Arrays.equals(overflowKeys[i], 0, len, buf, off, off + len)) {
                return overflowNames[i];
            }
        }
        return null;
    }

    public void freeze() {
        if (frozen || learnCount == 0) {
            frozen = true;
            return;
        }

        tableSize = learnCount;
        bucketCount = Math.max(1, (tableSize + 3) / 4);
        displacements = new int[bucketCount * 2];
        frozenNames = new String[tableSize];
        frozenKeys = new byte[tableSize][];
        frozenLens = new int[tableSize];
        overflowNames = new String[8];
        overflowKeys = new byte[8][];
        overflowLens = new int[8];
        overflowCount = 0;

        buildPerfectHash();
        frozen = true;

        learnNames = null;
        learnKeys = null;
        learnLens = null;
    }

    public boolean isFrozen() {
        return frozen;
    }

    public int size() {
        return frozen ? tableSize + overflowCount : learnCount;
    }

    private void buildPerfectHash() {
        int[][] buckets = new int[bucketCount][];
        int[] bucketSizes = new int[bucketCount];
        for (int i = 0; i < learnCount; i++) {
            int b = (primaryHash(learnKeys[i], 0, learnLens[i]) & 0x7FFFFFFF) % bucketCount;
            if (buckets[b] == null) {
                buckets[b] = new int[4];
            } else if (bucketSizes[b] >= buckets[b].length) {
                buckets[b] = Arrays.copyOf(buckets[b], buckets[b].length * 2);
            }
            buckets[b][bucketSizes[b]++] = i;
        }

        Integer[] bucketOrder = new Integer[bucketCount];
        for (int i = 0; i < bucketCount; i++) {
            bucketOrder[i] = i;
        }
        Arrays.sort(bucketOrder, (a, b) -> Integer.compare(bucketSizes[b], bucketSizes[a]));

        boolean[] occupied = new boolean[tableSize];
        int[] tempSlots = new int[16];

        for (int bi : bucketOrder) {
            int bSize = bucketSizes[bi];
            if (bSize == 0) {
                displacements[bi * 2] = 0;
                displacements[bi * 2 + 1] = 0;
                continue;
            }

            boolean found = false;
            outer: for (int d1 = 0; d1 < tableSize * 4; d1++) {
                for (int d2 = 0; d2 < tableSize * 4; d2++) {
                    boolean ok = true;
                    for (int k = 0; k < bSize; k++) {
                        int item = buckets[bi][k];
                        int slot = secondaryHash(d1, d2, learnKeys[item], 0, learnLens[item]) % tableSize;
                        if (slot < 0) slot += tableSize;
                        if (occupied[slot]) {
                            ok = false;
                            break;
                        }
                        for (int prev = 0; prev < k; prev++) {
                            if (tempSlots[prev] == slot) {
                                ok = false;
                                break;
                            }
                        }
                        if (!ok) break;
                        if (k >= tempSlots.length) {
                            tempSlots = Arrays.copyOf(tempSlots, tempSlots.length * 2);
                        }
                        tempSlots[k] = slot;
                    }
                    if (ok) {
                        displacements[bi * 2] = d1;
                        displacements[bi * 2 + 1] = d2;
                        for (int k = 0; k < bSize; k++) {
                            int item = buckets[bi][k];
                            int slot = tempSlots[k];
                            occupied[slot] = true;
                            frozenNames[slot] = learnNames[item];
                            frozenKeys[slot] = learnKeys[item];
                            frozenLens[slot] = learnLens[item];
                        }
                        found = true;
                        break outer;
                    }
                }
            }
            if (!found) {
                for (int k = 0; k < bSize; k++) {
                    int item = buckets[bi][k];
                    addOverflow(learnKeys[item], 0, learnLens[item], learnNames[item]);
                }
            }
        }
    }

    private int perfectHashIndex(byte[] buf, int off, int len) {
        int b = (primaryHash(buf, off, len) & 0x7FFFFFFF) % bucketCount;
        int d1 = displacements[b * 2];
        int d2 = displacements[b * 2 + 1];
        int slot = secondaryHash(d1, d2, buf, off, len) % tableSize;
        return slot < 0 ? slot + tableSize : slot;
    }

    private static int primaryHash(byte[] buf, int off, int len) {
        return FieldNameHash.hashName(buf, off, len);
    }

    private static int secondaryHash(int d1, int d2, byte[] buf, int off, int len) {
        long h = d1 * 0x9E3779B97F4A7C15L;
        h ^= d2 * 0x517CC1B727220A95L;
        for (int i = 0; i < len; i++) {
            h = h * 31 + (buf[off + i] & 0xFF);
        }
        return (int) (h ^ (h >>> 32));
    }

    private void addOverflow(byte[] buf, int off, int len, String name) {
        if (overflowNames == null) {
            overflowNames = new String[8];
            overflowKeys = new byte[8][];
            overflowLens = new int[8];
            overflowCount = 0;
        }
        if (overflowCount >= overflowNames.length) {
            int newCap = overflowNames.length * 2;
            overflowNames = Arrays.copyOf(overflowNames, newCap);
            overflowKeys = Arrays.copyOf(overflowKeys, newCap);
            overflowLens = Arrays.copyOf(overflowLens, newCap);
        }
        overflowNames[overflowCount] = name;
        overflowKeys[overflowCount] = Arrays.copyOfRange(buf, off, off + len);
        overflowLens[overflowCount] = len;
        overflowCount++;
    }
}
