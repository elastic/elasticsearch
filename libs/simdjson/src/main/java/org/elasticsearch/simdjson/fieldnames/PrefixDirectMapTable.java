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
 * Field name table using a direct-mapped approach: the first 2 bytes plus the length
 * form a 24-bit key that indexes into a sparse table. For typical JSON schemas where
 * field names have distinct prefixes, this yields O(1) lookup with zero hashing.
 */
public final class PrefixDirectMapTable {

    private static final int TABLE_BITS = 14;
    private static final int TABLE_SIZE = 1 << TABLE_BITS;
    private static final int TABLE_MASK = TABLE_SIZE - 1;

    private boolean frozen;

    private String[] learnNames;
    private byte[][] learnKeys;
    private int[] learnLens;
    private int learnCount;

    private int[] tableIdx;
    private String[] entNames;
    private byte[][] entKeys;
    private int[] entLens;
    private int[] entNext;
    private int entCount;

    public PrefixDirectMapTable() {
        learnNames = new String[128];
        learnKeys = new byte[128][];
        learnLens = new int[128];
        learnCount = 0;
    }

    public String lookupOrInsert(byte[] buf, int off, int len) {
        if (frozen) {
            return frozenLookup(buf, off, len);
        }
        return learningLookupOrInsert(buf, off, len);
    }

    private String learningLookupOrInsert(byte[] buf, int off, int len) {
        for (int i = 0; i < learnCount; i++) {
            if (learnLens[i] == len && Arrays.equals(learnKeys[i], 0, len, buf, off, off + len)) {
                return learnNames[i];
            }
        }
        String s = new String(buf, off, len, StandardCharsets.UTF_8);
        if (learnCount >= learnNames.length) {
            int newCap = learnNames.length * 2;
            learnNames = Arrays.copyOf(learnNames, newCap);
            learnKeys = Arrays.copyOf(learnKeys, newCap);
            learnLens = Arrays.copyOf(learnLens, newCap);
        }
        learnNames[learnCount] = s;
        learnKeys[learnCount] = Arrays.copyOfRange(buf, off, off + len);
        learnLens[learnCount] = len;
        learnCount++;
        return s;
    }

    private String frozenLookup(byte[] buf, int off, int len) {
        int slot = prefixSlot(buf, off, len);
        int idx = tableIdx[slot];
        while (idx >= 0) {
            if (entLens[idx] == len && Arrays.equals(entKeys[idx], 0, len, buf, off, off + len)) {
                return entNames[idx];
            }
            idx = entNext[idx];
        }
        return null;
    }

    public void freeze() {
        if (frozen || learnCount == 0) {
            frozen = true;
            return;
        }

        tableIdx = new int[TABLE_SIZE];
        Arrays.fill(tableIdx, -1);
        entNames = new String[learnCount];
        entKeys = new byte[learnCount][];
        entLens = new int[learnCount];
        entNext = new int[learnCount];
        Arrays.fill(entNext, -1);
        entCount = 0;

        for (int i = 0; i < learnCount; i++) {
            int slot = prefixSlot(learnKeys[i], 0, learnLens[i]);
            entNames[entCount] = learnNames[i];
            entKeys[entCount] = learnKeys[i];
            entLens[entCount] = learnLens[i];
            entNext[entCount] = tableIdx[slot];
            tableIdx[slot] = entCount;
            entCount++;
        }

        frozen = true;
        learnNames = null;
        learnKeys = null;
        learnLens = null;
    }

    public boolean isFrozen() {
        return frozen;
    }

    private static int prefixSlot(byte[] buf, int off, int len) {
        int b0 = len > 0 ? buf[off] & 0xFF : 0;
        int b1 = len > 1 ? buf[off + 1] & 0xFF : 0;
        return ((b0 << 6) ^ (b1 << 2) ^ (len & 0x3F)) & TABLE_MASK;
    }
}
