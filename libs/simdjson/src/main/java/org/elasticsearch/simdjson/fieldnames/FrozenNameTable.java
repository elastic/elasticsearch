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
import java.util.Arrays;

/**
 * Field name table that compacts into a dense array after freeze, using open-addressing
 * with a power-of-two table sized to 2x the entry count. This reduces probing distance
 * compared to {@link FieldNameTable} which uses a fixed 2048-slot table.
 *
 * <p>The frozen table also stores the first 8 bytes of each key inline as a {@code long},
 * enabling a fast rejection test before falling back to full key comparison.
 */
public final class FrozenNameTable {

    private static final VarHandle LONG_LE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);

    private boolean frozen;

    private String[] names;
    private byte[][] keys;
    private int[] keyLens;
    private int count;

    private int mask;
    private int[] fHashes;
    private int[] fLens;
    private long[] fPrefix8;
    private byte[][] fKeys;
    private String[] fNames;

    public FrozenNameTable() {
        names = new String[128];
        keys = new byte[128][];
        keyLens = new int[128];
        count = 0;
        frozen = false;
    }

    public String lookupOrInsert(byte[] buf, int off, int len) {
        if (frozen) {
            return frozenLookup(buf, off, len);
        }
        return learningLookupOrInsert(buf, off, len);
    }

    public String lookupOrInsert(byte[] buf, int off, int len, int hash) {
        if (frozen) {
            return frozenLookupWithHash(buf, off, len, hash);
        }
        return learningLookupOrInsert(buf, off, len);
    }

    private String learningLookupOrInsert(byte[] buf, int off, int len) {
        for (int i = 0; i < count; i++) {
            if (keyLens[i] == len && Arrays.equals(keys[i], 0, len, buf, off, off + len)) {
                return names[i];
            }
        }
        String s = new String(buf, off, len, StandardCharsets.UTF_8);
        if (count >= names.length) {
            int newCap = names.length * 2;
            names = Arrays.copyOf(names, newCap);
            keys = Arrays.copyOf(keys, newCap);
            keyLens = Arrays.copyOf(keyLens, newCap);
        }
        names[count] = s;
        keys[count] = Arrays.copyOfRange(buf, off, off + len);
        keyLens[count] = len;
        count++;
        return s;
    }

    private String frozenLookup(byte[] buf, int off, int len) {
        int h = FieldNameHash.hashName(buf, off, len);
        return frozenLookupWithHash(buf, off, len, h);
    }

    private String frozenLookupWithHash(byte[] buf, int off, int len, int h) {
        long prefix = readPrefix8(buf, off, len);
        for (int i = h & mask;; i = (i + 1) & mask) {
            int sh = fHashes[i];
            if (sh == 0) return null;
            if (sh == h && fLens[i] == len && fPrefix8[i] == prefix) {
                if (len <= 8 || Arrays.equals(fKeys[i], 0, len, buf, off, off + len)) {
                    return fNames[i];
                }
            }
        }
    }

    public void freeze() {
        if (frozen || count == 0) {
            frozen = true;
            return;
        }

        int tableSize = Integer.highestOneBit(count * 2 - 1) << 1;
        if (tableSize < 16) tableSize = 16;
        mask = tableSize - 1;

        fHashes = new int[tableSize];
        fLens = new int[tableSize];
        fPrefix8 = new long[tableSize];
        fKeys = new byte[tableSize][];
        fNames = new String[tableSize];

        for (int i = 0; i < count; i++) {
            int h = FieldNameHash.hashName(keys[i], 0, keyLens[i]);
            long prefix = readPrefix8(keys[i], 0, keyLens[i]);
            int slot = h & mask;
            while (fHashes[slot] != 0) {
                slot = (slot + 1) & mask;
            }
            fHashes[slot] = h;
            fLens[slot] = keyLens[i];
            fPrefix8[slot] = prefix;
            fKeys[slot] = keys[i];
            fNames[slot] = names[i];
        }

        frozen = true;
        names = null;
        keys = null;
        keyLens = null;
    }

    public boolean isFrozen() {
        return frozen;
    }

    public int size() {
        return count;
    }

    private static long readPrefix8(byte[] buf, int off, int len) {
        if (len >= 8) {
            return (long) LONG_LE.get(buf, off);
        }
        long v = 0;
        for (int i = 0; i < len; i++) {
            v |= (long) (buf[off + i] & 0xFF) << (i * 8);
        }
        return v;
    }
}
