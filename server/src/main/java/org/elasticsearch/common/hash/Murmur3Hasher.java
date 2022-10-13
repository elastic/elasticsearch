/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.hash;

import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Numbers;

/**
 * Wraps {@link MurmurHash3} to provide an interface similar to {@link java.security.MessageDigest} that
 * allows hashing of byte arrays passed through multiple calls to {@link #update(byte[])}. Like
 * {@link java.security.MessageDigest}, this class maintains internal state during the calculation of the
 * hash and is <b>not</b> threadsafe. If concurrent hashes are to be computed, each must be done on a
 * separate instance.
 */
public class Murmur3Hasher {

    public static final String METHOD = "MurmurHash3";

    private final long seed;
    private final byte[] remainder = new byte[16];
    private int remainderLength = 0;
    private int length;
    private long h1, h2;

    public Murmur3Hasher(long seed) {
        this.seed = seed;
        h1 = h2 = seed;
    }

    /**
     * Supplies some or all of the bytes to be hashed. Multiple calls to this method may
     * be made to sequentially supply the bytes for hashing. Once all bytes have been supplied, the
     * {@link #digest()} method should be called to complete the hash calculation.
     */
    public void update(byte[] inputBytes) {
        int totalLength = remainderLength + inputBytes.length;
        if (totalLength >= 16) {
            // hash as many bytes as available in integer multiples of 16
            int numBytesToHash = totalLength & 0xFFFFFFF0;
            byte[] bytesToHash;
            if (remainderLength > 0) {
                bytesToHash = new byte[numBytesToHash];
                System.arraycopy(remainder, 0, bytesToHash, 0, remainderLength);
                System.arraycopy(inputBytes, 0, bytesToHash, remainderLength, numBytesToHash - remainderLength);
            } else {
                bytesToHash = inputBytes;
            }

            MurmurHash3.IntermediateResult result = MurmurHash3.intermediateHash(bytesToHash, 0, numBytesToHash, h1, h2);
            h1 = result.h1;
            h2 = result.h2;
            this.length += numBytesToHash;

            // save the remaining bytes, if any
            if (totalLength > numBytesToHash) {
                System.arraycopy(inputBytes, numBytesToHash - remainderLength, remainder, 0, totalLength - numBytesToHash);
                remainderLength = totalLength - numBytesToHash;
            } else {
                remainderLength = 0;
            }
        } else {
            System.arraycopy(inputBytes, 0, remainder, remainderLength, inputBytes.length);
            remainderLength += inputBytes.length;
        }
    }

    /**
     * Clears all bytes previously passed to {@link #update(byte[])} and prepares for the calculation
     * of a new hash.
     */
    public void reset() {
        length = 0;
        remainderLength = 0;
        h1 = h2 = seed;
    }

    /**
     * Completes the hash of all bytes previously passed to {@link #update(byte[])}.
     */
    public byte[] digest() {
        length += remainderLength;
        MurmurHash3.Hash128 h = MurmurHash3.finalizeHash(new MurmurHash3.Hash128(), remainder, 0, length, h1, h2);
        byte[] hash = new byte[16];
        System.arraycopy(Numbers.longToBytes(h.h1), 0, hash, 0, 8);
        System.arraycopy(Numbers.longToBytes(h.h2), 0, hash, 8, 8);
        return hash;
    }

    public static String getAlgorithm() {
        return METHOD;
    }

    /**
     * Converts the 128-bit byte array returned by {@link #digest()} to a
     * {@link org.elasticsearch.common.hash.MurmurHash3.Hash128}
     */
    public static MurmurHash3.Hash128 toHash128(byte[] doubleLongBytes) {
        MurmurHash3.Hash128 hash128 = new MurmurHash3.Hash128();
        hash128.h1 = Numbers.bytesToLong(new BytesRef(doubleLongBytes, 0, 8));
        hash128.h2 = Numbers.bytesToLong(new BytesRef(doubleLongBytes, 8, 8));
        return hash128;
    }
}
