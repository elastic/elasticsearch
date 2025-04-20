/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.hash;

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
     * be made to sequentially supply the bytes for hashing. Once all bytes have been supplied, either the
     * {@link #digestHash} method (preferred) or the {@link #digest()} method should be called to complete the hash calculation.
     */
    public void update(byte[] inputBytes) {
        update(inputBytes, 0, inputBytes.length);
    }

    /**
     * Similar to {@link #update(byte[])}, but processes a specific portion of the input bytes
     * starting from the given {@code offset} for the specified {@code length}.
     * @see #update(byte[])
     */
    public void update(byte[] inputBytes, int offset, int length) {
        if (remainderLength + length >= remainder.length) {
            if (remainderLength > 0) {
                // fill rest of remainder from inputBytes and hash remainder
                int bytesToCopyFromInputToRemainder = remainder.length - remainderLength;
                System.arraycopy(inputBytes, offset, remainder, remainderLength, bytesToCopyFromInputToRemainder);
                offset = bytesToCopyFromInputToRemainder;
                length = length - bytesToCopyFromInputToRemainder;

                MurmurHash3.IntermediateResult result = MurmurHash3.intermediateHash(remainder, 0, remainder.length, h1, h2);
                h1 = result.h1;
                h2 = result.h2;
                remainderLength = 0;
                this.length += remainder.length;
            }
            // hash as many bytes as available in integer multiples of 16 as intermediateHash can only process multiples of 16
            int numBytesToHash = length & 0xFFFFFFF0;
            if (numBytesToHash > 0) {
                MurmurHash3.IntermediateResult result = MurmurHash3.intermediateHash(inputBytes, offset, numBytesToHash, h1, h2);
                h1 = result.h1;
                h2 = result.h2;
                this.length += numBytesToHash;
            }

            // save the remaining bytes, if any
            if (length > numBytesToHash) {
                this.remainderLength = length - numBytesToHash;
                System.arraycopy(inputBytes, offset + numBytesToHash, remainder, 0, remainderLength);
            }
        } else {
            System.arraycopy(inputBytes, 0, remainder, remainderLength, length);
            remainderLength += length;
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
     * Completes the hash of all bytes previously passed to {@link #update}.
     */
    public byte[] digest() {
        return digestHash().getBytes();
    }

    /**
     * Completes the hash of all bytes previously passed to {@link #update}.
     */
    public MurmurHash3.Hash128 digestHash() {
        return digestHash(new MurmurHash3.Hash128());
    }

    /**
     * Completes the hash of all bytes previously passed to {@link #update}.
     * Allows passing in a re-usable {@link org.elasticsearch.common.hash.MurmurHash3.Hash128} instance to avoid allocations.
     */
    public MurmurHash3.Hash128 digestHash(MurmurHash3.Hash128 hash) {
        length += remainderLength;
        MurmurHash3.finalizeHash(hash, remainder, 0, length, h1, h2);
        return hash;
    }

    public static String getAlgorithm() {
        return METHOD;
    }
}
