/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.swisshash;

import jdk.incubator.vector.IntVector;
import jdk.incubator.vector.VectorSpecies;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.StringHelper;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.util.Arrays;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static jdk.incubator.vector.VectorOperators.ROL;
import static jdk.incubator.vector.VectorOperators.XOR;

/**
 * High-performance, vectorized MurmurHash3_x86_32 implementation for {@link BytesRef} arrays,
 * using fixed 128-bit SIMD (4 lanes) with length bucketing.
 *
 * <p> This class is designed for bulk hashing in scenarios where many byte sequences
 * need to be hashed efficiently, such as in search engine indexing or analytics.
 * It leverages the Panama Vector API to process multiple elements simultaneously.
 *
 * <p><strong>Key features:</strong>
 * <ul>
 *     <li>Fixed 128-bit SIMD vectorization (4 elements at a time) for the main 4-byte block loop.</li>
 *     <li>Bucketing by {@code length & ~3} to maximize SIMD utilization even when element lengths
 *         vary by up to 3 bytes.</li>
 *     <li>Scalar tail handling for the remaining 0–3 bytes per element, preserving bit-exact
 *         MurmurHash3 semantics.</li>
 *     <li>Fallback to scalar hashing for small buckets or extremely short sequences.</li>
 * </ul>
 *
 * <p><strong>Usage notes:</strong>
 * <ul>
 *     <li>All input arrays must contain valid {@link BytesRef} elements.</li>
 *     <li>Performance benefits are most pronounced when the input array contains many
 *         elements with similar lengths, allowing vectorized block processing to be
 *         fully utilized.</li>
 * </ul>
 */
public final class Murmur3BytesRefUtils {

    // Fixed-width SIMD (simple and predictable)
    private static final VectorSpecies<Integer> SPECIES = IntVector.SPECIES_128;
    private static final int LANES = SPECIES.length(); // 4

    static final VarHandle VH_LE_INT = MethodHandles.byteArrayViewVarHandle(int[].class, LITTLE_ENDIAN);

    static final int SEED = StringHelper.GOOD_FAST_HASH_SEED;
    static final int C1 = 0xcc9e2d51;
    static final int C2 = 0x1b873593;

    /**
     * Computes MurmurHash3_x86_32 hashes for an array of {@link BytesRef} elements.
     *
     * <p> This method uses a high-performance, vectorized implementation with fixed 128-bit SIMD
     * (4 lanes) for the main 4-byte block loop. Elements are grouped by "rounded length"
     * (length rounded down to the nearest multiple of 4) to maximize SIMD utilization.
     * The remaining 0–3 tail bytes of each element are processed individually in scalar code,
     * preserving exact MurmurHash3 semantics.
     *
     * @param refs the array of {@link BytesRef} elements to hash
     * @param hashes an array to store the 32-bit hash codes, with
     *               {@code hashes[i]} corresponding to {@code refs[i]}
     */
    @SuppressWarnings("fallthrough")
    public static void hashAll(BytesRef[] refs, int[] hashes) {
        if (hashes.length < refs.length) {
            throw new IllegalArgumentException("hashes array too small");
        }

        final int n = refs.length;
        if (n == 0) {
            return;
        }

        int[][] buckets = bucketByRoundedLength(refs);

        final int[] gather = new int[LANES];
        final int[] indexBuf = new int[LANES];

        for (int bucket = 0; bucket < buckets.length; bucket++) {
            int[] idxs = buckets[bucket];
            if (idxs == null) {
                continue;
            }

            // fullBlocks is already (length & ~3)
            final int fullBlocks = bucket;
            final int tailStart = bucket << 2;

            // Process batches of BytesRefs
            for (int p = 0; p < idxs.length; p += LANES) {
                final int lanes = Math.min(LANES, idxs.length - p);

                // prepare scatter indices
                if (lanes >= 0) System.arraycopy(idxs, p, indexBuf, 0, lanes);

                // load initial hash state
                IntVector h1 = IntVector.broadcast(SPECIES, SEED);
                // === SIMD: full 4-byte blocks ===
                for (int block = 0; block < fullBlocks; block++) {
                    final int byteOffset = block << 2;
                    for (int l = 0; l < lanes; l++) {
                        BytesRef br = refs[indexBuf[l]];
                        gather[l] = (int) VH_LE_INT.get(br.bytes, br.offset + byteOffset);
                    }
                    IntVector k1 = IntVector.fromArray(SPECIES, gather, 0);
                    k1 = k1.mul(C1).lanewise(ROL, 15).mul(C2);
                    h1 = h1.lanewise(XOR, k1).lanewise(ROL, 13).mul(5).add(0xe6546b64);
                }

                // === Scalar tail + finalization ===
                for (int l = 0; l < lanes; l++) {
                    int i = indexBuf[l];
                    BytesRef br = refs[i];
                    byte[] b = br.bytes;
                    int off = br.offset;
                    int len = br.length;

                    int hv = h1.lane(l);
                    int k1 = 0;

                    switch (len - tailStart) {
                        case 3:
                            k1 ^= (b[off + tailStart + 2] & 0xff) << 16;
                        case 2:
                            k1 ^= (b[off + tailStart + 1] & 0xff) << 8;
                        case 1:
                            k1 ^= (b[off + tailStart] & 0xff);
                            k1 *= C1;
                            k1 = Integer.rotateLeft(k1, 15);
                            k1 *= C2;
                            hv ^= k1;
                    }

                    hv ^= len;
                    hv ^= hv >>> 16;
                    hv *= 0x85ebca6b;
                    hv ^= hv >>> 13;
                    hv *= 0xc2b2ae35;
                    hv ^= hv >>> 16;

                    hashes[i] = hv;
                }
            }
        }
    }

    /**
     * Groups the indices of the input BytesRef array into buckets based on their
     * "rounded length," which is the length rounded down to the nearest multiple of 4
     * (i.e., {@code length & ~3}).
     *
     * <p> This bucketing allows us to efficiently vectorize the main 4-byte block loop
     * in MurmurHash3 for multiple elements at once. All elements in the same bucket
     * have the same number of full 4-byte blocks, so they can be processed together
     * using SIMD instructions. The remaining 0–3 tail bytes of each element are
     * handled separately in scalar code after the vectorized loop.
     *
     * <p> The returned array is indexed by (roundedLength >>> 2), where
     * {@code roundedLength = length & ~3}. Each
     * entry contains an {@code int[]} of indices into the original {@code refs} array
     * that belong to that rounded length. Buckets with no elements are {@code null}.
     *
     * @param refs the array of BytesRef elements to bucket
     * @return an array of index arrays, grouped by rounded length
     */
    static int[][] bucketByRoundedLength(BytesRef[] refs) {
        int maxRoundedLen = 0;
        for (BytesRef br : refs) {
            maxRoundedLen = Math.max(maxRoundedLen, br.length & ~3);
        }

        int maxBucket = maxRoundedLen >>> 2;

        int[][] buckets = new int[maxBucket + 1][];
        int[] counts = new int[maxBucket + 1];

        // count elements per bucket
        for (BytesRef br : refs) {
            counts[(br.length & ~3) >>> 2]++;
        }

        // allocate buckets
        for (int i = 0; i <= maxBucket; i++) {
            if (counts[i] != 0) {
                buckets[i] = new int[counts[i]];
            }
        }

        // fill buckets
        Arrays.fill(counts, 0);
        for (int i = 0; i < refs.length; i++) {
            int bucket = (refs[i].length & ~3) >>> 2;
            buckets[bucket][counts[bucket]++] = i;
        }

        return buckets;
    }
}
