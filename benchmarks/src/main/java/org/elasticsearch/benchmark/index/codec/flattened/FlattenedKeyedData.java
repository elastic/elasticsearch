/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.benchmark.index.codec.flattened;

import org.apache.lucene.util.BytesRef;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeMap;

/**
 * Deterministic flattened key-value data shapes for the benchmarks. A given workload always produces
 * the same per-document key-value pairs so every format arm sees byte-identical input.
 *
 * <p>Each document is represented as a sorted list of {@code key\0value} {@link BytesRef} pairs,
 * mirroring the canonical document order produced by {@code FlattenedFieldParser} when JSON object
 * keys happen to be sorted. Using sorted blobs means the columnar writer can elide the ORDER_STREAM,
 * giving the cleanest comparison between the two layouts.
 */
final class FlattenedKeyedData {

    private FlattenedKeyedData() {}

    /**
     * Generates {@code docCount} documents, each a sorted list of {@code key\0value} pairs encoded
     * as raw bytes ready for passing to
     * {@link org.elasticsearch.index.mapper.MultiValuedBinaryDocValuesField.KeyedArrayOrderInlineNull#recordValue}.
     *
     * <p>All docs have at least two keys to ensure the multi-value framing is always exercised.
     */
    static List<List<BytesRef>> generate(String workload, int docCount) {
        int keyPoolSize = switch (workload) {
            case "FEW_KEYS" -> 10;
            case "MEDIUM_KEYS" -> 50;
            case "MANY_KEYS" -> 200;
            case "HIGH_CARDINALITY" -> 1000;
            default -> throw new IllegalArgumentException("Unknown workload: " + workload);
        };
        int keysPerDocTypical = switch (workload) {
            case "FEW_KEYS" -> 5;
            case "MEDIUM_KEYS" -> 10;
            case "MANY_KEYS" -> 20;
            case "HIGH_CARDINALITY" -> 3;
            default -> throw new IllegalArgumentException("Unknown workload: " + workload);
        };

        Rng rng = new Rng(workloadSeed(workload));

        // Plain ASCII key pool — no 0x00 bytes so the separator is unambiguous.
        byte[][] keyPool = new byte[keyPoolSize][];
        for (int k = 0; k < keyPoolSize; k++) {
            keyPool[k] = ("key" + k).getBytes(StandardCharsets.UTF_8);
        }

        // Small fixed-length value pool: promotes compression and keeps data generation cheap.
        int valueLen = 8;
        byte[][] valuePool = new byte[32][];
        for (int v = 0; v < 32; v++) {
            valuePool[v] = new byte[valueLen];
            for (int b = 0; b < valueLen; b++) {
                valuePool[v][b] = (byte) rng.nextInt(256);
            }
        }

        // Scratch array for partial Fisher-Yates shuffle (sample without replacement).
        int[] indices = new int[keyPoolSize];
        for (int i = 0; i < keyPoolSize; i++) {
            indices[i] = i;
        }

        List<List<BytesRef>> result = new ArrayList<>(docCount);
        for (int d = 0; d < docCount; d++) {
            int numKeys = Math.max(2, Math.min(keyPoolSize, 1 + rng.nextInt(2 * keysPerDocTypical - 1)));

            // Partial Fisher-Yates to sample numKeys distinct indices without replacement.
            // Restore swapped positions after use so the array stays usable across docs.
            int[] swapped = new int[numKeys];
            for (int i = 0; i < numKeys; i++) {
                int j = i + rng.nextInt(keyPoolSize - i);
                swapped[i] = j;
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }

            // Build sorted key\0value pairs via a TreeMap so the output is in canonical key order.
            TreeMap<BytesRef, byte[]> kvMap = new TreeMap<>();
            for (int i = 0; i < numKeys; i++) {
                kvMap.put(new BytesRef(keyPool[indices[i]]), valuePool[rng.nextInt(32)]);
            }

            List<BytesRef> pairs = new ArrayList<>(kvMap.size());
            for (var e : kvMap.entrySet()) {
                byte[] key = e.getKey().bytes;
                int keyLen = e.getKey().length;
                byte[] val = e.getValue();
                byte[] kv = new byte[keyLen + 1 + val.length];
                System.arraycopy(key, 0, kv, 0, keyLen);
                kv[keyLen] = 0; // separator
                System.arraycopy(val, 0, kv, keyLen + 1, val.length);
                pairs.add(new BytesRef(kv));
            }
            result.add(pairs);

            // Undo the partial shuffle so the indices array is ready for the next document.
            for (int i = numKeys - 1; i >= 0; i--) {
                int j = swapped[i];
                int tmp = indices[i];
                indices[i] = indices[j];
                indices[j] = tmp;
            }
        }
        return result;
    }

    /**
     * Returns the raw key bytes for key at position {@code keyIndex} in the pool.
     * Used to build the search target ({@code key + 0x00}) in the read benchmark.
     */
    static byte[] keyBytes(int keyIndex) {
        return ("key" + keyIndex).getBytes(StandardCharsets.UTF_8);
    }

    private static long workloadSeed(String workload) {
        return switch (workload) {
            case "FEW_KEYS" -> 1L;
            case "MEDIUM_KEYS" -> 2L;
            case "MANY_KEYS" -> 3L;
            case "HIGH_CARDINALITY" -> 4L;
            default -> throw new IllegalArgumentException("Unknown workload: " + workload);
        };
    }

    /** A tiny deterministic SplitMix64 generator — reproducible and dependency-free. */
    static final class Rng {
        private long state;

        Rng(long seed) {
            this.state = seed;
        }

        long nextLong() {
            long z = (state += 0x9E3779B97F4A7C15L);
            z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
            z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
            return z ^ (z >>> 31);
        }

        int nextInt(int bound) {
            return Math.floorMod(nextLong(), bound);
        }
    }
}
