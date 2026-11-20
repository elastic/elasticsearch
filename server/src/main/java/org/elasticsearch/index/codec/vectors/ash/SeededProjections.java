/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.ash;

import org.apache.lucene.search.TaskExecutor;

import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;

/**
 * Random orthonormal projections, generated from a seed rather than learned from data. Segments too small to learn a
 * projection from, and fields that are not reduced in dimensions, use one. Such a projection depends on nothing but its
 * shape and seed, so segments store only the seed, and the projection is generated once and shared by every segment that
 * uses it.
 * <p>
 * Generated projections are kept in a JVM-wide cache with weak references, so they are shared for as long as something
 * uses them: readers hold their query projection for as long as their segment is open. Soft references would keep unused
 * projections until the heap is nearly full, where the real-memory circuit breaker counts them. The cache is bounded by the
 * number of distinct shapes.
 */
final class SeededProjections {

    private record Key(int originalDim, int nDims, long seed) {}

    private static final ConcurrentHashMap<Key, WeakReference<float[]>> W_T = new ConcurrentHashMap<>();
    private static final ConcurrentHashMap<Key, WeakReference<AshQueryProjection>> QUERY_PROJECTIONS = new ConcurrentHashMap<>();

    private SeededProjections() {}

    /**
     * Returns the id of the seeded projection of the given shape and seed: a name-based (version 3) UUID, the same for
     * every segment and node, so that segments using the projection can be recognized as sharing it.
     */
    static UUID id(int originalDim, int nDims, long seed) {
        ByteBuffer name = ByteBuffer.allocate(Integer.BYTES * 3 + Long.BYTES)
            .putInt(0x41534853) // "ASHS", so that the id is not one of another name-based UUID of the same bytes
            .putInt(originalDim)
            .putInt(nDims)
            .putLong(seed);
        return UUID.nameUUIDFromBytes(name.array());
    }

    /**
     * Returns W^T, row-major (nDims x originalDim), of the seeded projection, generating it with {@code executor}, or on
     * the calling thread if {@code null}, if it is not cached. The result does not depend on the executor. The array is
     * shared, so it must not be modified.
     */
    static float[] wT(int originalDim, int nDims, long seed, TaskExecutor executor) {
        Key key = new Key(originalDim, nDims, seed);
        return shared(W_T, key, () -> generate(key, executor));
    }

    /** Returns the int8 query projection of the seeded projection, shared by the segments that use it. */
    static AshQueryProjection queryProjection(int originalDim, int nDims, long seed) {
        return shared(QUERY_PROJECTIONS, new Key(originalDim, nDims, seed), () -> {
            float[] wT = wT(originalDim, nDims, seed, null);
            return AshQueryProjection.quantize(id(originalDim, nDims, seed), wT, originalDim, nDims);
        });
    }

    /**
     * Generates W^T: Gaussian entries from {@code new Random(seed)}, row by row, then orthonormalized. Segments store only
     * the seed, so this defines what a stored seed means and is part of the format: changing it, including
     * {@link AshUtils#randomGaussians} or {@link AshUtils#orthonormalizeRows}, requires a new format version.
     */
    private static float[] generate(Key key, TaskExecutor executor) {
        // orthonormalizeRows orthonormalizes rows, so it produces W^T (nDims x originalDim)
        float[] wT = AshUtils.randomGaussians(new Random(key.seed()), key.originalDim() * key.nDims());
        AshUtils.orthonormalizeRows(wT, key.originalDim(), key.nDims(), executor);
        return wT;
    }

    /** Returns the cached value for {@code key}, generating it if it is not cached or was collected. */
    private static <V> V shared(ConcurrentHashMap<Key, WeakReference<V>> cache, Key key, Supplier<V> generator) {
        // holds the value strongly until it is returned, so that it cannot be collected in between
        AtomicReference<V> value = new AtomicReference<>();
        cache.compute(key, (k, cached) -> {
            value.set(cached == null ? null : cached.get());
            if (value.get() != null) {
                return cached;
            }
            value.set(generator.get());
            return new WeakReference<>(value.get());
        });
        return value.get();
    }
}
