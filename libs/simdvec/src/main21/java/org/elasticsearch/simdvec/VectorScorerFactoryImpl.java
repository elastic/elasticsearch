/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.simdvec;

import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FilterIndexInput;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MemorySegmentAccessInput;
import org.apache.lucene.util.SuppressForbidden;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.hnsw.RandomVectorScorerSupplier;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.elasticsearch.nativeaccess.NativeAccess;
import org.elasticsearch.simdvec.internal.ByteVectorScorer;
import org.elasticsearch.simdvec.internal.ByteVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.FloatVectorScorer;
import org.elasticsearch.simdvec.internal.FloatVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorer;
import org.elasticsearch.simdvec.internal.Int7SQVectorScorerSupplier;
import org.elasticsearch.simdvec.internal.MemorySegmentAccessor;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Optional;

import static org.elasticsearch.simdvec.internal.FloatVectorScorerSupplier.SUPPORTS_HEAP_SEGMENTS;

final class VectorScorerFactoryImpl implements VectorScorerFactory {

    static final VectorScorerFactoryImpl INSTANCE;

    private VectorScorerFactoryImpl() {}

    static {
        INSTANCE = NativeAccess.instance().getVectorSimilarityFunctions().map(ignore -> new VectorScorerFactoryImpl()).orElse(null);
    }

    // -- Float adapters --

    static class FloatMemorySegmentAccessInputAdapter implements MemorySegmentAccessor {
        private final MemorySegmentAccessInput input;
        private final long length;
        private final int dims;

        FloatMemorySegmentAccessInputAdapter(MemorySegmentAccessInput input, int dims) {
            this.input = input;
            this.length = (long) dims * Float.BYTES;
            this.dims = dims;
        }

        @Override
        public MemorySegment entireSegmentOrNull() throws IOException {
            return input.segmentSliceOrNull(0, input.length());
        }

        @Override
        public MemorySegment segmentForEntryOrNull(int ordinal) throws IOException {
            return input.segmentSliceOrNull((long) ordinal * length, length);
        }

        @Override
        public MemorySegmentAccessor clone() {
            return new FloatMemorySegmentAccessInputAdapter(input.clone(), dims);
        }
    }

    static class FloatMemorySegmentHeapAdapter implements MemorySegmentAccessor {
        private final FloatVectorValues values;

        FloatMemorySegmentHeapAdapter(FloatVectorValues values) {
            this.values = values;
        }

        @Override
        public MemorySegment entireSegmentOrNull() {
            return null;
        }

        @Override
        public MemorySegment segmentForEntryOrNull(int ordinal) throws IOException {
            return MemorySegment.ofArray(values.vectorValue(ordinal));
        }

        @Override
        public MemorySegmentAccessor clone() {
            return new FloatMemorySegmentHeapAdapter(values);
        }
    }

    static class OffHeapFloatStoreAdapter implements MemorySegmentAccessor {
        private final OffHeapFloatVectorStore store;

        OffHeapFloatStoreAdapter(OffHeapFloatVectorStore store) {
            this.store = store;
        }

        @Override
        public MemorySegment entireSegmentOrNull() {
            return null;
        }

        @Override
        public MemorySegment segmentForEntryOrNull(int ordinal) {
            return store.getVectorSegment(ordinal);
        }

        @Override
        public MemorySegment segmentForEntriesOrNull(int[] ordinals, Arena arena) {
            var pointers = ArenaUtil.allocate(arena, ValueLayout.ADDRESS, ordinals.length);
            for (int i = 0; i < ordinals.length; i++) {
                pointers.setAtIndex(ValueLayout.ADDRESS, i, store.getVectorSegment(ordinals[i]));
            }
            return pointers;
        }

        @Override
        public MemorySegmentAccessor clone() {
            return new OffHeapFloatStoreAdapter(store);
        }
    }

    // -- Byte adapters --

    static class ByteMemorySegmentAccessInputAdapter implements MemorySegmentAccessor {
        private final MemorySegmentAccessInput input;
        private final int dims;

        ByteMemorySegmentAccessInputAdapter(MemorySegmentAccessInput input, int dims) {
            this.input = input;
            this.dims = dims;
        }

        @Override
        public MemorySegment entireSegmentOrNull() throws IOException {
            return input.segmentSliceOrNull(0, input.length());
        }

        @Override
        public MemorySegment segmentForEntryOrNull(int ordinal) throws IOException {
            return input.segmentSliceOrNull((long) ordinal * dims, dims);
        }

        @Override
        public MemorySegmentAccessor clone() {
            return new ByteMemorySegmentAccessInputAdapter(input.clone(), dims);
        }
    }

    static class ByteMemorySegmentHeapAdapter implements MemorySegmentAccessor {
        private final ByteVectorValues values;

        ByteMemorySegmentHeapAdapter(ByteVectorValues values) {
            this.values = values;
        }

        @Override
        public MemorySegment entireSegmentOrNull() {
            return null;
        }

        @Override
        public MemorySegment segmentForEntryOrNull(int ordinal) throws IOException {
            return MemorySegment.ofArray(values.vectorValue(ordinal));
        }

        @Override
        public MemorySegmentAccessor clone() {
            return new ByteMemorySegmentHeapAdapter(values);
        }
    }

    static class OffHeapByteStoreAdapter implements MemorySegmentAccessor {
        private final OffHeapByteVectorStore store;

        OffHeapByteStoreAdapter(OffHeapByteVectorStore store) {
            this.store = store;
        }

        @Override
        public MemorySegment entireSegmentOrNull() {
            return null;
        }

        @Override
        public MemorySegment segmentForEntryOrNull(int ordinal) {
            return store.getVectorSegment(ordinal);
        }

        @Override
        public MemorySegmentAccessor clone() {
            return new OffHeapByteStoreAdapter(store);
        }
    }

    // -- Reflection helpers --

    /**
     * Attempts to extract an {@link OffHeapFloatVectorStore} from a {@link FloatVectorValues}
     * created by {@code FloatVectorValues.fromFloats(list, dim)}. The anonymous class captures
     * the list as a synthetic field; we use reflection to access it.
     */
    @SuppressForbidden(reason = "we need reflection to access the vector store")
    static OffHeapFloatVectorStore extractOffHeapFloatStore(FloatVectorValues values) {
        for (var field : values.getClass().getDeclaredFields()) {
            if (java.util.List.class.isAssignableFrom(field.getType())) {
                try {
                    field.setAccessible(true);
                    Object list = field.get(values);
                    if (list instanceof WrappedNativeFloatVectors wrapper) {
                        return wrapper.getStore();
                    }
                } catch (IllegalAccessException e) {
                    return null;
                }
            }
        }
        return null;
    }

    /**
     * Attempts to extract an {@link OffHeapByteVectorStore} from a {@link ByteVectorValues}
     * created by {@code ByteVectorValues.fromBytes(list, dim)}. The anonymous class captures
     * the list as a synthetic field; we use reflection to access it.
     */
    @SuppressForbidden(reason = "we need reflection to access the vector store")
    static OffHeapByteVectorStore extractOffHeapByteStore(ByteVectorValues values) {
        for (var field : values.getClass().getDeclaredFields()) {
            if (java.util.List.class.isAssignableFrom(field.getType())) {
                try {
                    field.setAccessible(true);
                    Object list = field.get(values);
                    if (list instanceof WrappedNativeByteVectors wrapper) {
                        return wrapper.getStore();
                    }
                } catch (IllegalAccessException e) {
                    return null;
                }
            }
        }
        return null;
    }

    // -- Supplier creation --

    @Override
    public Optional<RandomVectorScorerSupplier> getFloatVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        FloatVectorValues values
    ) {
        if (input != null) {
            input = FilterIndexInput.unwrapOnlyTest(input);
            input = MemorySegmentAccessInputAccess.unwrap(input);
        }
        if (input instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(values.size(), values.dimension(), input);
            var adapter = new FloatMemorySegmentAccessInputAdapter(msInput, values.dimension());
            return createFloatScorerSupplier(similarityType, values, adapter);
        }
        OffHeapFloatVectorStore offHeapStore = extractOffHeapFloatStore(values);
        if (offHeapStore != null) {
            var adapter = new OffHeapFloatStoreAdapter(offHeapStore);
            return createFloatScorerSupplier(similarityType, values, adapter);
        }
        if (SUPPORTS_HEAP_SEGMENTS) {
            var adapter = new FloatMemorySegmentHeapAdapter(values);
            return createFloatScorerSupplier(similarityType, values, adapter);
        }
        return Optional.empty();
    }

    private static Optional<RandomVectorScorerSupplier> createFloatScorerSupplier(
        VectorSimilarityType similarityType,
        FloatVectorValues values,
        MemorySegmentAccessor adapter
    ) {
        return switch (similarityType) {
            case COSINE, DOT_PRODUCT -> Optional.of(new FloatVectorScorerSupplier.DotProductSupplier(adapter, values));
            case EUCLIDEAN -> Optional.of(new FloatVectorScorerSupplier.EuclideanSupplier(adapter, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new FloatVectorScorerSupplier.MaxInnerProductSupplier(adapter, values));
        };
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getByteVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        ByteVectorValues values
    ) {
        if (input != null) {
            input = FilterIndexInput.unwrapOnlyTest(input);
            input = MemorySegmentAccessInputAccess.unwrap(input);
        }
        if (input instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(values.size(), values.dimension(), input);
            var adapter = new ByteMemorySegmentAccessInputAdapter(msInput, values.dimension());
            return createByteScorerSupplier(similarityType, values, adapter);
        }
        OffHeapByteVectorStore offHeapStore = extractOffHeapByteStore(values);
        if (offHeapStore != null) {
            var adapter = new OffHeapByteStoreAdapter(offHeapStore);
            return createByteScorerSupplier(similarityType, values, adapter);
        }
        if (SUPPORTS_HEAP_SEGMENTS) {
            var adapter = new ByteMemorySegmentHeapAdapter(values);
            return createByteScorerSupplier(similarityType, values, adapter);
        }
        return Optional.empty();
    }

    private static Optional<RandomVectorScorerSupplier> createByteScorerSupplier(
        VectorSimilarityType similarityType,
        ByteVectorValues values,
        MemorySegmentAccessor adapter
    ) {
        return switch (similarityType) {
            case COSINE -> Optional.of(new ByteVectorScorerSupplier.CosineSupplier(adapter, values));
            case DOT_PRODUCT -> Optional.of(new ByteVectorScorerSupplier.DotProductSupplier(adapter, values));
            case EUCLIDEAN -> Optional.of(new ByteVectorScorerSupplier.EuclideanSupplier(adapter, values));
            case MAXIMUM_INNER_PRODUCT -> Optional.of(new ByteVectorScorerSupplier.MaxInnerProductSupplier(adapter, values));
        };
    }

    @Override
    public Optional<RandomVectorScorer> getFloatVectorScorer(VectorSimilarityFunction sim, FloatVectorValues values, float[] queryVector) {
        return FloatVectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorer> getByteVectorScorer(VectorSimilarityFunction sim, ByteVectorValues values, byte[] queryVector) {
        return ByteVectorScorer.create(sim, values, queryVector);
    }

    @Override
    public Optional<RandomVectorScorerSupplier> getInt7SQVectorScorerSupplier(
        VectorSimilarityType similarityType,
        IndexInput input,
        QuantizedByteVectorValues values,
        float scoreCorrectionConstant
    ) {
        input = FilterIndexInput.unwrapOnlyTest(input);
        input = MemorySegmentAccessInputAccess.unwrap(input);
        if (input instanceof MemorySegmentAccessInput msInput) {
            checkInvariants(values.size(), values.dimension(), input);
            return switch (similarityType) {
                case COSINE, DOT_PRODUCT -> Optional.of(
                    new Int7SQVectorScorerSupplier.DotProductSupplier(msInput, values, scoreCorrectionConstant)
                );
                case EUCLIDEAN -> Optional.of(new Int7SQVectorScorerSupplier.EuclideanSupplier(msInput, values, scoreCorrectionConstant));
                case MAXIMUM_INNER_PRODUCT -> Optional.of(
                    new Int7SQVectorScorerSupplier.MaxInnerProductSupplier(msInput, values, scoreCorrectionConstant)
                );
            };
        }
        return Optional.empty();
    }

    @Override
    public Optional<RandomVectorScorer> getInt7SQVectorScorer(
        VectorSimilarityFunction sim,
        QuantizedByteVectorValues values,
        float[] queryVector
    ) {
        return Int7SQVectorScorer.create(sim, values, queryVector);
    }

    static void checkInvariants(int maxOrd, int vectorByteLength, IndexInput input) {
        if (input.length() < (long) vectorByteLength * maxOrd) {
            throw new IllegalArgumentException("input length is less than expected vector data");
        }
    }
}
