/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;
import org.apache.lucene.util.VectorUtil;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Unified class that can represent on-heap and off-heap vector values.
 */
public final class KMeansFloatVectorValues extends ClusteringFloatVectorValues {

    private final VectorSupplier vectors;
    private final DocSupplier docs;
    private final int numVectors;
    // Non-null when the underlying data is byte-backed, enabling native byte[] quantization
    private final ByteSupplier byteSupplier;

    private KMeansFloatVectorValues(VectorSupplier vectors, DocSupplier docs, int numVectors) {
        this(vectors, docs, numVectors, null);
    }

    private KMeansFloatVectorValues(VectorSupplier vectors, DocSupplier docs, int numVectors, ByteSupplier byteSupplier) {
        this.vectors = vectors;
        this.docs = docs;
        this.numVectors = numVectors;
        this.byteSupplier = byteSupplier;
    }

    /**
     * Build an instance from on-heap data structures.
     */
    public static KMeansFloatVectorValues build(List<float[]> vectors, int[] docs, int dim) {
        VectorSupplier vectorSupplier = new OnHeapVectorSupplier(vectors, dim);
        DocSupplier docSupplier = docs == null ? null : new OnHeapDocSupplier(docs);
        return new KMeansFloatVectorValues(vectorSupplier, docSupplier, vectors.size());
    }

    /**
     * View over {@link FloatVectorValues} using {@code ordinals[i]} as the delegate ordinal for local ordinal {@code i}.
     * Returned vectors are the delegate's live buffers and must not be retained across a later {@code vectorValue} call
     * on this instance. Clustering code satisfies that contract; callers that need a stable copy must copy themselves.
     */
    public static KMeansFloatVectorValues wrap(FloatVectorValues fvv, int[] ordinals) {
        VectorSupplier supplier = new FloatVectorValuesSupplier(fvv, ordinals);
        return new KMeansFloatVectorValues(supplier, null, ordinals.length);
    }

    /**
     * Like {@link #wrap(FloatVectorValues, int[])} but only the first {@code length} ordinals are used
     * (local ordinals {@code 0 .. length-1} map to {@code fvv.vectorValue(ordinals[i])}).
     * Reuses the backing {@code ordinals} array without copying when a prefix of the full corpus is needed.
     * See {@link #wrap(FloatVectorValues, int[])} for vector reuse semantics.
     */
    public static KMeansFloatVectorValues wrap(FloatVectorValues fvv, int[] ordinals, int length) {
        if (length < 0 || length > ordinals.length) {
            throw new IllegalArgumentException("length must be in [0, ordinals.length]");
        }
        VectorSupplier supplier = new FloatVectorValuesSupplier(fvv, ordinals);
        return new KMeansFloatVectorValues(supplier, null, length);
    }

    /**
     * Builds an instance from off-heap data structures. Vectors are expected to be written as
     * little endian floats one after the other. Docs are expected to be written as little endian ints
     * one after the other.
     */
    public static KMeansFloatVectorValues build(IndexInput vectors, IndexInput docs, int numVectors, int dims) throws IOException {
        long vectorLength = (long) dims * Float.BYTES;
        float[] vector = new float[dims];
        VectorSupplier vectorSupplier = new OffHeapVectorSupplier(vectors, vector, vectorLength);
        DocSupplier docSupplier;
        if (docs == null) {
            docSupplier = null;
        } else {
            RandomAccessInput randomDocs = docs.randomAccessSlice(0, docs.length());
            docSupplier = new OffHeapDocSupplier(docs, randomDocs);
        }
        return new KMeansFloatVectorValues(vectorSupplier, docSupplier, numVectors);
    }

    /**
     * Build an instance backed by on-heap byte vectors. Each byte value [-128, 127] is lazily
     * converted to the corresponding float on {@link #vectorValue(int)}. When {@code normalize}
     * is true (cosine similarity), each converted float vector is L2-normalized.
     * <p>
     * Use {@link #isByteBacked()} and {@link #byteVectorValue(int)} to access raw bytes
     * for native byte quantization without the float conversion overhead.
     */
    public static KMeansFloatVectorValues buildFromBytes(List<byte[]> vectors, int[] docs, int dim, boolean normalize) {
        return buildFromBytes(vectors, docs, dim, normalize, null);
    }

    /**
     * Build an instance backed by on-heap byte vectors with optional preconditioning.
     * When a {@code preconditioner} is provided, the rotation is applied lazily during
     * {@link #vectorValue(int)} after the byte-to-float conversion.
     */
    public static KMeansFloatVectorValues buildFromBytes(
        List<byte[]> vectors,
        int[] docs,
        int dim,
        boolean normalize,
        Preconditioner preconditioner
    ) {
        OnHeapByteVectorSupplier byteVectorSupplier = new OnHeapByteVectorSupplier(vectors, dim, normalize, preconditioner);
        DocSupplier docSupplier = docs == null ? null : new OnHeapDocSupplier(docs);
        return new KMeansFloatVectorValues(byteVectorSupplier, docSupplier, vectors.size(), byteVectorSupplier);
    }

    /**
     * Builds an instance from off-heap byte vectors with optional preconditioning.
     * When a {@code preconditioner} is provided, the rotation is applied lazily during
     * {@link #vectorValue(int)} after the byte-to-float conversion.
     */
    public static KMeansFloatVectorValues buildFromBytes(
        IndexInput vectors,
        IndexInput docs,
        int numVectors,
        int dims,
        boolean normalize,
        Preconditioner preconditioner
    ) throws IOException {
        OffHeapByteVectorSupplier byteVectorSupplier = new OffHeapByteVectorSupplier(vectors, dims, normalize, preconditioner);
        DocSupplier docSupplier;
        if (docs == null) {
            docSupplier = null;
        } else {
            RandomAccessInput randomDocs = docs.randomAccessSlice(0, docs.length());
            docSupplier = new OffHeapDocSupplier(docs, randomDocs);
        }
        return new KMeansFloatVectorValues(byteVectorSupplier, docSupplier, numVectors, byteVectorSupplier);
    }

    /**
     * Returns true if the underlying data is byte-backed, enabling native byte[]
     * quantization via {@link #byteVectorValue(int)}.
     */
    public boolean isByteBacked() {
        return byteSupplier != null;
    }

    /**
     * Returns true if this byte-backed instance has preconditioning applied.
     * When preconditioned, {@link #byteVectorValue(int)} returns raw (un-preconditioned) bytes,
     * so native byte quantization must NOT be used — callers should use
     * {@link #vectorValue(int)} which returns the preconditioned float vector.
     * <p>
     * Note: even when this returns {@code false}, native byte quantization may still be
     * inappropriate. For COSINE similarity, {@link #vectorValue(int)} returns L2-normalized
     * floats which differ from the raw byte values returned by {@link #byteVectorValue(int)}.
     * Callers must additionally check the similarity function before using the byte path.
     */
    public boolean isPreconditioned() {
        if (vectors instanceof OnHeapByteVectorSupplier s) {
            return s.preconditioner != null;
        }
        if (vectors instanceof OffHeapByteVectorSupplier s) {
            return s.preconditioner != null;
        }
        return false;
    }

    /**
     * Returns the raw byte vector for the given ordinal without conversion to float.
     * Only valid when {@link #isByteBacked()} returns true.
     */
    public byte[] byteVectorValue(int ord) throws IOException {
        assert byteSupplier != null;
        return byteSupplier.byteVector(ord);
    }

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return vectors.vector(ord);
    }

    @Override
    public ClusteringFloatVectorValues copy() {
        VectorSupplier copiedVectors = vectors.copy();
        // When the vectors supplier also implements ByteSupplier, the copy is the same object
        ByteSupplier copiedByteSupplier = copiedVectors instanceof ByteSupplier bs ? bs : null;
        return new KMeansFloatVectorValues(copiedVectors, docs != null ? docs.copy() : null, numVectors, copiedByteSupplier);
    }

    @Override
    public DocIndexIterator iterator() {
        return docs == null ? createDenseIterator() : createSparseIterator();
    }

    @Override
    public int dimension() {
        return vectors.dims();
    }

    @Override
    public int size() {
        return numVectors;
    }

    @Override
    public int ordToDoc(int ord) {
        return docs == null ? ord : docs.ordToDoc(ord);
    }

    private sealed interface VectorSupplier permits OffHeapVectorSupplier, OnHeapVectorSupplier, FloatVectorValuesSupplier,
        OffHeapByteVectorSupplier, OnHeapByteVectorSupplier {

        float[] vector(int ord) throws IOException;

        int dims();

        VectorSupplier copy();
    }

    private record OnHeapVectorSupplier(List<float[]> vectors, int dims) implements VectorSupplier {

        @Override
        public float[] vector(int ord) {
            return vectors.get(ord);
        }

        @Override
        public int dims() {
            return dims;
        }

        @Override
        public VectorSupplier copy() {
            return this;
        }
    }

    private record OffHeapVectorSupplier(IndexInput vectors, float[] vector, long vectorLength) implements VectorSupplier {

        @Override
        public float[] vector(int ord) throws IOException {
            vectors.seek(ord * vectorLength);
            vectors.readFloats(vector, 0, vector.length);
            return vector;
        }

        @Override
        public int dims() {
            return vector.length;
        }

        @Override
        public VectorSupplier copy() {
            return new OffHeapVectorSupplier(vectors.clone(), vector.clone(), vectorLength);
        }
    }

    /**
     * Provides raw byte[] access for native byte quantization.
     */
    private sealed interface ByteSupplier permits OnHeapByteVectorSupplier, OffHeapByteVectorSupplier {
        byte[] byteVector(int ord) throws IOException;
    }

    private static final class OnHeapByteVectorSupplier implements VectorSupplier, ByteSupplier {
        private final List<byte[]> vectors;
        private final int dims;
        private final boolean normalize;
        private final Preconditioner preconditioner;
        private final float[] floatScratch;
        // When preconditioner is non-null, we need a second scratch for the rotation output
        private final float[] preconditionedScratch;

        OnHeapByteVectorSupplier(List<byte[]> vectors, int dims, boolean normalize, Preconditioner preconditioner) {
            this.vectors = vectors;
            this.dims = dims;
            this.normalize = normalize;
            this.preconditioner = preconditioner;
            this.floatScratch = new float[dims];
            this.preconditionedScratch = preconditioner != null ? new float[dims] : null;
        }

        @Override
        public float[] vector(int ord) {
            byte[] bytes = vectors.get(ord);
            if (preconditioner != null) {
                if (normalize) {
                    // Convert byte→float, normalize, then apply preconditioner
                    for (int i = 0; i < bytes.length; i++) {
                        floatScratch[i] = bytes[i];
                    }
                    VectorUtil.l2normalize(floatScratch);
                    preconditioner.applyTransform(floatScratch, preconditionedScratch);
                } else {
                    // Apply preconditioner directly on byte[] (avoids intermediate float[] copy)
                    preconditioner.applyTransform(bytes, preconditionedScratch);
                }
                return preconditionedScratch;
            }
            for (int i = 0; i < bytes.length; i++) {
                floatScratch[i] = bytes[i];
            }
            if (normalize) {
                VectorUtil.l2normalize(floatScratch);
            }
            return floatScratch;
        }

        @Override
        public byte[] byteVector(int ord) {
            return vectors.get(ord);
        }

        @Override
        public int dims() {
            return dims;
        }

        @Override
        public VectorSupplier copy() {
            return new OnHeapByteVectorSupplier(vectors, dims, normalize, preconditioner);
        }
    }

    private static final class OffHeapByteVectorSupplier implements VectorSupplier, ByteSupplier {
        private final IndexInput vectors;
        private final int dims;
        private final boolean normalize;
        private final Preconditioner preconditioner;
        private final byte[] byteScratch;
        private final float[] floatScratch;
        private final float[] preconditionedScratch;

        OffHeapByteVectorSupplier(IndexInput vectors, int dims, boolean normalize, Preconditioner preconditioner) {
            this.vectors = vectors;
            this.dims = dims;
            this.normalize = normalize;
            this.preconditioner = preconditioner;
            this.byteScratch = new byte[dims];
            this.floatScratch = new float[dims];
            this.preconditionedScratch = preconditioner != null ? new float[dims] : null;
        }

        @Override
        public float[] vector(int ord) throws IOException {
            vectors.seek((long) ord * dims);
            vectors.readBytes(byteScratch, 0, dims);
            if (preconditioner != null) {
                if (normalize) {
                    for (int i = 0; i < dims; i++) {
                        floatScratch[i] = byteScratch[i];
                    }
                    VectorUtil.l2normalize(floatScratch);
                    preconditioner.applyTransform(floatScratch, preconditionedScratch);
                } else {
                    preconditioner.applyTransform(byteScratch, preconditionedScratch);
                }
                return preconditionedScratch;
            }
            for (int i = 0; i < dims; i++) {
                floatScratch[i] = byteScratch[i];
            }
            if (normalize) {
                VectorUtil.l2normalize(floatScratch);
            }
            return floatScratch;
        }

        @Override
        public byte[] byteVector(int ord) throws IOException {
            vectors.seek((long) ord * dims);
            vectors.readBytes(byteScratch, 0, dims);
            return byteScratch;
        }

        @Override
        public int dims() {
            return dims;
        }

        @Override
        public VectorSupplier copy() {
            return new OffHeapByteVectorSupplier(vectors.clone(), dims, normalize, preconditioner);
        }
    }

    private sealed interface DocSupplier permits OnHeapDocSupplier, OffHeapDocSupplier {
        int ordToDoc(int ord);

        DocSupplier copy();
    }

    private record OnHeapDocSupplier(int[] docs) implements DocSupplier {
        @Override
        public int ordToDoc(int ord) {
            return docs[ord];
        }

        @Override
        public DocSupplier copy() {
            return this;
        }
    }

    private record OffHeapDocSupplier(IndexInput docs, RandomAccessInput randomDocs) implements DocSupplier {
        @Override
        public int ordToDoc(int ord) {
            try {
                return randomDocs.readInt((long) ord * Integer.BYTES);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public DocSupplier copy() {
            IndexInput docsCopy = docs.clone();
            try {
                RandomAccessInput randomDocsCopy = docsCopy.randomAccessSlice(0, docsCopy.length());
                return new OffHeapDocSupplier(docsCopy, randomDocsCopy);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static final class FloatVectorValuesSupplier implements VectorSupplier {

        private final FloatVectorValues fvv;
        private final int[] ordinals;

        FloatVectorValuesSupplier(FloatVectorValues fvv, int[] ordinals) {
            this.fvv = fvv;
            this.ordinals = ordinals;
        }

        @Override
        public float[] vector(int ord) throws IOException {
            return fvv.vectorValue(ordinals[ord]);
        }

        @Override
        public int dims() {
            return fvv.dimension();
        }

        @Override
        public VectorSupplier copy() {
            try {
                return new FloatVectorValuesSupplier(fvv.copy(), ordinals);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
