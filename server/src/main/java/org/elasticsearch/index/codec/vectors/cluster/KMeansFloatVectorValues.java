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
import org.elasticsearch.core.Nullable;

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

    private KMeansFloatVectorValues(VectorSupplier vectors, DocSupplier docs, int numVectors) {
        this.vectors = vectors;
        this.docs = docs;
        this.numVectors = numVectors;
    }

    /**
     * Build an instance from on-heap data structures.
     *
     * @param vectors   The vectors
     * @param docs      Array of document IDs. Maps the vector ordinal to its docID. Null if ordinal == docID.
     * @param dim       Vector dimensions
     */
    public static KMeansFloatVectorValues build(List<float[]> vectors, @Nullable int[] docs, int dim) {
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
     * Builds an instance from off-heap data structures.
     *
     * @param vectors    Vectors as little-endian floats concatenated together.
     * @param docs       Document IDs in ordinal order, as little-endian int32. Null if ordinal == docID.
     * @param numVectors The number of vectors
     * @param dims       Vector dimensions
     */
    public static KMeansFloatVectorValues build(IndexInput vectors, @Nullable IndexInput docs, int numVectors, int dims)
        throws IOException {
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

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return vectors.vector(ord);
    }

    @Override
    public ClusteringFloatVectorValues copy() throws IOException {
        return new KMeansFloatVectorValues(vectors.copy(), docs != null ? docs.copy() : null, numVectors);
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

    private sealed interface VectorSupplier permits OffHeapVectorSupplier, OnHeapVectorSupplier, FloatVectorValuesSupplier {

        float[] vector(int ord) throws IOException;

        int dims();

        VectorSupplier copy() throws IOException;
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

    private sealed interface DocSupplier permits OnHeapDocSupplier, OffHeapDocSupplier {
        int ordToDoc(int ord);

        DocSupplier copy() throws IOException;
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
        public DocSupplier copy() throws IOException {
            IndexInput docsCopy = docs.clone();
            RandomAccessInput randomDocsCopy = docsCopy.randomAccessSlice(0, docsCopy.length());
            return new OffHeapDocSupplier(docsCopy, randomDocsCopy);
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
        public VectorSupplier copy() throws IOException {
            return new FloatVectorValuesSupplier(fvv.copy(), ordinals);
        }
    }
}
