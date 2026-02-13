/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.cluster;

import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.RandomAccessInput;

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
     */
    public static KMeansFloatVectorValues build(List<float[]> vectors, int[] docs, int dim) {
        VectorSupplier vectorSupplier = new OnHeapVectorSupplier(vectors, dim);
        DocSupplier docSupplier = docs == null ? null : new OnHeapDocSupplier(docs);
        return new KMeansFloatVectorValues(vectorSupplier, docSupplier, vectors.size());
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

    @Override
    public float[] vectorValue(int ord) throws IOException {
        return vectors.vector(ord);
    }

    @Override
    public ClusteringFloatVectorValues copy() {
        return new KMeansFloatVectorValues(vectors.copy(), docs != null ? docs.copy() : null, numVectors);
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
        if (docs == null) {
            return ord;
        }
        return docs.ordToDoc(ord);
    }

    private sealed interface VectorSupplier permits OffHeapVectorSupplier, OnHeapVectorSupplier {

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
}
