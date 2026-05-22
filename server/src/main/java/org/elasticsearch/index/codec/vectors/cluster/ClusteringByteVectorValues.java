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
import java.util.List;

/**
 * {@link ClusteringVectorValues} implementation for {@code byte[]} vectors.
 * <p>
 * Provides both on-heap ({@link #build(List, int[], int)}) and off-heap
 * ({@link #build(IndexInput, IndexInput, int, int)}) construction. Unlike
 * {@link KMeansFloatVectorValues}, this class does NOT extend any Lucene
 * vector values class — it is used exclusively by the generic k-means
 * clustering infrastructure.
 */
public final class ClusteringByteVectorValues implements ClusteringVectorValues<byte[]> {

    private final ByteVectorSupplier vectors;
    private final DocSupplier docs;
    private final int numVectors;

    private ClusteringByteVectorValues(ByteVectorSupplier vectors, DocSupplier docs, int numVectors) {
        this.vectors = vectors;
        this.docs = docs;
        this.numVectors = numVectors;
    }

    /**
     * Build an instance from on-heap data structures.
     */
    public static ClusteringByteVectorValues build(List<byte[]> vectors, int[] docs, int dim) {
        ByteVectorSupplier vectorSupplier = new OnHeapByteSupplier(vectors, dim);
        DocSupplier docSupplier = docs == null ? null : new OnHeapDocSupplier(docs);
        return new ClusteringByteVectorValues(vectorSupplier, docSupplier, vectors.size());
    }

    /**
     * Build an instance from off-heap data structures.
     * Vectors are expected to be written as bytes one after the other.
     */
    public static ClusteringByteVectorValues build(IndexInput vectors, IndexInput docs, int numVectors, int dims) throws IOException {
        OffHeapByteSupplier vectorSupplier = new OffHeapByteSupplier(vectors, dims);
        DocSupplier docSupplier;
        if (docs == null) {
            docSupplier = null;
        } else {
            docSupplier = new OffHeapDocSupplier(docs);
        }
        return new ClusteringByteVectorValues(vectorSupplier, docSupplier, numVectors);
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
        return vectors.vector(ord);
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

    @Override
    public ClusteringByteVectorValues copy() throws IOException {
        ByteVectorSupplier copiedVectors = vectors.copy();
        return new ClusteringByteVectorValues(copiedVectors, docs != null ? docs.copy() : null, numVectors);
    }

    // ---- Internal suppliers ----

    private sealed interface ByteVectorSupplier permits OnHeapByteSupplier, OffHeapByteSupplier {
        byte[] vector(int ord) throws IOException;

        int dims();

        ByteVectorSupplier copy();
    }

    private record OnHeapByteSupplier(List<byte[]> vectors, int dims) implements ByteVectorSupplier {
        @Override
        public byte[] vector(int ord) {
            return vectors.get(ord);
        }

        @Override
        public ByteVectorSupplier copy() {
            return this;
        }
    }

    private static final class OffHeapByteSupplier implements ByteVectorSupplier {
        private final IndexInput vectors;
        private final int dims;
        private final byte[] scratch;

        OffHeapByteSupplier(IndexInput vectors, int dims) {
            this.vectors = vectors;
            this.dims = dims;
            this.scratch = new byte[dims];
        }

        @Override
        public byte[] vector(int ord) throws IOException {
            vectors.seek((long) ord * dims);
            vectors.readBytes(scratch, 0, dims);
            return scratch;
        }

        @Override
        public int dims() {
            return dims;
        }

        @Override
        public ByteVectorSupplier copy() {
            return new OffHeapByteSupplier(vectors.clone(), dims);
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

    private static final class OffHeapDocSupplier implements DocSupplier {
        private final IndexInput docs;
        private final RandomAccessInput randomDocs;

        OffHeapDocSupplier(IndexInput docs) throws IOException {
            this.docs = docs;
            this.randomDocs = docs.randomAccessSlice(0, docs.length());
        }

        @Override
        public int ordToDoc(int ord) {
            try {
                return randomDocs.readInt((long) ord * Integer.BYTES);
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }

        @Override
        public DocSupplier copy() {
            try {
                return new OffHeapDocSupplier(docs.clone());
            } catch (IOException e) {
                throw new java.io.UncheckedIOException(e);
            }
        }
    }
}
