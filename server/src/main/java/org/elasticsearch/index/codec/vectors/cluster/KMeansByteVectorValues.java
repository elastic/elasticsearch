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
import org.elasticsearch.core.Nullable;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;

/**
 * Unified class that can represent on-heap and off-heap byte vector values.
 */
public final class KMeansByteVectorValues extends ClusteringByteVectorValues {

    private final ByteVectorSupplier vectors;
    private final DocSupplier docs;
    private final int numVectors;

    private KMeansByteVectorValues(ByteVectorSupplier vectors, DocSupplier docs, int numVectors) {
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
    public static KMeansByteVectorValues build(List<byte[]> vectors, @Nullable int[] docs, int dim) {
        ByteVectorSupplier vectorSupplier = new OnHeapByteSupplier(vectors, dim);
        DocSupplier docSupplier = docs == null ? null : new OnHeapDocSupplier(docs);
        return new KMeansByteVectorValues(vectorSupplier, docSupplier, vectors.size());
    }

    /**
     * Builds an instance from off-heap data structures.
     *
     * @param vectors    Vectors as bytes concatenated together.
     * @param docs       Document IDs in ordinal order, as little-endian int32. Null if ordinal == docID.
     * @param numVectors The number of vectors
     * @param dims       Vector dimensions
     */
    public static KMeansByteVectorValues build(IndexInput vectors, @Nullable IndexInput docs, int numVectors, int dims) throws IOException {
        OffHeapByteSupplier vectorSupplier = new OffHeapByteSupplier(vectors, dims);
        DocSupplier docSupplier;
        if (docs == null) {
            docSupplier = null;
        } else {
            docSupplier = new OffHeapDocSupplier(docs);
        }
        return new KMeansByteVectorValues(vectorSupplier, docSupplier, numVectors);
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
        return new KMeansByteVectorValues(copiedVectors, docs != null ? docs.copy() : null, numVectors);
    }

    @Override
    public DocIndexIterator iterator() {
        return docs == null ? createDenseIterator() : createSparseIterator();
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
                throw new UncheckedIOException(e);
            }
        }

        @Override
        public DocSupplier copy() throws IOException {
            return new OffHeapDocSupplier(docs.clone());
        }
    }
}
