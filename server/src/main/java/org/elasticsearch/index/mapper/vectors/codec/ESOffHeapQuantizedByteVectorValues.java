/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper.vectors.codec;

import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.packed.DirectMonotonicReader;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Read the quantized vector values and their score correction values from the index input. This
 * supports both iterated and random access.
 */
abstract class ESOffHeapQuantizedByteVectorValues extends ESQuantizedByteVectorValues implements ESRandomAccessQuantizedByteVectorValues {

    protected final int dimension;
    protected final int size;
    protected final IndexInput slice;
    protected final byte[] binaryValue;
    protected final ByteBuffer byteBuffer;
    protected final int byteSize;
    protected int lastOrd = -1;
    protected final float[] scoreCorrectionConstant = new float[1];

    ESOffHeapQuantizedByteVectorValues(int dimension, int size, IndexInput slice) {
        this.dimension = dimension;
        this.size = size;
        this.slice = slice;
        this.byteSize = dimension + Float.BYTES;
        byteBuffer = ByteBuffer.allocate(dimension);
        binaryValue = byteBuffer.array();
    }

    @Override
    public int dimension() {
        return dimension;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public byte[] vectorValue(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return binaryValue;
        }
        slice.seek((long) targetOrd * byteSize);
        slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), dimension);
        slice.readFloats(scoreCorrectionConstant, 0, 1);
        lastOrd = targetOrd;
        return binaryValue;
    }

    @Override
    public float getScoreCorrectionConstant() {
        return scoreCorrectionConstant[0];
    }

    static ESOffHeapQuantizedByteVectorValues load(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        long quantizedVectorDataOffset,
        long quantizedVectorDataLength,
        IndexInput vectorData
    ) throws IOException {
        if (configuration.isEmpty()) {
            return new ESOffHeapQuantizedByteVectorValues.EmptyOffHeapVectorValues(dimension);
        }
        IndexInput bytesSlice = vectorData.slice("quantized-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);
        if (configuration.isDense()) {
            return new DenseOffHeapVectorValues(dimension, size, bytesSlice);
        } else {
            return new SparseOffHeapVectorValues(configuration, dimension, size, vectorData, bytesSlice);
        }
    }

    static class DenseOffHeapVectorValues extends ESOffHeapQuantizedByteVectorValues {

        private int doc = -1;

        DenseOffHeapVectorValues(int dimension, int size, IndexInput slice) {
            super(dimension, size, slice);
        }

        @Override
        public byte[] vectorValue() throws IOException {
            return vectorValue(doc);
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            assert docID() < target;
            if (target >= size) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
        }

        @Override
        public DenseOffHeapVectorValues copy() throws IOException {
            return new DenseOffHeapVectorValues(dimension, size, slice.clone());
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return acceptDocs;
        }
    }

    private static class SparseOffHeapVectorValues extends ESOffHeapQuantizedByteVectorValues {
        private final DirectMonotonicReader ordToDoc;
        private final IndexedDISI disi;
        // dataIn was used to init a new IndexedDIS for #randomAccess()
        private final IndexInput dataIn;
        private final OrdToDocDISIReaderConfiguration configuration;

        SparseOffHeapVectorValues(
            OrdToDocDISIReaderConfiguration configuration,
            int dimension,
            int size,
            IndexInput dataIn,
            IndexInput slice
        ) throws IOException {
            super(dimension, size, slice);
            this.configuration = configuration;
            this.dataIn = dataIn;
            this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
            this.disi = configuration.getIndexedDISI(dataIn);
        }

        @Override
        public byte[] vectorValue() throws IOException {
            return vectorValue(disi.index());
        }

        @Override
        public int docID() {
            return disi.docID();
        }

        @Override
        public int nextDoc() throws IOException {
            return disi.nextDoc();
        }

        @Override
        public int advance(int target) throws IOException {
            assert docID() < target;
            return disi.advance(target);
        }

        @Override
        public SparseOffHeapVectorValues copy() throws IOException {
            return new SparseOffHeapVectorValues(configuration, dimension, size, dataIn, slice.clone());
        }

        @Override
        public int ordToDoc(int ord) {
            return (int) ordToDoc.get(ord);
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            if (acceptDocs == null) {
                return null;
            }
            return new Bits() {
                @Override
                public boolean get(int index) {
                    return acceptDocs.get(ordToDoc(index));
                }

                @Override
                public int length() {
                    return size;
                }
            };
        }
    }

    private static class EmptyOffHeapVectorValues extends ESOffHeapQuantizedByteVectorValues {

        EmptyOffHeapVectorValues(int dimension) {
            super(dimension, 0, null);
        }

        private int doc = -1;

        @Override
        public int dimension() {
            return super.dimension();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public byte[] vectorValue() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() throws IOException {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) throws IOException {
            return doc = NO_MORE_DOCS;
        }

        @Override
        public EmptyOffHeapVectorValues copy() throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] vectorValue(int targetOrd) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public int ordToDoc(int ord) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return null;
        }
    }
}
