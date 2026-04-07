/*
 * @notice
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Modifications copyright (C) 2024 Elasticsearch B.V.
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.ScalarQuantizer;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Copied from Lucene 10.3.
 * Read the quantized vector values and their score correction values from the index input. This
 * supports both iterated and random access.
 */
public abstract class OffHeapQuantizedByteVectorValues extends QuantizedByteVectorValues {

    final int dimension;
    final int size;
    final int numBytes;
    final ScalarQuantizer scalarQuantizer;
    final VectorSimilarityFunction similarityFunction;
    final FlatVectorsScorer vectorsScorer;
    final boolean compress;

    final IndexInput slice;
    final byte[] binaryValue;
    final ByteBuffer byteBuffer;
    final int byteSize;
    int lastOrd = -1;
    final float[] scoreCorrectionConstant = new float[1];

    static void decompressBytes(byte[] compressed, int numBytes) {
        if (numBytes == compressed.length) {
            return;
        }
        if (numBytes << 1 != compressed.length) {
            throw new IllegalArgumentException("numBytes: " + numBytes + " does not match compressed length: " + compressed.length);
        }
        for (int i = 0; i < numBytes; ++i) {
            compressed[numBytes + i] = (byte) (compressed[i] & 0x0F);
            compressed[i] = (byte) ((compressed[i] & 0xFF) >> 4);
        }
    }

    static byte[] compressedArray(int dimension, byte bits) {
        if (bits <= 4) {
            return new byte[(dimension + 1) >> 1];
        } else {
            return null;
        }
    }

    static void compressBytes(byte[] raw, byte[] compressed) {
        if (compressed.length != ((raw.length + 1) >> 1)) {
            throw new IllegalArgumentException("compressed length: " + compressed.length + " does not match raw length: " + raw.length);
        }
        for (int i = 0; i < compressed.length; ++i) {
            int v = (raw[i] << 4) | raw[compressed.length + i];
            compressed[i] = (byte) v;
        }
    }

    OffHeapQuantizedByteVectorValues(
        int dimension,
        int size,
        ScalarQuantizer scalarQuantizer,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        boolean compress,
        IndexInput slice
    ) {
        this.dimension = dimension;
        this.size = size;
        this.slice = slice;
        this.scalarQuantizer = scalarQuantizer;
        this.compress = compress;
        if (scalarQuantizer.getBits() <= 4 && compress) {
            this.numBytes = (dimension + 1) >> 1;
        } else {
            this.numBytes = dimension;
        }
        this.byteSize = this.numBytes + Float.BYTES;
        byteBuffer = ByteBuffer.allocate(dimension);
        binaryValue = byteBuffer.array();
        this.similarityFunction = similarityFunction;
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public ScalarQuantizer getScalarQuantizer() {
        return scalarQuantizer;
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
        slice.readBytes(byteBuffer.array(), byteBuffer.arrayOffset(), numBytes);
        slice.readFloats(scoreCorrectionConstant, 0, 1);
        decompressBytes(binaryValue, numBytes);
        lastOrd = targetOrd;
        return binaryValue;
    }

    @Override
    public float getScoreCorrectionConstant(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return scoreCorrectionConstant[0];
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(scoreCorrectionConstant, 0, 1);
        return scoreCorrectionConstant[0];
    }

    @Override
    public IndexInput getSlice() {
        return slice;
    }

    @Override
    public int getVectorByteLength() {
        return numBytes;
    }

    static OffHeapQuantizedByteVectorValues load(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        ScalarQuantizer scalarQuantizer,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        boolean compress,
        long quantizedVectorDataOffset,
        long quantizedVectorDataLength,
        IndexInput vectorData
    ) throws IOException {
        if (configuration.isEmpty()) {
            return new EmptyOffHeapVectorValues(dimension, similarityFunction, vectorsScorer);
        }
        IndexInput bytesSlice = vectorData.slice("quantized-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);
        if (configuration.isDense()) {
            return new DenseOffHeapVectorValues(dimension, size, scalarQuantizer, compress, similarityFunction, vectorsScorer, bytesSlice);
        } else {
            return new SparseOffHeapVectorValues(
                configuration,
                dimension,
                size,
                scalarQuantizer,
                compress,
                vectorData,
                similarityFunction,
                vectorsScorer,
                bytesSlice
            );
        }
    }

    /**
     * Dense vector values that are stored off-heap. This is the most common case when every doc has a
     * vector.
     */
    public static class DenseOffHeapVectorValues extends OffHeapQuantizedByteVectorValues {

        /**
         * Create dense off-heap vector values
         *
         * @param dimension vector dimension
         * @param size number of vectors
         * @param scalarQuantizer the scalar quantizer
         * @param compress whether the vectors are compressed
         * @param similarityFunction the similarity function
         * @param vectorsScorer the vectors scorer
         * @param slice the index input slice containing the vector data
         */
        public DenseOffHeapVectorValues(
            int dimension,
            int size,
            ScalarQuantizer scalarQuantizer,
            boolean compress,
            VectorSimilarityFunction similarityFunction,
            FlatVectorsScorer vectorsScorer,
            IndexInput slice
        ) {
            super(dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, compress, slice);
        }

        @Override
        public DenseOffHeapVectorValues copy() throws IOException {
            return new DenseOffHeapVectorValues(
                dimension,
                size,
                scalarQuantizer,
                compress,
                similarityFunction,
                vectorsScorer,
                slice.clone()
            );
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return acceptDocs;
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            DenseOffHeapVectorValues copy = copy();
            DocIndexIterator iterator = copy.iterator();
            RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return vectorScorer.score(iterator.index());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }
    }

    private static class SparseOffHeapVectorValues extends OffHeapQuantizedByteVectorValues {
        private final DirectMonotonicReader ordToDoc;
        private final IndexedDISI disi;
        // dataIn was used to init a new IndexedDIS for #randomAccess()
        private final IndexInput dataIn;
        private final OrdToDocDISIReaderConfiguration configuration;

        SparseOffHeapVectorValues(
            OrdToDocDISIReaderConfiguration configuration,
            int dimension,
            int size,
            ScalarQuantizer scalarQuantizer,
            boolean compress,
            IndexInput dataIn,
            VectorSimilarityFunction similarityFunction,
            FlatVectorsScorer vectorsScorer,
            IndexInput slice
        ) throws IOException {
            super(dimension, size, scalarQuantizer, similarityFunction, vectorsScorer, compress, slice);
            this.configuration = configuration;
            this.dataIn = dataIn;
            this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
            this.disi = configuration.getIndexedDISI(dataIn);
        }

        @Override
        public DocIndexIterator iterator() {
            return IndexedDISI.asDocIndexIterator(disi);
        }

        @Override
        public SparseOffHeapVectorValues copy() throws IOException {
            return new SparseOffHeapVectorValues(
                configuration,
                dimension,
                size,
                scalarQuantizer,
                compress,
                dataIn,
                similarityFunction,
                vectorsScorer,
                slice.clone()
            );
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

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            SparseOffHeapVectorValues copy = copy();
            DocIndexIterator iterator = copy.iterator();
            RandomVectorScorer vectorScorer = vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return vectorScorer.score(iterator.index());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }
    }

    private static class EmptyOffHeapVectorValues extends OffHeapQuantizedByteVectorValues {

        EmptyOffHeapVectorValues(int dimension, VectorSimilarityFunction similarityFunction, FlatVectorsScorer vectorsScorer) {
            super(dimension, 0, new ScalarQuantizer(-1, 1, (byte) 7), similarityFunction, vectorsScorer, false, null);
        }

        @Override
        public int dimension() {
            return super.dimension();
        }

        @Override
        public int size() {
            return 0;
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }

        @Override
        public EmptyOffHeapVectorValues copy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public byte[] vectorValue(int targetOrd) {
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

        @Override
        public VectorScorer scorer(float[] target) {
            return null;
        }
    }
}
