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
package org.elasticsearch.index.codec.vectors.es816;

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
import org.elasticsearch.index.codec.vectors.BQVectorUtils;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.constSqrt;

/** Binarized vector values loaded from off-heap */
abstract class OffHeapBinarizedVectorValues extends BinarizedByteVectorValues {

    protected final int dimension;
    protected final int size;
    protected final int numBytes;
    protected final VectorSimilarityFunction similarityFunction;
    protected final FlatVectorsScorer vectorsScorer;

    protected final IndexInput slice;
    protected final byte[] binaryValue;
    protected final ByteBuffer byteBuffer;
    protected final int byteSize;
    private int lastOrd = -1;
    protected final float[] correctiveValues;
    protected final BinaryQuantizer binaryQuantizer;
    protected final float[] centroid;
    protected final float centroidDp;
    private final int discretizedDimensions;
    private final float maxX1;
    private final float sqrtDimensions;
    private final int correctionsCount;

    OffHeapBinarizedVectorValues(
        int dimension,
        int size,
        float[] centroid,
        float centroidDp,
        BinaryQuantizer quantizer,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        IndexInput slice
    ) {
        this.dimension = dimension;
        this.size = size;
        this.similarityFunction = similarityFunction;
        this.vectorsScorer = vectorsScorer;
        this.slice = slice;
        this.centroid = centroid;
        this.centroidDp = centroidDp;
        this.numBytes = BQVectorUtils.discretize(dimension, 64) / 8;
        this.correctionsCount = similarityFunction != EUCLIDEAN ? 3 : 2;
        this.correctiveValues = new float[this.correctionsCount];
        this.byteSize = numBytes + (Float.BYTES * correctionsCount);
        this.byteBuffer = ByteBuffer.allocate(numBytes);
        this.binaryValue = byteBuffer.array();
        this.binaryQuantizer = quantizer;
        this.discretizedDimensions = BQVectorUtils.discretize(dimension, 64);
        this.sqrtDimensions = (float) constSqrt(dimension);
        this.maxX1 = (float) (1.9 / constSqrt(discretizedDimensions - 1.0));
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
        slice.readFloats(correctiveValues, 0, correctionsCount);
        lastOrd = targetOrd;
        return binaryValue;
    }

    @Override
    public int discretizedDimensions() {
        return discretizedDimensions;
    }

    @Override
    public float sqrtDimensions() {
        return sqrtDimensions;
    }

    @Override
    public float maxX1() {
        return maxX1;
    }

    @Override
    public float getCentroidDP() {
        return centroidDp;
    }

    @Override
    public float[] getCorrectiveTerms(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return correctiveValues;
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, correctionsCount);
        return correctiveValues;
    }

    @Override
    public float getCentroidDistance(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return correctiveValues[0];
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, correctionsCount);
        return correctiveValues[0];
    }

    @Override
    public float getVectorMagnitude(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return correctiveValues[1];
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, correctionsCount);
        return correctiveValues[1];
    }

    @Override
    public float getOOQ(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return correctiveValues[0];
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, correctionsCount);
        return correctiveValues[0];
    }

    @Override
    public float getNormOC(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return correctiveValues[1];
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, correctionsCount);
        return correctiveValues[1];
    }

    @Override
    public float getODotC(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return correctiveValues[2];
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, correctionsCount);
        return correctiveValues[2];
    }

    @Override
    public BinaryQuantizer getQuantizer() {
        return binaryQuantizer;
    }

    @Override
    public float[] getCentroid() {
        return centroid;
    }

    @Override
    public int getVectorByteLength() {
        return numBytes;
    }

    public static OffHeapBinarizedVectorValues load(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        BinaryQuantizer binaryQuantizer,
        VectorSimilarityFunction similarityFunction,
        FlatVectorsScorer vectorsScorer,
        float[] centroid,
        float centroidDp,
        long quantizedVectorDataOffset,
        long quantizedVectorDataLength,
        IndexInput vectorData
    ) throws IOException {
        if (configuration.isEmpty()) {
            return new EmptyOffHeapVectorValues(dimension, similarityFunction, vectorsScorer);
        }
        assert centroid != null;
        IndexInput bytesSlice = vectorData.slice("quantized-vector-data", quantizedVectorDataOffset, quantizedVectorDataLength);
        if (configuration.isDense()) {
            return new DenseOffHeapVectorValues(
                dimension,
                size,
                centroid,
                centroidDp,
                binaryQuantizer,
                similarityFunction,
                vectorsScorer,
                bytesSlice
            );
        } else {
            return new SparseOffHeapVectorValues(
                configuration,
                dimension,
                size,
                centroid,
                centroidDp,
                binaryQuantizer,
                vectorData,
                similarityFunction,
                vectorsScorer,
                bytesSlice
            );
        }
    }

    /** Dense off-heap binarized vector values */
    static class DenseOffHeapVectorValues extends OffHeapBinarizedVectorValues {
        DenseOffHeapVectorValues(
            int dimension,
            int size,
            float[] centroid,
            float centroidDp,
            BinaryQuantizer binaryQuantizer,
            VectorSimilarityFunction similarityFunction,
            FlatVectorsScorer vectorsScorer,
            IndexInput slice
        ) {
            super(dimension, size, centroid, centroidDp, binaryQuantizer, similarityFunction, vectorsScorer, slice);
        }

        @Override
        public DenseOffHeapVectorValues copy() throws IOException {
            return new DenseOffHeapVectorValues(
                dimension,
                size,
                centroid,
                centroidDp,
                binaryQuantizer,
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
            RandomVectorScorer scorer = vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return scorer.score(iterator.index());
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

    /** Sparse off-heap binarized vector values */
    private static class SparseOffHeapVectorValues extends OffHeapBinarizedVectorValues {
        private final DirectMonotonicReader ordToDoc;
        private final IndexedDISI disi;
        // dataIn was used to init a new IndexedDIS for #randomAccess()
        private final IndexInput dataIn;
        private final OrdToDocDISIReaderConfiguration configuration;

        SparseOffHeapVectorValues(
            OrdToDocDISIReaderConfiguration configuration,
            int dimension,
            int size,
            float[] centroid,
            float centroidDp,
            BinaryQuantizer binaryQuantizer,
            IndexInput dataIn,
            VectorSimilarityFunction similarityFunction,
            FlatVectorsScorer vectorsScorer,
            IndexInput slice
        ) throws IOException {
            super(dimension, size, centroid, centroidDp, binaryQuantizer, similarityFunction, vectorsScorer, slice);
            this.configuration = configuration;
            this.dataIn = dataIn;
            this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
            this.disi = configuration.getIndexedDISI(dataIn);
        }

        @Override
        public SparseOffHeapVectorValues copy() throws IOException {
            return new SparseOffHeapVectorValues(
                configuration,
                dimension,
                size,
                centroid,
                centroidDp,
                binaryQuantizer,
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
        public DocIndexIterator iterator() {
            return IndexedDISI.asDocIndexIterator(disi);
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            SparseOffHeapVectorValues copy = copy();
            DocIndexIterator iterator = copy.iterator();
            RandomVectorScorer scorer = vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return scorer.score(iterator.index());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }
    }

    private static class EmptyOffHeapVectorValues extends OffHeapBinarizedVectorValues {
        private int doc = -1;

        EmptyOffHeapVectorValues(int dimension, VectorSimilarityFunction similarityFunction, FlatVectorsScorer vectorsScorer) {
            super(dimension, 0, null, Float.NaN, null, similarityFunction, vectorsScorer, null);
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }

        @Override
        public DenseOffHeapVectorValues copy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return null;
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            return null;
        }
    }
}
