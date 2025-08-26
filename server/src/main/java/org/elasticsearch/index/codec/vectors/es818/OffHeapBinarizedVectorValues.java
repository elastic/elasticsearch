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
package org.elasticsearch.index.codec.vectors.es818;

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
import org.elasticsearch.index.codec.vectors.OptimizedScalarQuantizer;

import java.io.IOException;
import java.nio.ByteBuffer;

/** Binarized vector values loaded from off-heap */
abstract class OffHeapBinarizedVectorValues extends BinarizedByteVectorValues implements RandomAccessBinarizedByteVectorValues {

    final int dimension;
    final int size;
    final int numBytes;
    final VectorSimilarityFunction similarityFunction;
    final FlatVectorsScorer vectorsScorer;

    final IndexInput slice;
    final byte[] binaryValue;
    final ByteBuffer byteBuffer;
    final int byteSize;
    private int lastOrd = -1;
    final float[] correctiveValues;
    int quantizedComponentSum;
    final OptimizedScalarQuantizer binaryQuantizer;
    final float[] centroid;
    final float centroidDp;
    private final int discretizedDimensions;

    OffHeapBinarizedVectorValues(
        int dimension,
        int size,
        float[] centroid,
        float centroidDp,
        OptimizedScalarQuantizer quantizer,
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
        this.correctiveValues = new float[3];
        this.byteSize = numBytes + (Float.BYTES * 3) + Short.BYTES;
        this.byteBuffer = ByteBuffer.allocate(numBytes);
        this.binaryValue = byteBuffer.array();
        this.binaryQuantizer = quantizer;
        this.discretizedDimensions = BQVectorUtils.discretize(dimension, 64);
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
        slice.readFloats(correctiveValues, 0, 3);
        quantizedComponentSum = Short.toUnsignedInt(slice.readShort());
        lastOrd = targetOrd;
        return binaryValue;
    }

    @Override
    public int discretizedDimensions() {
        return discretizedDimensions;
    }

    @Override
    public float getCentroidDP() {
        return centroidDp;
    }

    @Override
    public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms() {
        return new OptimizedScalarQuantizer.QuantizationResult(
            correctiveValues[0],
            correctiveValues[1],
            correctiveValues[2],
            quantizedComponentSum
        );
    }

    @Override
    public OptimizedScalarQuantizer.QuantizationResult getCorrectiveTerms(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return new OptimizedScalarQuantizer.QuantizationResult(
                correctiveValues[0],
                correctiveValues[1],
                correctiveValues[2],
                quantizedComponentSum
            );
        }
        slice.seek(((long) targetOrd * byteSize) + numBytes);
        slice.readFloats(correctiveValues, 0, 3);
        quantizedComponentSum = Short.toUnsignedInt(slice.readShort());
        return new OptimizedScalarQuantizer.QuantizationResult(
            correctiveValues[0],
            correctiveValues[1],
            correctiveValues[2],
            quantizedComponentSum
        );
    }

    @Override
    public OptimizedScalarQuantizer getQuantizer() {
        return binaryQuantizer;
    }

    @Override
    public float[] getCentroid() {
        return centroid;
    }

    @Override
    public IndexInput getSlice() {
        return slice;
    }

    @Override
    public int getVectorByteLength() {
        return numBytes;
    }

    static OffHeapBinarizedVectorValues load(
        OrdToDocDISIReaderConfiguration configuration,
        int dimension,
        int size,
        OptimizedScalarQuantizer binaryQuantizer,
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
        private int doc = -1;

        DenseOffHeapVectorValues(
            int dimension,
            int size,
            float[] centroid,
            float centroidDp,
            OptimizedScalarQuantizer binaryQuantizer,
            VectorSimilarityFunction similarityFunction,
            FlatVectorsScorer vectorsScorer,
            IndexInput slice
        ) {
            super(dimension, size, centroid, centroidDp, binaryQuantizer, similarityFunction, vectorsScorer, slice);
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
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            assert docID() < target;
            if (target >= size) {
                return doc = NO_MORE_DOCS;
            }
            return doc = target;
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
            RandomVectorScorer scorer = vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return scorer.score(copy.doc);
                }

                @Override
                public DocIdSetIterator iterator() {
                    return copy;
                }
            };
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
            OptimizedScalarQuantizer binaryQuantizer,
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
        public VectorScorer scorer(float[] target) throws IOException {
            SparseOffHeapVectorValues copy = copy();
            RandomVectorScorer scorer = vectorsScorer.getRandomVectorScorer(similarityFunction, copy, target);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return scorer.score(copy.disi.index());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return copy;
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
        public int docID() {
            return doc;
        }

        @Override
        public int nextDoc() {
            return advance(doc + 1);
        }

        @Override
        public int advance(int target) {
            return doc = NO_MORE_DOCS;
        }

        @Override
        public byte[] vectorValue() {
            throw new UnsupportedOperationException();
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
        public VectorScorer scorer(float[] target) {
            return null;
        }
    }
}
