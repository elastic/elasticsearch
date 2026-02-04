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
 * Modifications copyright (C) 2025 Elasticsearch B.V.
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.lucene90.IndexedDISI;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.codecs.lucene95.OrdToDocDISIReaderConfiguration;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.apache.lucene.util.packed.DirectMonotonicReader;
import org.elasticsearch.index.codec.vectors.BFloat16;

import java.io.IOException;

abstract class OffHeapBFloat16VectorValues extends FloatVectorValues {

    protected final int dimension;
    protected final int size;
    protected final IndexInput slice;
    protected final int byteSize;
    protected int lastOrd = -1;
    protected final byte[] bfloatBytes;
    protected final float[] value;
    protected final VectorSimilarityFunction similarityFunction;
    protected final FlatVectorsScorer flatVectorsScorer;

    OffHeapBFloat16VectorValues(
        int dimension,
        int size,
        IndexInput slice,
        int byteSize,
        FlatVectorsScorer flatVectorsScorer,
        VectorSimilarityFunction similarityFunction
    ) {
        this.dimension = dimension;
        this.size = size;
        this.slice = slice;
        this.byteSize = byteSize;
        this.similarityFunction = similarityFunction;
        this.flatVectorsScorer = flatVectorsScorer;
        bfloatBytes = new byte[dimension * BFloat16.BYTES];
        value = new float[dimension];

        assert (this instanceof HasIndexSlice) == false
            : "BFloat16 should not implement HasIndexSlice until a bfloat16 scorer is created,"
                + " else Lucene99MemorySegmentFlatVectorsScorer will try to access 4-byte floats here";
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
    public float[] vectorValue(int targetOrd) throws IOException {
        if (lastOrd == targetOrd) {
            return value;
        }
        slice.seek((long) targetOrd * byteSize);
        // no readShorts() method
        slice.readBytes(bfloatBytes, 0, bfloatBytes.length);
        BFloat16.bFloat16ToFloat(bfloatBytes, value);
        lastOrd = targetOrd;
        return value;
    }

    static OffHeapBFloat16VectorValues load(
        VectorSimilarityFunction vectorSimilarityFunction,
        FlatVectorsScorer flatVectorsScorer,
        OrdToDocDISIReaderConfiguration configuration,
        VectorEncoding vectorEncoding,
        int dimension,
        int size,
        long vectorDataOffset,
        long vectorDataLength,
        IndexInput vectorData
    ) throws IOException {
        if (configuration.isEmpty() || vectorEncoding != VectorEncoding.FLOAT32) {
            return new EmptyOffHeapVectorValues(dimension, flatVectorsScorer, vectorSimilarityFunction);
        }
        IndexInput bytesSlice = vectorData.slice("vector-data", vectorDataOffset, vectorDataLength);
        int byteSize = dimension * BFloat16.BYTES;
        if (configuration.isDense()) {
            return new DenseOffHeapVectorValues(dimension, size, bytesSlice, byteSize, flatVectorsScorer, vectorSimilarityFunction);
        } else {
            return new SparseOffHeapVectorValues(
                configuration,
                vectorData,
                bytesSlice,
                dimension,
                size,
                byteSize,
                flatVectorsScorer,
                vectorSimilarityFunction
            );
        }
    }

    /**
     * Dense vector values that are stored off-heap. This is the most common case when every doc has a
     * vector.
     */
    static class DenseOffHeapVectorValues extends OffHeapBFloat16VectorValues {

        DenseOffHeapVectorValues(
            int dimension,
            int size,
            IndexInput slice,
            int byteSize,
            FlatVectorsScorer flatVectorsScorer,
            VectorSimilarityFunction similarityFunction
        ) {
            super(dimension, size, slice, byteSize, flatVectorsScorer, similarityFunction);
        }

        @Override
        public DenseOffHeapVectorValues copy() throws IOException {
            return new DenseOffHeapVectorValues(dimension, size, slice.clone(), byteSize, flatVectorsScorer, similarityFunction);
        }

        @Override
        public int ordToDoc(int ord) {
            return ord;
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return acceptDocs;
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }

        @Override
        public VectorScorer scorer(float[] query) throws IOException {
            DenseOffHeapVectorValues copy = copy();
            DocIndexIterator iterator = copy.iterator();
            RandomVectorScorer randomVectorScorer = flatVectorsScorer.getRandomVectorScorer(similarityFunction, copy, query);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return randomVectorScorer.score(iterator.docID());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }
    }

    private static class SparseOffHeapVectorValues extends OffHeapBFloat16VectorValues {
        private final DirectMonotonicReader ordToDoc;
        private final IndexedDISI disi;
        // dataIn was used to init a new IndexedDIS for #randomAccess()
        private final IndexInput dataIn;
        private final OrdToDocDISIReaderConfiguration configuration;

        SparseOffHeapVectorValues(
            OrdToDocDISIReaderConfiguration configuration,
            IndexInput dataIn,
            IndexInput slice,
            int dimension,
            int size,
            int byteSize,
            FlatVectorsScorer flatVectorsScorer,
            VectorSimilarityFunction similarityFunction
        ) throws IOException {

            super(dimension, size, slice, byteSize, flatVectorsScorer, similarityFunction);
            this.configuration = configuration;
            this.dataIn = dataIn;
            this.ordToDoc = configuration.getDirectMonotonicReader(dataIn);
            this.disi = configuration.getIndexedDISI(dataIn);
        }

        @Override
        public SparseOffHeapVectorValues copy() throws IOException {
            return new SparseOffHeapVectorValues(
                configuration,
                dataIn,
                slice.clone(),
                dimension,
                size,
                byteSize,
                flatVectorsScorer,
                similarityFunction
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
        public VectorScorer scorer(float[] query) throws IOException {
            SparseOffHeapVectorValues copy = copy();
            DocIndexIterator iterator = copy.iterator();
            RandomVectorScorer randomVectorScorer = flatVectorsScorer.getRandomVectorScorer(similarityFunction, copy, query);
            return new VectorScorer() {
                @Override
                public float score() throws IOException {
                    return randomVectorScorer.score(iterator.index());
                }

                @Override
                public DocIdSetIterator iterator() {
                    return iterator;
                }
            };
        }
    }

    private static class EmptyOffHeapVectorValues extends OffHeapBFloat16VectorValues {

        EmptyOffHeapVectorValues(int dimension, FlatVectorsScorer flatVectorsScorer, VectorSimilarityFunction similarityFunction) {
            super(dimension, 0, null, 0, flatVectorsScorer, similarityFunction);
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
        public EmptyOffHeapVectorValues copy() {
            throw new UnsupportedOperationException();
        }

        @Override
        public float[] vectorValue(int targetOrd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public DocIndexIterator iterator() {
            return createDenseIterator();
        }

        @Override
        public Bits getAcceptOrds(Bits acceptDocs) {
            return null;
        }

        @Override
        public VectorScorer scorer(float[] query) {
            return null;
        }
    }
}
