/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.index.codec.vectors.es93;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.codecs.hnsw.FlatVectorsScorer;
import org.apache.lucene.codecs.hnsw.FlatVectorsWriter;
import org.apache.lucene.codecs.lucene95.HasIndexSlice;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsReader;
import org.apache.lucene.codecs.lucene99.Lucene99FlatVectorsWriter;
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.AcceptDocs;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.MergeReaderWrapper;

import java.io.IOException;
import java.util.Map;

public class DirectIOCapableLucene99FlatVectorsFormat extends DirectIOCapableFlatVectorsFormat {

    static final String NAME = "Lucene99FlatVectorsFormat";

    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public DirectIOCapableLucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    public FlatVectorsScorer flatVectorsScorer() {
        return vectorsScorer;
    }

    @Override
    protected FlatVectorsReader createReader(SegmentReadState state) throws IOException {
        return new Lucene99FlatVectorsReader(state, vectorsScorer);
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state, boolean useDirectIO) throws IOException {
        if (state.context.context() == IOContext.Context.DEFAULT && useDirectIO && canUseDirectIO(state)) {
            // only override the context for the random-access use case
            SegmentReadState directIOState = new SegmentReadState(
                state.directory,
                state.segmentInfo,
                state.fieldInfos,
                new DirectIOContext(state.context.hints()),
                state.segmentSuffix
            );
            // Use mmap for merges and direct I/O for searches.
            return new MergeReaderWrapper(
                new Lucene99FlatBulkScoringVectorsReader(
                    directIOState,
                    new Lucene99FlatVectorsReader(directIOState, vectorsScorer),
                    vectorsScorer
                ),
                new Lucene99FlatBulkScoringVectorsReader(state, new Lucene99FlatVectorsReader(state, vectorsScorer), vectorsScorer)
            );
        } else {
            return new Lucene99FlatBulkScoringVectorsReader(state, new Lucene99FlatVectorsReader(state, vectorsScorer), vectorsScorer);
        }
    }

    static class Lucene99FlatBulkScoringVectorsReader extends FlatVectorsReader {
        private final Lucene99FlatVectorsReader inner;
        private final SegmentReadState state;

        Lucene99FlatBulkScoringVectorsReader(SegmentReadState state, Lucene99FlatVectorsReader inner, FlatVectorsScorer scorer) {
            super(scorer);
            this.inner = inner;
            this.state = state;
        }

        @Override
        public void close() throws IOException {
            inner.close();
        }

        @Override
        public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
            return inner.getOffHeapByteSize(fieldInfo);
        }

        @Override
        public void finishMerge() throws IOException {
            inner.finishMerge();
        }

        @Override
        public FlatVectorsReader getMergeInstance() throws IOException {
            return inner.getMergeInstance();
        }

        @Override
        public FlatVectorsScorer getFlatVectorScorer() {
            return inner.getFlatVectorScorer();
        }

        @Override
        public void search(String field, float[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            inner.search(field, target, knnCollector, acceptDocs);
        }

        @Override
        public void search(String field, byte[] target, KnnCollector knnCollector, AcceptDocs acceptDocs) throws IOException {
            inner.search(field, target, knnCollector, acceptDocs);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, float[] target) throws IOException {
            return inner.getRandomVectorScorer(field, target);
        }

        @Override
        public RandomVectorScorer getRandomVectorScorer(String field, byte[] target) throws IOException {
            return inner.getRandomVectorScorer(field, target);
        }

        @Override
        public void checkIntegrity() throws IOException {
            inner.checkIntegrity();
        }

        @Override
        public FloatVectorValues getFloatVectorValues(String field) throws IOException {
            FloatVectorValues vectorValues = inner.getFloatVectorValues(field);
            if (vectorValues == null) {
                return null;
            }
            if (vectorValues.size() == 0) {
                return vectorValues;
            }
            FieldInfo info = state.fieldInfos.fieldInfo(field);
            return vectorValues instanceof HasIndexSlice slicer
                ? new SliceableRescorerOffHeapVectorValues(vectorValues, slicer, info.getVectorSimilarityFunction(), vectorScorer)
                : new RescorerOffHeapVectorValues(vectorValues, info.getVectorSimilarityFunction(), vectorScorer);
        }

        @Override
        public ByteVectorValues getByteVectorValues(String field) throws IOException {
            return inner.getByteVectorValues(field);
        }

        @Override
        public long ramBytesUsed() {
            return inner.ramBytesUsed();
        }
    }

    static class RescorerOffHeapVectorValues extends FloatVectorValues {
        final VectorSimilarityFunction similarityFunction;
        final FloatVectorValues inner;
        final IndexInput inputSlice;
        final FlatVectorsScorer scorer;

        RescorerOffHeapVectorValues(FloatVectorValues inner, VectorSimilarityFunction similarityFunction, FlatVectorsScorer scorer) {
            this.inner = inner;
            if (inner instanceof HasIndexSlice slice) {
                this.inputSlice = slice.getSlice();
            } else {
                this.inputSlice = null;
            }
            this.similarityFunction = similarityFunction;
            this.scorer = scorer;
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            return inner.vectorValue(ord);
        }

        @Override
        public int dimension() {
            return inner.dimension();
        }

        @Override
        public int size() {
            return inner.size();
        }

        @Override
        public DocIndexIterator iterator() {
            return inner.iterator();
        }

        @Override
        public RescorerOffHeapVectorValues copy() throws IOException {
            return new RescorerOffHeapVectorValues(inner.copy(), similarityFunction, scorer);
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            return inner.scorer(target);
        }

        @Override
        public int ordToDoc(int ord) {
            return inner.ordToDoc(ord);
        }
    }

    static class SliceableRescorerOffHeapVectorValues extends RescorerOffHeapVectorValues implements HasIndexSlice {
        private final HasIndexSlice slicer;

        SliceableRescorerOffHeapVectorValues(
            FloatVectorValues inner,
            HasIndexSlice slicer,
            VectorSimilarityFunction similarityFunction,
            FlatVectorsScorer scorer
        ) {
            super(inner, similarityFunction, scorer);
            this.slicer = slicer;
        }

        @Override
        public RescorerOffHeapVectorValues copy() throws IOException {
            assert slicer instanceof RescorerOffHeapVectorValues;
            var innerCopy = inner.copy();
            var slicerCopy = (HasIndexSlice) innerCopy;
            return new SliceableRescorerOffHeapVectorValues(innerCopy, slicerCopy, similarityFunction, scorer);
        }

        @Override
        public IndexInput getSlice() {
            return slicer.getSlice();
        }
    }
}
