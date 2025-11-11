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
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocAndFloatFeatureBuffer;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.index.codec.vectors.BulkScorableFloatVectorValues;
import org.elasticsearch.index.codec.vectors.BulkScorableVectorValues;
import org.elasticsearch.index.codec.vectors.DirectIOCapableFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.MergeReaderWrapper;

import java.io.IOException;
import java.util.List;

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
                new Lucene99FlatVectorsReader(state, vectorsScorer)
            );
        } else {
            return new Lucene99FlatVectorsReader(state, vectorsScorer);
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
            if (vectorValues == null || vectorValues.size() == 0) {
                return null;
            }
            FieldInfo info = state.fieldInfos.fieldInfo(field);
            return new RescorerOffHeapVectorValues(vectorValues, info.getVectorSimilarityFunction(), vectorScorer);
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

    static class RescorerOffHeapVectorValues extends FloatVectorValues implements BulkScorableFloatVectorValues {
        private final VectorSimilarityFunction similarityFunction;
        private final FloatVectorValues inner;
        private final IndexInput inputSlice;
        private final FlatVectorsScorer scorer;

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
        public BulkVectorScorer bulkRescorer(float[] target) throws IOException {
            return bulkScorer(target);
        }

        @Override
        public BulkVectorScorer bulkScorer(float[] target) throws IOException {
            DocIndexIterator indexIterator = inner.iterator();
            RandomVectorScorer randomScorer = scorer.getRandomVectorScorer(similarityFunction, inner, target);
            return new PreFetchingFloatBulkScorer(randomScorer, indexIterator, inputSlice, dimension() * Float.BYTES);
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            return inner.scorer(target);
        }
    }

    private record PreFetchingFloatBulkScorer(
        RandomVectorScorer inner,
        KnnVectorValues.DocIndexIterator indexIterator,
        IndexInput inputSlice,
        int byteSize
    ) implements BulkScorableVectorValues.BulkVectorScorer {

        @Override
        public float score() throws IOException {
            return inner.score(indexIterator.index());
        }

        @Override
        public DocIdSetIterator iterator() {
            return indexIterator;
        }

        @Override
        public BulkScorer bulkScore(DocIdSetIterator matchingDocs) throws IOException {
            DocIdSetIterator conjunctionScorer = matchingDocs == null
                ? indexIterator
                : ConjunctionUtils.intersectIterators(List.of(matchingDocs, indexIterator));
            if (conjunctionScorer.docID() == -1) {
                conjunctionScorer.nextDoc();
            }
            return new FloatBulkScorer(inner, inputSlice, byteSize, 32, indexIterator, conjunctionScorer);
        }
    }

    private static class FloatBulkScorer implements BulkScorableVectorValues.BulkVectorScorer.BulkScorer {
        private final KnnVectorValues.DocIndexIterator indexIterator;
        private final DocIdSetIterator matchingDocs;
        private final RandomVectorScorer inner;
        private final int bulkSize;
        private final IndexInput inputSlice;
        private final int byteSize;
        private final int[] docBuffer;
        private final float[] scoreBuffer;

        FloatBulkScorer(
            RandomVectorScorer fvv,
            IndexInput inputSlice,
            int byteSize,
            int bulkSize,
            KnnVectorValues.DocIndexIterator iterator,
            DocIdSetIterator matchingDocs
        ) {
            this.indexIterator = iterator;
            this.matchingDocs = matchingDocs;
            this.inner = fvv;
            this.bulkSize = bulkSize;
            this.inputSlice = inputSlice;
            this.docBuffer = new int[bulkSize];
            this.scoreBuffer = new float[bulkSize];
            this.byteSize = byteSize;
        }

        @Override
        public void nextDocsAndScores(int nextCount, Bits liveDocs, DocAndFloatFeatureBuffer buffer) throws IOException {
            buffer.growNoCopy(nextCount);
            int size = 0;
            for (int doc = matchingDocs.docID(); doc != DocIdSetIterator.NO_MORE_DOCS && size < nextCount; doc = matchingDocs.nextDoc()) {
                if (liveDocs == null || liveDocs.get(doc)) {
                    buffer.docs[size++] = indexIterator.index();
                }
            }
            final int firstBulkSize = Math.min(bulkSize, size);
            for (int j = 0; j < firstBulkSize; j++) {
                final long ord = buffer.docs[j];
                inputSlice.prefetch(ord * byteSize, byteSize);
            }
            final int loopBound = size - (size % bulkSize);
            int i = 0;
            for (; i < loopBound; i += bulkSize) {
                final int nextI = i + bulkSize;
                final int nextBulkSize = Math.min(bulkSize, size - nextI);
                for (int j = 0; j < nextBulkSize; j++) {
                    final long ord = buffer.docs[nextI + j];
                    inputSlice.prefetch(ord * byteSize, byteSize);
                }
                System.arraycopy(buffer.docs, i, docBuffer, 0, bulkSize);
                inner.bulkScore(docBuffer, scoreBuffer, bulkSize);
                System.arraycopy(scoreBuffer, 0, buffer.features, i, bulkSize);
            }
            final int countLeft = size - i;
            System.arraycopy(buffer.docs, i, docBuffer, 0, countLeft);
            inner.bulkScore(docBuffer, scoreBuffer, countLeft);
            System.arraycopy(scoreBuffer, 0, buffer.features, i, countLeft);
            buffer.size = size;
            // fix the docIds in buffer
            for (int j = 0; j < size; j++) {
                buffer.docs[j] = inner.ordToDoc(buffer.docs[j]);
            }
        }
    }
}
