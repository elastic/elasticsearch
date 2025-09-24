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
import org.apache.lucene.store.FlushInfo;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.MergeInfo;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.hnsw.RandomVectorScorer;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.index.codec.vectors.AbstractFlatVectorsFormat;
import org.elasticsearch.index.codec.vectors.BulkScorableFloatVectorValues;
import org.elasticsearch.index.codec.vectors.BulkScorableVectorValues;
import org.elasticsearch.index.codec.vectors.MergeReaderWrapper;
import org.elasticsearch.index.store.FsDirectoryFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * Copied from Lucene99FlatVectorsFormat in Lucene 10.1
 *
 * This is copied to change the implementation of {@link #fieldsReader} only.
 * The codec format itself is not changed, so we keep the original {@link #NAME}
 */
public class DirectIOLucene99FlatVectorsFormat extends AbstractFlatVectorsFormat {

    static final String NAME = "Lucene99FlatVectorsFormat";

    public static final int VERSION_START = 0;
    public static final int VERSION_CURRENT = VERSION_START;

    private final FlatVectorsScorer vectorsScorer;

    /** Constructs a format */
    public DirectIOLucene99FlatVectorsFormat(FlatVectorsScorer vectorsScorer) {
        super(NAME);
        this.vectorsScorer = vectorsScorer;
    }

    @Override
    protected FlatVectorsScorer flatVectorsScorer() {
        return vectorsScorer;
    }

    @Override
    public FlatVectorsWriter fieldsWriter(SegmentWriteState state) throws IOException {
        return new Lucene99FlatVectorsWriter(state, vectorsScorer);
    }

    static boolean shouldUseDirectIO(SegmentReadState state) {
        assert USE_DIRECT_IO;
        return FsDirectoryFactory.isHybridFs(state.directory);
    }

    @Override
    public FlatVectorsReader fieldsReader(SegmentReadState state) throws IOException {
        if (shouldUseDirectIO(state) && state.context.context() == IOContext.Context.DEFAULT) {
            // only override the context for the random-access use case
            SegmentReadState directIOState = new SegmentReadState(
                state.directory,
                state.segmentInfo,
                state.fieldInfos,
                new DirectIOContext(state.context.hints()),
                state.segmentSuffix
            );
            // Use mmap for merges and direct I/O for searches.
            // TODO: Open the mmap file with sequential access instead of random (current behavior).
            // TODO: maybe we should force completely RANDOM access always for inner reader formats (outside of merges)?
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

    static class DirectIOContext implements IOContext {

        final Set<FileOpenHint> hints;

        DirectIOContext(Set<FileOpenHint> hints) {
            // always add DirectIOHint to the hints given
            this.hints = Sets.union(hints, Set.of(DirectIOHint.INSTANCE));
        }

        @Override
        public Context context() {
            return Context.DEFAULT;
        }

        @Override
        public MergeInfo mergeInfo() {
            return null;
        }

        @Override
        public FlushInfo flushInfo() {
            return null;
        }

        @Override
        public Set<FileOpenHint> hints() {
            return hints;
        }

        @Override
        public IOContext withHints(FileOpenHint... hints) {
            return new DirectIOContext(Set.of(hints));
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

        static class RescorerOffHeapVectorValues extends FloatVectorValues implements BulkScorableFloatVectorValues {
            VectorSimilarityFunction similarityFunction;
            FloatVectorValues inner;
            IndexInput inputSlice;
            FlatVectorsScorer scorer;

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
            public Bulk bulk(DocIdSetIterator matchingDocs) throws IOException {
                DocIdSetIterator conjunctionScorer = matchingDocs == null
                    ? indexIterator
                    : ConjunctionUtils.intersectIterators(List.of(matchingDocs, indexIterator));
                if (conjunctionScorer.docID() == -1) {
                    conjunctionScorer.nextDoc();
                }
                return new FloatBulkScorer(inner, inputSlice, byteSize, 32, indexIterator, conjunctionScorer);
            }
        }

        private static class FloatBulkScorer implements BulkScorableVectorValues.BulkVectorScorer.Bulk {
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
                for (int doc = matchingDocs.docID(); doc != DocIdSetIterator.NO_MORE_DOCS && size < nextCount; doc = matchingDocs
                    .nextDoc()) {
                    if (liveDocs == null || liveDocs.get(doc)) {
                        buffer.docs[size++] = indexIterator.index();
                    }
                }
                int loopBound = size - (size % bulkSize);
                int i = 0;
                for (; i < loopBound; i += bulkSize) {
                    for (int j = 0; j < bulkSize; j++) {
                        long ord = buffer.docs[i + j];
                        inputSlice.prefetch(ord * byteSize, byteSize);
                    }
                    System.arraycopy(buffer.docs, i, docBuffer, 0, bulkSize);
                    inner.bulkScore(docBuffer, scoreBuffer, bulkSize);
                    System.arraycopy(scoreBuffer, 0, buffer.features, i, bulkSize);
                }
                int countLeft = size - i;
                for (int j = i; j < size; j++) {
                    long ord = buffer.docs[j];
                    inputSlice.prefetch(ord * byteSize, byteSize);
                }
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
}
