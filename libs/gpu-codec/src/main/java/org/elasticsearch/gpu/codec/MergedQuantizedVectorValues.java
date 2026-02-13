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

package org.elasticsearch.gpu.codec;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.DocIDMerger;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.VectorScorer;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.quantization.QuantizedByteVectorValues;
import org.apache.lucene.util.quantization.QuantizedVectorsReader;
import org.apache.lucene.util.quantization.ScalarQuantizer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.apache.lucene.codecs.KnnVectorsWriter.MergedVectorValues.hasVectorValues;

/**
 * A copy from Lucene99ScalarQuantizedVectorsWriter to access mergeQuantizedByteVectorValues
 * during segment merge.
 */
class MergedQuantizedVectorValues extends QuantizedByteVectorValues {
    private static final float REQUANTIZATION_LIMIT = 0.2f;

    private final List<QuantizedByteVectorValueSub> subs;
    private final DocIDMerger<QuantizedByteVectorValueSub> docIdMerger;
    private final int size;
    private QuantizedByteVectorValueSub current;

    private MergedQuantizedVectorValues(List<QuantizedByteVectorValueSub> subs, MergeState mergeState) throws IOException {
        this.subs = subs;
        docIdMerger = DocIDMerger.of(subs, mergeState.needsIndexSort);
        int totalSize = 0;
        for (QuantizedByteVectorValueSub sub : subs) {
            totalSize += sub.values.size();
        }
        size = totalSize;
    }

    @Override
    public byte[] vectorValue(int ord) throws IOException {
        return current.values.vectorValue(current.index());
    }

    @Override
    public DocIndexIterator iterator() {
        return new MergedQuantizedVectorValues.CompositeIterator();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public int dimension() {
        return subs.get(0).values.dimension();
    }

    @Override
    public float getScoreCorrectionConstant(int ord) throws IOException {
        return current.values.getScoreCorrectionConstant(current.index());
    }

    private class CompositeIterator extends DocIndexIterator {
        private int docId;
        private int ord;

        CompositeIterator() {
            docId = -1;
            ord = -1;
        }

        @Override
        public int index() {
            return ord;
        }

        @Override
        public int docID() {
            return docId;
        }

        @Override
        public int nextDoc() throws IOException {
            current = docIdMerger.next();
            if (current == null) {
                docId = NO_MORE_DOCS;
                ord = NO_MORE_DOCS;
            } else {
                docId = current.mappedDocID;
                ++ord;
            }
            return docId;
        }

        @Override
        public int advance(int target) throws IOException {
            throw new UnsupportedOperationException();
        }

        @Override
        public long cost() {
            return size;
        }
    }

    private static QuantizedVectorsReader getQuantizedKnnVectorsReader(KnnVectorsReader vectorsReader, String fieldName) {
        if (vectorsReader instanceof PerFieldKnnVectorsFormat.FieldsReader candidateReader) {
            vectorsReader = candidateReader.getFieldReader(fieldName);
        }
        if (vectorsReader instanceof QuantizedVectorsReader reader) {
            return reader;
        }
        return null;
    }

    static MergedQuantizedVectorValues mergeQuantizedByteVectorValues(
        FieldInfo fieldInfo,
        MergeState mergeState,
        ScalarQuantizer scalarQuantizer
    ) throws IOException {
        assert fieldInfo != null && fieldInfo.hasVectorValues();

        List<QuantizedByteVectorValueSub> subs = new ArrayList<>();
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            if (hasVectorValues(mergeState.fieldInfos[i], fieldInfo.name)) {
                QuantizedVectorsReader reader = getQuantizedKnnVectorsReader(mergeState.knnVectorsReaders[i], fieldInfo.name);
                assert scalarQuantizer != null;
                final QuantizedByteVectorValueSub sub;
                // Either our quantization parameters are way different than the merged ones
                // Or we have never been quantized.
                if (reader == null || reader.getQuantizationState(fieldInfo.name) == null
                // For smaller `bits` values, we should always recalculate the quantiles
                // TODO: this is very conservative, could we reuse information for even int4
                // quantization?
                    || scalarQuantizer.getBits() <= 4
                    || shouldRequantize(reader.getQuantizationState(fieldInfo.name), scalarQuantizer)) {
                    FloatVectorValues toQuantize = mergeState.knnVectorsReaders[i].getFloatVectorValues(fieldInfo.name);
                    if (fieldInfo.getVectorSimilarityFunction() == VectorSimilarityFunction.COSINE) {
                        toQuantize = new NormalizedFloatVectorValues(toQuantize);
                    }
                    sub = new QuantizedByteVectorValueSub(
                        mergeState.docMaps[i],
                        new QuantizedFloatVectorValues(toQuantize, fieldInfo.getVectorSimilarityFunction(), scalarQuantizer)
                    );
                } else {
                    sub = new QuantizedByteVectorValueSub(
                        mergeState.docMaps[i],
                        new OffsetCorrectedQuantizedByteVectorValues(
                            reader.getQuantizedVectorValues(fieldInfo.name),
                            fieldInfo.getVectorSimilarityFunction(),
                            scalarQuantizer,
                            reader.getQuantizationState(fieldInfo.name)
                        )
                    );
                }
                subs.add(sub);
            }
        }
        return new MergedQuantizedVectorValues(subs, mergeState);
    }

    private static boolean shouldRequantize(ScalarQuantizer existingQuantiles, ScalarQuantizer newQuantiles) {
        float tol = REQUANTIZATION_LIMIT * (newQuantiles.getUpperQuantile() - newQuantiles.getLowerQuantile()) / 128f;
        if (Math.abs(existingQuantiles.getUpperQuantile() - newQuantiles.getUpperQuantile()) > tol) {
            return true;
        }
        return Math.abs(existingQuantiles.getLowerQuantile() - newQuantiles.getLowerQuantile()) > tol;
    }

    private static class QuantizedByteVectorValueSub extends DocIDMerger.Sub {
        private final QuantizedByteVectorValues values;
        private final KnnVectorValues.DocIndexIterator iterator;

        QuantizedByteVectorValueSub(MergeState.DocMap docMap, QuantizedByteVectorValues values) {
            super(docMap);
            this.values = values;
            iterator = values.iterator();
            assert iterator.docID() == -1;
        }

        @Override
        public int nextDoc() throws IOException {
            return iterator.nextDoc();
        }

        public int index() {
            return iterator.index();
        }
    }

    private static class QuantizedFloatVectorValues extends QuantizedByteVectorValues {
        private final FloatVectorValues values;
        private final ScalarQuantizer quantizer;
        private final byte[] quantizedVector;
        private int lastOrd = -1;
        private float offsetValue = 0f;

        private final VectorSimilarityFunction vectorSimilarityFunction;

        QuantizedFloatVectorValues(FloatVectorValues values, VectorSimilarityFunction vectorSimilarityFunction, ScalarQuantizer quantizer) {
            this.values = values;
            this.quantizer = quantizer;
            this.quantizedVector = new byte[values.dimension()];
            this.vectorSimilarityFunction = vectorSimilarityFunction;
        }

        @Override
        public float getScoreCorrectionConstant(int ord) {
            if (ord != lastOrd) {
                throw new IllegalStateException(
                    "attempt to retrieve score correction for different ord " + ord + " than the quantization was done for: " + lastOrd
                );
            }
            return offsetValue;
        }

        @Override
        public int dimension() {
            return values.dimension();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            if (ord != lastOrd) {
                offsetValue = quantize(ord);
                lastOrd = ord;
            }
            return quantizedVector;
        }

        @Override
        public VectorScorer scorer(float[] target) throws IOException {
            throw new UnsupportedOperationException();
        }

        private float quantize(int ord) throws IOException {
            return quantizer.quantize(values.vectorValue(ord), quantizedVector, vectorSimilarityFunction);
        }

        @Override
        public int ordToDoc(int ord) {
            return values.ordToDoc(ord);
        }

        @Override
        public DocIndexIterator iterator() {
            return values.iterator();
        }
    }

    private static final class NormalizedFloatVectorValues extends FloatVectorValues {
        private final FloatVectorValues values;
        private final float[] normalizedVector;

        NormalizedFloatVectorValues(FloatVectorValues values) {
            this.values = values;
            this.normalizedVector = new float[values.dimension()];
        }

        @Override
        public int dimension() {
            return values.dimension();
        }

        @Override
        public int size() {
            return values.size();
        }

        @Override
        public int ordToDoc(int ord) {
            return values.ordToDoc(ord);
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            System.arraycopy(values.vectorValue(ord), 0, normalizedVector, 0, normalizedVector.length);
            VectorUtil.l2normalize(normalizedVector);
            return normalizedVector;
        }

        @Override
        public DocIndexIterator iterator() {
            return values.iterator();
        }

        @Override
        public NormalizedFloatVectorValues copy() throws IOException {
            return new NormalizedFloatVectorValues(values.copy());
        }
    }

    private static final class OffsetCorrectedQuantizedByteVectorValues extends QuantizedByteVectorValues {
        private final QuantizedByteVectorValues in;
        private final VectorSimilarityFunction vectorSimilarityFunction;
        private final ScalarQuantizer scalarQuantizer, oldScalarQuantizer;

        OffsetCorrectedQuantizedByteVectorValues(
            QuantizedByteVectorValues in,
            VectorSimilarityFunction vectorSimilarityFunction,
            ScalarQuantizer scalarQuantizer,
            ScalarQuantizer oldScalarQuantizer
        ) {
            this.in = in;
            this.vectorSimilarityFunction = vectorSimilarityFunction;
            this.scalarQuantizer = scalarQuantizer;
            this.oldScalarQuantizer = oldScalarQuantizer;
        }

        @Override
        public float getScoreCorrectionConstant(int ord) throws IOException {
            return scalarQuantizer.recalculateCorrectiveOffset(in.vectorValue(ord), oldScalarQuantizer, vectorSimilarityFunction);
        }

        @Override
        public int dimension() {
            return in.dimension();
        }

        @Override
        public int size() {
            return in.size();
        }

        @Override
        public byte[] vectorValue(int ord) throws IOException {
            return in.vectorValue(ord);
        }

        @Override
        public int ordToDoc(int ord) {
            return in.ordToDoc(ord);
        }

        @Override
        public DocIndexIterator iterator() {
            return in.iterator();
        }
    }
}
