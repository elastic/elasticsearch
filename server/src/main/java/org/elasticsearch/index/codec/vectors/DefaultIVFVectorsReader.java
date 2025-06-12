/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors;

import org.apache.lucene.codecs.hnsw.FlatVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.SegmentReadState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.KnnCollector;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.VectorUtil;
import org.apache.lucene.util.hnsw.NeighborQueue;
import org.apache.lucene.util.quantization.OptimizedScalarQuantizer;
import org.elasticsearch.index.codec.vectors.reflect.OffHeapStats;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.discretize;
import static org.apache.lucene.util.quantization.OptimizedScalarQuantizer.transposeHalfByte;
import static org.elasticsearch.simdvec.ES91OSQVectorsScorer.BULK_SIZE;

/**
 * Default implementation of {@link IVFVectorsReader}. It scores the posting lists centroids using
 * brute force and then scores the top ones using the posting list.
 */
public class DefaultIVFVectorsReader extends IVFVectorsReader implements OffHeapStats {
    private static final float FOUR_BIT_SCALE = 1f / ((1 << 4) - 1);

    public DefaultIVFVectorsReader(SegmentReadState state, FlatVectorsReader rawVectorsReader) throws IOException {
        super(state, rawVectorsReader);
    }

    @Override
    CentroidQueryScorer getCentroidScorer(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        float[] targetQuery,
        IndexInput clusters
    ) throws IOException {
        FieldEntry fieldEntry = fields.get(fieldInfo.number);
        float[] globalCentroid = fieldEntry.globalCentroid();
        float globalCentroidDp = fieldEntry.globalCentroidDp();
        OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        byte[] quantized = new byte[targetQuery.length];
        float[] targetScratch = ArrayUtil.copyArray(targetQuery);
        OptimizedScalarQuantizer.QuantizationResult queryParams = scalarQuantizer.scalarQuantize(
            targetScratch,
            quantized,
            (byte) 4,
            globalCentroid
        );
        return new CentroidQueryScorer() {
            int currentCentroid = -1;
            private final byte[] quantizedCentroid = new byte[fieldInfo.getVectorDimension()];
            private final float[] centroid = new float[fieldInfo.getVectorDimension()];
            private final float[] centroidCorrectiveValues = new float[3];
            private int quantizedCentroidComponentSum;
            private final long centroidByteSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Short.BYTES;

            @Override
            public int size() {
                return numCentroids;
            }

            @Override
            public float[] centroid(int centroidOrdinal) throws IOException {
                readQuantizedAndRawCentroid(centroidOrdinal);
                return centroid;
            }

            private void readQuantizedAndRawCentroid(int centroidOrdinal) throws IOException {
                if (centroidOrdinal == currentCentroid) {
                    return;
                }
                centroids.seek(centroidOrdinal * centroidByteSize);
                quantizedCentroidComponentSum = readQuantizedValue(centroids, quantizedCentroid, centroidCorrectiveValues);
                centroids.seek(numCentroids * centroidByteSize + (long) Float.BYTES * quantizedCentroid.length * centroidOrdinal);
                centroids.readFloats(centroid, 0, centroid.length);
                currentCentroid = centroidOrdinal;
            }

            @Override
            public float score(int centroidOrdinal) throws IOException {
                readQuantizedAndRawCentroid(centroidOrdinal);
                return int4QuantizedScore(
                    quantized,
                    queryParams,
                    fieldInfo.getVectorDimension(),
                    quantizedCentroid,
                    centroidCorrectiveValues,
                    quantizedCentroidComponentSum,
                    globalCentroidDp,
                    fieldInfo.getVectorSimilarityFunction()
                );
            }
        };
    }

    @Override
    NeighborQueue scorePostingLists(FieldInfo fieldInfo, KnnCollector knnCollector, CentroidQueryScorer centroidQueryScorer, int nProbe)
        throws IOException {
        NeighborQueue neighborQueue = new NeighborQueue(centroidQueryScorer.size(), true);
        // TODO Off heap scoring for quantized centroids?
        for (int centroid = 0; centroid < centroidQueryScorer.size(); centroid++) {
            neighborQueue.add(centroid, centroidQueryScorer.score(centroid));
        }
        return neighborQueue;
    }

    @Override
    PostingVisitor getPostingVisitor(FieldInfo fieldInfo, IndexInput indexInput, float[] target, IntPredicate needsScoring)
        throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        return new MemorySegmentPostingsVisitor(target, indexInput, entry, fieldInfo, needsScoring);
    }

    // TODO can we do this in off-heap blocks?
    static float int4QuantizedScore(
        byte[] quantizedQuery,
        OptimizedScalarQuantizer.QuantizationResult queryCorrections,
        int dims,
        byte[] binaryCode,
        float[] targetCorrections,
        int targetComponentSum,
        float centroidDp,
        VectorSimilarityFunction similarityFunction
    ) {
        float qcDist = VectorUtil.int4DotProduct(quantizedQuery, binaryCode);
        float ax = targetCorrections[0];
        // Here we assume `lx` is simply bit vectors, so the scaling isn't necessary
        float lx = (targetCorrections[1] - ax) * FOUR_BIT_SCALE;
        float ay = queryCorrections.lowerInterval();
        float ly = (queryCorrections.upperInterval() - ay) * FOUR_BIT_SCALE;
        float y1 = queryCorrections.quantizedComponentSum();
        float score = ax * ay * dims + ay * lx * (float) targetComponentSum + ax * ly * y1 + lx * ly * qcDist;
        if (similarityFunction == EUCLIDEAN) {
            score = queryCorrections.additionalCorrection() + targetCorrections[2] - 2 * score;
            return Math.max(1 / (1f + score), 0);
        } else {
            // For cosine and max inner product, we need to apply the additional correction, which is
            // assumed to be the non-centered dot-product between the vector and the centroid
            score += queryCorrections.additionalCorrection() + targetCorrections[2] - centroidDp;
            if (similarityFunction == MAXIMUM_INNER_PRODUCT) {
                return VectorUtil.scaleMaxInnerProductScore(score);
            }
            return Math.max((1f + score) / 2f, 0);
        }
    }

    @Override
    public Map<String, Long> getOffHeapByteSize(FieldInfo fieldInfo) {
        return Map.of();
    }

    private static class MemorySegmentPostingsVisitor implements PostingVisitor {
        final long quantizedByteLength;
        final IndexInput indexInput;
        final float[] target;
        final FieldEntry entry;
        final FieldInfo fieldInfo;
        final IntPredicate needsScoring;
        private final ES91OSQVectorsScorer osqVectorsScorer;
        final float[] scores = new float[BULK_SIZE];
        final float[] correctionsLower = new float[BULK_SIZE];
        final float[] correctionsUpper = new float[BULK_SIZE];
        final int[] correctionsSum = new int[BULK_SIZE];
        final float[] correctionsAdd = new float[BULK_SIZE];

        int[] docIdsScratch = new int[0];
        int vectors;
        boolean quantized = false;
        float centroidDp;
        float[] centroid;
        long slicePos;
        OptimizedScalarQuantizer.QuantizationResult queryCorrections;
        DocIdsWriter docIdsWriter = new DocIdsWriter();

        final float[] scratch;
        final byte[] quantizationScratch;
        final byte[] quantizedQueryScratch;
        final OptimizedScalarQuantizer quantizer;
        final float[] correctiveValues = new float[3];
        final long quantizedVectorByteSize;

        MemorySegmentPostingsVisitor(
            float[] target,
            IndexInput indexInput,
            FieldEntry entry,
            FieldInfo fieldInfo,
            IntPredicate needsScoring
        ) throws IOException {
            this.target = target;
            this.indexInput = indexInput;
            this.entry = entry;
            this.fieldInfo = fieldInfo;
            this.needsScoring = needsScoring;

            scratch = new float[target.length];
            quantizationScratch = new byte[target.length];
            final int discretizedDimensions = discretize(fieldInfo.getVectorDimension(), 64);
            quantizedQueryScratch = new byte[QUERY_BITS * discretizedDimensions / 8];
            quantizedByteLength = discretizedDimensions / 8 + (Float.BYTES * 3) + Short.BYTES;
            quantizedVectorByteSize = (discretizedDimensions / 8);
            quantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
            osqVectorsScorer = ESVectorUtil.getES91OSQVectorsScorer(indexInput, fieldInfo.getVectorDimension());
        }

        @Override
        public int resetPostingsScorer(int centroidOrdinal, float[] centroid) throws IOException {
            quantized = false;
            indexInput.seek(entry.postingListOffsets()[centroidOrdinal]);
            vectors = indexInput.readVInt();
            centroidDp = Float.intBitsToFloat(indexInput.readInt());
            this.centroid = centroid;
            // read the doc ids
            docIdsScratch = vectors > docIdsScratch.length ? new int[vectors] : docIdsScratch;
            docIdsWriter.readInts(indexInput, vectors, docIdsScratch);
            slicePos = indexInput.getFilePointer();
            return vectors;
        }

        void scoreIndividually(int offset) throws IOException {
            // score individually, first the quantized byte chunk
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[j + offset];
                if (doc != -1) {
                    indexInput.seek(slicePos + (offset * quantizedByteLength) + (j * quantizedVectorByteSize));
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    scores[j] = qcDist;
                }
            }
            // read in all corrections
            indexInput.seek(slicePos + (offset * quantizedByteLength) + (BULK_SIZE * quantizedVectorByteSize));
            indexInput.readFloats(correctionsLower, 0, BULK_SIZE);
            indexInput.readFloats(correctionsUpper, 0, BULK_SIZE);
            for (int j = 0; j < BULK_SIZE; j++) {
                correctionsSum[j] = Short.toUnsignedInt(indexInput.readShort());
            }
            indexInput.readFloats(correctionsAdd, 0, BULK_SIZE);
            // Now apply corrections
            for (int j = 0; j < BULK_SIZE; j++) {
                int doc = docIdsScratch[offset + j];
                if (doc != -1) {
                    scores[j] = osqVectorsScorer.score(
                        queryCorrections,
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        correctionsLower[j],
                        correctionsUpper[j],
                        correctionsSum[j],
                        correctionsAdd[j],
                        scores[j]
                    );
                }
            }
        }

        @Override
        public int visit(KnnCollector knnCollector) throws IOException {
            // block processing
            int scoredDocs = 0;
            int limit = vectors - BULK_SIZE + 1;
            int i = 0;
            for (; i < limit; i += BULK_SIZE) {
                int docsToScore = BULK_SIZE;
                for (int j = 0; j < BULK_SIZE; j++) {
                    int doc = docIdsScratch[i + j];
                    if (needsScoring.test(doc) == false) {
                        docIdsScratch[i + j] = -1;
                        docsToScore--;
                    }
                }
                if (docsToScore == 0) {
                    continue;
                }
                quantizeQueryIfNecessary();
                indexInput.seek(slicePos + i * quantizedByteLength);
                if (docsToScore < BULK_SIZE / 2) {
                    scoreIndividually(i);
                } else {
                    osqVectorsScorer.scoreBulk(
                        quantizedQueryScratch,
                        queryCorrections,
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        scores
                    );
                }
                for (int j = 0; j < BULK_SIZE; j++) {
                    int doc = docIdsScratch[i + j];
                    if (doc != -1) {
                        scoredDocs++;
                        knnCollector.collect(doc, scores[j]);
                    }
                }
            }
            // process tail
            for (; i < vectors; i++) {
                int doc = docIdsScratch[i];
                if (needsScoring.test(doc)) {
                    quantizeQueryIfNecessary();
                    indexInput.seek(slicePos + i * quantizedByteLength);
                    float qcDist = osqVectorsScorer.quantizeScore(quantizedQueryScratch);
                    indexInput.readFloats(correctiveValues, 0, 3);
                    final int quantizedComponentSum = Short.toUnsignedInt(indexInput.readShort());
                    float score = osqVectorsScorer.score(
                        queryCorrections,
                        fieldInfo.getVectorSimilarityFunction(),
                        centroidDp,
                        correctiveValues[0],
                        correctiveValues[1],
                        quantizedComponentSum,
                        correctiveValues[2],
                        qcDist
                    );
                    scoredDocs++;
                    knnCollector.collect(doc, score);
                }
            }
            if (scoredDocs > 0) {
                knnCollector.incVisitedCount(scoredDocs);
            }
            return scoredDocs;
        }

        private void quantizeQueryIfNecessary() {
            if (quantized == false) {
                System.arraycopy(target, 0, scratch, 0, target.length);
                if (fieldInfo.getVectorSimilarityFunction() == COSINE) {
                    VectorUtil.l2normalize(scratch);
                }
                queryCorrections = quantizer.scalarQuantize(scratch, quantizationScratch, (byte) 4, centroid);
                transposeHalfByte(quantizationScratch, quantizedQueryScratch);
                quantized = true;
            }
        }
    }

    static int readQuantizedValue(IndexInput indexInput, byte[] binaryValue, float[] corrections) throws IOException {
        assert corrections.length == 3;
        indexInput.readBytes(binaryValue, 0, binaryValue.length);
        corrections[0] = Float.intBitsToFloat(indexInput.readInt());
        corrections[1] = Float.intBitsToFloat(indexInput.readInt());
        corrections[2] = Float.intBitsToFloat(indexInput.readInt());
        return Short.toUnsignedInt(indexInput.readShort());
    }
}
