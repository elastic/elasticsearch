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
import org.elasticsearch.index.codec.vectors.reflect.OffHeapStats;
import org.elasticsearch.simdvec.ES91Int4VectorsScorer;
import org.elasticsearch.simdvec.ES91OSQVectorsScorer;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.Map;
import java.util.function.IntPredicate;

import static org.apache.lucene.codecs.lucene102.Lucene102BinaryQuantizedVectorsFormat.QUERY_BITS;
import static org.apache.lucene.index.VectorSimilarityFunction.COSINE;
import static org.apache.lucene.index.VectorSimilarityFunction.EUCLIDEAN;
import static org.apache.lucene.index.VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
import static org.elasticsearch.index.codec.vectors.BQSpaceUtils.transposeHalfByte;
import static org.elasticsearch.index.codec.vectors.BQVectorUtils.discretize;
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
    CentroidQueryScorer getCentroidScorer(FieldInfo fieldInfo, int numParentCentroids,
                                          int numCentroids, IndexInput centroids, float[] targetQuery)
        throws IOException {
        final FieldEntry fieldEntry = fields.get(fieldInfo.number);
        final float globalCentroidDp = fieldEntry.globalCentroidDp();
        final OptimizedScalarQuantizer scalarQuantizer = new OptimizedScalarQuantizer(fieldInfo.getVectorSimilarityFunction());
        final byte[] quantized = new byte[targetQuery.length];
        final OptimizedScalarQuantizer.QuantizationResult queryParams = scalarQuantizer.scalarQuantize(
            ArrayUtil.copyArray(targetQuery),
            quantized,
            (byte) 4,
            fieldEntry.globalCentroid()
        );
        final ES91Int4VectorsScorer scorer = ESVectorUtil.getES91Int4VectorsScorer(centroids, fieldInfo.getVectorDimension());
        return new CentroidQueryScorer() {
            int currentCentroid = -1;
            private final float[] centroid = new float[fieldInfo.getVectorDimension()];
            private final float[] centroidCorrectiveValues = new float[3];
            private final long quantizedVectorByteSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Short.BYTES;
            private final long parentNodeByteSize = quantizedVectorByteSize + 2 * Integer.BYTES;
            private final long quantizedCentroidsOffset = numParentCentroids * parentNodeByteSize;
            private final long rawCentroidsOffset = numParentCentroids * parentNodeByteSize + numCentroids * quantizedVectorByteSize;
            private final long rawCentroidsByteSize = (long) Float.BYTES * fieldInfo.getVectorDimension();

            @Override
            public int size() {
                return numCentroids;
            }

            @Override
            public float[] centroid(int centroidOrdinal) throws IOException {
                if (centroidOrdinal != currentCentroid) {
                    centroids.seek(rawCentroidsOffset + rawCentroidsByteSize * centroidOrdinal);
                    centroids.readFloats(centroid, 0, centroid.length);
                    currentCentroid = centroidOrdinal;
                }
                return centroid;
            }

            public void bulkScore(NeighborQueue queue) throws IOException {
                // TODO: bulk score centroids like we do with posting lists
                centroids.seek(quantizedCentroidsOffset);
                for (int i = 0; i < numCentroids; i++) {
                    queue.add(i, score());
                }
            }

            @Override
            public void bulkScore(NeighborQueue queue, int start, int end) throws IOException {
                // TODO: bulk score centroids like we do with posting lists
                centroids.seek(quantizedCentroidsOffset + quantizedVectorByteSize * start);
                for (int i = start; i < end; i++) {
                    queue.add(i, score());
                }
            }

            private float score() throws IOException {
                final float qcDist = scorer.int4DotProduct(quantized);
                centroids.readFloats(centroidCorrectiveValues, 0, 3);
                final int quantizedCentroidComponentSum = Short.toUnsignedInt(centroids.readShort());
                return int4QuantizedScore(
                    qcDist,
                    queryParams,
                    fieldInfo.getVectorDimension(),
                    centroidCorrectiveValues,
                    quantizedCentroidComponentSum,
                    globalCentroidDp,
                    fieldInfo.getVectorSimilarityFunction()
                );
            }

            // TODO can we do this in off-heap blocks?
            private float int4QuantizedScore(
                float qcDist,
                OptimizedScalarQuantizer.QuantizationResult queryCorrections,
                int dims,
                float[] targetCorrections,
                int targetComponentSum,
                float centroidDp,
                VectorSimilarityFunction similarityFunction
            ) {
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
        };
    }

    // FIXME: clean up duplicative code between the scorers
    @Override
    CentroidQueryScorerWChildren getCentroidScorerWChildren(
        FieldInfo fieldInfo,
        int numCentroids,
        IndexInput centroids,
        float[] targetQuery
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
        final ES91Int4VectorsScorer scorer = ESVectorUtil.getES91Int4VectorsScorer(centroids, fieldInfo.getVectorDimension());
        return new CentroidQueryScorerWChildren() {
            int currentCentroid = -1;
            private final float[] centroidCorrectiveValues = new float[3];
            private final long quantizedVectorByteSize = fieldInfo.getVectorDimension() + 3 * Float.BYTES + Short.BYTES;
            private final long parentNodeByteSize = quantizedVectorByteSize + 2 * Integer.BYTES;

            private int childCentroidStart;
            private int childCount;

            @Override
            public int size() {
                return numCentroids;
            }

            @Override
            public float[] centroid(int centroidOrdinal) throws IOException {
                throw new UnsupportedOperationException("can't score at the parent level");
            }

            private void readQuantizedAndRawCentroid(int centroidOrdinal) throws IOException {
                if (centroidOrdinal == currentCentroid) {
                    return;
                }
                centroids.seek(parentNodeByteSize * centroidOrdinal + quantizedVectorByteSize);
                childCentroidStart = centroids.readInt();
                childCount = centroids.readInt();
                currentCentroid = centroidOrdinal;
            }

            public int getChildCentroidStart(int centroidOrdinal) throws IOException {
                readQuantizedAndRawCentroid(centroidOrdinal);
                return childCentroidStart;
            }

            public int getChildCount(int centroidOrdinal) throws IOException {
                readQuantizedAndRawCentroid(centroidOrdinal);
                return childCount;
            }

            @Override
            public void bulkScore(NeighborQueue queue) throws IOException {
                // TODO: bulk score centroids like we do with posting lists
                centroids.seek(0L);
                for (int i = 0; i < numCentroids; i++) {
                    queue.add(i, score());
                }
            }

            @Override
            public void bulkScore(NeighborQueue queue, int start, int end) throws IOException {
                // FIXME: this never gets used ... I wonder if we just need an entirely different interface for this
                // TODO: bulk score centroids like we do with posting lists
                centroids.seek(parentNodeByteSize * start);
                for (int i = start; i < end; i++) {
                    queue.add(i, score());
                }
            }

            private float score() throws IOException {
                final float qcDist = scorer.int4DotProduct(quantized);
                centroids.readFloats(centroidCorrectiveValues, 0, 3);
                final int quantizedCentroidComponentSum = Short.toUnsignedInt(centroids.readShort());

                // FIXME: move these now? to a different place in the file?
                // TODO: cache these at this point when scoring since we'll likely read many of them?
                centroids.readInt(); // child partition start
                centroids.readInt(); // child partition count

                return int4QuantizedScore(
                    qcDist,
                    queryParams,
                    fieldInfo.getVectorDimension(),
                    centroidCorrectiveValues,
                    quantizedCentroidComponentSum,
                    globalCentroidDp,
                    fieldInfo.getVectorSimilarityFunction()
                );
            }

            // TODO can we do this in off-heap blocks?
            private float int4QuantizedScore(
                float qcDist,
                OptimizedScalarQuantizer.QuantizationResult queryCorrections,
                int dims,
                float[] targetCorrections,
                int targetComponentSum,
                float centroidDp,
                VectorSimilarityFunction similarityFunction
            ) {
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
        };
    }

    @Override
    NeighborQueue scorePostingLists(FieldInfo fieldInfo, KnnCollector knnCollector, CentroidQueryScorer centroidQueryScorer, int nProbe)
        throws IOException {
        NeighborQueue neighborQueue = new NeighborQueue(centroidQueryScorer.size(), true);
        centroidQueryScorer.bulkScore(neighborQueue);
        return neighborQueue;
    }

    @Override
    PostingVisitor getPostingVisitor(FieldInfo fieldInfo, IndexInput indexInput, float[] target, IntPredicate needsScoring)
        throws IOException {
        FieldEntry entry = fields.get(fieldInfo.number);
        return new MemorySegmentPostingsVisitor(target, indexInput.clone(), entry, fieldInfo, needsScoring);
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
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
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
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
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
                        queryCorrections.lowerInterval(),
                        queryCorrections.upperInterval(),
                        queryCorrections.quantizedComponentSum(),
                        queryCorrections.additionalCorrection(),
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

}
