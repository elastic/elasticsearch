/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.StoredFields;
import org.elasticsearch.index.mapper.IdFieldMapper;
import org.elasticsearch.index.mapper.Uid;
import org.elasticsearch.index.mapper.vectors.DenseVectorFieldMapper;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Random;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Shard-level aggregator for outlier detection on dense_vector fields.
 * <p>
 * Strategy:
 * 1. Reservoir-sample vectors during collection
 * 2. After collection, apply random projection for dimensionality reduction
 * 3. Compute k-th nearest neighbor distance for each sampled vector
 * 4. Return top candidates (overfetched) as the shard result
 */
public class OutlierDetectionAggregator extends MetricsAggregator {

    private final String field;
    private final DenseVectorFieldMapper.DenseVectorFieldType fieldType;
    private final int dims;
    private final int topN;
    private final int nNeighbors;
    private final Integer sampleSize;
    private final int projectionDim;
    private final long seed;
    private final int overfetchFactor;
    private final OutlierDetectionMethod method;
    private final int maxCoordSample;
    private final ScoreNormalization normalize;

    private final List<CollectedVector> collectedVectors = new ArrayList<>();
    private final Random reservoirRng;
    private int totalDocsCollected = 0;

    /**
     * Stores a collected vector with enough information to resolve its _id later.
     */
    private record CollectedVector(float[] vector, int leafOrd, int docId) {}

    private final List<LeafReaderContext> leafContexts = new ArrayList<>();

    public OutlierDetectionAggregator(
        String name,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata,
        String field,
        DenseVectorFieldMapper.DenseVectorFieldType fieldType,
        int dims,
        int topN,
        int nNeighbors,
        Integer sampleSize,
        int projectionDim,
        long seed,
        int overfetchFactor,
        OutlierDetectionMethod method,
        int maxCoordSample,
        ScoreNormalization normalize
    ) throws IOException {
        super(name, context, parent, metadata);
        this.field = field;
        this.fieldType = fieldType;
        this.dims = dims;
        this.topN = topN;
        this.nNeighbors = nNeighbors;
        this.sampleSize = sampleSize;
        this.projectionDim = projectionDim;
        this.seed = seed;
        this.overfetchFactor = overfetchFactor;
        this.method = method;
        this.maxCoordSample = maxCoordSample;
        this.normalize = normalize;
        this.reservoirRng = new Random(seed);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, LeafBucketCollector sub) throws IOException {
        LeafReaderContext leafCtx = aggCtx.getLeafReaderContext();
        int leafOrd = leafContexts.size();
        leafContexts.add(leafCtx);

        FloatVectorValues floatVectorValues = leafCtx.reader().getFloatVectorValues(field);
        KnnVectorValues.DocIndexIterator iterator = floatVectorValues != null ? floatVectorValues.iterator() : null;

        return new LeafBucketCollector() {
            @Override
            public void collect(int docId, long bucket) throws IOException {
                float[] vector = readVector(docId);
                if (vector == null) {
                    return;
                }

                int effectiveSampleSize = getEffectiveSampleSize();
                totalDocsCollected++;

                if (collectedVectors.size() < effectiveSampleSize) {
                    collectedVectors.add(new CollectedVector(Arrays.copyOf(vector, vector.length), leafOrd, docId));
                } else {
                    int r = reservoirRng.nextInt(totalDocsCollected);
                    if (r < effectiveSampleSize) {
                        collectedVectors.set(r, new CollectedVector(Arrays.copyOf(vector, vector.length), leafOrd, docId));
                    }
                }
            }

            private float[] readVector(int docId) throws IOException {
                if (floatVectorValues == null || iterator == null) {
                    return null;
                }
                int currentDoc = iterator.docID();
                if (currentDoc == NO_MORE_DOCS) {
                    return null;
                }
                if (docId > currentDoc) {
                    currentDoc = iterator.advance(docId);
                }
                if (currentDoc != docId) {
                    return null;
                }
                return floatVectorValues.vectorValue(iterator.index());
            }
        };
    }

    private int getEffectiveSampleSize() {
        if (sampleSize != null) {
            return sampleSize;
        }
        return Math.max(nNeighbors + 1, (int) Math.ceil(Math.sqrt(Math.max(totalDocsCollected, 100))));
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        if (collectedVectors.isEmpty()) {
            return buildEmptyAggregation();
        }

        int n = collectedVectors.size();

        // Apply random projection
        RandomProjection projection = (projectionDim < dims)
            ? new RandomProjection(dims, projectionDim, seed)
            : null;

        float[][] projectedVectors = new float[n][];
        for (int i = 0; i < n; i++) {
            float[] vec = collectedVectors.get(i).vector();
            projectedVectors[i] = projection != null ? projection.project(vec) : Arrays.copyOf(vec, vec.length);
        }

        // Compute outlier scores for each vector in the sample
        double[] scores = computeScores(projectedVectors, nNeighbors, method);

        // Select top candidates
        int candidateCount = Math.min(n, topN * overfetchFactor);
        int[] topIndices = selectTopN(scores, candidateCount);

        // Resolve doc IDs to _id strings
        List<OutlierCandidate> candidates = new ArrayList<>(candidateCount);
        for (int idx : topIndices) {
            CollectedVector cv = collectedVectors.get(idx);
            String docIdStr = resolveDocId(cv.leafOrd(), cv.docId());
            candidates.add(new OutlierCandidate(docIdStr, projectedVectors[idx], scores[idx], 0));
        }

        // Build sample summary: truncate to maxCoordSample to limit coord-node payload
        float[][] sampleSummary;
        if (projectedVectors.length > maxCoordSample) {
            sampleSummary = Arrays.copyOf(projectedVectors, maxCoordSample);
        } else {
            sampleSummary = projectedVectors;
        }

        return new InternalOutlierDetection(
            name,
            metadata(),
            candidates,
            sampleSummary,
            n,
            topN,
            nNeighbors,
            seed,
            method,
            normalize
        );
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalOutlierDetection(
            name,
            metadata(),
            List.of(),
            new float[0][],
            0,
            topN,
            nNeighbors,
            seed,
            method,
            normalize
        );
    }

    /**
     * Compute kNN distance (distance to k-th nearest neighbor) for each vector.
     * Uses squared Euclidean distance.
     */
    static double[] computeKnnDistances(float[][] vectors, int k) {
        int n = vectors.length;
        double[] scores = new double[n];
        int effectiveK = Math.min(k, n - 1);
        if (effectiveK <= 0) {
            Arrays.fill(scores, 0.0);
            return scores;
        }

        for (int i = 0; i < n; i++) {
            // Min-heap of size k to track the k largest distances (i.e., k nearest neighbors)
            PriorityQueue<Double> kNearest = new PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b, a));
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                double dist = squaredEuclidean(vectors[i], vectors[j]);
                if (kNearest.size() < effectiveK) {
                    kNearest.add(dist);
                } else if (dist < kNearest.peek()) {
                    kNearest.poll();
                    kNearest.add(dist);
                }
            }
            // The k-th nearest neighbor distance is the maximum in our min-heap
            scores[i] = kNearest.isEmpty() ? 0.0 : kNearest.peek();
        }
        return scores;
    }

    static double squaredEuclidean(float[] a, float[] b) {
        double sum = 0.0;
        for (int i = 0; i < a.length; i++) {
            double diff = a[i] - b[i];
            sum += diff * diff;
        }
        return sum;
    }

    /**
     * Dispatch scoring to the appropriate method.
     */
    static double[] computeScores(float[][] vectors, int k, OutlierDetectionMethod method) {
        return switch (method) {
            case KTH_NN -> computeKnnDistances(vectors, k);
            case TNN -> computeTnnScores(vectors, k);
            case LDOF -> computeLdofScores(vectors, k);
            case LOF -> computeLofScores(vectors, k);
        };
    }

    /**
     * Compute TNN (average distance to k nearest neighbors) for each vector.
     */
    static double[] computeTnnScores(float[][] vectors, int k) {
        int n = vectors.length;
        double[] scores = new double[n];
        int effectiveK = Math.min(k, n - 1);
        if (effectiveK <= 0) {
            return scores;
        }

        for (int i = 0; i < n; i++) {
            PriorityQueue<Double> kNearest = new PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b, a));
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                double dist = squaredEuclidean(vectors[i], vectors[j]);
                if (kNearest.size() < effectiveK) {
                    kNearest.add(dist);
                } else if (dist < kNearest.peek()) {
                    kNearest.poll();
                    kNearest.add(dist);
                }
            }
            double sum = 0;
            for (double d : kNearest) {
                sum += d;
            }
            scores[i] = kNearest.isEmpty() ? 0.0 : sum / kNearest.size();
        }
        return scores;
    }

    /**
     * Helper record storing k nearest neighbor indices and distances for each vector.
     */
    record KnnResult(int[][] neighborIndices, double[][] neighborDistances) {}

    /**
     * Compute k nearest neighbor indices and distances for every vector.
     * Distances are sorted ascending in each row.
     */
    static KnnResult computeKnn(float[][] vectors, int k) {
        int n = vectors.length;
        int effectiveK = Math.min(k, n - 1);
        int[][] indices = new int[n][Math.max(effectiveK, 0)];
        double[][] distances = new double[n][Math.max(effectiveK, 0)];
        if (effectiveK <= 0) {
            return new KnnResult(indices, distances);
        }

        for (int i = 0; i < n; i++) {
            // Max-heap of (distance, index), keeping k smallest
            PriorityQueue<double[]> heap = new PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b[0], a[0]));
            for (int j = 0; j < n; j++) {
                if (i == j) continue;
                double dist = squaredEuclidean(vectors[i], vectors[j]);
                if (heap.size() < effectiveK) {
                    heap.add(new double[] { dist, j });
                } else if (dist < heap.peek()[0]) {
                    heap.poll();
                    heap.add(new double[] { dist, j });
                }
            }
            int idx = heap.size() - 1;
            while (heap.isEmpty() == false) {
                double[] entry = heap.poll();
                if (idx >= 0) {
                    distances[i][idx] = entry[0];
                    indices[i][idx] = (int) entry[1];
                }
                idx--;
            }
        }
        return new KnnResult(indices, distances);
    }

    /**
     * Compute LDoF (Local Distance-based Outlier Factor) for each vector.
     * LDoF(x) = TNN(x) / avg(TNN(neighbor) for each neighbor of x).
     */
    static double[] computeLdofScores(float[][] vectors, int k) {
        int n = vectors.length;
        double[] scores = new double[n];
        if (n <= 1) {
            return scores;
        }

        KnnResult knn = computeKnn(vectors, k);

        // Compute TNN for each point
        double[] tnn = new double[n];
        for (int i = 0; i < n; i++) {
            double sum = 0;
            for (double d : knn.neighborDistances[i]) {
                sum += d;
            }
            tnn[i] = knn.neighborDistances[i].length > 0 ? sum / knn.neighborDistances[i].length : 0;
        }

        // LDoF = TNN(x) / avg TNN of x's neighbors
        for (int i = 0; i < n; i++) {
            double innerSum = 0;
            int count = 0;
            for (int ni : knn.neighborIndices[i]) {
                innerSum += tnn[ni];
                count++;
            }
            double innerAvg = count > 0 ? innerSum / count : 1.0;
            scores[i] = innerAvg > 0 ? tnn[i] / innerAvg : 0;
        }
        return scores;
    }

    /**
     * Compute LoF (Local Outlier Factor) for each vector.
     * Uses reachability distance and local reachable density.
     */
    static double[] computeLofScores(float[][] vectors, int k) {
        int n = vectors.length;
        double[] scores = new double[n];
        int effectiveK = Math.min(k, n - 1);
        if (effectiveK <= 0) {
            return scores;
        }

        KnnResult knn = computeKnn(vectors, k);

        // k-distance for each point (distance to k-th nearest neighbor = max in neighborhood)
        double[] kDist = new double[n];
        for (int i = 0; i < n; i++) {
            double max = 0;
            for (double d : knn.neighborDistances[i]) {
                max = Math.max(max, d);
            }
            kDist[i] = max;
        }

        // Local reachable density for each point
        double[] lrd = new double[n];
        for (int i = 0; i < n; i++) {
            double reachSum = 0;
            int count = 0;
            for (int ki = 0; ki < knn.neighborIndices[i].length; ki++) {
                int ni = knn.neighborIndices[i][ki];
                double dist = knn.neighborDistances[i][ki];
                double reachDist = Math.max(dist, kDist[ni]);
                reachSum += reachDist;
                count++;
            }
            lrd[i] = (count > 0 && reachSum > 0) ? (double) count / reachSum : 0;
        }

        // LoF for each point
        for (int i = 0; i < n; i++) {
            if (lrd[i] == 0) {
                scores[i] = 0;
                continue;
            }
            double neighborLrdSum = 0;
            int count = 0;
            for (int ni : knn.neighborIndices[i]) {
                neighborLrdSum += lrd[ni];
                count++;
            }
            scores[i] = count > 0 ? (neighborLrdSum / count) / lrd[i] : 0;
        }
        return scores;
    }

    /**
     * Select the indices of the top N scores (highest first).
     */
    private static int[] selectTopN(double[] scores, int n) {
        n = Math.min(n, scores.length);
        // Min-heap storing (score, index) — we keep the N largest
        PriorityQueue<int[]> heap = new PriorityQueue<>(n, (a, b) -> Double.compare(scores[a[0]], scores[b[0]]));
        for (int i = 0; i < scores.length; i++) {
            if (heap.size() < n) {
                heap.add(new int[] { i });
            } else if (scores[i] > scores[heap.peek()[0]]) {
                heap.poll();
                heap.add(new int[] { i });
            }
        }
        int[] result = new int[heap.size()];
        int idx = heap.size() - 1;
        while (heap.isEmpty() == false) {
            result[idx--] = heap.poll()[0];
        }
        return result;
    }

    private String resolveDocId(int leafOrd, int docId) throws IOException {
        LeafReaderContext leafCtx = leafContexts.get(leafOrd);
        StoredFields storedFields = leafCtx.reader().storedFields();
        var doc = storedFields.document(docId);
        var idField = doc.getBinaryValue(IdFieldMapper.NAME);
        if (idField != null) {
            return Uid.decodeId(idField.bytes, idField.offset, idField.length);
        }
        return String.valueOf(leafCtx.docBase + docId);
    }
}
