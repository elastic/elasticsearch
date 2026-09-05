/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.outlierdetection;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorReducer;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Internal (shard-level) result of the outlier detection aggregation.
 * Carries outlier candidates and the projected sample vectors for cross-shard re-scoring.
 */
public class InternalOutlierDetection extends InternalAggregation {

    private final List<OutlierCandidate> candidates;
    private final float[][] sampleVectors;
    private final int sampleCount;
    private final int topN;
    private final int nNeighbors;
    private final long seed;
    private final OutlierDetectionMethod method;
    private final ScoreNormalization normalize;

    public InternalOutlierDetection(
        String name,
        Map<String, Object> metadata,
        List<OutlierCandidate> candidates,
        float[][] sampleVectors,
        int sampleCount,
        int topN,
        int nNeighbors,
        long seed,
        OutlierDetectionMethod method,
        ScoreNormalization normalize
    ) {
        super(name, metadata);
        this.candidates = candidates;
        this.sampleVectors = sampleVectors;
        this.sampleCount = sampleCount;
        this.topN = topN;
        this.nNeighbors = nNeighbors;
        this.seed = seed;
        this.method = method;
        this.normalize = normalize;
    }

    public InternalOutlierDetection(StreamInput in) throws IOException {
        super(in);
        int numCandidates = in.readVInt();
        this.candidates = new ArrayList<>(numCandidates);
        for (int i = 0; i < numCandidates; i++) {
            this.candidates.add(new OutlierCandidate(in));
        }
        int numSamples = in.readVInt();
        this.sampleVectors = new float[numSamples][];
        for (int i = 0; i < numSamples; i++) {
            this.sampleVectors[i] = in.readFloatArray();
        }
        this.sampleCount = in.readVInt();
        this.topN = in.readVInt();
        this.nNeighbors = in.readVInt();
        this.seed = in.readLong();
        this.method = in.readEnum(OutlierDetectionMethod.class);
        this.normalize = in.readEnum(ScoreNormalization.class);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVInt(candidates.size());
        for (OutlierCandidate candidate : candidates) {
            candidate.writeTo(out);
        }
        out.writeVInt(sampleVectors.length);
        for (float[] vec : sampleVectors) {
            out.writeFloatArray(vec);
        }
        out.writeVInt(sampleCount);
        out.writeVInt(topN);
        out.writeVInt(nNeighbors);
        out.writeLong(seed);
        out.writeEnum(method);
        out.writeEnum(normalize);
    }

    @Override
    public String getWriteableName() {
        return OutlierDetectionAggregationBuilder.NAME;
    }

    @Override
    protected AggregatorReducer getLeaderReducer(AggregationReduceContext reduceContext, int size) {
        return new AggregatorReducer() {
            final List<InternalOutlierDetection> shardResults = new ArrayList<>(size);

            @Override
            public void accept(InternalAggregation aggregation) {
                shardResults.add((InternalOutlierDetection) aggregation);
            }

            @Override
            public InternalAggregation get() {
                if (shardResults.isEmpty()) {
                    return InternalOutlierDetection.this;
                }

                // Single shard — no need to re-score
                if (shardResults.size() == 1) {
                    InternalOutlierDetection single = shardResults.get(0);
                    if (reduceContext.isFinalReduce()) {
                        return finalTrim(single);
                    }
                    return single;
                }

                // Merge sample vectors from all shards
                List<float[]> mergedSample = new ArrayList<>();
                List<OutlierCandidate> allCandidates = new ArrayList<>();
                int totalSampleCount = 0;

                for (InternalOutlierDetection shardResult : shardResults) {
                    Collections.addAll(mergedSample, shardResult.sampleVectors);
                    allCandidates.addAll(shardResult.candidates);
                    totalSampleCount += shardResult.sampleCount;
                }

                if (allCandidates.isEmpty()) {
                    return new InternalOutlierDetection(
                        getName(),
                        getMetadata(),
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

                float[][] mergedSampleArray = mergedSample.toArray(new float[0][]);

                // Precompute sample-level data for methods that need it
                double[] sampleTnn = null;
                double[] sampleKDist = null;
                double[] sampleLrd = null;
                if (method == OutlierDetectionMethod.LDOF) {
                    sampleTnn = precomputeSampleTnn(mergedSampleArray, nNeighbors);
                } else if (method == OutlierDetectionMethod.LOF) {
                    sampleKDist = precomputeSampleKDist(mergedSampleArray, nNeighbors);
                    sampleLrd = precomputeSampleLrd(mergedSampleArray, nNeighbors, sampleKDist);
                }

                // Re-score each candidate against the merged sample
                List<OutlierCandidate> rescoredCandidates = new ArrayList<>(allCandidates.size());
                for (OutlierCandidate candidate : allCandidates) {
                    double newScore = reScoreCandidate(
                        candidate.getProjectedVector(),
                        mergedSampleArray,
                        nNeighbors,
                        method,
                        sampleTnn,
                        sampleKDist,
                        sampleLrd
                    );
                    rescoredCandidates.add(
                        new OutlierCandidate(candidate.getDocId(), candidate.getProjectedVector(), newScore, candidate.getShardIndex())
                    );
                }

                // Sort descending by score
                rescoredCandidates.sort(null);

                if (reduceContext.isFinalReduce()) {
                    int limit = Math.min(topN, rescoredCandidates.size());
                    List<OutlierCandidate> finalCandidates = new ArrayList<>(rescoredCandidates.subList(0, limit));
                    finalCandidates = applyNormalization(finalCandidates);
                    return new InternalOutlierDetection(
                        getName(),
                        getMetadata(),
                        finalCandidates,
                        mergedSampleArray,
                        totalSampleCount,
                        topN,
                        nNeighbors,
                        seed,
                        method,
                        normalize
                    );
                } else {
                    return new InternalOutlierDetection(
                        getName(),
                        getMetadata(),
                        rescoredCandidates,
                        mergedSampleArray,
                        totalSampleCount,
                        topN,
                        nNeighbors,
                        seed,
                        method,
                        normalize
                    );
                }
            }
        };
    }

    private InternalOutlierDetection finalTrim(InternalOutlierDetection result) {
        if (result.candidates.size() <= topN) {
            return result;
        }
        List<OutlierCandidate> sorted = new ArrayList<>(result.candidates);
        sorted.sort(null);
        List<OutlierCandidate> trimmed = new ArrayList<>(sorted.subList(0, topN));
        return new InternalOutlierDetection(
            getName(),
            getMetadata(),
            trimmed,
            result.sampleVectors,
            result.sampleCount,
            topN,
            nNeighbors,
            seed,
            method,
            normalize
        );
    }

    /**
     * Apply score normalisation to the final candidate list.
     * Extracts scores, normalises in-place, then rebuilds candidates with new scores.
     */
    private List<OutlierCandidate> applyNormalization(List<OutlierCandidate> candidates) {
        if (normalize == ScoreNormalization.NONE || candidates.isEmpty()) {
            return candidates;
        }
        double[] scores = new double[candidates.size()];
        for (int i = 0; i < candidates.size(); i++) {
            scores[i] = candidates.get(i).getScore();
        }
        normalize.apply(scores);
        List<OutlierCandidate> normalized = new ArrayList<>(candidates.size());
        for (int i = 0; i < candidates.size(); i++) {
            OutlierCandidate c = candidates.get(i);
            normalized.add(new OutlierCandidate(c.getDocId(), c.getProjectedVector(), scores[i], c.getShardIndex()));
        }
        // Re-sort after normalisation (order may not change for min-max, but z-score could flip signs)
        normalized.sort(null);
        return normalized;
    }

    /**
     * Compute the distance to the k-th nearest neighbor in the sample for a given point.
     */
    static double computeKthNnDistance(float[] point, float[][] sample, int k) {
        int effectiveK = Math.min(k, sample.length);
        if (effectiveK <= 0) {
            return 0.0;
        }
        // Max-heap of the k smallest distances
        java.util.PriorityQueue<Double> kNearest = new java.util.PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b, a));
        for (float[] sampleVec : sample) {
            double dist = OutlierDetectionAggregator.squaredEuclidean(point, sampleVec);
            if (dist == 0.0) {
                continue; // skip self-match
            }
            if (kNearest.size() < effectiveK) {
                kNearest.add(dist);
            } else if (dist < kNearest.peek()) {
                kNearest.poll();
                kNearest.add(dist);
            }
        }
        return kNearest.isEmpty() ? 0.0 : kNearest.peek();
    }

    /**
     * Compute TNN (average distance to k nearest neighbors) for a single point against a sample.
     */
    static double computeTnnScore(float[] point, float[][] sample, int k) {
        int effectiveK = Math.min(k, sample.length);
        if (effectiveK <= 0) {
            return 0.0;
        }
        java.util.PriorityQueue<Double> kNearest = new java.util.PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b, a));
        for (float[] sampleVec : sample) {
            double dist = OutlierDetectionAggregator.squaredEuclidean(point, sampleVec);
            if (dist == 0.0) {
                continue;
            }
            if (kNearest.size() < effectiveK) {
                kNearest.add(dist);
            } else if (dist < kNearest.peek()) {
                kNearest.poll();
                kNearest.add(dist);
            }
        }
        if (kNearest.isEmpty()) {
            return 0.0;
        }
        double sum = 0;
        for (double d : kNearest) {
            sum += d;
        }
        return sum / kNearest.size();
    }

    /**
     * Compute LDoF score for a single candidate against the sample.
     * Requires precomputed TNN values for each sample vector.
     */
    static double computeLdofScore(float[] point, float[][] sample, int k, double[] sampleTnn) {
        int effectiveK = Math.min(k, sample.length);
        if (effectiveK <= 0) {
            return 0.0;
        }
        java.util.PriorityQueue<double[]> kNearest = new java.util.PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b[0], a[0]));
        for (int j = 0; j < sample.length; j++) {
            double dist = OutlierDetectionAggregator.squaredEuclidean(point, sample[j]);
            if (dist == 0.0) {
                continue;
            }
            if (kNearest.size() < effectiveK) {
                kNearest.add(new double[] { dist, j });
            } else if (dist < kNearest.peek()[0]) {
                kNearest.poll();
                kNearest.add(new double[] { dist, j });
            }
        }
        if (kNearest.isEmpty()) {
            return 0.0;
        }
        double distSum = 0;
        double innerTnnSum = 0;
        int count = 0;
        for (double[] entry : kNearest) {
            distSum += entry[0];
            innerTnnSum += sampleTnn[(int) entry[1]];
            count++;
        }
        double tnn = distSum / count;
        double innerAvg = innerTnnSum / count;
        return innerAvg > 0 ? tnn / innerAvg : 0;
    }

    /**
     * Compute LoF score for a single candidate against the sample.
     * Requires precomputed k-distances and LRD values for each sample vector.
     */
    static double computeLofScore(float[] point, float[][] sample, int k, double[] sampleKDist, double[] sampleLrd) {
        int effectiveK = Math.min(k, sample.length);
        if (effectiveK <= 0) {
            return 0.0;
        }
        java.util.PriorityQueue<double[]> kNearest = new java.util.PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b[0], a[0]));
        for (int j = 0; j < sample.length; j++) {
            double dist = OutlierDetectionAggregator.squaredEuclidean(point, sample[j]);
            if (dist == 0.0) {
                continue;
            }
            if (kNearest.size() < effectiveK) {
                kNearest.add(new double[] { dist, j });
            } else if (dist < kNearest.peek()[0]) {
                kNearest.poll();
                kNearest.add(new double[] { dist, j });
            }
        }
        if (kNearest.isEmpty()) {
            return 0.0;
        }
        double reachSum = 0;
        double neighborLrdSum = 0;
        int count = 0;
        for (double[] entry : kNearest) {
            int idx = (int) entry[1];
            double reachDist = Math.max(entry[0], sampleKDist[idx]);
            reachSum += reachDist;
            neighborLrdSum += sampleLrd[idx];
            count++;
        }
        double lrd = (count > 0 && reachSum > 0) ? (double) count / reachSum : 0;
        if (lrd == 0) {
            return 0;
        }
        return (neighborLrdSum / count) / lrd;
    }

    /**
     * Re-score a single candidate using the specified method.
     */
    static double reScoreCandidate(
        float[] point,
        float[][] sample,
        int k,
        OutlierDetectionMethod method,
        double[] sampleTnn,
        double[] sampleKDist,
        double[] sampleLrd
    ) {
        return switch (method) {
            case KTH_NN -> computeKthNnDistance(point, sample, k);
            case TNN -> computeTnnScore(point, sample, k);
            case LDOF -> computeLdofScore(point, sample, k, sampleTnn);
            case LOF -> computeLofScore(point, sample, k, sampleKDist, sampleLrd);
        };
    }

    /**
     * Precompute TNN for every sample vector (for LDoF re-scoring).
     */
    static double[] precomputeSampleTnn(float[][] sample, int k) {
        double[] tnn = new double[sample.length];
        for (int i = 0; i < sample.length; i++) {
            tnn[i] = computeTnnScore(sample[i], sample, k);
        }
        return tnn;
    }

    /**
     * Precompute k-th nearest neighbor distance for every sample vector (for LoF re-scoring).
     */
    static double[] precomputeSampleKDist(float[][] sample, int k) {
        double[] kDist = new double[sample.length];
        for (int i = 0; i < sample.length; i++) {
            kDist[i] = computeKthNnDistance(sample[i], sample, k);
        }
        return kDist;
    }

    /**
     * Precompute LRD (local reachable density) for every sample vector (for LoF re-scoring).
     */
    static double[] precomputeSampleLrd(float[][] sample, int k, double[] sampleKDist) {
        double[] lrd = new double[sample.length];
        int effectiveK = Math.min(k, sample.length - 1);
        if (effectiveK <= 0) {
            return lrd;
        }
        for (int i = 0; i < sample.length; i++) {
            java.util.PriorityQueue<double[]> kNearest = new java.util.PriorityQueue<>(effectiveK, (a, b) -> Double.compare(b[0], a[0]));
            for (int j = 0; j < sample.length; j++) {
                if (i == j) continue;
                double dist = OutlierDetectionAggregator.squaredEuclidean(sample[i], sample[j]);
                if (kNearest.size() < effectiveK) {
                    kNearest.add(new double[] { dist, j });
                } else if (dist < kNearest.peek()[0]) {
                    kNearest.poll();
                    kNearest.add(new double[] { dist, j });
                }
            }
            double reachSum = 0;
            int count = 0;
            for (double[] entry : kNearest) {
                double reachDist = Math.max(entry[0], sampleKDist[(int) entry[1]]);
                reachSum += reachDist;
                count++;
            }
            lrd[i] = (count > 0 && reachSum > 0) ? (double) count / reachSum : 0;
        }
        return lrd;
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        if (path.size() == 1 && "outliers".equals(path.get(0))) {
            return candidates;
        }
        throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return false;
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.startArray("outliers");
        for (OutlierCandidate candidate : candidates) {
            candidate.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    public List<OutlierCandidate> getCandidates() {
        return candidates;
    }

    public float[][] getSampleVectors() {
        return sampleVectors;
    }

    public int getSampleCount() {
        return sampleCount;
    }

    public int getTopN() {
        return topN;
    }

    public int getNNeighbors() {
        return nNeighbors;
    }

    public long getSeed() {
        return seed;
    }

    public OutlierDetectionMethod getMethod() {
        return method;
    }

    public ScoreNormalization getNormalize() {
        return normalize;
    }
}
