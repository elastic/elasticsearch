/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Random;

/**
 * Utility methods for automatic calibration.
 *
 * <p>Merge-time vector access for calibration reads raw vectors from per-segment
 * {@link KnnVectorsReader}s via {@link #build(FieldInfo, MergeState)} without materializing
 * corpus vectors on the heap. Ordinal subsets from {@link #sampleData(FloatVectorValues)} are
 * the only large allocations.
 */
public final class CalibrationUtils {

    static final int MAX_QUERY_SAMPLE = 1024;
    static final int MAX_CORPUS_SAMPLE = 16384;
    static final long CALIBRATION_SEED = 215873873L;

    private CalibrationUtils() {}

    /**
     * Sampled data from a {@link FloatVectorValues}: query ordinals into the segment and
     * corpus ordinals.
     */
    public record SampledData(int[] queryOrdinals, int[] corpusOrdinals) {}

    /**
     * Whether to apply the Neyshabur–Srebro lift (dot product -> euclidean in one higher dimension)
     * before calibration.
     */
    public static boolean needsNeyshaburSrebroLift(VectorSimilarityFunction similarityFunction) {
        return similarityFunction == VectorSimilarityFunction.DOT_PRODUCT
            || similarityFunction == VectorSimilarityFunction.MAXIMUM_INNER_PRODUCT;
    }

    /**
     * Maximum squared L2 norm over the sampled corpus vectors (same statistic as reference
     * {@code neyshaburSrebroTransform} over the calibration corpus subset).
     */
    public static double maxSquaredNormOverCorpusSample(FloatVectorValues vectorValues, int[] corpusOrdinals, int dim) throws IOException {
        double maxNormSq = 0;
        for (int ord : corpusOrdinals) {
            float[] v = vectorValues.vectorValue(ord);
            double normSq = ESVectorUtil.dotProduct(v, v);
            if (normSq > maxNormSq) {
                maxNormSq = normSq;
            }
        }
        return maxNormSq;
    }

    /**
     * Corpus view that maps each vector {@code x} to {@code [x, sqrt(M - ||x||^2)]} with
     * {@code M = maxNormSq} over the calibration corpus sample, per Neyshabur and Srebro (ICML 2015).
     */
    public static final class NeyshaburCorpusFloatVectorValues extends FloatVectorValues {
        private final FloatVectorValues delegate;
        private final int dim;
        private final double maxNormSq;
        private final float[] buffer;

        public NeyshaburCorpusFloatVectorValues(FloatVectorValues delegate, int dim, double maxNormSq) {
            this.delegate = delegate;
            this.dim = dim;
            this.maxNormSq = maxNormSq;
            this.buffer = new float[dim + 1];
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            float[] v = delegate.vectorValue(ord);
            System.arraycopy(v, 0, buffer, 0, dim);
            double normSq = ESVectorUtil.dotProduct(v, v);
            buffer[dim] = (float) Math.sqrt(Math.max(0.0, maxNormSq - normSq));
            return buffer;
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new NeyshaburCorpusFloatVectorValues(delegate.copy(), dim, maxNormSq);
        }

        @Override
        public int dimension() {
            return dim + 1;
        }

        @Override
        public int size() {
            return delegate.size();
        }

        @Override
        public DocIndexIterator iterator() {
            return delegate.iterator();
        }
    }

    /**
     * Sample random, disjoint query and corpus subsets from {@link FloatVectorValues}
     * using default (full) sample sizes.
     */
    public static SampledData sampleData(FloatVectorValues vectorValues) throws IOException {
        return sampleData(vectorValues, MAX_QUERY_SAMPLE, MAX_CORPUS_SAMPLE);
    }

    /**
     * Sample random, disjoint query and corpus subsets from {@link FloatVectorValues}.
     */
    static SampledData sampleData(FloatVectorValues vectorValues, int maxQuerySample, int maxCorpusSample) throws IOException {
        int n = vectorValues.size();
        Random rng = new Random(CALIBRATION_SEED);
        int nQueries = Math.min(maxQuerySample, n / 2);
        int nDocs = Math.min(maxCorpusSample, n - nQueries);

        HashSet<Integer> querySet = new HashSet<>(Math.max(16, nQueries * 2));
        while (querySet.size() < nQueries) {
            querySet.add(rng.nextInt(n));
        }
        HashSet<Integer> corpusSet = new HashSet<>(Math.max(16, nDocs * 2));
        while (corpusSet.size() < nDocs) {
            int c = rng.nextInt(n);
            if (querySet.contains(c)) {
                continue;
            }
            corpusSet.add(c);
        }

        int[] queryOrdinals = new int[nQueries];
        int qi = 0;
        for (Integer o : querySet) {
            queryOrdinals[qi++] = o;
        }
        Arrays.sort(queryOrdinals);

        int[] corpusOrdinals = new int[nDocs];
        int ci = 0;
        for (Integer o : corpusSet) {
            corpusOrdinals[ci++] = o;
        }
        Arrays.sort(corpusOrdinals);
        return new SampledData(queryOrdinals, corpusOrdinals);
    }

    /**
     * Builds a reader-backed {@link FloatVectorValues} view over all input segments being merged.
     * Vector data is not copied to the heap; callers sample via {@link #sampleData(FloatVectorValues)}.
     */
    public static FloatVectorValues build(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        Objects.requireNonNull(fieldInfo, "fieldInfo");
        Objects.requireNonNull(mergeState, "mergeState");
        List<FloatVectorValues> parts = loadSegmentVectorValues(fieldInfo, mergeState);
        if (parts.isEmpty()) {
            return KMeansFloatVectorValues.build(List.of(), null, fieldInfo.getVectorDimension());
        }
        if (parts.size() == 1) {
            return parts.getFirst();
        }
        return new ConcatenatedFloatVectorValues(parts.toArray(FloatVectorValues[]::new));
    }

    private static List<FloatVectorValues> loadSegmentVectorValues(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        List<FloatVectorValues> segments = new ArrayList<>();
        if (mergeState.knnVectorsReaders == null) {
            return segments;
        }
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            FloatVectorValues segmentVectors = segmentFloatVectorValues(fieldInfo, mergeState, i);
            if (segmentVectors != null && segmentVectors.size() > 0) {
                segments.add(segmentVectors);
            }
        }
        return segments;
    }

    private static FloatVectorValues segmentFloatVectorValues(FieldInfo fieldInfo, MergeState mergeState, int segmentIndex)
        throws IOException {
        if (mergeState.fieldInfos != null) {
            if (mergeState.fieldInfos[segmentIndex].fieldInfo(fieldInfo.name) == null) {
                return null;
            }
        }
        KnnVectorsReader reader = mergeState.knnVectorsReaders[segmentIndex];
        if (reader == null) {
            return null;
        }
        return reader.getFloatVectorValues(fieldInfo.name);
    }

    /**
     * Total live vectors for {@code fieldInfo} across merge inputs.
     */
    public static int countMergedVectors(FieldInfo fieldInfo, MergeState mergeState) throws IOException {
        Objects.requireNonNull(fieldInfo, "fieldInfo");
        Objects.requireNonNull(mergeState, "mergeState");
        if (mergeState.knnVectorsReaders == null) {
            return 0;
        }
        int total = 0;
        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            FloatVectorValues segmentVectors = segmentFloatVectorValues(fieldInfo, mergeState, i);
            if (segmentVectors != null) {
                total += segmentVectors.size();
            }
        }
        return total;
    }
}
