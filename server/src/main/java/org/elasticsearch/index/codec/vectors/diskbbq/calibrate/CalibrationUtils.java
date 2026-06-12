/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues.DocIndexIterator;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

/**
 * Utility methods for quantization calibration: vector math and sampling from
 * {@link FloatVectorValues}.
 */
public final class CalibrationUtils {

    static final int MAX_QUERY_SAMPLE = 512;
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
     * Copies every vector from {@code fvv} into a dense on-heap {@link FloatVectorValues} that supports
     * {@link FloatVectorValues#vectorValue(int)} for arbitrary ordinals. Lucene's merged float vector
     * implementation only allows {@code vectorValue(iterator.index())} in lockstep with a forward
     * {@link DocIndexIterator}.
     * This helper is useful for tests and small in-memory sources.
     */
    public static FloatVectorValues toHeapDenseFloatVectorValues(FloatVectorValues fvv) throws IOException {
        final int size = fvv.size();
        final int dim = fvv.dimension();
        final ArrayList<float[]> rows = new ArrayList<>(Math.max(16, size));
        final DocIndexIterator it = fvv.iterator();
        for (int doc = it.nextDoc(); doc != NO_MORE_DOCS; doc = it.nextDoc()) {
            float[] v = fvv.vectorValue(it.index());
            rows.add(Arrays.copyOf(v, dim));
        }
        if (rows.size() != size) {
            throw new IllegalStateException("expected [" + size + "] vectors from iterator but read [" + rows.size() + "]");
        }
        return KMeansFloatVectorValues.build(rows, null, dim);
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
}
