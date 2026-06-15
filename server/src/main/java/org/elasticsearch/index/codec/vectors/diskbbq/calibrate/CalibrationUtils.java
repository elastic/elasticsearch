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
import org.apache.lucene.search.DocIdSetIterator;
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

    /**
     * Concatenation of multiple {@link FloatVectorValues} into a single logical view for calibration
     * sampling during merge. Vectors are addressed by a global ordinal; lookups dispatch to the
     * underlying segment reader via a prefix-sum offset table without copying vector data to the heap.
     *
     * <p>{@link #vectorValue(int)} forwards to the owning part, which may return a reused scratch
     * buffer; callers that need to retain a vector across subsequent calls must copy it. Doc IDs
     * exposed by {@link #iterator()} remain segment-local, not merged-global.
     */
    private static final class ConcatenatedFloatVectorValues extends FloatVectorValues {

        private final FloatVectorValues[] parts;
        private final int[] offsets;
        private final int totalSize;
        private final int dims;

        ConcatenatedFloatVectorValues(FloatVectorValues[] parts) {
            if (parts.length == 0) {
                throw new IllegalArgumentException("parts must not be empty");
            }
            this.parts = parts;
            this.offsets = new int[parts.length + 1];
            this.dims = parts[0].dimension();
            int running = 0;
            for (int i = 0; i < parts.length; i++) {
                if (parts[i].dimension() != dims) {
                    throw new IllegalArgumentException("all parts must share dimension");
                }
                offsets[i] = running;
                running += parts[i].size();
            }
            offsets[parts.length] = running;
            this.totalSize = running;
        }

        private int partFor(int ord) {
            int lo = 0;
            int hi = parts.length - 1;
            while (lo < hi) {
                int mid = (lo + hi + 1) >>> 1;
                if (offsets[mid] <= ord) {
                    lo = mid;
                } else {
                    hi = mid - 1;
                }
            }
            return lo;
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            int p = partFor(ord);
            return parts[p].vectorValue(ord - offsets[p]);
        }

        @Override
        public int dimension() {
            return dims;
        }

        @Override
        public int size() {
            return totalSize;
        }

        @Override
        public DocIndexIterator iterator() {
            return new ConcatenatedDocIndexIterator(Arrays.stream(parts).map(FloatVectorValues::iterator).toArray(DocIndexIterator[]::new));
        }

        @Override
        public ConcatenatedFloatVectorValues copy() throws IOException {
            FloatVectorValues[] copies = new FloatVectorValues[parts.length];
            for (int i = 0; i < parts.length; i++) {
                copies[i] = parts[i].copy();
            }
            return new ConcatenatedFloatVectorValues(copies);
        }

        private static final class ConcatenatedDocIndexIterator extends DocIndexIterator {
            private final DocIndexIterator[] partIterators;
            private int partIndex;
            private int globalOrd = -1;

            private ConcatenatedDocIndexIterator(DocIndexIterator[] partIterators) {
                this.partIterators = partIterators;
                this.partIndex = 0;
            }

            @Override
            public int docID() {
                return partIterators[partIndex].docID();
            }

            @Override
            public int nextDoc() throws IOException {
                while (partIndex < partIterators.length) {
                    int doc = partIterators[partIndex].nextDoc();
                    if (doc != DocIdSetIterator.NO_MORE_DOCS) {
                        globalOrd++;
                        return doc;
                    }
                    partIndex++;
                }
                return DocIdSetIterator.NO_MORE_DOCS;
            }

            @Override
            public int advance(int target) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long cost() {
                long cost = 0;
                for (DocIndexIterator it : partIterators) {
                    cost += it.cost();
                }
                return cost;
            }

            @Override
            public int index() {
                return globalOrd;
            }
        }
    }
}
