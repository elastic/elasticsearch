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
import org.apache.lucene.index.ByteVectorValues;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FloatVectorValues;
import org.apache.lucene.index.KnnVectorValues;
import org.apache.lucene.index.MergeState;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.util.Bits;
import org.elasticsearch.index.codec.vectors.cluster.KMeansFloatVectorValues;
import org.elasticsearch.index.codec.vectors.diskbbq.Preconditioner;
import org.elasticsearch.simdvec.ESVectorUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Shared utilities for DiskBBQ merge-time calibration.
 */
public final class CalibrationUtils {

    static final int MAX_QUERY_SAMPLE = 1024;
    static final int MAX_CORPUS_SAMPLE = 16384;
    static final long CALIBRATION_SEED = 215873873L;

    private CalibrationUtils() {}

    /**
     * Buffer length for calibration query scratch arrays: {@code baseDim}, or {@code baseDim + 1}
     * when Neyshabur lift applies.
     */
    public static int calibrationQueryDimension(int baseDim, boolean neyshabur) {
        return neyshabur ? baseDim + 1 : baseDim;
    }

    /**
     * Materializes one calibration query from {@code querySource} at {@code queryOrdinal} into
     * {@code queryScratch}. {@code queryScratch.length} must be at least {@code dimWork}. When
     * {@code usePreconditioned} is true and {@code preconditioner} is non-null, applies it using
     * {@code preconditionScratch} (length at least {@code dimWork}).
     */
    public static void materializeCalibrationQuery(
        FloatVectorValues querySource,
        int queryOrdinal,
        int baseDim,
        int dimWork,
        boolean cosine,
        boolean neyshabur,
        Preconditioner preconditioner,
        boolean usePreconditioned,
        float[] queryScratch,
        float[] preconditionScratch
    ) throws IOException {
        float[] raw = querySource.vectorValue(queryOrdinal);
        System.arraycopy(raw, 0, queryScratch, 0, baseDim);
        if (cosine) {
            ESVectorUtil.l2Normalize(queryScratch, baseDim);
        }
        if (neyshabur) {
            queryScratch[baseDim] = 0f;
        }
        if (usePreconditioned && preconditioner != null) {
            Objects.requireNonNull(preconditionScratch, "preconditionScratch");
            preconditioner.applyTransform(queryScratch, preconditionScratch);
            System.arraycopy(preconditionScratch, 0, queryScratch, 0, dimWork);
        }
    }

    /**
     * Sampled data from a {@link FloatVectorValues}: query ordinals into the segment and
     * corpus ordinals.
     */
    public record SampledData(int[] queryOrdinals, int[] corpusOrdinals) {}

    /**
     * Copy {@code src} into {@code scratch} and L2-normalize in place.
     */
    public static float[] copyAndNormalize(float[] src, float[] scratch) {
        System.arraycopy(src, 0, scratch, 0, src.length);
        ESVectorUtil.l2Normalize(scratch, src.length);
        return scratch;
    }

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
    public static double maxSquaredNormOverCorpusSample(FloatVectorValues vectorValues, int[] corpusOrdinals) throws IOException {
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
     * Applies the Neyshabur–Srebro lift (dot product -> euclidean in one higher dimension) to vectors from an
     * underlying source, mapping each vector {@code x} to {@code [x, sqrt(M - ||x||^2)]} with {@code M = maxNormSq}
     * over the calibration corpus sample, per Neyshabur and Srebro (ICML 2015).
     * <p>
     * The lift is agnostic to the source element type: the output is always {@code float}, but the source may be a
     * {@link FloatVectorValues} or a {@link ByteVectorValues}. {@link #liftedVector(int, float[])} reads the base
     * vector, widens it to float, and appends the lift component into a caller-provided scratch buffer, so the extra
     * dimension never needs to fit in the source element type. Consumers that require a Lucene
     * {@link FloatVectorValues} (k-means, bulk scorers) use {@link #asFloatVectorValues()}.
     *
     * @param <V> the underlying vector-values type ({@link FloatVectorValues} or {@link ByteVectorValues})
     */
    public static final class NeyshaburLiftedSource<V> {
        private final V vectorValues;
        private final int baseDim;
        private final double maxNormSq;

        public NeyshaburLiftedSource(V vectorValues, int baseDim, double maxNormSq) {
            this.vectorValues = Objects.requireNonNull(vectorValues, "vectorValues");
            this.baseDim = baseDim;
            this.maxNormSq = maxNormSq;
        }

        /** The underlying, un-lifted source. */
        public V delegate() {
            return vectorValues;
        }

        /** Dimension of a lifted vector: {@code baseDim + 1}. */
        public int liftedDimension() {
            return baseDim + 1;
        }

        /** Number of vectors in the underlying source. */
        public int size() {
            if (vectorValues instanceof FloatVectorValues fvv) {
                return fvv.size();
            }
            if (vectorValues instanceof ByteVectorValues bvv) {
                return bvv.size();
            }
            throw new IllegalStateException("unsupported vector values type: " + vectorValues.getClass().getName());
        }

        /**
         * Reads the base vector at {@code ord}, widens it to float in {@code dst[0..baseDim)}, and writes the lift
         * component {@code sqrt(max(0, M - ||x||^2))} at {@code dst[baseDim]}. {@code dst.length} must be at least
         * {@link #liftedDimension()}.
         */
        public void liftedVector(int ord, float[] dst) throws IOException {
            double normSq;
            if (vectorValues instanceof FloatVectorValues fvv) {
                float[] base = fvv.vectorValue(ord);
                System.arraycopy(base, 0, dst, 0, baseDim);
                normSq = ESVectorUtil.dotProduct(base, base);
            } else if (vectorValues instanceof ByteVectorValues bvv) {
                byte[] base = bvv.vectorValue(ord);
                long dot = 0;
                for (int i = 0; i < baseDim; i++) {
                    int b = base[i];
                    dst[i] = b;
                    dot += (long) b * b;
                }
                normSq = dot;
            } else {
                throw new IllegalStateException("unsupported vector values type: " + vectorValues.getClass().getName());
            }
            dst[baseDim] = (float) Math.sqrt(Math.max(0.0, maxNormSq - normSq));
        }

        /**
         * A {@link FloatVectorValues} view over the lifted vectors, for consumers that require the Lucene interface
         * (k-means, bulk scorers). Follows the {@link FloatVectorValues} shared-buffer contract: the returned array
         * is only valid until the next {@link FloatVectorValues#vectorValue(int)} call. Requires a
         * {@link FloatVectorValues} source.
         */
        public FloatVectorValues asFloatVectorValues() {
            if (vectorValues instanceof FloatVectorValues) {
                // safe: guarded by the instanceof above, so V is FloatVectorValues
                @SuppressWarnings("unchecked")
                NeyshaburLiftedSource<FloatVectorValues> floatSource = (NeyshaburLiftedSource<FloatVectorValues>) this;
                return new LiftedFloatVectorValues(floatSource);
            }
            throw new IllegalStateException(
                "asFloatVectorValues requires a FloatVectorValues source, got: " + vectorValues.getClass().getName()
            );
        }
    }

    /**
     * {@link FloatVectorValues} bridge exposing a {@link NeyshaburLiftedSource}'s lifted vectors to consumers that
     * require the Lucene interface. Reads into a shared per-instance buffer, so the returned array is only valid
     * until the next {@link #vectorValue(int)} call and callers must not hold two lifted vectors at once.
     */
    private static final class LiftedFloatVectorValues extends FloatVectorValues {
        private final NeyshaburLiftedSource<FloatVectorValues> source;
        private final float[] buffer;

        LiftedFloatVectorValues(NeyshaburLiftedSource<FloatVectorValues> source) {
            this.source = source;
            this.buffer = new float[source.liftedDimension()];
        }

        @Override
        public float[] vectorValue(int ord) throws IOException {
            source.liftedVector(ord, buffer);
            return buffer;
        }

        @Override
        public FloatVectorValues copy() throws IOException {
            return new LiftedFloatVectorValues(new NeyshaburLiftedSource<>(source.delegate().copy(), source.baseDim, source.maxNormSq));
        }

        @Override
        public int dimension() {
            return source.liftedDimension();
        }

        @Override
        public int size() {
            return source.delegate().size();
        }

        @Override
        public DocIndexIterator iterator() {
            return source.delegate().iterator();
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
     * Uses a sparse Fisher-Yates shuffle: only {@code nQueries + nDocs} swap steps are performed,
     * tracked via a {@link HashMap} keyed on the swapped positions. This is O(totalSample) time and
     * space with no rejection retries and no boxing beyond the map entries.
     */
    static SampledData sampleData(FloatVectorValues vectorValues, int maxQuerySample, int maxCorpusSample) throws IOException {
        int n = vectorValues.size();
        Random rng = new Random(CALIBRATION_SEED);
        int nQueries = Math.min(maxQuerySample, n / 2);
        int nDocs = Math.min(maxCorpusSample, n - nQueries);
        int totalSample = nQueries + nDocs;

        // sparse Fisher-Yates: conceptually shuffle [0, n) but only materialise the first
        // totalSample picks, storing displaced values in a map instead of a full int[n] array.
        Map<Integer, Integer> swaps = new HashMap<>(totalSample * 2);
        int[] selected = new int[totalSample];
        for (int i = 0; i < totalSample; i++) {
            int j = i + rng.nextInt(n - i);
            selected[i] = swaps.getOrDefault(j, j);
            swaps.put(j, swaps.getOrDefault(i, i));
        }

        int[] queryOrdinals = Arrays.copyOfRange(selected, 0, nQueries);
        int[] corpusOrdinals = Arrays.copyOfRange(selected, nQueries, totalSample);
        Arrays.sort(queryOrdinals);
        Arrays.sort(corpusOrdinals);
        return new SampledData(queryOrdinals, corpusOrdinals);
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
     * Total live (non-deleted) vectors for {@code fieldInfo} across merge inputs. Segments without deletes
     * ({@code liveDocs == null}) contribute {@link FloatVectorValues#size()} directly; segments with deletes
     * are counted by intersecting their vector doc ids with {@code liveDocs} (one pass over the segment's
     * vectors), the only way to exclude deleted vectors since Lucene's merged sub-readers are package-private.
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
            if (segmentVectors == null) {
                continue;
            }
            Bits liveDocs = mergeState.liveDocs == null ? null : mergeState.liveDocs[i];
            if (liveDocs == null) {
                total += segmentVectors.size();
            } else {
                KnnVectorValues.DocIndexIterator iterator = segmentVectors.iterator();
                for (int doc = iterator.nextDoc(); doc != DocIdSetIterator.NO_MORE_DOCS; doc = iterator.nextDoc()) {
                    if (liveDocs.get(doc)) {
                        total++;
                    }
                }
            }
        }
        return total;
    }

    /**
     * Stride-samples up to {@code MAX_QUERY_SAMPLE + MAX_CORPUS_SAMPLE} vectors using a
     * pre-computed total vector count (avoids a second metadata pass when the caller already
     * holds the result of {@link #countMergedVectors}).
     */
    public static KMeansFloatVectorValues buildSampled(FieldInfo fieldInfo, MergeState mergeState, int totalVectors) throws IOException {
        return buildSampled(fieldInfo, mergeState, totalVectors, MAX_QUERY_SAMPLE + MAX_CORPUS_SAMPLE);
    }

    /**
     * Samples up to {@code totalSample} vectors from the merge inputs using stride-based direct
     * ordinal access, one segment at a time.
     *
     * <p>Each segment contributes a quota proportional to its size
     * ({@code segQuota = segSize × totalSample / totalVectors}). Within the segment, vectors
     * are read at evenly-spaced ordinals ({@code stride = segSize / segQuota}) via
     * {@link FloatVectorValues#vectorValue(int)}, which on flat-binary storage is an O(1) seek.
     * At most {@code totalSample} {@code float[]} arrays are materialised on the heap.
     *
     * <p>Compared with a full-scan reservoir approach, this reads only
     * {@code totalSample / totalVectors} of the data — typically hundreds of times less IO for
     * large corpora — while giving uniform coverage across the ordinal range of every segment.
     * Each segment's {@link FloatVectorValues} is opened, read at strided ordinals, then
     * immediately released; no readers are left dangling.
     */
    public static KMeansFloatVectorValues buildSampled(FieldInfo fieldInfo, MergeState mergeState, int totalVectors, int totalSample)
        throws IOException {
        Objects.requireNonNull(fieldInfo, "fieldInfo");
        Objects.requireNonNull(mergeState, "mergeState");
        int dim = fieldInfo.getVectorDimension();
        if (mergeState.knnVectorsReaders == null || totalSample <= 0 || totalVectors == 0) {
            return KMeansFloatVectorValues.build(List.of(), null, dim);
        }

        List<float[]> result = new ArrayList<>(Math.min(totalSample, totalVectors));

        for (int i = 0; i < mergeState.knnVectorsReaders.length; i++) {
            FloatVectorValues fvv = segmentFloatVectorValues(fieldInfo, mergeState, i);
            if (fvv == null || fvv.size() == 0) {
                continue;
            }
            int segSize = fvv.size();
            // Proportional quota: segment contributes in proportion to its share of total vectors.
            int segQuota = Math.max(1, Math.min(segSize, (int) ((long) segSize * totalSample / totalVectors)));
            // Stride gives even coverage across the full ordinal range of this segment.
            int stride = Math.max(1, segSize / segQuota);

            for (int ord = 0; ord < segSize && result.size() < totalSample; ord += stride) {
                result.add(fvv.vectorValue(ord).clone());
            }
            // fvv goes out of scope; no reference to the segment reader is retained.
        }

        return KMeansFloatVectorValues.build(result, null, dim);
    }

}
