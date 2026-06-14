/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;

/**
 * Runtime safeguard for very long series. Detection cost grows with the number of buckets (PELT's reset loop and
 * the selector's O(m^2) dynamic program), so before any detection we collapse an over-long series to at most
 * {@code maxSamples} points.
 *
 * <p>Each macro-bucket of adjacent buckets contributes two samples, chosen to preserve exactly the two things the
 * pipeline detects: the bucket's robust centre (its median) preserves the underlying level/trend for the structural
 * pass, and the single point furthest from that centre (the largest absolute deviation) preserves a spike/dip for
 * the pulse pass. A simple mean would average a one-bucket excursion away; keeping the extreme guarantees a genuine
 * spike still reaches the proposer. Two samples per bucket (rather than M4's four) lets us spend the sample budget
 * on more buckets, i.e. finer localization, for the same cap.
 *
 * <p>Each retained sample carries the original bucket index of the position it stands for, so every detected event
 * is remapped back to the source bucket for free by the existing {@link MlAggsHelper.DoubleBucketValues#getBucketIndex}
 * plumbing. Series at or below the cap are returned unchanged (the returned object is the input instance), so the
 * common case is a no-op.
 *
 * <p>Note: detection treats samples as evenly spaced, so after downsampling {@code minSegmentLength} is measured in
 * samples, i.e. its effective time span scales with the downsample factor on very long series.
 */
final class SeriesDownsampler {

    private static final int SAMPLES_PER_BUCKET = 2;

    private SeriesDownsampler() {}

    /**
     * Returns a series with at most {@code maxSamples} points. If the input already fits (or the cap is too small to
     * be meaningful) the input is returned unchanged.
     */
    static MlAggsHelper.DoubleBucketValues downsample(MlAggsHelper.DoubleBucketValues input, int maxSamples) {
        double[] values = input.getValues();
        int n = values.length;
        if (n <= maxSamples || maxSamples < 2 * SAMPLES_PER_BUCKET) {
            return input;
        }

        long[] docCounts = input.getDocCounts();
        int maxBuckets = Math.max(1, maxSamples / SAMPLES_PER_BUCKET);
        int bucketWidth = (int) Math.ceil((double) n / maxBuckets);

        int capacity = SAMPLES_PER_BUCKET * ((n + bucketWidth - 1) / bucketWidth);
        double[] outValues = new double[capacity];
        int[] outBuckets = new int[capacity];
        long[] outDocs = new long[capacity];
        int k = 0;

        for (int a = 0; a < n; a += bucketWidth) {
            int b = Math.min(n, a + bucketWidth);
            double center = median(values, a, b);

            int extremeIndex = a;
            double bestDeviation = -1.0;
            long bucketDocs = 0L;
            for (int i = a; i < b; i++) {
                double deviation = Math.abs(values[i] - center);
                if (deviation > bestDeviation) {
                    bestDeviation = deviation;
                    extremeIndex = i;
                }
                bucketDocs += docCounts != null && i < docCounts.length ? docCounts[i] : 1L;
            }
            int centerIndex = a + (b - a) / 2;

            // Emit the two representative points in original-index order so the downsampled series stays temporal.
            int firstIndex;
            double firstValue;
            int secondIndex;
            double secondValue;
            if (centerIndex <= extremeIndex) {
                firstIndex = centerIndex;
                firstValue = center;
                secondIndex = extremeIndex;
                secondValue = values[extremeIndex];
            } else {
                firstIndex = extremeIndex;
                firstValue = values[extremeIndex];
                secondIndex = centerIndex;
                secondValue = center;
            }

            outValues[k] = firstValue;
            outBuckets[k] = input.getBucketIndex(firstIndex);
            outDocs[k] = bucketDocs;
            k++;
            // Skip the second only when it is an exact duplicate of the first (e.g. a width-1 trailing bucket).
            if (secondIndex != firstIndex || secondValue != firstValue) {
                outValues[k] = secondValue;
                outBuckets[k] = input.getBucketIndex(secondIndex);
                outDocs[k] = bucketDocs;
                k++;
            }
        }

        return new MlAggsHelper.DoubleBucketValues(Arrays.copyOf(outDocs, k), Arrays.copyOf(outValues, k), Arrays.copyOf(outBuckets, k));
    }

    private static double median(double[] values, int start, int end) {
        double[] window = Arrays.copyOfRange(values, start, end);
        Arrays.sort(window);
        int m = window.length;
        if (m == 0) {
            return 0.0;
        }
        return (m % 2 == 1) ? window[m / 2] : 0.5 * (window[m / 2 - 1] + window[m / 2]);
    }
}
