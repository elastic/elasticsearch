/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

/** Tests for {@link SeriesDownsampler}: the runtime safeguard that collapses an over-long series. */
public class SeriesDownsamplerTests extends ESTestCase {

    public void testReturnsInputUnchangedBelowTheCap() {
        MlAggsHelper.DoubleBucketValues input = bucketValues(constant(100, 1.0));
        assertSame("below the cap the input is returned unchanged", input, SeriesDownsampler.downsample(input, 2000));
    }

    public void testReducesLengthAboveTheCap() {
        MlAggsHelper.DoubleBucketValues result = SeriesDownsampler.downsample(bucketValues(constant(5000, 1.0)), 2000);
        assertTrue("the series must be collapsed to at most the cap", result.getValues().length <= 2000);
        assertTrue(result.getValues().length > 0);
    }

    public void testPreservesAnExtremeAndMapsItBackToItsSourceBucket() {
        int n = 4000;
        int spikeIndex = 1234;
        double[] values = constant(n, 1.0);
        values[spikeIndex] = 1000.0;

        MlAggsHelper.DoubleBucketValues result = SeriesDownsampler.downsample(bucketValues(values), 2000);

        double[] out = result.getValues();
        int spikePos = -1;
        for (int k = 0; k < out.length; k++) {
            if (out[k] == 1000.0) {
                spikePos = k;
                break;
            }
        }
        assertTrue("the extreme value of a bucket must be retained by the downsampler", spikePos >= 0);
        assertEquals("the retained extreme must map back to its source index", spikeIndex, result.getBucketIndex(spikePos));
    }

    private static double[] constant(int n, double value) {
        double[] values = new double[n];
        java.util.Arrays.fill(values, value);
        return values;
    }

    private static MlAggsHelper.DoubleBucketValues bucketValues(double[] values) {
        long[] docCounts = new long[values.length];
        java.util.Arrays.fill(docCounts, 1L);
        return new MlAggsHelper.DoubleBucketValues(docCounts, values);
    }
}
