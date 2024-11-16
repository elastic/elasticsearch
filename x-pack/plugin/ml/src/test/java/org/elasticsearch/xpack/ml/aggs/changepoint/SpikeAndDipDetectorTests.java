/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.ml.aggs.MlAggsHelper;

import java.util.Arrays;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SpikeAndDipDetectorTests extends ESTestCase {

    public void testTooLittleData() {
        for (int i = 0; i < 4; i++) {
            long[] docCounts = new long[i];
            double[] values = new double[i];
            Arrays.fill(docCounts, 1);
            Arrays.fill(values, 1.0);
            MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(docCounts, values);
            SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues);
            assertThat(detect.detect(0.01), instanceOf(ChangeType.Indeterminable.class));
        }
    }

    public void testSpikeAndDipValues() {
        double[] values = new double[] { 2.0, 1.0, 3.0, 5.0, 4.0 };
        SpikeAndDipDetector detector = new SpikeAndDipDetector(new MlAggsHelper.DoubleBucketValues(null, values));
        assertThat(detector.spikeValue(), equalTo(5.0));
        assertThat(detector.dipValue(), equalTo(1.0));
    }

    public void testExludedValues() {

        // We expect to exclude the values at indices 7, 8 and 17 from the spike and at 8, 17, 18
        // from the dip KDE data.

        double[] values = new double[] {
            1.0,
            -1.0,
            3.0,
            2.0,
            1.5,
            2.0,
            3.0,
            3.0,
            10.0,
            2.0,
            2.1,
            0.5,
            1.0,
            1.4,
            2.0,
            -0.0,
            1.0,
            3.1,
            2.2,
            2.1,
            2.0,
            1.0,
            2.0,
            3.0,
            4.0,
            7.0,
            4.0,
            -2.0,
            0.0,
            1.0 };
        double[] expectedDipKDEValues = new double[] {
            1.0,
            -1.0,
            3.0,
            2.0,
            1.5,
            2.0,
            3.0,
            3.0,
            2.0,
            2.1,
            0.5,
            1.0,
            1.4,
            2.0,
            -0.0,
            1.0,
            3.1,
            2.2,
            2.1,
            2.0,
            1.0,
            2.0,
            3.0,
            4.0,
            7.0,
            4.0,
            1.0 };
        double[] expectedSpikeKDEValues = new double[] {
            1.0,
            -1.0,
            3.0,
            2.0,
            1.5,
            2.0,
            3.0,
            2.0,
            2.1,
            0.5,
            1.0,
            1.4,
            2.0,
            -0.0,
            1.0,
            3.1,
            2.2,
            2.1,
            2.0,
            1.0,
            2.0,
            3.0,
            4.0,
            7.0,
            4.0,
            0.0,
            1.0 };

        Arrays.sort(expectedSpikeKDEValues);
        Arrays.sort(expectedDipKDEValues);

        SpikeAndDipDetector detector = new SpikeAndDipDetector(new MlAggsHelper.DoubleBucketValues(null, values));

        assertThat(detector.spikeValue(), equalTo(10.0));
        assertThat(detector.dipValue(), equalTo(-2.0));
        assertThat(detector.spikeTestKDE().data(), equalTo(expectedSpikeKDEValues));
        assertThat(detector.dipTestKDE().data(), equalTo(expectedDipKDEValues));
    }

    public void testDetection() {

        // Check vs some expected values.

        {
            long[] docCounts = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
            double[] values = new double[] { 0.1, 3.1, 1.2, 1.7, 0.9, 2.3, -0.8, 3.2, 1.2, 1.3, 1.1, 1.0, 8.5, 0.5, 2.6, 0.7 };
            MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(docCounts, values);

            SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues);

            ChangeType change = detect.detect(0.05);

            assertThat(change, instanceOf(ChangeType.Spike.class));
            assertThat(change.pValue(), closeTo(3.0465e-12, 1e-15));
        }
        {
            long[] docCounts = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
            double[] values = new double[] { 0.1, 3.1, 1.2, 1.7, 0.9, 2.3, -4.2, 3.2, 1.2, 1.3, 1.1, 1.0, 3.5, 0.5, 2.6, 0.7 };
            MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(docCounts, values);

            SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues);

            ChangeType change = detect.detect(0.05);

            assertThat(change, instanceOf(ChangeType.Dip.class));
            assertThat(change.pValue(), closeTo(1.2589e-08, 1e-11));
        }
    }

    public void testMissingBuckets() {
        long[] docCounts = new long[] { 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        double[] values = new double[] { 1.0, 2.0, 0.7, 1.0, 1.5, 1.1, 2.2, 10.0, 0.3, 0.4, 0.7, 0.9, 1.4, 2.1, 1.2, 1.0 };
        int[] buckets = new int[] { 0, 2, 3, 6, 7, 8, 9, 10, 11, 12, 13, 15, 17, 18, 19, 20 };
        MlAggsHelper.DoubleBucketValues bucketValues = new MlAggsHelper.DoubleBucketValues(docCounts, values, buckets);

        SpikeAndDipDetector detect = new SpikeAndDipDetector(bucketValues);

        ChangeType change = detect.detect(0.01);

        assertThat(change, instanceOf(ChangeType.Spike.class));
        assertThat(change.changePoint(), equalTo(10));
    }
}
