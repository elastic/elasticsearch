/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.changepoint;

import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SpikeAndDipDetectorTests extends ESTestCase {
    
    public void testTooLittleData() {
        double[] values = new double[] { 1.0, 2.0, 3.0 };
        SpikeAndDipDetector detect = new SpikeAndDipDetector(values);
        assertThat(detect.at(0.01), instanceOf(ChangeType.Indeterminable.class));
    }

    public void testSpikeAndDipValues() {
        double[] values = new double[] { 2.0, 1.0, 3.0, 5.0, 4.0 };
        SpikeAndDipDetector detector = new SpikeAndDipDetector(values);
        assertThat(detector.spikeValue(), equalTo(5.0));
        assertThat(detector.dipValue(), equalTo(1.0));
    }

    public void testExludedValues() {

        // We expect to exclude the values at indices 7, 8 and 17 from the spike and at 8, 17, 18
        // from the dip KDE data.

        double[] values = new double[] { 1.0, -1.0, 3.0, 2.0, 1.5,  2.0, 3.0,  3.0, 10.0, 2.0,
                                         2.1,  0.5, 1.0, 1.4, 2.0, -0.0, 1.0,  3.1,  2.2, 2.1,
                                         2.0,  1.0, 2.0, 3.0, 4.0,  7.0, 4.0, -2.0,  0.0, 1.0 };
        double[] expectedDipKDEValues = new double[] { 1.0, -1.0, 3.0, 2.0, 1.5,  2.0, 3.0, 3.0, 2.0,
                                                       2.1,  0.5, 1.0, 1.4, 2.0, -0.0, 1.0, 3.1, 2.2, 2.1,
                                                       2.0,  1.0, 2.0, 3.0, 4.0,  7.0, 4.0, 1.0 };
        double[] expectedSpikeKDEValues = new double[] { 1.0, -1.0, 3.0, 2.0, 1.5,  2.0, 3.0, 2.0,
                                                         2.1,  0.5, 1.0, 1.4, 2.0, -0.0, 1.0, 3.1, 2.2, 2.1,
                                                         2.0,  1.0, 2.0, 3.0, 4.0,  7.0, 4.0, 0.0, 1.0 };

        Arrays.sort(expectedSpikeKDEValues);
        Arrays.sort(expectedDipKDEValues);

        SpikeAndDipDetector detector = new SpikeAndDipDetector(values);

        assertThat(detector.spikeValue(), equalTo(10.0));
        assertThat(detector.dipValue(), equalTo(-2.0));
        assertThat(detector.spikeTestKDE().data(), equalTo(expectedSpikeKDEValues));
        assertThat(detector.dipTestKDE().data(), equalTo(expectedDipKDEValues));
    }

    public void testDetection() {

        // Check vs expected values...
    }
}
