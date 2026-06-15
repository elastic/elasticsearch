/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public License
 * v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.codec.vectors.diskbbq.calibrate;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class QuantizationErrorStdModelTests extends ESTestCase {

    public void testErrorStdIncreasesWithClusterSize() {
        Regression.OLSResult params = new Regression.OLSResult(-5.0, 0.2, 0.01, 0.001, 0.0, 0.01);
        QuantizationErrorStdModel model = new QuantizationErrorStdModel(params);
        int sampleSize = 4096;
        double smallCluster = model.errorStd(64, sampleSize);
        double largeCluster = model.errorStd(256, sampleSize);
        assertThat(smallCluster, greaterThan(0.0));
        assertThat(largeCluster, greaterThan(smallCluster));
    }

    public void testErrorStdDecreasesWithSampleSize() {
        Regression.OLSResult params = new Regression.OLSResult(-4.0, 0.15, 0.01, 0.001, 0.0, 0.01);
        QuantizationErrorStdModel model = new QuantizationErrorStdModel(params);
        int nDocsPerCluster = 128;
        double smallSample = model.errorStd(nDocsPerCluster, 4096);
        double largeSample = model.errorStd(nDocsPerCluster, 16384);
        assertThat(smallSample, greaterThan(0.0));
        assertThat(largeSample, lessThan(smallSample));
    }
}
