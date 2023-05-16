/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;

public class FloatHistogramTest extends HistogramTestCases {
    @Before
    public void setup() {
        useLinearBuckets = true;
        factory = new HistogramFactory() {
            @Override
            public Histogram create(double min, double max) {
                return new FloatHistogram(min, max);
            }
        };
    }

    @Test
    public void testBins() {
        super.testBinSizes(79, 141, new FloatHistogram(10e-6, 5, 20));
    }

    @Test
    public void testLinear() throws FileNotFoundException {
        super.doLinear(165.4, 18, 212);
    }
}
