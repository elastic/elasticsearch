/*
 * Licensed to Elasticsearch B.V. under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch B.V. licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 * This project is based on a modification of https://github.com/tdunning/t-digest which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.tdigest;

import org.junit.Before;

import java.io.IOException;

public class LogHistogramTests extends HistogramTestCases {
    @Before
    public void setup() {
        useLinearBuckets = false;
        factory = new HistogramFactory() {
            @Override
            public Histogram create(double min, double max) {
                return new LogHistogram(min, max, 0.05);
            }
        };
    }

    public void testApproxLog() {
        double x = 1e-6;
        for (int i = 0; i < 1000; i++) {
            assertEquals(Math.log(x) / Math.log(2), LogHistogram.approxLog2(x), 0.01);
            x *= 1.0 + Math.PI / 100.0;
        }
        assertTrue("Insufficient range", x > 1e6);
    }

    public void testInverse() {
        for (double x = 0.001; x <= 100; x += 1e-3) {
            double log = LogHistogram.approxLog2(x);
            double roundTrip = LogHistogram.pow2(log);
            assertEquals(x, roundTrip, 1e-13);
        }

    }

    public void testBins() {
        super.testBinSizes(72, 129, new LogHistogram(10e-6, 5, 0.1));
    }

    public void testLinear() throws IOException {
        super.doLinear(146, 17, 189);
    }
}
