/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

/*
 * Licensed to Ted Dunning under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.tdigest;

import org.junit.Before;
import org.junit.Test;

import java.io.FileNotFoundException;
import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LogHistogramTest extends HistogramTestCases {
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


    @Test
    public void testApproxLog() {
        double x = 1e-6;
        for (int i = 0; i < 1000; i++) {
            assertEquals(Math.log(x) / Math.log(2), LogHistogram.approxLog2(x), 0.01);
            x *= 1.0 + Math.PI / 100.0;
        }
        assertTrue("Insufficient range", x > 1e6);
    }

    @Test
    public void testInverse() {
        for (double x = 0.001; x <= 100; x += 1e-3) {
            double log = LogHistogram.approxLog2(x);
            double roundTrip = LogHistogram.pow2(log);
            assertEquals(x, roundTrip, 1e-13);
        }

    }

    @Test
    public void testBins() {
        super.testBinSizes(72, 129, new LogHistogram(10e-6, 5, 0.1));
    }

    @Test
    public void testLinear() throws FileNotFoundException {
        super.doLinear(146, 17, 189);
    }

    @Override
    public void testCompression() {
        //ignore
    }

    @Override
    public void testSerialization() {
        //ignore
    }
}
