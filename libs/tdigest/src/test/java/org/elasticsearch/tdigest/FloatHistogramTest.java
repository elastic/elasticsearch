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
