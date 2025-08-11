/*
 * Copyright Elasticsearch B.V., and/or licensed to Elasticsearch B.V.
 * under one or more license agreements. See the NOTICE file distributed with
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
 * This file is based on a modification of https://github.com/open-telemetry/opentelemetry-java which is licensed under the Apache 2.0 License.
 */

package org.elasticsearch.exponentialhistogram;

import org.elasticsearch.test.ESTestCase;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class ExponentialHistogramGeneratorTests extends ESTestCase {

    public void testVeryLargeValue() {
        double value = Double.MAX_VALUE / 10;
        ExponentialHistogram histo = ExponentialHistogram.create(1, value);

        long index = histo.positiveBuckets().iterator().peekIndex();
        int scale = histo.scale();

        double lowerBound = ExponentialScaleUtils.getLowerBucketBoundary(index, scale);
        double upperBound = ExponentialScaleUtils.getUpperBucketBoundary(index, scale);

        assertThat("Lower bucket boundary should be smaller than value", lowerBound, lessThanOrEqualTo(value));
        assertThat("Upper bucket boundary should be greater than value", upperBound, greaterThanOrEqualTo(value));
    }

}
