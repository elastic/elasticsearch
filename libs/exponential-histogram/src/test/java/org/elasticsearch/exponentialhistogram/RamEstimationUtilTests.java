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

import org.apache.lucene.util.RamUsageEstimator;

import static org.hamcrest.Matchers.equalTo;

public class RamEstimationUtilTests extends ExponentialHistogramTestCase {

    public void testLongArrayEstimation() {
        assertThat(RamEstimationUtil.estimateLongArray(0), equalTo(RamUsageEstimator.sizeOf(new long[0])));
        assertThat(RamEstimationUtil.estimateLongArray(1), equalTo(RamUsageEstimator.sizeOf(new long[1])));
        assertThat(RamEstimationUtil.estimateLongArray(1000), equalTo(RamUsageEstimator.sizeOf(new long[1000])));
    }

    public void testDoubleArrayEstimation() {
        assertThat(RamEstimationUtil.estimateDoubleArray(0), equalTo(RamUsageEstimator.sizeOf(new double[0])));
        assertThat(RamEstimationUtil.estimateDoubleArray(1), equalTo(RamUsageEstimator.sizeOf(new double[1])));
        assertThat(RamEstimationUtil.estimateDoubleArray(1000), equalTo(RamUsageEstimator.sizeOf(new double[1000])));
    }

    public void testIntArrayEstimation() {
        assertThat(RamEstimationUtil.estimateIntArray(0), equalTo(RamUsageEstimator.sizeOf(new int[0])));
        assertThat(RamEstimationUtil.estimateIntArray(1), equalTo(RamUsageEstimator.sizeOf(new int[1])));
        assertThat(RamEstimationUtil.estimateIntArray(1000), equalTo(RamUsageEstimator.sizeOf(new int[1000])));
    }

}
