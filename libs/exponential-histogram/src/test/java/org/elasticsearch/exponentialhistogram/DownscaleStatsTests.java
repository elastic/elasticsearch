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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.IntStream;
import java.util.stream.LongStream;

import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MAX_INDEX_BITS;
import static org.elasticsearch.exponentialhistogram.ExponentialHistogram.MIN_INDEX;
import static org.hamcrest.Matchers.equalTo;

public class DownscaleStatsTests extends ExponentialHistogramTestCase {

    public void testExponential() {
        long[] values = IntStream.range(0, 100).mapToLong(i -> (long) Math.min(MAX_INDEX, Math.pow(1.1, i))).distinct().toArray();
        verifyFor(values);
    }

    public void testNumericalLimits() {
        verifyFor(MIN_INDEX, MAX_INDEX);
    }

    public void testRandom() {
        for (int i = 0; i < 100; i++) {
            List<Long> values = IntStream.range(0, 1000).mapToObj(j -> randomLongBetween(MIN_INDEX, MAX_INDEX)).distinct().toList();
            verifyFor(values);
        }
    }

    void verifyFor(long... indices) {
        verifyFor(LongStream.of(indices).boxed().toList());
    }

    void verifyFor(Collection<Long> indices) {
        // sanity check, we require unique indices
        assertThat(indices.size(), equalTo(new HashSet<>(indices).size()));

        List<Long> sorted = new ArrayList<>(indices);
        sorted.sort(Long::compareTo);

        DownscaleStats stats = new DownscaleStats();
        for (int i = 1; i < sorted.size(); i++) {
            long prev = sorted.get(i - 1);
            long curr = sorted.get(i);
            stats.add(prev, curr);
        }

        for (int i = 0; i <= MAX_INDEX_BITS; i++) {
            int scaleReduction = i;
            long remainingCount = indices.stream().mapToLong(Long::longValue).map(index -> index >> scaleReduction).distinct().count();
            long reduction = sorted.size() - remainingCount;

            assertThat(
                "Expected size after reduction of " + i + " to match",
                stats.getCollapsedBucketCountAfterScaleReduction(scaleReduction),
                equalTo((int) reduction)
            );
        }

    }
}
