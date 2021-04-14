/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.common.metrics;

import java.util.concurrent.atomic.LongAdder;

public final class MeanMetric {

    private final LongAdder counter = new LongAdder();
    private final LongAdder sum = new LongAdder();

    public void inc(long n) {
        counter.increment();
        sum.add(n);
    }

    /**
     * Returns the current count of this metric. This metric supports only {@link #inc(long)} that increases the counter
     * whenever it's invoked; hence, the returned count is always non-negative.
     */
    public long count() {
        final long count = counter.sum();
        assert count >= 0 : "Count of MeanMetric must always be non-negative; got " + count;
        return count;
    }

    public long sum() {
        return sum.sum();
    }

    public double mean() {
        long count = count();
        if (count > 0) {
            return sum.sum() / (double) count;
        }
        return 0.0;
    }
}
