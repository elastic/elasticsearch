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
package org.elasticsearch.search.aggregations.metrics;

import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Map;

public abstract class NumericMetricsAggregator extends MetricsAggregator {

    private NumericMetricsAggregator(String name, SearchContext context, Aggregator parent,
            Map<String, Object> metadata) throws IOException {
        super(name, context, parent, metadata);
    }

    public abstract static class SingleValue extends NumericMetricsAggregator {

        protected SingleValue(String name, SearchContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
            super(name, context, parent, metadata);
        }

        public abstract double metric(long owningBucketOrd);

        @Override
        public BucketComparator bucketComparator(String key, SortOrder order) {
            if (key != null && false == "value".equals(key)) {
                throw new IllegalArgumentException("Ordering on a single-value metrics aggregation can only be done on its value. " +
                        "Either drop the key (a la \"" + name() + "\") or change it to \"value\" (a la \"" + name() + ".value\")");
            }
            return (lhs, rhs) -> Comparators.compareDiscardNaN(metric(lhs), metric(rhs), order == SortOrder.ASC);
        }
    }

    public abstract static class MultiValue extends NumericMetricsAggregator {

        protected MultiValue(String name, SearchContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
            super(name, context, parent, metadata);
        }

        public abstract boolean hasMetric(String name);

        public abstract double metric(String name, long owningBucketOrd);

        @Override
        public BucketComparator bucketComparator(String key, SortOrder order) {
            if (key == null) {
                throw new IllegalArgumentException("When ordering on a multi-value metrics aggregation a metric name must be specified.");
            }
            if (false == hasMetric(key)) {
                throw new IllegalArgumentException(
                        "Unknown metric name [" + key + "] on multi-value metrics aggregation [" + name() + "]");
            }
            // TODO it'd be faster replace hasMetric and metric with something that returned a function from long to double.
            return (lhs, rhs) -> Comparators.compareDiscardNaN(metric(key, lhs), metric(key, rhs), order == SortOrder.ASC);
        }
    }
}
