/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.search.ScoreMode;
import org.elasticsearch.common.util.Comparators;
import org.elasticsearch.index.fielddata.FieldData;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;

public abstract class NumericMetricsAggregator extends MetricsAggregator {

    private NumericMetricsAggregator(String name, AggregationContext context, Aggregator parent, Map<String, Object> metadata)
        throws IOException {
        super(name, context, parent, metadata);
    }

    public abstract static class SingleValue extends NumericMetricsAggregator {

        protected SingleValue(String name, AggregationContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
            super(name, context, parent, metadata);
        }

        public abstract double metric(long owningBucketOrd);

        @Override
        public BucketComparator bucketComparator(String key, SortOrder order) {
            if (key != null && false == "value".equals(key)) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, """
                    Ordering on a single-value metrics aggregation can only be done on its value. \
                    Either drop the key (a la "%s") or change it to "value" (a la "%s.value")""", name(), name()));
            }
            return (lhs, rhs) -> Comparators.compareDiscardNaN(metric(lhs), metric(rhs), order == SortOrder.ASC);
        }
    }

    public abstract static class SingleDoubleValue extends SingleValue {

        private final ValuesSource.Numeric valuesSource;

        protected SingleDoubleValue(
            String name,
            ValuesSourceConfig valuesSourceConfig,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, context, parent, metadata);
            this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        }

        @Override
        public ScoreMode scoreMode() {
            return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public final LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub)
            throws IOException {
            final SortedNumericDoubleValues values = valuesSource.doubleValues(aggCtx.getLeafReaderContext());
            final NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
            return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
        }

        protected abstract LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, LeafBucketCollector sub);

        protected abstract LeafBucketCollector getLeafCollector(NumericDoubleValues values, LeafBucketCollector sub);
    }

    public abstract static class MultiValue extends NumericMetricsAggregator {

        protected MultiValue(String name, AggregationContext context, Aggregator parent, Map<String, Object> metadata) throws IOException {
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
                throw new IllegalArgumentException("Unknown metric name [" + key + "] on multi-value metrics aggregation [" + name() + "]");
            }
            // TODO it'd be faster replace hasMetric and metric with something that returned a function from long to double.
            return (lhs, rhs) -> Comparators.compareDiscardNaN(metric(key, lhs), metric(key, rhs), order == SortOrder.ASC);
        }
    }

    public abstract static class MultiDoubleValue extends MultiValue {

        private final ValuesSource.Numeric valuesSource;

        protected MultiDoubleValue(
            String name,
            ValuesSourceConfig valuesSourceConfig,
            AggregationContext context,
            Aggregator parent,
            Map<String, Object> metadata
        ) throws IOException {
            super(name, context, parent, metadata);
            this.valuesSource = (ValuesSource.Numeric) valuesSourceConfig.getValuesSource();
        }

        @Override
        public ScoreMode scoreMode() {
            return valuesSource.needsScores() ? ScoreMode.COMPLETE : ScoreMode.COMPLETE_NO_SCORES;
        }

        @Override
        public final LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub)
            throws IOException {
            final SortedNumericDoubleValues values = valuesSource.doubleValues(aggCtx.getLeafReaderContext());
            final NumericDoubleValues singleton = FieldData.unwrapSingleton(values);
            return singleton != null ? getLeafCollector(singleton, sub) : getLeafCollector(values, sub);
        }

        protected abstract LeafBucketCollector getLeafCollector(SortedNumericDoubleValues values, LeafBucketCollector sub);

        protected abstract LeafBucketCollector getLeafCollector(NumericDoubleValues values, LeafBucketCollector sub);
    }
}
