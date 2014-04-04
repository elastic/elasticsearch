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
package org.elasticsearch.search.aggregations.metrics.percentiles;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

/**
 *
 */
public class PercentilesAggregator extends MetricsAggregator.MultiValue {

    private final ValuesSource.Numeric valuesSource;
    private DoubleValues values;

    private final PercentilesEstimator estimator;
    private final boolean keyed;


    public PercentilesAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valuesSource, AggregationContext context,
                                 Aggregator parent, PercentilesEstimator estimator, boolean keyed) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.estimator = estimator;
    }

    @Override
    public boolean shouldCollect() {
        return valuesSource != null;
    }

    @Override
    public void setNextReader(AtomicReaderContext reader) {
        values = valuesSource.doubleValues();
    }

    @Override
    public void collect(int doc, long owningBucketOrdinal) throws IOException {
        final int valueCount = values.setDocument(doc);
        for (int i = 0; i < valueCount; i++) {
            estimator.offer(values.nextValue(), owningBucketOrdinal);
        }
    }

    @Override
    public boolean hasMetric(String name) {
        return PercentilesEstimator.indexOfPercent(estimator.percents, Double.parseDouble(name)) >= 0;
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        return estimator.result(owningBucketOrd).estimate(Double.parseDouble(name));
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        if (valuesSource == null) {
            return buildEmptyAggregation();
        }
        return new InternalPercentiles(name, estimator.result(owningBucketOrdinal), keyed);
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentiles(name, estimator.emptyResult(), keyed);
    }

    @Override
    protected void doClose() {
        estimator.close();
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private final PercentilesEstimator.Factory estimatorFactory;
        private final double[] percents;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig,
                       double[] percents, PercentilesEstimator.Factory estimatorFactory, boolean keyed) {
            super(name, InternalPercentiles.TYPE.name(), valuesSourceConfig);
            this.estimatorFactory = estimatorFactory;
            this.percents = percents;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentilesAggregator(name, 0, null, aggregationContext, parent, estimatorFactory.create(percents, 0, aggregationContext), keyed);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            PercentilesEstimator estimator = estimatorFactory.create(percents, expectedBucketsCount, aggregationContext);
            return new PercentilesAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, estimator, keyed);
        }
    }

}
