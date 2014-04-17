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
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.ArrayUtils;
import org.elasticsearch.common.util.ObjectArray;
import org.elasticsearch.index.fielddata.DoubleValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;

/**
 *
 */
public class PercentilesAggregator extends MetricsAggregator.MultiValue {

    private static int indexOfPercent(double[] percents, double percent) {
        return ArrayUtils.binarySearch(percents, percent, 0.001);
    }

    private final double[] percents;
    private final ValuesSource.Numeric valuesSource;
    private DoubleValues values;

    private ObjectArray<TDigestState> states;
    private final double compression;
    private final boolean keyed;


    public PercentilesAggregator(String name, long estimatedBucketsCount, ValuesSource.Numeric valuesSource, AggregationContext context,
                                 Aggregator parent, double[] percents, double compression, boolean keyed) {
        super(name, estimatedBucketsCount, context, parent);
        this.valuesSource = valuesSource;
        this.keyed = keyed;
        this.states = bigArrays.newObjectArray(estimatedBucketsCount);
        this.percents = percents;
        this.compression = compression;
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
    public void collect(int doc, long bucketOrd) throws IOException {
        states = bigArrays.grow(states, bucketOrd + 1);

        TDigestState state = states.get(bucketOrd);
        if (state == null) {
            state = new TDigestState(compression);
            states.set(bucketOrd, state);
        }

        final int valueCount = values.setDocument(doc);
        for (int i = 0; i < valueCount; i++) {
            state.add(values.nextValue());
        }
    }

    @Override
    public boolean hasMetric(String name) {
        return indexOfPercent(percents, Double.parseDouble(name)) >= 0;
    }

    private TDigestState getState(long bucketOrd) {
        if (bucketOrd >= states.size()) {
            return null;
        }
        final TDigestState state = states.get(bucketOrd);
        return state;
    }

    @Override
    public double metric(String name, long bucketOrd) {
        TDigestState state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return state.quantile(Double.parseDouble(name) / 100);
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalPercentiles(name, percents, state, keyed);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentiles(name, percents, new TDigestState(compression), keyed);
    }

    @Override
    protected void doClose() {
        Releasables.close(states);
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private final double[] percents;
        private final double compression;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig,
                double[] percents, double compression, boolean keyed) {
            super(name, InternalPercentiles.TYPE.name(), valuesSourceConfig);
            this.percents = percents;
            this.compression = compression;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentilesAggregator(name, 0, null, aggregationContext, parent, percents, compression, keyed);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new PercentilesAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, percents, compression, keyed);
        }
    }
}
