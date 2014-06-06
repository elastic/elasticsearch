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

import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.percentiles.tdigest.TDigestState;
import org.elasticsearch.search.aggregations.support.*;
import org.elasticsearch.search.aggregations.support.ValuesSource.Numeric;

/**
 *
 */
public class PercentileRanksAggregator extends AbstractPercentilesAggregator {

    public PercentileRanksAggregator(String name, long estimatedBucketsCount, Numeric valuesSource, AggregationContext context,
            Aggregator parent, double[] percents, double compression, boolean keyed) {
        super(name, estimatedBucketsCount, valuesSource, context, parent, percents, compression, keyed);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalPercentileRanks(name, keys, state, keyed);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPercentileRanks(name, keys, new TDigestState(compression), keyed);
    }

    @Override
    public double metric(String name, long bucketOrd) {
        TDigestState state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return InternalPercentileRanks.percentileRank(state, Double.valueOf(name));
        }
    }

    public static class Factory extends ValuesSourceAggregatorFactory.LeafOnly<ValuesSource.Numeric> {

        private final double[] values;
        private final double compression;
        private final boolean keyed;

        public Factory(String name, ValuesSourceConfig<ValuesSource.Numeric> valuesSourceConfig,
                double[] values, double compression, boolean keyed) {
            super(name, InternalPercentiles.TYPE.name(), valuesSourceConfig);
            this.values = values;
            this.compression = compression;
            this.keyed = keyed;
        }

        @Override
        protected Aggregator createUnmapped(AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileRanksAggregator(name, 0, null, aggregationContext, parent, values, compression, keyed);
        }

        @Override
        protected Aggregator create(ValuesSource.Numeric valuesSource, long expectedBucketsCount, AggregationContext aggregationContext, Aggregator parent) {
            return new PercentileRanksAggregator(name, expectedBucketsCount, valuesSource, aggregationContext, parent, values, compression, keyed);
        }
    }
}
