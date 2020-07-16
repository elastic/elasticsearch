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

package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Constructs the per-shard aggregator instance for histogram aggregation.  Selects the numeric or range field implementation based on the
 * field type.
 */
public final class HistogramAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final double interval, offset;
    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final double minBound, maxBound;
    private final DoubleBounds hardBounds;

    static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(HistogramAggregationBuilder.NAME, CoreValuesSourceType.RANGE,
            (HistogramAggregatorSupplier) RangeHistogramAggregator::new);

        builder.register(HistogramAggregationBuilder.NAME,
            List.of(CoreValuesSourceType.NUMERIC, CoreValuesSourceType.DATE, CoreValuesSourceType.BOOLEAN),
            (HistogramAggregatorSupplier) NumericHistogramAggregator::new);
    }

    public HistogramAggregatorFactory(String name,
                                        ValuesSourceConfig config,
                                        double interval,
                                        double offset,
                                        BucketOrder order,
                                        boolean keyed,
                                        long minDocCount,
                                        double minBound,
                                        double maxBound,
                                        DoubleBounds hardBounds,
                                        QueryShardContext queryShardContext,
                                        AggregatorFactory parent,
                                        AggregatorFactories.Builder subFactoriesBuilder,
                                        Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.interval = interval;
        this.offset = offset;
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.hardBounds = hardBounds;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @Override
    protected Aggregator doCreateInternal(SearchContext searchContext,
                                          Aggregator parent,
                                          CardinalityUpperBound cardinality,
                                          Map<String, Object> metadata) throws IOException {
        AggregatorSupplier aggregatorSupplier = queryShardContext.getValuesSourceRegistry().getAggregator(config,
            HistogramAggregationBuilder.NAME);
        if (aggregatorSupplier instanceof HistogramAggregatorSupplier == false) {
            throw new AggregationExecutionException("Registry miss-match - expected HistogramAggregatorSupplier, found [" +
                aggregatorSupplier.getClass().toString() + "]");
        }
        HistogramAggregatorSupplier histogramAggregatorSupplier = (HistogramAggregatorSupplier) aggregatorSupplier;
        return histogramAggregatorSupplier.build(name, factories, interval, offset, order, keyed, minDocCount, minBound, maxBound,
            hardBounds, config, searchContext, parent, cardinality, metadata);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new NumericHistogramAggregator(name, factories, interval, offset, order, keyed, minDocCount, minBound, maxBound,
            hardBounds, config, searchContext, parent, CardinalityUpperBound.NONE, metadata);
    }
}
