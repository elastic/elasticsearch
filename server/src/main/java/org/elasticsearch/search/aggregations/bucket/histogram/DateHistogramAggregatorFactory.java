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

import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.mapper.RangeType;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public final class DateHistogramAggregatorFactory
        extends ValuesSourceAggregatorFactory<ValuesSource> {

    private final BucketOrder order;
    private final boolean keyed;
    private final long minDocCount;
    private final ExtendedBounds extendedBounds;
    private final Rounding rounding;
    private final Rounding shardRounding;

    public DateHistogramAggregatorFactory(String name, ValuesSourceConfig<ValuesSource> config,
            BucketOrder order, boolean keyed, long minDocCount,
            Rounding rounding, Rounding shardRounding, ExtendedBounds extendedBounds, QueryShardContext queryShardContext,
            AggregatorFactory parent, AggregatorFactories.Builder subFactoriesBuilder,
            Map<String, Object> metaData) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metaData);
        this.order = order;
        this.keyed = keyed;
        this.minDocCount = minDocCount;
        this.extendedBounds = extendedBounds;
        this.rounding = rounding;
        this.shardRounding = shardRounding;
    }

    public long minDocCount() {
        return minDocCount;
    }

    @Override
    protected ValuesSource resolveMissingAny(Object missing) {
        if (missing instanceof Number) {
            return ValuesSource.Numeric.EMPTY;
        }
        throw new IllegalArgumentException("Only numeric missing values are supported for date histogram aggregation, found ["
            + missing + "]");
    }

    @Override
    protected Aggregator doCreateInternal(ValuesSource valuesSource,
                                            SearchContext searchContext,
                                            Aggregator parent,
                                            boolean collectsFromSingleBucket,
                                            List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData) throws IOException {
        if (collectsFromSingleBucket == false) {
            return asMultiBucketAggregator(this, searchContext, parent);
        }
        if (valuesSource instanceof ValuesSource.Numeric) {
            return createAggregator((ValuesSource.Numeric) valuesSource, searchContext, parent, pipelineAggregators, metaData);
        } else if (valuesSource instanceof ValuesSource.Range) {
            ValuesSource.Range rangeValueSource = (ValuesSource.Range) valuesSource;
            if (rangeValueSource.rangeType() != RangeType.DATE) {
                throw new IllegalArgumentException("Expected date range type but found range type [" + rangeValueSource.rangeType().name
                    + "]");
            }
            return createRangeAggregator((ValuesSource.Range) valuesSource, searchContext, parent, pipelineAggregators, metaData);
        }
        else {
            throw new IllegalArgumentException("Expected one of [Date, Range] values source, found ["
                + valuesSource.toString() + "]");
        }
    }

    private Aggregator createAggregator(ValuesSource.Numeric valuesSource, SearchContext searchContext,
                                        Aggregator parent, List<PipelineAggregator> pipelineAggregators,
            Map<String, Object> metaData) throws IOException {
        return new DateHistogramAggregator(name, factories, rounding, shardRounding, order, keyed, minDocCount, extendedBounds,
                valuesSource, config.format(), searchContext, parent, pipelineAggregators, metaData);
    }

    private Aggregator createRangeAggregator(ValuesSource.Range valuesSource,
                                             SearchContext searchContext,
                                             Aggregator parent,
                                             List<PipelineAggregator> pipelineAggregators,
                                             Map<String, Object> metaData) throws IOException {
        return new DateRangeHistogramAggregator(name, factories, rounding, shardRounding, order, keyed, minDocCount, extendedBounds,
            valuesSource, config.format(), searchContext, parent, pipelineAggregators, metaData);
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            List<PipelineAggregator> pipelineAggregators,
                                            Map<String, Object> metaData) throws IOException {
        return createAggregator(null, searchContext, parent, pipelineAggregators, metaData);
    }
}
