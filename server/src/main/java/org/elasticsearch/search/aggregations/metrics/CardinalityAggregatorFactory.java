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

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.index.query.QueryShardContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final Long precisionThreshold;

    CardinalityAggregatorFactory(String name, ValuesSourceConfig config,
                                    Long precisionThreshold,
                                    QueryShardContext queryShardContext,
                                    AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactoriesBuilder,
                                    Map<String, Object> metadata) throws IOException {
        super(name, config, queryShardContext, parent, subFactoriesBuilder, metadata);
        this.precisionThreshold = precisionThreshold;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.ALL_CORE,
            (name, valuesSourceConfig, precision, context, parent, metadata) -> {
                // super hacky but it shows the point of the approach
                if (valuesSourceConfig.hasValues()) {
                    ValuesSource valuesSource = valuesSourceConfig.getValuesSource();
                    if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
                        ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
                        // we compute the total number of terms across all segments
                        final long totalNonDistinctTerms = totalNonDistinctTerms(context, source);
                        final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
                        //  we assume there are 25% of repeated values
                        final long ordinalsMemoryUsage = totalNonDistinctTerms * 3;
                        // we do not consider the size if the bitSet, I think at most they can be ~1MB per bucket
                        if (ordinalsMemoryUsage < countsMemoryUsage) {
                            final long maxOrd = source.globalMaxOrd(context.searcher());
                            return new GlobalOrdCardinalityAggregator(name, source, precision, Math.toIntExact(maxOrd),
                                context, parent, metadata);
                        }
                    }
                }
                // fall back in default aggregator
                return new CardinalityAggregator(name, valuesSourceConfig, precision, context, parent, metadata);
            }, true);
    }

    private static long totalNonDistinctTerms(SearchContext context, ValuesSource.Bytes.WithOrdinals source) throws IOException {
        List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        long total = 0;
        for (LeafReaderContext leaf : leaves) {
            total += source.ordinalsValues(leaf).getValueCount();
        }
        return total;
    }

    @Override
    protected Aggregator createUnmapped(SearchContext searchContext,
                                            Aggregator parent,
                                            Map<String, Object> metadata) throws IOException {
        return new CardinalityAggregator(name, config, precision(), searchContext, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        SearchContext searchContext,
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return queryShardContext.getValuesSourceRegistry()
            .getAggregator(CardinalityAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, precision(), searchContext, parent, metadata);
    }

    private int precision() {
        return precisionThreshold == null
                ? HyperLogLogPlusPlus.DEFAULT_PRECISION
                : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }
}
