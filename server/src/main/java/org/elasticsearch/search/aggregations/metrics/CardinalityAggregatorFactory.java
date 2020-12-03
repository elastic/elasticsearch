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
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.AggregatorFactory;
import org.elasticsearch.search.aggregations.CardinalityUpperBound;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceAggregatorFactory;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.search.aggregations.support.ValuesSourceRegistry;

import java.io.IOException;
import java.util.List;
import java.util.Map;

class CardinalityAggregatorFactory extends ValuesSourceAggregatorFactory {

    private final Long precisionThreshold;

    CardinalityAggregatorFactory(String name, ValuesSourceConfig config,
                                    Long precisionThreshold,
                                    AggregationContext context,
                                    AggregatorFactory parent,
                                    AggregatorFactories.Builder subFactoriesBuilder,
                                    Map<String, Object> metadata) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);
        this.precisionThreshold = precisionThreshold;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(CardinalityAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.ALL_CORE,
            (name, valuesSourceConfig, precision, context, parent, metadata) -> {
                // check global ords
                if (valuesSourceConfig.hasValues()) {
                    final ValuesSource valuesSource = valuesSourceConfig.getValuesSource();
                    if (valuesSource instanceof ValuesSource.Bytes.WithOrdinals) {
                        final ValuesSource.Bytes.WithOrdinals source = (ValuesSource.Bytes.WithOrdinals) valuesSource;
                        if (useGlobalOrds(context, source, precision)) {
                            final long maxOrd = source.globalMaxOrd(context.searcher());
                            return new GlobalOrdCardinalityAggregator(name, source, precision, Math.toIntExact(maxOrd),
                                context, parent, metadata);
                        }
                    }
                }
                // fallback in the default aggregator
                return new CardinalityAggregator(name, valuesSourceConfig, precision, context, parent, metadata);
            }, true);
    }

    private static boolean useGlobalOrds(AggregationContext context,
                                         ValuesSource.Bytes.WithOrdinals source,
                                         int precision) throws IOException {
        final List<LeafReaderContext> leaves = context.searcher().getIndexReader().leaves();
        // we compute the total number of terms across all segments
        long total = 0;
        for (LeafReaderContext leaf : leaves) {
            total += source.ordinalsValues(leaf).getValueCount();
        }
        final long countsMemoryUsage = HyperLogLogPlusPlus.memoryUsage(precision);
        // we assume there are 25% of repeated values when there is more than one leaf
        final long ordinalsMemoryUsage = leaves.size() == 1 ? total * 4L : total * 3L;
        // we do not consider the size if the bitSet, I think at most they can be ~1MB per bucket
        return ordinalsMemoryUsage < countsMemoryUsage;
    }

    @Override
    protected Aggregator createUnmapped(Aggregator parent, Map<String, Object> metadata) throws IOException {
        return new CardinalityAggregator(name, config, precision(), context, parent, metadata);
    }

    @Override
    protected Aggregator doCreateInternal(
        Aggregator parent,
        CardinalityUpperBound cardinality,
        Map<String, Object> metadata
    ) throws IOException {
        return context.getValuesSourceRegistry()
            .getAggregator(CardinalityAggregationBuilder.REGISTRY_KEY, config)
            .build(name, config, precision(), context, parent, metadata);
    }

    private int precision() {
        return precisionThreshold == null
                ? HyperLogLogPlusPlus.DEFAULT_PRECISION
                : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }
}
