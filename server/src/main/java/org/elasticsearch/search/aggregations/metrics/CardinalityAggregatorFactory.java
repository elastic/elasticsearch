/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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
    private final CardinalityAggregatorSupplier aggregatorSupplier;

    CardinalityAggregatorFactory(
        String name,
        ValuesSourceConfig config,
        Long precisionThreshold,
        AggregationContext context,
        AggregatorFactory parent,
        AggregatorFactories.Builder subFactoriesBuilder,
        Map<String, Object> metadata,
        CardinalityAggregatorSupplier aggregatorSupplier
    ) throws IOException {
        super(name, config, context, parent, subFactoriesBuilder, metadata);

        this.aggregatorSupplier = aggregatorSupplier;
        this.precisionThreshold = precisionThreshold;
    }

    public static void registerAggregators(ValuesSourceRegistry.Builder builder) {
        builder.register(
            CardinalityAggregationBuilder.REGISTRY_KEY,
            CoreValuesSourceType.ALL_CORE,
            (name, valuesSourceConfig, precision, context, parent, metadata) -> {
                // check global ords
                if (valuesSourceConfig.hasValues()) {
                    if (valuesSourceConfig.getValuesSource()instanceof final ValuesSource.Bytes.WithOrdinals source) {
                        if (useGlobalOrds(context, source, precision)) {
                            final long maxOrd = source.globalMaxOrd(context.searcher());
                            return new GlobalOrdCardinalityAggregator(
                                name,
                                source,
                                precision,
                                Math.toIntExact(maxOrd),
                                context,
                                parent,
                                metadata
                            );
                        }
                    }
                }
                // fallback in the default aggregator
                return new CardinalityAggregator(name, valuesSourceConfig, precision, context, parent, metadata);
            },
            true
        );
    }

    private static boolean useGlobalOrds(AggregationContext context, ValuesSource.Bytes.WithOrdinals source, int precision)
        throws IOException {
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
    protected Aggregator doCreateInternal(Aggregator parent, CardinalityUpperBound cardinality, Map<String, Object> metadata)
        throws IOException {
        return aggregatorSupplier.build(name, config, precision(), context, parent, metadata);
    }

    private int precision() {
        return precisionThreshold == null
            ? HyperLogLogPlusPlus.DEFAULT_PRECISION
            : HyperLogLogPlusPlus.precisionFromThreshold(precisionThreshold);
    }
}
