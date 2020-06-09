package org.elasticsearch.search.aggregations.bucket.histogram;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

@FunctionalInterface
public interface VariableWidthHistogramAggregatorSupplier extends AggregatorSupplier {
    Aggregator build(
        String name,
        AggregatorFactories factories,
        int numBuckets,
        int shardSize,
        int initialBuffer,
        @Nullable ValuesSource valuesSource,
        DocValueFormat formatter,
        SearchContext aggregationContext,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException;
}
