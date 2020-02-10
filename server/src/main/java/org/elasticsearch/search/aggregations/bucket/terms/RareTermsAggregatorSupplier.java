package org.elasticsearch.search.aggregations.bucket.terms;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregatorSupplier;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

interface RareTermsAggregatorSupplier extends AggregatorSupplier {
    Aggregator build(String name,
                     AggregatorFactories factories,
                     ValuesSource valuesSource,
                     DocValueFormat format,
                     int maxDocCount,
                     double precision,
                     IncludeExclude includeExclude,
                     SearchContext context,
                     Aggregator parent,
                     List<PipelineAggregator> pipelineAggregators,
                     Map<String, Object> metaData) throws IOException;
}
