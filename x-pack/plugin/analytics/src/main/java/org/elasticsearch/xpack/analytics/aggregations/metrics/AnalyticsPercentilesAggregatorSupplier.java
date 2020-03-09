/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.metrics.PercentilesAggregatorSupplier;
import org.elasticsearch.search.aggregations.metrics.PercentilesConfig;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface AnalyticsPercentilesAggregatorSupplier extends PercentilesAggregatorSupplier {
    Aggregator build(String name,
                     ValuesSource valuesSource,
                     SearchContext context,
                     Aggregator parent,
                     double[] percents,
                     PercentilesConfig percentilesConfig,
                     boolean keyed,
                     DocValueFormat formatter,
                     List<PipelineAggregator> pipelineAggregators,
                     Map<String, Object> metaData) throws IOException;
}
