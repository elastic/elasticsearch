/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.aggregations.pca;

import org.elasticsearch.search.MultiValueMode;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

final class PCAAggregator extends MatrixStatsAggregator {

    PCAAggregator(String name, Map<String, ValuesSource.Numeric> valuesSources, SearchContext context,
                  Aggregator parent, MultiValueMode multiValueMode, List<PipelineAggregator> pipelineAggregators,
                  Map<String,Object> metaData) throws IOException {
        super(name, valuesSources, context, parent, multiValueMode, pipelineAggregators, metaData);
    }

    @Override
    public InternalAggregation buildAggregation(long bucket) {
        if (valuesSources == null || bucket >= stats.size()) {
            return buildEmptyAggregation();
        }
        return new InternalPCAStats(name, stats.size(), stats.get(bucket), null, pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalPCAStats(name, 0, null, null, pipelineAggregators(), metaData());
    }
}
