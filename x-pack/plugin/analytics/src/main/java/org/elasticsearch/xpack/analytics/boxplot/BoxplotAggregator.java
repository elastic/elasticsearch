/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.AbstractTDigestPercentilesAggregator;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class BoxplotAggregator extends AbstractTDigestPercentilesAggregator {

    BoxplotAggregator(String name, ValuesSource valuesSource, DocValueFormat formatter, double compression,
                      SearchContext context, Aggregator parent, List<PipelineAggregator> pipelineAggregators,
                      Map<String, Object> metaData) throws IOException {
        super(name, valuesSource, context, parent, null, compression, false, formatter, pipelineAggregators,
            metaData);
    }

    @Override
    public boolean hasMetric(String name) {
        try {
            InternalBoxplot.Metrics.resolve(name);
            return true;
        } catch (IllegalArgumentException iae) {
            return false;
        }
    }

    @Override
    public double metric(String name, long owningBucketOrd) {
        TDigestState state = null;
        if (valuesSource != null && owningBucketOrd < states.size()) {
            state = states.get(owningBucketOrd);
        }
        return InternalBoxplot.Metrics.resolve(name).value(state);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalBoxplot(name, state, formatter, pipelineAggregators(), metaData());
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalBoxplot(name, new TDigestState(compression), formatter, pipelineAggregators(), metaData());
    }
}
