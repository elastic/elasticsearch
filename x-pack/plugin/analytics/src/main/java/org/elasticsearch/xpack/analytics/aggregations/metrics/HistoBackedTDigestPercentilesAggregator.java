/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalTDigestPercentiles;
import org.elasticsearch.search.aggregations.metrics.TDigestState;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

class HistoBackedTDigestPercentilesAggregator extends AbstractHistoBackedTDigestPercentilesAggregator {

    HistoBackedTDigestPercentilesAggregator(String name,
                                 ValuesSource valuesSource,
                                 SearchContext context,
                                 Aggregator parent,
                                 double[] percents,
                                 double compression,
                                 boolean keyed,
                                 DocValueFormat formatter,
                                 Map<String, Object> metadata) throws IOException {
        super(name, valuesSource, context, parent, percents, compression, keyed, formatter, metadata);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        TDigestState state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalTDigestPercentiles(name, keys, state, keyed, formatter, metadata());
        }
    }

    @Override
    public double metric(String name, long bucketOrd) {
        TDigestState state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return state.quantile(Double.parseDouble(name) / 100);
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalTDigestPercentiles(name, keys, new TDigestState(compression), keyed, formatter, metadata());
    }
}
