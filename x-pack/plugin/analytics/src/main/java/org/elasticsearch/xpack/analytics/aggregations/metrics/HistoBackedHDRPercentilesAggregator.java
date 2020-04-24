/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.aggregations.metrics;

import org.HdrHistogram.DoubleHistogram;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.metrics.InternalHDRPercentiles;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public class HistoBackedHDRPercentilesAggregator extends AbstractHistoBackedHDRPercentilesAggregator {

    HistoBackedHDRPercentilesAggregator(String name, ValuesSource valuesSource, SearchContext context, Aggregator parent, double[] percents,
                             int numberOfSignificantValueDigits, boolean keyed, DocValueFormat formatter,
                             Map<String, Object> metadata) throws IOException {
        super(name, valuesSource, context, parent, percents, numberOfSignificantValueDigits, keyed, formatter, metadata);
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        DoubleHistogram state = getState(owningBucketOrdinal);
        if (state == null) {
            return buildEmptyAggregation();
        } else {
            return new InternalHDRPercentiles(name, keys, state, keyed, format, metadata());
        }
    }

    @Override
    public double metric(String name, long bucketOrd) {
        DoubleHistogram state = getState(bucketOrd);
        if (state == null) {
            return Double.NaN;
        } else {
            return state.getValueAtPercentile(Double.parseDouble(name));
        }
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        DoubleHistogram state;
        state = new DoubleHistogram(numberOfSignificantValueDigits);
        state.setAutoResize(true);
        return new InternalHDRPercentiles(name, keys, state, keyed, format, metadata());
    }
}
