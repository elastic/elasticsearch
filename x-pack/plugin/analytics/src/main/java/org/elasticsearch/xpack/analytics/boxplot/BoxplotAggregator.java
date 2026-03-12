/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.HistogramUnionState;
import org.elasticsearch.search.aggregations.metrics.TDigestExecutionHint;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

public class BoxplotAggregator extends AbstractBoxplotAggregator {

    BoxplotAggregator(
        String name,
        ValuesSourceConfig config,
        DocValueFormat formatter,
        double compression,
        TDigestExecutionHint executionHint,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, config, formatter, compression, executionHint, context, parent, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource instanceof HistogramValuesSource.Histogram) {
            final HistogramValues values = ((HistogramValuesSource.Histogram) valuesSource).getHistogramValues(
                aggCtx.getLeafReaderContext()
            );
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (values.advanceExact(doc)) {
                        HistogramUnionState state = getExistingOrNewHistogram(bigArrays(), bucket);
                        final HistogramValue sketch = values.histogram();
                        while (sketch.next()) {
                            state.add(sketch.value(), sketch.count());
                        }
                    }
                }
            };
        } else {
            final SortedNumericDoubleValues values = ((ValuesSource.Numeric) valuesSource).doubleValues(aggCtx.getLeafReaderContext());
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    if (values.advanceExact(doc)) {
                        HistogramUnionState state = getExistingOrNewHistogram(bigArrays(), bucket);
                        final int valueCount = values.docValueCount();
                        for (int i = 0; i < valueCount; i++) {
                            state.add(values.nextValue());
                        }
                    }
                }
            };
        }
    }
}
