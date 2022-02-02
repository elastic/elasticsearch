/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.apache.lucene.index.LeafReaderContext;
import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.analytics.aggregations.support.HistogramValuesSource;

import java.io.IOException;
import java.util.Map;

public class HistogramRateAggregator extends AbstractRateAggregator {
    public HistogramRateAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        Rounding.DateTimeUnit rateUnit,
        RateMode rateMode,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, rateUnit, rateMode, context, parent, metadata);
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, final LeafBucketCollector sub) throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        final HistogramValues values = ((HistogramValuesSource.Histogram) valuesSource).getHistogramValues(ctx);
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long bucket) throws IOException {
                sums = bigArrays().grow(sums, bucket + 1);
                compensations = bigArrays().grow(compensations, bucket + 1);

                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();
                    while (sketch.next()) {
                        double sum = sums.get(bucket);
                        double compensation = compensations.get(bucket);
                        kahanSummation.reset(sum, compensation);
                        final double value = switch (rateMode) {
                            case SUM -> sketch.value();
                            case VALUE_COUNT -> sketch.count();
                        };
                        kahanSummation.add(value);
                        compensations.set(bucket, kahanSummation.delta());
                        sums.set(bucket, kahanSummation.value());
                    }
                }
            }
        };
    }
}
