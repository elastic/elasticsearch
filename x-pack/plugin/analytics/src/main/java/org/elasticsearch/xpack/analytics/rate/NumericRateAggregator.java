/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.AggregationExecutionContext;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.LeafBucketCollectorBase;
import org.elasticsearch.search.aggregations.bucket.DocCountProvider;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;

import java.io.IOException;
import java.util.Map;

public class NumericRateAggregator extends AbstractRateAggregator {

    private final DocCountProvider docCountProvider;

    public NumericRateAggregator(
        String name,
        ValuesSourceConfig valuesSourceConfig,
        Rounding.DateTimeUnit rateUnit,
        RateMode rateMode,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, valuesSourceConfig, rateUnit, rateMode, context, parent, metadata);
        docCountProvider = computeWithDocCount ? new DocCountProvider() : null;
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        final CompensatedSum kahanSummation = new CompensatedSum(0, 0);
        if (computeWithDocCount) {
            // No field or script has been set at the rate agg. So, rate will be computed based on the doc_counts.
            // This implementation hard-wires the DocCountProvider and reads the _doc_count fields when available.
            // A better approach would be to create a DOC_COUNT ValuesSource type and use that as valuesSource
            // In that case the computeRateOnDocs variable and this branch of the if-statement are not required.
            docCountProvider.setLeafReaderContext(aggCtx.getLeafReaderContext());
            return new LeafBucketCollectorBase(sub, null) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    sums = bigArrays().grow(sums, bucket + 1);
                    compensations = bigArrays().grow(compensations, bucket + 1);
                    // Compute the sum of double values with Kahan summation algorithm which is more
                    // accurate than naive summation.
                    double sum = sums.get(bucket);
                    double compensation = compensations.get(bucket);
                    kahanSummation.reset(sum, compensation);

                    final int docCount = docCountProvider.getDocCount(doc);
                    kahanSummation.add(docCount);
                    compensations.set(bucket, kahanSummation.delta());
                    sums.set(bucket, kahanSummation.value());
                }
            };
        } else {
            final SortedNumericDoubleValues values = ((ValuesSource.Numeric) valuesSource).doubleValues(aggCtx.getLeafReaderContext());
            return new LeafBucketCollectorBase(sub, values) {
                @Override
                public void collect(int doc, long bucket) throws IOException {
                    sums = bigArrays().grow(sums, bucket + 1);
                    compensations = bigArrays().grow(compensations, bucket + 1);

                    if (values.advanceExact(doc)) {
                        final int valuesCount = values.docValueCount();
                        // Compute the sum of double values with Kahan summation algorithm which is more
                        // accurate than naive summation.
                        double sum = sums.get(bucket);
                        double compensation = compensations.get(bucket);
                        kahanSummation.reset(sum, compensation);
                        switch (rateMode) {
                            case SUM:
                                for (int i = 0; i < valuesCount; i++) {
                                    kahanSummation.add(values.nextValue());
                                }
                                break;
                            case VALUE_COUNT:
                                kahanSummation.add(valuesCount);
                                break;
                            default:
                                throw new IllegalArgumentException("Unsupported rate mode " + rateMode);
                        }

                        compensations.set(bucket, kahanSummation.delta());
                        sums.set(bucket, kahanSummation.value());
                    }
                }
            };
        }
    }
}
