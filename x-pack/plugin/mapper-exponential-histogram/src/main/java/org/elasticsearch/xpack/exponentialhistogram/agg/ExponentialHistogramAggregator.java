/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.exponentialhistogram.agg;

import org.elasticsearch.index.fielddata.HistogramValue;
import org.elasticsearch.index.fielddata.HistogramValues;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.*;
import org.elasticsearch.search.aggregations.metrics.MetricsAggregator;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.ValuesSourceConfig;
import org.elasticsearch.xpack.exponentialhistogram.ExponentialHistogramValuesSource;
import org.elasticsearch.xpack.exponentialhistogram.otel.Base2ExponentialHistogramIndexer;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public final class ExponentialHistogramAggregator extends MetricsAggregator {

    private final ExponentialHistogramValuesSource.Histogram valuesSource;
    private final DocValueFormat formatter;

    private final int maxBuckets;
    private int maxScale;

    public ExponentialHistogramAggregator(
        String name,
        AggregatorFactories factories,
        int maxBuckets,
        int maxScale,
        ValuesSourceConfig valuesSourceConfig,
        AggregationContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.valuesSource = (ExponentialHistogramValuesSource.Histogram) valuesSourceConfig.getValuesSource();
        this.formatter = valuesSourceConfig.format();
        this.maxBuckets = maxBuckets;
        this.maxScale = maxScale;

        // Sub aggregations are not allowed when running histogram agg over histograms
        if (subAggregators().length > 0) {
            throw new IllegalArgumentException("Histogram aggregation on histogram fields does not support sub-aggregations");
        }
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrd) throws IOException {
        System.out.println("buildAggregation: " + owningBucketOrd);
        return buildEmptyAggregation();
        /*
        InternalExponentialHistogram(
            String name,
            List< InternalExponentialHistogram.Bucket > buckets,
            InternalExponentialHistogram.EmptyBucketInfo emptyBucketInfo,
        int maxBuckets,
        int maxScale,
        DocValueFormat formatter,
        Map<String, Object> metaData
        */
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        final InternalExponentialHistogram.EmptyBucketInfo emptyBucketInfo =
            new InternalExponentialHistogram.EmptyBucketInfo(buildEmptySubAggregations());
        return new InternalExponentialHistogram(
            name(),
            Collections.emptyList(),
            emptyBucketInfo,
            maxBuckets,
            maxScale,
            formatter,
            metadata()
        );
    }

    @Override
    public LeafBucketCollector getLeafCollector(AggregationExecutionContext aggCtx, final LeafBucketCollector sub) throws IOException {
        if (valuesSource == null) {
            return LeafBucketCollector.NO_OP_COLLECTOR;
        }

        final HistogramValues values = valuesSource.getHistogramValues(aggCtx.getLeafReaderContext());
        return new LeafBucketCollectorBase(sub, values) {
            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                System.out.println("Leaf: owningBucketOrder: " + owningBucketOrd);

                final int scale = maxScale;
                final double scaleBase = Math.pow(2, Math.pow(2, -scale));
                final Base2ExponentialHistogramIndexer indexer = Base2ExponentialHistogramIndexer.get(scale);
                //Math.pow(scaleBase, index+1) * (iterator >= numNegativeCounts ? 1 : -1)

                if (values.advanceExact(doc)) {
                    final HistogramValue sketch = values.histogram();

                    double previousKey = Double.NEGATIVE_INFINITY;
                    while (sketch.next()) {
                        final double value = sketch.value();
                        if (value == 0) {
                            throw new RuntimeException("zero_count not implemented");
                        }
                        final long count = sketch.count();

                        final int index = indexer.computeIndex(Math.abs(value));
                        final double key = Math.pow(scaleBase, index+1) * Math.signum(value);
                        System.out.printf("value=%.2f count=%d index=%d key=%.2f\n", value, count, index, key);

                        /*
                        assert key >= previousKey;
                        long bucketOrd = bucketOrds.add(owningBucketOrd, Double.doubleToLongBits(key));
                        System.out.println("  bucketOrd: " + bucketOrd);
                        if (bucketOrd < 0) { // already seen
                            bucketOrd = -1 - bucketOrd;
                            System.out.println("  already seen, new bucketOrd: " + bucketOrd);
                            collectExistingBucket(sub, doc, bucketOrd);
                        } else {
                            collectBucket(sub, doc, bucketOrd);
                        }
                        // We have added the document already and we have incremented bucket doc_count
                        // by _doc_count times. To compensate for this, we should increment doc_count by
                        // (count - _doc_count) so that we have added it count times.
                        incrementBucketDocCount(bucketOrd, count - docCountProvider.getDocCount(doc));
                        previousKey = key;
                         */
                    }
                }
            }
        };
    }
}
