/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.analytics.cumulativecardinality;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.metrics.HyperLogLogPlusPlus;
import org.elasticsearch.search.aggregations.metrics.InternalCardinality;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class CumulativeCardinalityPipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;

    CumulativeCardinalityPipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
            histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
            InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;
        List<Bucket> newBuckets = new ArrayList<>(buckets.size());
        HyperLogLogPlusPlus hll = null;

        try {
            long cardinality = 0;
            for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                HyperLogLogPlusPlus bucketHll = resolveBucketValue(histo, bucket, bucketsPaths()[0]);
                if (hll == null && bucketHll != null) {
                    // We have to create a new HLL because otherwise it will alter the
                    // existing cardinality sketch and bucket value
                    hll = new HyperLogLogPlusPlus(bucketHll.precision(), reduceContext.bigArrays(), 1);
                }
                if (bucketHll != null) {
                    hll.merge(0, bucketHll, 0);
                    cardinality = hll.cardinality(0);
                }

                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                    .map((p) -> (InternalAggregation) p)
                    .collect(Collectors.toList());
                aggs.add(new InternalSimpleLongValue(name(), cardinality, formatter, metadata()));
                Bucket newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
                newBuckets.add(newBucket);
            }
            return factory.createAggregation(newBuckets);
        } finally {
            if (hll != null) {
                hll.close();
            }
        }
    }

    private HyperLogLogPlusPlus resolveBucketValue(MultiBucketsAggregation agg,
                                                   InternalMultiBucketAggregation.InternalBucket bucket,
                                                   String aggPath) {
        List<String> aggPathsList = AggregationPath.parse(aggPath).getPathElementsAsStringList();
        Object propertyValue = bucket.getProperty(agg.getName(), aggPathsList);
        if (propertyValue == null) {
            throw new AggregationExecutionException(AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
                + " must reference a cardinality aggregation");
        }

        if (propertyValue instanceof InternalCardinality) {
            return ((InternalCardinality) propertyValue).getCounts();
        }

        String currentAggName;
        if (aggPathsList.isEmpty()) {
            currentAggName = agg.getName();
        } else {
            currentAggName = aggPathsList.get(0);
        }

        throw new AggregationExecutionException(AbstractPipelineAggregationBuilder.BUCKETS_PATH_FIELD.getPreferredName()
            + " must reference a cardinality aggregation, got: ["
            + propertyValue.getClass().getSimpleName() + "] at aggregation [" + currentAggName + "]");
    }

}
