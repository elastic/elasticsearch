/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.elasticsearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

public class DerivativePipelineAggregator extends PipelineAggregator {
    private final DocValueFormat formatter;
    private final GapPolicy gapPolicy;
    private final Double xAxisUnits;

    DerivativePipelineAggregator(String name, String[] bucketsPaths, DocValueFormat formatter, GapPolicy gapPolicy, Long xAxisUnits,
                                 Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.xAxisUnits = xAxisUnits == null ? null : (double) xAxisUnits;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends InternalMultiBucketAggregation.InternalBucket>
                histo = (InternalMultiBucketAggregation<? extends InternalMultiBucketAggregation, ? extends
                InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        Number lastBucketKey = null;
        Double lastBucketValue = null;
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Number thisBucketKey = factory.getKey(bucket);
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            if (lastBucketValue != null && thisBucketValue != null) {
                double gradient = thisBucketValue - lastBucketValue;
                double xDiff = -1;
                if (xAxisUnits != null) {
                    xDiff = (thisBucketKey.doubleValue() - lastBucketKey.doubleValue()) / xAxisUnits;
                }
                final List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false).map((p) -> {
                    return (InternalAggregation) p;
                }).collect(Collectors.toList());
                aggs.add(new InternalDerivative(name(), gradient, xDiff, formatter, metadata()));
                Bucket newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), new InternalAggregations(aggs));
                newBuckets.add(newBucket);
            } else {
                newBuckets.add(bucket);
            }
            lastBucketKey = thisBucketKey;
            lastBucketValue = thisBucketValue;
        }
        return factory.createAggregation(newBuckets);
    }
}
