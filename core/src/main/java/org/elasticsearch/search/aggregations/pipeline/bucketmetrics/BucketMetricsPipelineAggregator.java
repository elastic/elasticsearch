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

package org.elasticsearch.search.aggregations.pipeline.bucketmetrics;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.format.ValueFormatter;
import org.elasticsearch.search.aggregations.support.format.ValueFormatterStreams;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A class of sibling pipeline aggregations which calculate metrics across the
 * buckets of a sibling aggregation
 */
public abstract class BucketMetricsPipelineAggregator extends SiblingPipelineAggregator {

    protected ValueFormatter formatter;
    protected GapPolicy gapPolicy;

    public BucketMetricsPipelineAggregator() {
        super();
    }

    protected BucketMetricsPipelineAggregator(String name, String[] bucketsPaths, GapPolicy gapPolicy, ValueFormatter formatter,
            Map<String, Object> metaData) {
        super(name, bucketsPaths, metaData);
        this.gapPolicy = gapPolicy;
        this.formatter = formatter;
    }

    @Override
    public final InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {
        preCollection();
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                bucketsPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation multiBucketsAgg = (InternalMultiBucketAggregation) aggregation;
                List<? extends Bucket> buckets = multiBucketsAgg.getBuckets();
                for (int i = 0; i < buckets.size(); i++) {
                    Bucket bucket = buckets.get(i);
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, bucketsPath, gapPolicy);
                    if (bucketValue != null && !Double.isNaN(bucketValue)) {
                        collectBucketValue(bucket.getKeyAsString(), bucketValue);
                    }
                }
            }
        }
        return buildAggregation(Collections.EMPTY_LIST, metaData());
    }

    /**
     * Called before initial collection and between successive collection runs.
     * A chance to initialize or re-initialize state
     */
    protected void preCollection() {
    }

    /**
     * Called after a collection run is finished to build the aggregation for
     * the collected state.
     *
     * @param pipelineAggregators
     *            the pipeline aggregators to add to the resulting aggregation
     * @param metadata
     *            the metadata to add to the resulting aggregation
     * @return
     */
    protected abstract InternalAggregation buildAggregation(List<PipelineAggregator> pipelineAggregators, Map<String, Object> metadata);

    /**
     * Called for each bucket with a value so the state can be modified based on
     * the key and metric value for this bucket
     *
     * @param bucketKey
     *            the key for this bucket as a String
     * @param bucketValue
     *            the value of the metric specified in <code>bucketsPath</code>
     *            for this bucket
     */
    protected abstract void collectBucketValue(String bucketKey, Double bucketValue);

    @Override
    public void doReadFrom(StreamInput in) throws IOException {
        formatter = ValueFormatterStreams.readOptional(in);
        gapPolicy = GapPolicy.readFrom(in);
    }

    @Override
    public void doWriteTo(StreamOutput out) throws IOException {
        ValueFormatterStreams.writeOptional(formatter, out);
        gapPolicy.writeTo(out);
    }

}