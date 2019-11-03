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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.InternalSingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public abstract class SiblingPipelineAggregator extends PipelineAggregator {
    protected SiblingPipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metaData) {
        super(name, bucketsPaths, metaData);
    }

    /**
     * Read from a stream.
     */
    SiblingPipelineAggregator(StreamInput in) throws IOException {
        super(in);
    }

    @SuppressWarnings("unchecked")
    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        if (aggregation instanceof InternalMultiBucketAggregation) {
            @SuppressWarnings("rawtypes")
            InternalMultiBucketAggregation multiBucketsAgg = (InternalMultiBucketAggregation) aggregation;
            List<? extends Bucket> buckets = multiBucketsAgg.getBuckets();
            List<Bucket> newBuckets = new ArrayList<>();
            for (Bucket bucket1 : buckets) {
                InternalMultiBucketAggregation.InternalBucket bucket = (InternalMultiBucketAggregation.InternalBucket) bucket1;
                InternalAggregation aggToAdd = doReduce(bucket.getAggregations(), reduceContext);
                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                    .map((p) -> (InternalAggregation) p)
                    .collect(Collectors.toList());
                aggs.add(aggToAdd);
                InternalMultiBucketAggregation.InternalBucket newBucket = multiBucketsAgg.createBucket(new InternalAggregations(aggs),
                    bucket);
                newBuckets.add(newBucket);
            }

            return multiBucketsAgg.create(newBuckets);
        } else if (aggregation instanceof InternalSingleBucketAggregation) {
            InternalSingleBucketAggregation singleBucketAgg = (InternalSingleBucketAggregation) aggregation;
            InternalAggregation aggToAdd = doReduce(singleBucketAgg.getAggregations(), reduceContext);
            List<InternalAggregation> aggs = StreamSupport.stream(singleBucketAgg.getAggregations().spliterator(), false)
                .map((p) -> (InternalAggregation) p)
                .collect(Collectors.toList());
            aggs.add(aggToAdd);
            return singleBucketAgg.create(new InternalAggregations(aggs));
        } else {
            throw new IllegalStateException("Aggregation [" + aggregation.getName() + "] must be a bucket aggregation ["
                    + aggregation.getWriteableName() + "]");
        }
    }

    public abstract InternalAggregation doReduce(Aggregations aggregations, ReduceContext context);
}
