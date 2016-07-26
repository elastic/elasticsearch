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

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public abstract class InternalMultiBucketAggregation<A extends InternalMultiBucketAggregation, B extends InternalMultiBucketAggregation.InternalBucket>
        extends InternalAggregation implements MultiBucketsAggregation {
    public InternalMultiBucketAggregation(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name, pipelineAggregators, metaData);
    }

    /**
     * Read from a stream.
     */
    protected InternalMultiBucketAggregation(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Create a new copy of this {@link Aggregation} with the same settings as
     * this {@link Aggregation} and contains the provided buckets.
     *
     * @param buckets
     *            the buckets to use in the new {@link Aggregation}
     * @return the new {@link Aggregation}
     */
    public abstract A create(List<B> buckets);

    /**
     * Create a new {@link InternalBucket} using the provided prototype bucket
     * and aggregations.
     *
     * @param aggregations
     *            the aggregations for the new bucket
     * @param prototype
     *            the bucket to use as a prototype
     * @return the new bucket
     */
    public abstract B createBucket(InternalAggregations aggregations, B prototype);

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else if (path.get(0).equals("_bucket_count")) {
            return getBuckets().size();
        } else {
            List<? extends Bucket> buckets = getBuckets();
            Object[] propertyArray = new Object[buckets.size()];
            for (int i = 0; i < buckets.size(); i++) {
                propertyArray[i] = buckets.get(i).getProperty(getName(), path);
            }
            return propertyArray;
        }
    }

    public abstract static class InternalBucket implements Bucket {
        @Override
        public Object getProperty(String containingAggName, List<String> path) {
            if (path.isEmpty()) {
                return this;
            }
            Aggregations aggregations = getAggregations();
            String aggName = path.get(0);
            if (aggName.equals("_count")) {
                if (path.size() > 1) {
                    throw new InvalidAggregationPathException("_count must be the last element in the path");
                }
                return getDocCount();
            } else if (aggName.equals("_key")) {
                if (path.size() > 1) {
                    throw new InvalidAggregationPathException("_key must be the last element in the path");
                }
                return getKey();
            }
            InternalAggregation aggregation = aggregations.get(aggName);
            if (aggregation == null) {
                throw new InvalidAggregationPathException("Cannot find an aggregation named [" + aggName + "] in [" + containingAggName
                        + "]");
            }
            return aggregation.getProperty(path.subList(1, path.size()));
        }
    }
}
