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
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

public abstract class InternalMultiBucketAggregation<A extends InternalMultiBucketAggregation,
            B extends InternalMultiBucketAggregation.InternalBucket>
        extends InternalAggregation implements MultiBucketsAggregation {

    public InternalMultiBucketAggregation(String name, Map<String, Object> metadata) {
        super(name, metadata);
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

    /**
     * Reduce a list of same-keyed buckets (from multiple shards) to a single bucket. This
     * requires all buckets to have the same key.
     */
    protected abstract B reduceBucket(List<B> buckets, ReduceContext context);

    @Override
    public abstract List<B> getBuckets();

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        }
        return resolvePropertyFromPath(path, getBuckets(), getName());
    }

    static Object resolvePropertyFromPath(List<String> path, List<? extends InternalBucket> buckets, String name) {
        String aggName = path.get(0);
        if (aggName.equals("_bucket_count")) {
            return buckets.size();
        }

        // This is a bucket key, look through our buckets and see if we can find a match
        if (aggName.startsWith("'") && aggName.endsWith("'")) {
            for (InternalBucket bucket : buckets) {
                if (bucket.getKeyAsString().equals(aggName.substring(1, aggName.length() - 1))) {
                    return bucket.getProperty(name, path.subList(1, path.size()));
                }
            }
            // No key match, time to give up
            throw new InvalidAggregationPathException("Cannot find an key [" + aggName + "] in [" + name + "]");
        }

        Object[] propertyArray = new Object[buckets.size()];
        for (int i = 0; i < buckets.size(); i++) {
            propertyArray[i] = buckets.get(i).getProperty(name, path);
        }
        return propertyArray;

    }

    /**
     * Counts the number of inner buckets inside the provided {@link InternalBucket}
     */
    public static int countInnerBucket(InternalBucket bucket) {
        int count = 0;
        for (Aggregation agg : bucket.getAggregations().asList()) {
            count += countInnerBucket(agg);
        }
        return count;
    }

    /**
     * Counts the number of inner buckets inside the provided {@link Aggregation}
     */
    public static int countInnerBucket(Aggregation agg) {
        int size = 0;
        if (agg instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multi = (MultiBucketsAggregation) agg;
            for (MultiBucketsAggregation.Bucket bucket : multi.getBuckets()) {
                ++ size;
                for (Aggregation bucketAgg : bucket.getAggregations().asList()) {
                    size += countInnerBucket(bucketAgg);
                }
            }
        } else if (agg instanceof SingleBucketAggregation) {
            SingleBucketAggregation single = (SingleBucketAggregation) agg;
            for (Aggregation bucketAgg : single.getAggregations().asList()) {
                size += countInnerBucket(bucketAgg);
            }
        }
        return size;
    }

    /**
     * A multi-bucket agg needs to first reduce the buckets and *their* pipelines
     * before allowing sibling pipelines to materialize.
     */
    @Override
    public final InternalAggregation reducePipelines(
            InternalAggregation reducedAggs, ReduceContext reduceContext, PipelineTree pipelineTree) {
        assert reduceContext.isFinalReduce();
        InternalAggregation reduced = this;
        if (pipelineTree.hasSubTrees()) {
            List<B> materializedBuckets = reducePipelineBuckets(reduceContext, pipelineTree);
            reduced = create(materializedBuckets);
        }
        return super.reducePipelines(reduced, reduceContext, pipelineTree);
    }

    @Override
    public InternalAggregation copyWithRewritenBuckets(Function<InternalAggregations, InternalAggregations> rewriter) {
        boolean modified = false;
        List<B> newBuckets = new ArrayList<>();
        for (B bucket : getBuckets()) {
            InternalAggregations rewritten = rewriter.apply((InternalAggregations) bucket.getAggregations());
            if (rewritten == null) {
                newBuckets.add(bucket);
                continue;
            }
            modified = true;
            B newBucket = createBucket(rewritten, bucket);
            newBuckets.add(newBucket);
        }
        return modified ? create(newBuckets) : this;
    }

    @Override
    public void forEachBucket(Consumer<InternalAggregations> consumer) {
        for (B bucket : getBuckets()) {
            consumer.accept((InternalAggregations) bucket.getAggregations());
        }
    }

    private List<B> reducePipelineBuckets(ReduceContext reduceContext, PipelineTree pipelineTree) {
        List<B> reducedBuckets = new ArrayList<>();
        for (B bucket : getBuckets()) {
            List<InternalAggregation> aggs = new ArrayList<>();
            for (Aggregation agg : bucket.getAggregations()) {
                PipelineTree subTree = pipelineTree.subTree(agg.getName());
                aggs.add(((InternalAggregation)agg).reducePipelines((InternalAggregation)agg, reduceContext, subTree));
            }
            reducedBuckets.add(createBucket(InternalAggregations.from(aggs), bucket));
        }
        return reducedBuckets;
    }

    public abstract static class InternalBucket implements Bucket, Writeable {

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
