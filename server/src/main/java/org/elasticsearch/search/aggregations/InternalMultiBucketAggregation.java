/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import java.io.IOException;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Function;

@SuppressWarnings("rawtypes")
public abstract class InternalMultiBucketAggregation<
    A extends InternalMultiBucketAggregation,
    B extends InternalMultiBucketAggregation.InternalBucket> extends InternalAggregation implements MultiBucketsAggregation {

    public static final int REPORT_EMPTY_EVERY = 10_000;

    public InternalMultiBucketAggregation(String name, Map<String, Object> metadata) {
        super(name, metadata);
//        setBucketCount(countBuckets());
    }

    protected InternalMultiBucketAggregation(StreamInput in) throws IOException {
        super(in);
//        setBucketCount(countBuckets());
    }

    private int bucketCount;

    @Override
    public int countBuckets() {
        int count = 0;
        List<B> buckets = getBuckets();
        if(buckets == null) return 0;
        for(Bucket B : buckets){
            InternalAggregations subAggregations = B.getAggregations();
            if (subAggregations == null || subAggregations.asList().isEmpty()) {
                count++;
                continue;
            }
            for (Aggregation aggregation : subAggregations) {
                count += aggregation.getBucketCount();
            }
        }
        return count;
    }

    @Override
    public int getBucketCount(){ return bucketCount; }

    @Override
    public void setBucketCount(int count){ bucketCount = count; }

    public abstract A create(List<B> buckets);

    public abstract B createBucket(InternalAggregations aggregations, B prototype);

    protected static class BucketAggregationList<B extends Bucket> extends AbstractList<InternalAggregations> {
        private final List<B> buckets;

        public BucketAggregationList(List<B> buckets) {
            this.buckets = buckets;
        }

        @Override
        public InternalAggregations get(int index) {
            return buckets.get(index).getAggregations();
        }

        @Override
        public int size() {
            return buckets.size();
        }
    }

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

        if (aggName.startsWith("'") && aggName.endsWith("'")) {
            for (InternalBucket bucket : buckets) {
                if (bucket.getKeyAsString().equals(aggName.substring(1, aggName.length() - 1))) {
                    return bucket.getProperty(name, path.subList(1, path.size()));
                }
            }
            throw new InvalidAggregationPathException("Cannot find an key [" + aggName + "] in [" + name + "]");
        }

        Object[] propertyArray = new Object[buckets.size()];
        for (int i = 0; i < buckets.size(); i++) {
            propertyArray[i] = buckets.get(i).getProperty(name, path);
        }
        return propertyArray;
    }

    public static int countInnerBucket(InternalBucket bucket) {
        int count = 0;
        for (Aggregation agg : bucket.getAggregations().asList()) {
            count += countInnerBucket(agg);
        }
        return count;
    }

    public static int countInnerBucket(Aggregation agg) {
        int size = 0;
        if (agg instanceof MultiBucketsAggregation) {
            MultiBucketsAggregation multi = (MultiBucketsAggregation) agg;
            for (MultiBucketsAggregation.Bucket bucket : multi.getBuckets()) {
                ++size;
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

    @Override
    public final InternalAggregation reducePipelines(
        InternalAggregation reducedAggs,
        AggregationReduceContext reduceContext,
        PipelineTree pipelineTree
    ) {
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
            InternalAggregations rewritten = rewriter.apply(bucket.getAggregations());
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
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public void forEachBucket(Consumer<InternalAggregations> consumer) {
        for (B bucket : getBuckets()) {
            consumer.accept(bucket.getAggregations());
        }
    }

    private List<B> reducePipelineBuckets(AggregationReduceContext reduceContext, PipelineTree pipelineTree) {
        List<B> reducedBuckets = new ArrayList<>();
        for (B bucket : getBuckets()) {
            List<InternalAggregation> aggs = new ArrayList<>();
            for (Aggregation agg : bucket.getAggregations()) {
                PipelineTree subTree = pipelineTree.subTree(agg.getName());
                aggs.add(((InternalAggregation) agg).reducePipelines((InternalAggregation) agg, reduceContext, subTree));
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
            InternalAggregations aggregations = getAggregations();
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
                throw new InvalidAggregationPathException(
                    "Cannot find an aggregation named [" + aggName + "] in [" + containingAggName + "]"
                );
            }
            return aggregation.getProperty(path.subList(1, path.size()));
        }
    }
}
