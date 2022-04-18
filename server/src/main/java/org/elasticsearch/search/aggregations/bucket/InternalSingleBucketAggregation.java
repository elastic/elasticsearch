/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * A base class for all the single bucket aggregations.
 */
public abstract class InternalSingleBucketAggregation extends InternalAggregation implements SingleBucketAggregation {

    private long docCount;
    private InternalAggregations aggregations;

    /**
     * Creates a single bucket aggregation.
     *
     * @param name          The aggregation name.
     * @param docCount      The document count in the single bucket.
     * @param aggregations  The already built sub-aggregations that are associated with the bucket.
     */
    protected InternalSingleBucketAggregation(String name, long docCount, InternalAggregations aggregations, Map<String, Object> metadata) {
        super(name, metadata);
        this.docCount = docCount;
        this.aggregations = aggregations;
    }

    /**
     * Read from a stream.
     */
    protected InternalSingleBucketAggregation(StreamInput in) throws IOException {
        super(in);
        docCount = in.readVLong();
        aggregations = InternalAggregations.readFrom(in);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeVLong(docCount);
        aggregations.writeTo(out);
    }

    @Override
    public long getDocCount() {
        return docCount;
    }

    @Override
    public InternalAggregations getAggregations() {
        return aggregations;
    }

    /**
     * Create a new copy of this {@link Aggregation} with the same settings as
     * this {@link Aggregation} and contains the provided sub-aggregations.
     *
     * @param subAggregations
     *            the buckets to use in the new {@link Aggregation}
     * @return the new {@link Aggregation}
     */
    public InternalSingleBucketAggregation create(InternalAggregations subAggregations) {
        return newAggregation(getName(), getDocCount(), subAggregations);
    }

    /**
     * Create a <b>new</b> empty sub aggregation. This must be a new instance on each call.
     */
    protected abstract InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations);

    @Override
    public InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext) {
        long docCount = 0L;
        List<InternalAggregations> subAggregationsList = new ArrayList<>(aggregations.size());
        for (InternalAggregation aggregation : aggregations) {
            assert aggregation.getName().equals(getName());
            docCount += ((InternalSingleBucketAggregation) aggregation).docCount;
            subAggregationsList.add(((InternalSingleBucketAggregation) aggregation).aggregations);
        }
        final InternalAggregations aggs = InternalAggregations.reduce(subAggregationsList, reduceContext);
        return newAggregation(getName(), docCount, aggs);
    }

    /**
     * Amulti-bucket agg needs to first reduce the buckets and *their* pipelines
     * before allowing sibling pipelines to materialize.
     */
    @Override
    public final InternalAggregation reducePipelines(
        InternalAggregation reducedAggs,
        AggregationReduceContext reduceContext,
        PipelineTree pipelineTree
    ) {
        assert reduceContext.isFinalReduce();
        InternalAggregation reduced = this;
        if (pipelineTree.hasSubTrees()) {
            List<InternalAggregation> aggs = new ArrayList<>();
            for (Aggregation agg : getAggregations().asList()) {
                PipelineTree subTree = pipelineTree.subTree(agg.getName());
                aggs.add(((InternalAggregation) agg).reducePipelines((InternalAggregation) agg, reduceContext, subTree));
            }
            InternalAggregations reducedSubAggs = InternalAggregations.from(aggs);
            reduced = create(reducedSubAggs);
        }
        return super.reducePipelines(reduced, reduceContext, pipelineTree);
    }

    @Override
    public Object getProperty(List<String> path) {
        if (path.isEmpty()) {
            return this;
        } else {
            String aggName = path.get(0);
            if (aggName.equals("_count")) {
                if (path.size() > 1) {
                    throw new IllegalArgumentException("_count must be the last element in the path");
                }
                return getDocCount();
            }
            InternalAggregation aggregation = aggregations.get(aggName);
            if (aggregation == null) {
                throw new IllegalArgumentException("Cannot find an aggregation named [" + aggName + "] in [" + getName() + "]");
            }
            return aggregation.getProperty(path.subList(1, path.size()));
        }
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
        aggregations.toXContentInternal(builder, params);
        return builder;
    }

    @Override
    public final SortValue sortValue(String key) {
        if (key != null && false == key.equals("doc_count")) {
            throw new IllegalArgumentException(
                "Unknown value key ["
                    + key
                    + "] for single-bucket aggregation ["
                    + getName()
                    + "]. Either use [doc_count] as key or drop the key all together."
            );
        }
        return SortValue.from(docCount);
    }

    @Override
    public final SortValue sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        return aggregations.sortValue(head, tail);
    }

    @Override
    protected boolean mustReduceOnSingleInternalAgg() {
        return true;
    }

    @Override
    public InternalAggregation copyWithRewritenBuckets(Function<InternalAggregations, InternalAggregations> rewriter) {
        InternalAggregations rewritten = rewriter.apply(aggregations);
        if (rewritten == null) {
            return this;
        }
        return create(rewritten);
    }

    @Override
    public void forEachBucket(Consumer<InternalAggregations> consumer) {
        consumer.accept(aggregations);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        if (super.equals(obj) == false) return false;

        InternalSingleBucketAggregation other = (InternalSingleBucketAggregation) obj;
        return Objects.equals(docCount, other.docCount) && Objects.equals(aggregations, other.aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), docCount, aggregations);
    }
}
