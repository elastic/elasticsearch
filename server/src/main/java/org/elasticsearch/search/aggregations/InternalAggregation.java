/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * An internal implementation of {@link Aggregation}. Serves as a base class for all aggregation implementations.
 */
public abstract class InternalAggregation implements Aggregation, NamedWriteable {
    protected final String name;

    protected final Map<String, Object> metadata;

    /**
     * Constructs an aggregation result with a given name.
     *
     * @param name The name of the aggregation.
     */
    protected InternalAggregation(String name, Map<String, Object> metadata) {
        this.name = name;
        this.metadata = metadata;
    }

    /**
     * Read from a stream.
     */
    protected InternalAggregation(StreamInput in) throws IOException {
        name = in.readString();
        metadata = in.readMap();
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeGenericValue(metadata);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public String getName() {
        return name;
    }

    /**
     * Rewrite the sub-aggregations in the buckets in this aggregation.
     * Returns a copy of this {@linkplain InternalAggregation} with the
     * rewritten buckets, or, if there aren't any modifications to
     * the buckets then this method will return this aggregation. Either
     * way, it doesn't modify this aggregation.
     * <p>
     * Implementers of this should call the {@code rewriter} once per bucket
     * with its {@linkplain InternalAggregations}. The {@code rewriter}
     * should return {@code null} if it doen't have any rewriting to do or
     * it should return a new {@linkplain InternalAggregations} to make
     * changs.
     * <p>
     * The default implementation throws an exception because most
     * aggregations don't <strong>have</strong> buckets in them. It
     * should be overridden by aggregations that contain buckets. Implementers
     * should respect the description above.
     */
    public InternalAggregation copyWithRewritenBuckets(Function<InternalAggregations, InternalAggregations> rewriter) {
        throw new IllegalStateException(
            "Aggregation [" + getName() + "] must be a bucket aggregation but was [" + getWriteableName() + "]"
        );
    }

    /**
     * Run a {@linkplain Consumer} over all buckets in this aggregation.
     */
    public void forEachBucket(Consumer<InternalAggregations> consumer) {}

    /**
     * Creates the output from all pipeline aggs that this aggregation is associated with.  Should only
     * be called after all aggregations have been fully reduced
     */
    public InternalAggregation reducePipelines(
        InternalAggregation reducedAggs,
        AggregationReduceContext reduceContext,
        PipelineTree pipelinesForThisAgg
    ) {
        assert reduceContext.isFinalReduce();
        for (PipelineAggregator pipelineAggregator : pipelinesForThisAgg.aggregators()) {
            reducedAggs = pipelineAggregator.reduce(reducedAggs, reduceContext);
        }
        return reducedAggs;
    }

    /**
     * Reduces the given aggregations to a single one and returns it. In <b>most</b> cases, the assumption will be the all given
     * aggregations are of the same type (the same type as this aggregation). For best efficiency, when implementing,
     * try reusing an existing instance (typically the first in the given list) to save on redundant object
     * construction.
     *
     * @see #mustReduceOnSingleInternalAgg()
     */
    public abstract InternalAggregation reduce(List<InternalAggregation> aggregations, AggregationReduceContext reduceContext);

    /**
     * Called by the parent sampling context. Should only ever be called once as some aggregations scale their internal values
     * @param samplingContext the current sampling context
     * @return new aggregation with the sampling context applied, could be the same aggregation instance if nothing needs to be done
     */
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        throw new UnsupportedOperationException(getWriteableName() + " aggregation [" + getName() + "] does not support sampling");
    }

    /**
     * Signal the framework if the {@linkplain InternalAggregation#reduce(List, AggregationReduceContext)} phase needs to be called
     * when there is only one {@linkplain InternalAggregation}.
     */
    protected abstract boolean mustReduceOnSingleInternalAgg();

    /**
     * Return true if this aggregation can lead a reduction (ie, is not unmapped or empty).  If this agg returns
     * false, it should return itself if asked to lead a reduction.
     */
    public boolean canLeadReduction() {
        return true;
    }

    /**
     * Get the value of specified path in the aggregation.
     *
     * @param path
     *            the path to the property in the aggregation tree
     * @return the value of the property
     */
    public Object getProperty(String path) {
        AggregationPath aggPath = AggregationPath.parse(path);
        return getProperty(aggPath.getPathElementsAsStringList());
    }

    public abstract Object getProperty(List<String> path);

    /**
     * Read a size under the assumption that a value of 0 means unlimited.
     */
    protected static int readSize(StreamInput in) throws IOException {
        final int size = in.readVInt();
        return size == 0 ? Integer.MAX_VALUE : size;
    }

    /**
     * Write a size under the assumption that a value of 0 means unlimited.
     */
    protected static void writeSize(int size, StreamOutput out) throws IOException {
        if (size == Integer.MAX_VALUE) {
            size = 0;
        }
        out.writeVInt(size);
    }

    @Override
    public Map<String, Object> getMetadata() {
        return metadata;
    }

    @Override
    public String getType() {
        return getWriteableName();
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (params.paramAsBoolean(RestSearchAction.TYPED_KEYS_PARAM, false)) {
            // Concatenates the type and the name of the aggregation (ex: top_hits#foo)
            builder.startObject(String.join(TYPED_KEYS_DELIMITER, getType(), getName()));
        } else {
            builder.startObject(getName());
        }
        if (this.metadata != null) {
            builder.field(CommonFields.META.getPreferredName());
            builder.map(this.metadata);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(name, metadata);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (obj == this) {
            return true;
        }

        InternalAggregation other = (InternalAggregation) obj;
        return Objects.equals(name, other.name) && Objects.equals(metadata, other.metadata);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Get value to use when sorting by this aggregation.
     */
    public SortValue sortValue(String key) {
        // subclasses will override this with a real implementation if they can be sorted
        throw new IllegalArgumentException("Can't sort a [" + getType() + "] aggregation [" + getName() + "]");
    }

    /**
     * Get value to use when sorting by a descendant of this aggregation.
     */
    public SortValue sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        // subclasses will override this with a real implementation if you can sort on a descendant
        throw new IllegalArgumentException("Can't sort by a descendant of a [" + getType() + "] aggregation [" + head + "]");
    }
}
