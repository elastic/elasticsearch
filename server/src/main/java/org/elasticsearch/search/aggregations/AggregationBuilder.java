/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryRewriteContext;
import org.elasticsearch.index.query.Rewriteable;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.support.AggregationContext;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;

/**
 * A factory that knows how to create an {@link Aggregator} of a specific type.
 */
public abstract class AggregationBuilder
    implements
        VersionedNamedWriteable,
        ToXContentFragment,
        BaseAggregationBuilder,
        Rewriteable<AggregationBuilder> {
    public static final long DEFAULT_PREALLOCATION = 1024 * 6;

    protected final String name;
    protected AggregatorFactories.Builder factoriesBuilder = AggregatorFactories.builder();

    /**
     * Constructs a new aggregation builder.
     *
     * @param name  The aggregation name
     */
    protected AggregationBuilder(String name) {
        if (name == null) {
            throw new IllegalArgumentException("[name] must not be null: [" + name + "]");
        }
        this.name = name;
    }

    protected AggregationBuilder(AggregationBuilder clone, AggregatorFactories.Builder factoriesBuilder) {
        this.name = clone.name;
        this.factoriesBuilder = factoriesBuilder;
    }

    /** Return this aggregation's name. */
    public String getName() {
        return name;
    }

    /**
     * Return the field names this aggregation creates.
     *
     * This method is a optional helper for clients that need to know the output field names.
     *
     * @return The set of output field names this aggregation produces or Optional.empty() if not implemented or Optional.of(emptySet())
     *         if the fields are not known.
     */
    public Optional<Set<String>> getOutputFieldNames() {
        return Optional.empty();
    }

    /** Internal: build an {@link AggregatorFactory} based on the configuration of this builder. */
    protected abstract AggregatorFactory build(AggregationContext context, AggregatorFactory parent) throws IOException;

    /** Associate metadata with this {@link AggregationBuilder}. */
    @Override
    public abstract AggregationBuilder setMetadata(Map<String, Object> metadata);

    /** Return any associated metadata with this {@link AggregationBuilder}. */
    public abstract Map<String, Object> getMetadata();

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(AggregationBuilder aggregation);

    /** Add a sub aggregation to this builder. */
    public abstract AggregationBuilder subAggregation(PipelineAggregationBuilder aggregation);

    /** Return the configured set of subaggregations **/
    public Collection<AggregationBuilder> getSubAggregations() {
        return factoriesBuilder.getAggregatorFactories();
    }

    /** Return the aggregation's query if it's different from the search query, or null otherwise. */
    public QueryBuilder getQuery() {
        return null;
    }

    /** Return the configured set of pipeline aggregations **/
    public Collection<PipelineAggregationBuilder> getPipelineAggregations() {
        return factoriesBuilder.getPipelineAggregatorFactories();
    }

    /**
     * Internal: Registers sub-factories with this factory. The sub-factory will
     * be responsible for the creation of sub-aggregators under the aggregator
     * created by this factory. This is only for use by
     * {@link AggregatorFactories#parseAggregators(XContentParser)}.
     *
     * @param subFactories
     *            The sub-factories
     * @return this factory (fluent interface)
     */
    @Override
    public abstract AggregationBuilder subAggregations(AggregatorFactories.Builder subFactories);

    /**
     * Create a shallow copy of this builder and replacing {@link #factoriesBuilder} and <code>metadata</code>.
     */
    protected abstract AggregationBuilder shallowCopy(AggregatorFactories.Builder factoriesBuilder, Map<String, Object> metadata);

    /**
     * Creates a deep copy of {@param original} by recursively invoking {@code #shallowCopy} on the sub aggregations.
     * Each copied agg is passed through the {@param visitor} function that returns a possibly modified "copy".
     */
    public static AggregationBuilder deepCopy(AggregationBuilder original, Function<AggregationBuilder, AggregationBuilder> visitor) {
        AggregatorFactories.Builder subAggregations = new AggregatorFactories.Builder();
        // recursively copy sub aggs first
        for (AggregationBuilder subAggregation : original.getSubAggregations()) {
            subAggregations.addAggregator(deepCopy(subAggregation, visitor));
        }
        // pipeline aggs do not themselves contain sub aggs, and don't have a copy method, hence are simply copied by reference
        for (PipelineAggregationBuilder subPipelineAggregation : original.getPipelineAggregations()) {
            subAggregations.addPipelineAggregator(subPipelineAggregation);
        }
        return visitor.apply(original.shallowCopy(subAggregations, original.getMetadata()));
    }

    @Override
    public final AggregationBuilder rewrite(QueryRewriteContext context) throws IOException {
        AggregationBuilder rewritten = doRewrite(context);
        AggregatorFactories.Builder rewrittenSubAggs = factoriesBuilder.rewrite(context);
        if (rewritten != this) {
            return getMetadata() == null
                ? rewritten.subAggregations(rewrittenSubAggs)
                : rewritten.setMetadata(getMetadata()).subAggregations(rewrittenSubAggs);
        } else if (rewrittenSubAggs != factoriesBuilder) {
            return shallowCopy(rewrittenSubAggs, getMetadata());
        } else {
            return this;
        }
    }

    /**
     * Rewrites this aggregation builder into its primitive form. By default
     * this method return the builder itself. If the builder did not change the
     * identity reference must be returned otherwise the builder will be
     * rewritten infinitely.
     */
    protected AggregationBuilder doRewrite(QueryRewriteContext queryRewriteContext) throws IOException {
        return this;
    }

    /**
     * Build a tree of {@link PipelineAggregator}s to modify the tree of
     * aggregation results after the final reduction.
     */
    public PipelineTree buildPipelineTree() {
        return factoriesBuilder.buildPipelineTree();
    }

    /**
     * A rough count of the number of buckets that {@link Aggregator}s built
     * by this builder will contain per parent bucket used to validate sorts
     * and pipeline aggregations. Just "zero", "one", and "many".
     * <p>
     * Unlike {@link CardinalityUpperBound} which is <strong>total</strong>
     * instead of <strong>per parent bucket</strong>.
     */
    public enum BucketCardinality {
        NONE,
        ONE,
        MANY;
    }

    /**
     * A rough count of the number of buckets that {@link Aggregator}s built
     * by this builder will contain per owning parent bucket.
     */
    public abstract BucketCardinality bucketCardinality();

    /**
     * Bytes to preallocate on the "request" breaker for this aggregation. The
     * goal is to request a few more bytes than we expect to use at first to
     * cut down on contention on the "request" breaker when we are constructing
     * the aggs. Underestimating what we allocate up front will fail to
     * accomplish the goal. Overestimating will cause requests to fail for no
     * reason.
     */
    public long bytesToPreallocate() {
        return DEFAULT_PREALLOCATION;
    }

    /** Common xcontent fields shared among aggregator builders */
    public static final class CommonFields extends ParseField.CommonFields {
        public static final ParseField VALUE_TYPE = new ParseField("value_type");
    }

    /**
     * Does this aggregation support running with in a sampling context.
     *
     * By default, it's false for all aggregations.
     *
     * If the sub-classed builder supports sampling, be sure of the following that the resulting internal aggregation objects
     * override the {@link InternalAggregation#finalizeSampling(SamplingContext)} and scales any values that require scaling.
     * @return does this aggregation builder support sampling
     */
    public boolean supportsSampling() {
        return false;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    /**
     * Return true if any of the child aggregations is a time-series aggregation that requires an in-order execution
     */
    public boolean isInSortOrderExecutionRequired() {
        for (AggregationBuilder builder : factoriesBuilder.getAggregatorFactories()) {
            if (builder.isInSortOrderExecutionRequired()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Return false if this aggregation or any of the child aggregations does not support parallel collection.
     * As a result, a request including such aggregation is always executed sequentially despite concurrency is enabled for the query phase.
     */
    public boolean supportsParallelCollection(ToLongFunction<String> fieldCardinalityResolver) {
        if (isInSortOrderExecutionRequired()) {
            return false;
        }
        for (AggregationBuilder builder : factoriesBuilder.getAggregatorFactories()) {
            if (builder.supportsParallelCollection(fieldCardinalityResolver) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Called by aggregations whose parents must be sequentially ordered.
     * @param type the type of the aggregation being validated
     * @param name the name of the aggregation being validated
     * @param addValidationError callback to add validation errors
     */
    protected void validateSequentiallyOrdered(String type, String name, Consumer<String> addValidationError) {
        addValidationError.accept(
            type + " aggregation [" + name + "] must have a histogram, date_histogram or auto_date_histogram as parent"
        );
    }

    /**
     * Called by aggregations whose parents must be sequentially ordered without any gaps.
     * @param type the type of the aggregation being validated
     * @param name the name of the aggregation being validated
     * @param addValidationError callback to add validation errors
     */
    protected void validateSequentiallyOrderedWithoutGaps(String type, String name, Consumer<String> addValidationError) {
        validateSequentiallyOrdered(type, name, addValidationError);
    }
}
