/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.util.CollectionUtils;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;
import org.elasticsearch.search.aggregations.support.SamplingContext;
import org.elasticsearch.search.sort.SortValue;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableMap;

/**
 * Represents a set of {@link InternalAggregation}s
 */
public final class InternalAggregations implements Iterable<InternalAggregation>, ToXContentFragment, Writeable {

    public static final String AGGREGATIONS_FIELD = "aggregations";

    public static final InternalAggregations EMPTY = new InternalAggregations(List.of());
    private final List<InternalAggregation> aggregations;
    private Map<String, InternalAggregation> aggregationsAsMap;

    /**
     * Constructs a new aggregation.
     */
    private InternalAggregations(List<InternalAggregation> aggregations) {
        this.aggregations = aggregations;
        if (aggregations.isEmpty()) {
            aggregationsAsMap = Map.of();
        }
    }

    /**
     * Iterates over the {@link InternalAggregation}s.
     */
    @Override
    public Iterator<InternalAggregation> iterator() {
        return aggregations.iterator();
    }

    /**
     * The list of {@link InternalAggregation}s.
     */
    public List<InternalAggregation> asList() {
        return aggregations;
    }

    private Map<String, InternalAggregation> asMap() {
        var res = aggregationsAsMap;
        if (res == null) {
            Map<String, InternalAggregation> newAggregationsAsMap = Maps.newMapWithExpectedSize(aggregations.size());
            for (InternalAggregation aggregation : aggregations) {
                newAggregationsAsMap.put(aggregation.getName(), aggregation);
            }
            res = this.aggregationsAsMap = unmodifiableMap(newAggregationsAsMap);
        }
        return res;
    }

    /**
     * Returns the aggregation that is associated with the specified name.
     */
    @SuppressWarnings("unchecked")
    public <A extends InternalAggregation> A get(String name) {
        return (A) asMap().get(name);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        return aggregations.equals(((InternalAggregations) obj).aggregations);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getClass(), aggregations);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (aggregations.isEmpty()) {
            return builder;
        }
        builder.startObject(AGGREGATIONS_FIELD);
        toXContentInternal(builder, params);
        return builder.endObject();
    }

    /**
     * Directly write all the aggregations without their bounding object. Used by sub-aggregations (non top level aggs)
     */
    public XContentBuilder toXContentInternal(XContentBuilder builder, Params params) throws IOException {
        for (InternalAggregation aggregation : aggregations) {
            aggregation.toXContent(builder, params);
        }
        return builder;
    }

    public static InternalAggregations from(InternalAggregation aggregation) {
        return new InternalAggregations(List.of(aggregation));
    }

    public static InternalAggregations from(List<InternalAggregation> aggregations) {
        if (aggregations.isEmpty()) {
            return EMPTY;
        }
        if (aggregations.size() == 1) {
            return from(aggregations.getFirst());
        }
        return new InternalAggregations(aggregations);
    }

    public static InternalAggregations append(InternalAggregations aggs, InternalAggregation toAppend) {
        if (aggs.aggregations.isEmpty()) {
            return from(toAppend);
        }
        return new InternalAggregations(CollectionUtils.appendToCopyNoNullElements(aggs.aggregations, toAppend));
    }

    public static InternalAggregations readFrom(StreamInput in) throws IOException {
        return from(in.readNamedWriteableCollectionAsList(InternalAggregation.class));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableCollection(getInternalAggregations());
    }

    /**
     * Make a mutable copy of the aggregation results.
     */
    public List<InternalAggregation> copyResults() {
        return new ArrayList<>(getInternalAggregations());
    }

    private List<InternalAggregation> getInternalAggregations() {
        return aggregations;
    }

    /**
     * Get value to use when sorting by a descendant of the aggregation containing this.
     */
    public SortValue sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        InternalAggregation aggregation = get(head.name());
        if (aggregation == null) {
            throw new IllegalArgumentException("Cannot find aggregation named [" + head.name() + "]");
        }
        if (tail.hasNext()) {
            return aggregation.sortValue(tail.next(), tail);
        }
        // We can sort by either the `[value]` or `.value`
        return aggregation.sortValue(Optional.ofNullable(head.key()).orElse(head.metric()));
    }

    /**
     * Equivalent to {@link #topLevelReduce(List, AggregationReduceContext)} but it takes an iterator and a count.
     */
    public static InternalAggregations topLevelReduce(Iterator<InternalAggregations> aggs, int count, AggregationReduceContext context) {
        if (count == 0) {
            return null;
        }
        return maybeExecuteFinalReduce(context, count == 1 ? reduce(aggs.next(), context) : reduce(aggs, count, context));
    }

    private static InternalAggregations maybeExecuteFinalReduce(AggregationReduceContext context, InternalAggregations reduced) {
        if (reduced == null) {
            return null;
        }
        if (context.isFinalReduce()) {
            List<InternalAggregation> reducedInternalAggs = reduced.getInternalAggregations()
                .stream()
                .map(agg -> agg.reducePipelines(agg, context, context.pipelineTreeRoot().subTree(agg.getName())))
                .collect(Collectors.toCollection(ArrayList::new));

            for (PipelineAggregator pipelineAggregator : context.pipelineTreeRoot().aggregators()) {
                SiblingPipelineAggregator sib = (SiblingPipelineAggregator) pipelineAggregator;
                InternalAggregation newAgg = sib.doReduce(from(reducedInternalAggs), context);
                reducedInternalAggs.add(newAgg);
            }
            return from(reducedInternalAggs);
        }
        return reduced;
    }

    /**
     * Begin the reduction process.  This should be the entry point for the "first" reduction, e.g. called by
     * SearchPhaseController or anywhere else that wants to initiate a reduction.  It _should not_ be called
     * as an intermediate reduction step (e.g. in the middle of an aggregation tree).
     *
     * This method first reduces the aggregations, and if it is the final reduce, then reduce the pipeline
     * aggregations (both embedded parent/sibling as well as top-level sibling pipelines)
     */
    public static InternalAggregations topLevelReduce(List<InternalAggregations> aggregationsList, AggregationReduceContext context) {
        return maybeExecuteFinalReduce(context, reduce(aggregationsList, context));
    }

    /**
     * Reduces the given list of aggregations as well as the top-level pipeline aggregators extracted from the first
     * {@link InternalAggregations} object found in the list.
     * Note that pipeline aggregations _are not_ reduced by this method.  Pipelines are handled
     * separately by {@link InternalAggregations#topLevelReduce(List, AggregationReduceContext)}
     */
    public static InternalAggregations reduce(List<InternalAggregations> aggregationsList, AggregationReduceContext context) {
        if (aggregationsList.isEmpty()) {
            return null;
        }
        // handle special case when there is just one aggregation
        if (aggregationsList.size() == 1) {
            return reduce(aggregationsList.getFirst(), context);
        }
        // general case
        try (AggregatorsReducer reducer = new AggregatorsReducer(aggregationsList.get(0), context, aggregationsList.size())) {
            for (InternalAggregations aggregations : aggregationsList) {
                reducer.accept(aggregations);
            }
            return reducer.get();
        }
    }

    private static InternalAggregations reduce(Iterator<InternalAggregations> aggsIterator, int count, AggregationReduceContext context) {
        // general case
        var first = aggsIterator.next();
        try (AggregatorsReducer reducer = new AggregatorsReducer(first, context, count)) {
            reducer.accept(first);
            aggsIterator.forEachRemaining(reducer::accept);
            return reducer.get();
        }
    }

    public static InternalAggregations reduce(InternalAggregations aggregations, AggregationReduceContext context) {
        final List<InternalAggregation> internalAggregations = aggregations.asList();
        int size = internalAggregations.size();
        if (size == 0) {
            return EMPTY;
        }
        boolean noneReduced = true;
        final List<InternalAggregation> reduced = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            InternalAggregation aggregation = internalAggregations.get(i);
            if (aggregation.mustReduceOnSingleInternalAgg()) {
                noneReduced = false;
                try (AggregatorReducer aggregatorReducer = aggregation.getReducer(context.forAgg(aggregation.getName()), 1)) {
                    aggregatorReducer.accept(aggregation);
                    reduced.add(aggregatorReducer.get());
                }
            } else {
                reduced.add(aggregation);
            }
        }
        return noneReduced ? aggregations : from(reduced);
    }

    /**
     * Finalizes the sampling for all the internal aggregations
     * @param samplingContext the sampling context
     * @return the finalized aggregations
     */
    public static InternalAggregations finalizeSampling(InternalAggregations internalAggregations, SamplingContext samplingContext) {
        return from(
            internalAggregations.aggregations.stream().map(agg -> agg.finalizeSampling(samplingContext)).collect(Collectors.toList())
        );
    }
}
