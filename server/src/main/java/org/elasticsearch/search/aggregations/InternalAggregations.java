/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations;

import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * An internal implementation of {@link Aggregations}.
 */
public final class InternalAggregations extends Aggregations implements Writeable {

    public static final InternalAggregations EMPTY = new InternalAggregations(Collections.emptyList());

    private static final Comparator<InternalAggregation> INTERNAL_AGG_COMPARATOR = (agg1, agg2) -> {
        if (agg1.isMapped() == agg2.isMapped()) {
            return 0;
        } else if (agg1.isMapped() && agg2.isMapped() == false) {
            return -1;
        } else {
            return 1;
        }
    };

    /**
     * The way to build a tree of pipeline aggregators. Used only for
     * serialization backwards compatibility.
     */
    private final Supplier<PipelineAggregator.PipelineTree> pipelineTreeForBwcSerialization;

    /**
     * Constructs a new aggregation.
     */
    private InternalAggregations(List<InternalAggregation> aggregations) {
        super(aggregations);
        this.pipelineTreeForBwcSerialization = null;
    }

    /**
     * Constructs a node in the aggregation tree.
     * @param pipelineTreeSource must be null inside the tree or after final reduction. Should reference the
     *                           search request otherwise so we can properly serialize the response to
     *                           versions of Elasticsearch that require the pipelines to be serialized.
     */
    public InternalAggregations(List<InternalAggregation> aggregations, Supplier<PipelineAggregator.PipelineTree> pipelineTreeSource) {
        super(aggregations);
        this.pipelineTreeForBwcSerialization = pipelineTreeSource;
    }

    public static InternalAggregations from(List<InternalAggregation> aggregations) {
        if (aggregations.isEmpty()) {
            return EMPTY;
        }
        return new InternalAggregations(aggregations);
    }

    public static InternalAggregations readFrom(StreamInput in) throws IOException {
        final InternalAggregations res = from(in.readList(stream -> in.readNamedWriteable(InternalAggregation.class)));
        if (in.getVersion().before(Version.V_7_8_0) && in.getVersion().onOrAfter(Version.V_6_7_0)) {
            /*
             * Setting the pipeline tree source to null is here is correct but
             * only because we don't immediately pass the InternalAggregations
             * off to another node. Instead, we always reduce together with
             * many aggregations and that always adds the tree read from the
             * current request.
             */
            in.readNamedWriteableList(PipelineAggregator.class);
        }
        return res;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().before(Version.V_7_8_0)) {
            if (pipelineTreeForBwcSerialization == null) {
                mergePipelineTreeForBWCSerialization(PipelineTree.EMPTY);
                out.writeNamedWriteableList(getInternalAggregations());
                if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
                    out.writeNamedWriteableList(emptyList());
                }
            } else {
                PipelineAggregator.PipelineTree pipelineTree = pipelineTreeForBwcSerialization.get();
                mergePipelineTreeForBWCSerialization(pipelineTree);
                out.writeNamedWriteableList(getInternalAggregations());
                if (out.getVersion().onOrAfter(Version.V_6_7_0)) {
                    out.writeNamedWriteableList(pipelineTree.aggregators());
                }
            }
        } else {
            out.writeNamedWriteableList(getInternalAggregations());
        }
    }

    /**
     * Merge a {@linkplain PipelineAggregator.PipelineTree} into this
     * aggregation result tree before serializing to a node older than
     * 7.8.0.
     */
    public void mergePipelineTreeForBWCSerialization(PipelineAggregator.PipelineTree pipelineTree) {
        getInternalAggregations().stream()
            .forEach(agg -> { agg.mergePipelineTreeForBWCSerialization(pipelineTree.subTree(agg.getName())); });
    }

    /**
     * Make a mutable copy of the aggregation results.
     * <p>
     * IMPORTANT: The copy doesn't include any pipeline aggregations, if there are any.
     */
    public List<InternalAggregation> copyResults() {
        return new ArrayList<>(getInternalAggregations());
    }

    /**
     * Get the top level pipeline aggregators.
     * @deprecated these only exist for BWC serialization
     */
    @Deprecated
    public List<SiblingPipelineAggregator> getTopLevelPipelineAggregators() {
        if (pipelineTreeForBwcSerialization == null) {
            return emptyList();
        }
        return pipelineTreeForBwcSerialization.get().aggregators().stream().map(p -> (SiblingPipelineAggregator) p).collect(toList());
    }

    /**
     * Get the transient pipeline tree used to serialize pipeline aggregators to old nodes.
     */
    @Deprecated
    Supplier<PipelineAggregator.PipelineTree> getPipelineTreeForBwcSerialization() {
        return pipelineTreeForBwcSerialization;
    }

    @SuppressWarnings("unchecked")
    private List<InternalAggregation> getInternalAggregations() {
        return (List<InternalAggregation>) aggregations;
    }

    /**
     * Get value to use when sorting by a descendant of the aggregation containing this.
     */
    public double sortValue(AggregationPath.PathElement head, Iterator<AggregationPath.PathElement> tail) {
        InternalAggregation aggregation = get(head.name);
        if (aggregation == null) {
            throw new IllegalArgumentException("Cannot find aggregation named [" + head.name + "]");
        }
        if (tail.hasNext()) {
            return aggregation.sortValue(tail.next(), tail);
        }
        return aggregation.sortValue(head.key);
    }

    /**
     * Begin the reduction process.  This should be the entry point for the "first" reduction, e.g. called by
     * SearchPhaseController or anywhere else that wants to initiate a reduction.  It _should not_ be called
     * as an intermediate reduction step (e.g. in the middle of an aggregation tree).
     *
     * This method first reduces the aggregations, and if it is the final reduce, then reduce the pipeline
     * aggregations (both embedded parent/sibling as well as top-level sibling pipelines)
     */
    public static InternalAggregations topLevelReduce(List<InternalAggregations> aggregationsList, ReduceContext context) {
        InternalAggregations reduced = reduce(
            aggregationsList,
            context,
            reducedAggregations -> new InternalAggregations(reducedAggregations, context.pipelineTreeForBwcSerialization())
        );
        if (reduced == null) {
            return null;
        }

        if (context.isFinalReduce()) {
            List<InternalAggregation> reducedInternalAggs = reduced.getInternalAggregations();
            reducedInternalAggs = reducedInternalAggs.stream()
                .map(agg -> agg.reducePipelines(agg, context, context.pipelineTreeRoot().subTree(agg.getName())))
                .collect(Collectors.toList());

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
     * Reduces the given list of aggregations as well as the top-level pipeline aggregators extracted from the first
     * {@link InternalAggregations} object found in the list.
     * Note that pipeline aggregations _are not_ reduced by this method.  Pipelines are handled
     * separately by {@link InternalAggregations#topLevelReduce(List, ReduceContext)}
     * @param ctor used to build the {@link InternalAggregations}. The top level reduce specifies a constructor
     *            that adds pipeline aggregation information that is used to send pipeline aggregations to
     *            older versions of Elasticsearch that require the pipeline aggregations to be returned
     *            as part of the aggregation tree
     */
    public static InternalAggregations reduce(
        List<InternalAggregations> aggregationsList,
        ReduceContext context,
        Function<List<InternalAggregation>, InternalAggregations> ctor
    ) {
        if (aggregationsList.isEmpty()) {
            return null;
        }

        // first we collect all aggregations of the same type and list them together
        Map<String, List<InternalAggregation>> aggByName = new HashMap<>();
        for (InternalAggregations aggregations : aggregationsList) {
            for (Aggregation aggregation : aggregations.aggregations) {
                List<InternalAggregation> aggs = aggByName.computeIfAbsent(
                    aggregation.getName(),
                    k -> new ArrayList<>(aggregationsList.size())
                );
                aggs.add((InternalAggregation) aggregation);
            }
        }

        // now we can use the first aggregation of each list to handle the reduce of its list
        List<InternalAggregation> reducedAggregations = new ArrayList<>();
        for (Map.Entry<String, List<InternalAggregation>> entry : aggByName.entrySet()) {
            List<InternalAggregation> aggregations = entry.getValue();
            // Sort aggregations so that unmapped aggs come last in the list
            // If all aggs are unmapped, the agg that leads the reduction will just return itself
            aggregations.sort(INTERNAL_AGG_COMPARATOR);
            InternalAggregation first = aggregations.get(0); // the list can't be empty as it's created on demand
            if (first.mustReduceOnSingleInternalAgg() || aggregations.size() > 1) {
                reducedAggregations.add(first.reduce(aggregations, context));
            } else {
                // no need for reduce phase
                reducedAggregations.add(first);
            }
        }

        return ctor.apply(reducedAggregations);
    }

    /**
     * Version of {@link #reduce(List, ReduceContext, Function)} for nodes inside the aggregation tree.
     */
    public static InternalAggregations reduce(List<InternalAggregations> aggregationsList, ReduceContext context) {
        return reduce(aggregationsList, context, InternalAggregations::from);
    }
}
