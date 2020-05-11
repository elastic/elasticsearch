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
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
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
import java.util.stream.Collectors;

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
     * Constructs a new aggregation.
     */
    public InternalAggregations(List<InternalAggregation> aggregations) {
        super(aggregations);
    }

    public InternalAggregations(StreamInput in) throws IOException {
        super(in.readList(stream -> in.readNamedWriteable(InternalAggregation.class)));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList(getInternalAggregations());
    }

    /**
     * Make a mutable copy of the aggregation results.
     */
    public List<InternalAggregation> copyResults() {
        return new ArrayList<>(getInternalAggregations());
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
        InternalAggregations reduced = reduce(aggregationsList, context);
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
                InternalAggregation newAgg = sib.doReduce(new InternalAggregations(reducedInternalAggs), context);
                reducedInternalAggs.add(newAgg);
            }
            return new InternalAggregations(reducedInternalAggs);
        }
        return reduced;
    }

    /**
     * Reduces the given list of aggregations as well as the top-level pipeline aggregators extracted from the first
     * {@link InternalAggregations} object found in the list.
     * Note that pipeline aggregations _are not_ reduced by this method.  Pipelines are handled
     * separately by {@link InternalAggregations#topLevelReduce(List, ReduceContext)}
     */
    public static InternalAggregations reduce(List<InternalAggregations> aggregationsList, ReduceContext context) {
        if (aggregationsList.isEmpty()) {
            return null;
        }

        // first we collect all aggregations of the same type and list them together
        Map<String, List<InternalAggregation>> aggByName = new HashMap<>();
        for (InternalAggregations aggregations : aggregationsList) {
            for (Aggregation aggregation : aggregations.aggregations) {
                List<InternalAggregation> aggs = aggByName.computeIfAbsent(
                        aggregation.getName(), k -> new ArrayList<>(aggregationsList.size()));
                aggs.add((InternalAggregation)aggregation);
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
            reducedAggregations.add(first.reduce(aggregations, context));
        }

        return new InternalAggregations(reducedAggregations);
    }
}
