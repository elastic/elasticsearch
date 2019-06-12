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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

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

    private final List<SiblingPipelineAggregator> topLevelPipelineAggregators;

    /**
     * Constructs a new aggregation.
     */
    public InternalAggregations(List<InternalAggregation> aggregations) {
        super(aggregations);
        this.topLevelPipelineAggregators = Collections.emptyList();
    }

    /**
     * Constructs a new aggregation providing its {@link InternalAggregation}s and {@link SiblingPipelineAggregator}s
     */
    public InternalAggregations(List<InternalAggregation> aggregations, List<SiblingPipelineAggregator> topLevelPipelineAggregators) {
        super(aggregations);
        this.topLevelPipelineAggregators = Objects.requireNonNull(topLevelPipelineAggregators);
    }

    public InternalAggregations(StreamInput in) throws IOException {
        super(in.readList(stream -> in.readNamedWriteable(InternalAggregation.class)));
        this.topLevelPipelineAggregators = in.readList(
            stream -> (SiblingPipelineAggregator)in.readNamedWriteable(PipelineAggregator.class));
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList((List<InternalAggregation>)aggregations);
        out.writeNamedWriteableList(topLevelPipelineAggregators);
    }

    /**
     * Returns the top-level pipeline aggregators.
     * Note that top-level pipeline aggregators become normal aggregation once the final reduction has been performed, after which they
     * become part of the list of {@link InternalAggregation}s.
     */
    public List<SiblingPipelineAggregator> getTopLevelPipelineAggregators() {
        return topLevelPipelineAggregators;
    }

    /**
     * Reduces the given list of aggregations as well as the top-level pipeline aggregators extracted from the first
     * {@link InternalAggregations} object found in the list.
     * Note that top-level pipeline aggregators are reduced only as part of the final reduction phase, otherwise they are left untouched.
     */
    public static InternalAggregations reduce(List<InternalAggregations> aggregationsList, ReduceContext context) {
        if (aggregationsList.isEmpty()) {
            return null;
        }
        List<SiblingPipelineAggregator> topLevelPipelineAggregators = aggregationsList.get(0).getTopLevelPipelineAggregators();

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

        if (context.isFinalReduce()) {
            for (SiblingPipelineAggregator pipelineAggregator : topLevelPipelineAggregators) {
                InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(reducedAggregations), context);
                reducedAggregations.add(newAgg);
            }
            return new InternalAggregations(reducedAggregations);
        }
        return new InternalAggregations(reducedAggregations, topLevelPipelineAggregators);
    }
}
