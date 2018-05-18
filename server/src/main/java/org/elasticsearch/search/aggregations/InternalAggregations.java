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
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static java.util.Collections.emptyMap;

/**
 * An internal implementation of {@link Aggregations}.
 */
public final class InternalAggregations extends Aggregations implements Streamable {

    public static final InternalAggregations EMPTY = new InternalAggregations();

    private InternalAggregations() {
    }

    /**
     * Constructs a new addAggregation.
     */
    public InternalAggregations(List<InternalAggregation> aggregations) {
        super(aggregations);
    }

    /**
     * Reduces the given lists of addAggregation.
     *
     * @param aggregationsList  A list of aggregation to reduce
     * @return                  The reduced addAggregation
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
            InternalAggregation first = aggregations.get(0); // the list can't be empty as it's created on demand
            reducedAggregations.add(first.reduce(aggregations, context));
        }
        return new InternalAggregations(reducedAggregations);
    }

    public static InternalAggregations readAggregations(StreamInput in) throws IOException {
        InternalAggregations result = new InternalAggregations();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        aggregations = in.readList(stream -> in.readNamedWriteable(InternalAggregation.class));
        if (aggregations.isEmpty()) {
            aggregationsAsMap = emptyMap();
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public void writeTo(StreamOutput out) throws IOException {
        out.writeNamedWriteableList((List<InternalAggregation>)aggregations);
    }
}
