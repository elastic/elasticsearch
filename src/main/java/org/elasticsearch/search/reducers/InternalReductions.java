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
package org.elasticsearch.search.reducers;

import com.google.common.base.Function;
import com.google.common.collect.*;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.aggregations.Aggregation;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * An internal implementation of {@link Reductions}.
 */
public class InternalReductions implements Reductions, ToXContent, Streamable {

    public final static InternalReductions EMPTY = new InternalReductions();
    private static final Function<InternalReduction, Reduction> SUPERTYPE_CAST = new Function<InternalReduction, Reduction>() {
        @Override
        public Reduction apply(InternalReduction input) {
            return input;
        }
    };

    private List<InternalReduction> reductions = ImmutableList.of();

    private Map<String, InternalReduction> aggregationsAsMap;

    private InternalReductions() {
    }

    /**
     * Constructs a new addReduction.
     */
    public InternalReductions(List<InternalReduction> aggregations) {
        this.reductions = aggregations;
    }

    /**
     * Iterates over the {@link Reduction}s.
     */
    @Override
    public Iterator<Reduction> iterator() {
        return Iterators.transform(reductions.iterator(), SUPERTYPE_CAST);
    }

    /**
     * The list of {@link Reduction}s.
     */
    public List<Reduction> asList() {
        return Lists.transform(reductions, SUPERTYPE_CAST);
    }

    /**
     * Returns the {@link Aggregation}s keyed by map.
     */
    public Map<String, Reduction> asMap() {
        return getAsMap();
    }

    /**
     * Returns the {@link Reduction}s keyed by map.
     */
    public Map<String, Reduction> getAsMap() {
        if (aggregationsAsMap == null) {
            Map<String, InternalReduction> aggregationsAsMap = newHashMap();
            for (InternalReduction aggregation : reductions) {
                aggregationsAsMap.put(aggregation.getName(), aggregation);
            }
            this.aggregationsAsMap = aggregationsAsMap;
        }
        return Maps.transformValues(aggregationsAsMap, SUPERTYPE_CAST);
    }

    /**
     * @return the reduction of the specified name.
     */
    @SuppressWarnings("unchecked")
    @Override
    public <A extends Reduction> A get(String name) {
        return (A) asMap().get(name);
    }

    /** The fields required to write this addReduction to xcontent */
    static class Fields {
        public static final XContentBuilderString REDUCTIONS = new XContentBuilderString("reductions");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        if (reductions.isEmpty()) {
            return builder;
        }
        builder.startObject(Fields.REDUCTIONS);
        toXContentInternal(builder, params);
        return builder.endObject();
    }

    /**
     * Directly write all the addAggregation without their bounding object. Used by sub-addAggregation (non top level addAggregation)
     */
    public XContentBuilder toXContentInternal(XContentBuilder builder, Params params) throws IOException {
        for (Reduction aggregation : reductions) {
            ((InternalReduction) aggregation).toXContent(builder, params);
        }
        return builder;
    }

    public static InternalReductions readAggregations(StreamInput in) throws IOException {
        InternalReductions result = new InternalReductions();
        result.readFrom(in);
        return result;
    }

    public static InternalReductions readOptionalAggregations(StreamInput in) throws IOException {
        return in.readOptionalStreamable(new InternalReductions());
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        if (size == 0) {
            reductions = ImmutableList.of();
            aggregationsAsMap = ImmutableMap.of();
        } else {
            reductions = Lists.newArrayListWithCapacity(size);
            for (int i = 0; i < size; i++) {
                BytesReference type = in.readBytesReference();
                InternalReduction aggregation = ReductionStreams.stream(type).readResult(in);
                reductions.add(aggregation);
            }
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(reductions.size());
        for (Reduction aggregation : reductions) {
            InternalReduction internal = (InternalReduction) aggregation;
            out.writeBytesReference(internal.type().stream());
            internal.writeTo(out);
        }
    }

}
