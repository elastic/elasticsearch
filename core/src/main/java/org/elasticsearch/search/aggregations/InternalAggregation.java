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

import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregatorStreams;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * An internal implementation of {@link Aggregation}. Serves as a base class for all aggregation implementations.
 */
public abstract class InternalAggregation implements Aggregation, ToXContent, Streamable {


    /**
     * The aggregation type that holds all the string types that are associated with an aggregation:
     * <ul>
     *     <li>name - used as the parser type</li>
     *     <li>stream - used as the stream type</li>
     * </ul>
     */
    public static class Type {

        private String name;
        private BytesReference stream;

        public Type(String name) {
            this(name, new BytesArray(name));
        }

        public Type(String name, String stream) {
            this(name, new BytesArray(stream));
        }

        public Type(String name, BytesReference stream) {
            this.name = name;
            this.stream = stream;
        }

        /**
         * @return The name of the type of aggregation.  This is the key for parsing the aggregation from XContent and is the name of the
         * aggregation's builder when serialized.
         */
        public String name() {
            return name;
        }

        /**
         * @return  The name of the stream type (used for registering the aggregation stream
         *          (see {@link AggregationStreams#registerStream(AggregationStreams.Stream, org.elasticsearch.common.bytes.BytesReference...)}).
         */
        public BytesReference stream() {
            return stream;
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class ReduceContext {

        private final BigArrays bigArrays;
        private final ScriptService scriptService;
        private final ClusterState clusterState;

        public ReduceContext(BigArrays bigArrays, ScriptService scriptService, ClusterState clusterState) {
            this.bigArrays = bigArrays;
            this.scriptService = scriptService;
            this.clusterState = clusterState;
        }

        public BigArrays bigArrays() {
            return bigArrays;
        }

        public ScriptService scriptService() {
            return scriptService;
        }

        public ClusterState clusterState() {
            return clusterState;
        }
    }


    protected String name;

    protected Map<String, Object> metaData;

    private List<PipelineAggregator> pipelineAggregators;

    /** Constructs an un initialized addAggregation (used for serialization) **/
    protected InternalAggregation() {}

    /**
     * Constructs an get with a given name.
     *
     * @param name The name of the get.
     */
    protected InternalAggregation(String name, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        this.name = name;
        this.pipelineAggregators = pipelineAggregators;
        this.metaData = metaData;
    }

    @Override
    public String getName() {
        return name;
    }

    /**
     * @return The {@link Type} of this aggregation
     */
    public abstract Type type();

    /**
     * Reduces the given addAggregation to a single one and returns it. In <b>most</b> cases, the assumption will be the all given
     * addAggregation are of the same type (the same type as this aggregation). For best efficiency, when implementing,
     * try reusing an existing get instance (typically the first in the given list) to save on redundant object
     * construction.
     */
    public final InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalAggregation aggResult = doReduce(aggregations, reduceContext);
        for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
            aggResult = pipelineAggregator.reduce(aggResult, reduceContext);
        }
        return aggResult;
    }

    public abstract InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext);

    @Override
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
    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public List<PipelineAggregator> pipelineAggregators() {
        return pipelineAggregators;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        if (this.metaData != null) {
            builder.field(CommonFields.META);
            builder.map(this.metaData);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeGenericValue(metaData);
        out.writeVInt(pipelineAggregators.size());
        for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
            out.writeBytesReference(pipelineAggregator.type().stream());
            pipelineAggregator.writeTo(out);
        }
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        metaData = in.readMap();
        int size = in.readVInt();
        if (size == 0) {
            pipelineAggregators = Collections.emptyList();
        } else {
            pipelineAggregators = new ArrayList<>(size);
            for (int i = 0; i < size; i++) {
                BytesReference type = in.readBytesReference();
                PipelineAggregator pipelineAggregator = PipelineAggregatorStreams.stream(type).readResult(in);
                pipelineAggregators.add(pipelineAggregator);
            }
        }
        doReadFrom(in);
    }

    protected abstract void doReadFrom(StreamInput in) throws IOException;

    /**
     * Common xcontent fields that are shared among addAggregation
     */
    public static final class CommonFields extends ParseField.CommonFields {
        // todo convert these to ParseField
        public static final String META = "meta";
        public static final String BUCKETS = "buckets";
        public static final String VALUE = "value";
        public static final String VALUES = "values";
        public static final String VALUE_AS_STRING = "value_as_string";
        public static final String DOC_COUNT = "doc_count";
        public static final String KEY = "key";
        public static final String KEY_AS_STRING = "key_as_string";
        public static final String FROM = "from";
        public static final String FROM_AS_STRING = "from_as_string";
        public static final String TO = "to";
        public static final String TO_AS_STRING = "to_as_string";
    }

}
