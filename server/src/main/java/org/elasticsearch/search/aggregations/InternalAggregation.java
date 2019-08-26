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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.action.search.RestSearchAction;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.IntConsumer;

/**
 * An internal implementation of {@link Aggregation}. Serves as a base class for all aggregation implementations.
 */
public abstract class InternalAggregation implements Aggregation, NamedWriteable {

    public static class ReduceContext {

        private final BigArrays bigArrays;
        private final ScriptService scriptService;
        private final IntConsumer multiBucketConsumer;
        private final boolean isFinalReduce;

        public ReduceContext(BigArrays bigArrays, ScriptService scriptService, boolean isFinalReduce) {
            this(bigArrays, scriptService, (s) -> {}, isFinalReduce);
        }

        public ReduceContext(BigArrays bigArrays, ScriptService scriptService, IntConsumer multiBucketConsumer, boolean isFinalReduce) {
            this.bigArrays = bigArrays;
            this.scriptService = scriptService;
            this.multiBucketConsumer = multiBucketConsumer;
            this.isFinalReduce = isFinalReduce;
        }

        /**
         * Returns <code>true</code> iff the current reduce phase is the final reduce phase. This indicates if operations like
         * pipeline aggregations should be applied or if specific features like {@code minDocCount} should be taken into account.
         * Operations that are potentially losing information can only be applied during the final reduce phase.
         */
        public boolean isFinalReduce() {
            return isFinalReduce;
        }

        public BigArrays bigArrays() {
            return bigArrays;
        }

        public ScriptService scriptService() {
            return scriptService;
        }

        /**
         * Adds {@code count} buckets to the global count for the request and fails if this number is greater than
         * the maximum number of buckets allowed in a response
         */
        public void consumeBucketsAndMaybeBreak(int size) {
            multiBucketConsumer.accept(size);
        }

    }

    protected final String name;

    protected final Map<String, Object> metaData;

    private final List<PipelineAggregator> pipelineAggregators;

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

    /**
     * Read from a stream.
     */
    protected InternalAggregation(StreamInput in) throws IOException {
        name = in.readString();
        metaData = in.readMap();
        pipelineAggregators = in.readNamedWriteableList(PipelineAggregator.class);
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeGenericValue(metaData);
        out.writeNamedWriteableList(pipelineAggregators);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public String getName() {
        return name;
    }

    /**
     * Reduces the given aggregations to a single one and returns it. In <b>most</b> cases, the assumption will be the all given
     * aggregations are of the same type (the same type as this aggregation). For best efficiency, when implementing,
     * try reusing an existing instance (typically the first in the given list) to save on redundant object
     * construction.
     */
    public final InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        InternalAggregation aggResult = doReduce(aggregations, reduceContext);
        if (reduceContext.isFinalReduce()) {
            for (PipelineAggregator pipelineAggregator : pipelineAggregators) {
                aggResult = pipelineAggregator.reduce(aggResult, reduceContext);
            }
        }
        return aggResult;
    }

    public abstract InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext);

    /**
     * Return true if this aggregation is mapped, and can lead a reduction.  If this agg returns
     * false, it should return itself if asked to lead a reduction
     */
    public boolean isMapped() {
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
    public Map<String, Object> getMetaData() {
        return metaData;
    }

    public List<PipelineAggregator> pipelineAggregators() {
        return pipelineAggregators;
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
        if (this.metaData != null) {
            builder.field(CommonFields.META.getPreferredName());
            builder.map(this.metaData);
        }
        doXContentBody(builder, params);
        builder.endObject();
        return builder;
    }

    public abstract XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException;

    @Override
    public int hashCode() {
        return Objects.hash(name, metaData, pipelineAggregators);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        if (obj == this) { return true; }

        InternalAggregation other = (InternalAggregation) obj;
        return Objects.equals(name, other.name) &&
                Objects.equals(pipelineAggregators, other.pipelineAggregators) &&
                Objects.equals(metaData, other.metaData);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
