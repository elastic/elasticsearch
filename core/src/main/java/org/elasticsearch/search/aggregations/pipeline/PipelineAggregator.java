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

package org.elasticsearch.search.aggregations.pipeline;


import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.index.query.QueryParseContext;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.aggregations.PipelineAggregationBuilder;

import java.io.IOException;
import java.util.Map;

public abstract class PipelineAggregator implements Streamable, NamedWriteable {
    // NORELEASE remove Streamable

    /**
     * Parse the {@link PipelineAggregationBuilder} from a {@link QueryParseContext}.
     */
    @FunctionalInterface
    public interface Parser {
        ParseField BUCKETS_PATH = new ParseField("buckets_path");
        ParseField FORMAT = new ParseField("format");
        ParseField GAP_POLICY = new ParseField("gap_policy");

        /**
         * Returns the pipeline aggregator factory with which this parser is
         * associated.
         *
         * @param pipelineAggregatorName
         *            The name of the pipeline aggregation
         * @param context
         *            The search context
         * @return The resolved pipeline aggregator factory
         * @throws java.io.IOException
         *             When parsing fails
         */
        PipelineAggregationBuilder parse(String pipelineAggregatorName, QueryParseContext context)
                throws IOException;
    }

    private String name;
    private String[] bucketsPaths;
    private Map<String, Object> metaData;

    protected PipelineAggregator() { // for Serialisation
    }

    protected PipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metaData) {
        this.name = name;
        this.bucketsPaths = bucketsPaths;
        this.metaData = metaData;
    }

    /**
     * Read from a stream.
     */
    protected PipelineAggregator(StreamInput in) throws IOException {
        name = in.readString();
        bucketsPaths = in.readStringArray();
        metaData = in.readMap();
    }

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        try {
            getWriteableName(); // Throws UnsupportedOperationException if this aggregation should be read using old style Streams
            assert false : "Used reading constructor instead";
        } catch (UnsupportedOperationException e) {
            // OK
        }
        name = in.readString();
        bucketsPaths = in.readStringArray();
        metaData = in.readMap();
        doReadFrom(in);
    }

    protected void doReadFrom(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Use reading constructor instead"); // NORELEASE remove when we remove Streamable
    }

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);  // NORELEASE remote writing the name - it is automatically handled with writeNamedWriteable
        out.writeStringArray(bucketsPaths);
        out.writeMap(metaData);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public String getWriteableName() {
        // NORELEASE remove me when all InternalAggregations override it
        throw new UnsupportedOperationException("Override on every class");
    }

    public String name() {
        return name;
    }

    public String[] bucketsPaths() {
        return bucketsPaths;
    }

    public Map<String, Object> metaData() {
        return metaData;
    }

    public Type type() {
        // NORELEASE remove this method
        throw new UnsupportedOperationException(getClass().getName() + " used type but should Use getWriteableName instead");
    }

    public abstract InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext);
}
