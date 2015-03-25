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

package org.elasticsearch.search.aggregations.reducers;

import com.google.common.base.Function;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregation.Type;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.Map;

public abstract class Reducer implements Streamable {

    /**
     * Parses the reducer request and creates the appropriate reducer factory
     * for it.
     * 
     * @see {@link ReducerFactory}
     */
    public static interface Parser {

        public static final ParseField BUCKETS_PATH = new ParseField("bucketsPath");

        /**
         * @return The reducer type this parser is associated with.
         */
        String type();

        /**
         * Returns the reducer factory with which this parser is associated.
         * 
         * @param reducerName
         *            The name of the reducer
         * @param parser
         *            The xcontent parser
         * @param context
         *            The search context
         * @return The resolved reducer factory
         * @throws java.io.IOException
         *             When parsing fails
         */
        ReducerFactory parse(String reducerName, XContentParser parser, SearchContext context) throws IOException;

    }

    public static final Function<Aggregation, InternalAggregation> FUNCTION = new Function<Aggregation, InternalAggregation>() {
        @Override
        public InternalAggregation apply(Aggregation input) {
            return (InternalAggregation) input;
        }
    };

    private String name;
    private String[] bucketsPaths;
    private Map<String, Object> metaData;

    protected Reducer() { // for Serialisation
    }

    protected Reducer(String name, String[] bucketsPaths, Map<String, Object> metaData) {
        this.name = name;
        this.bucketsPaths = bucketsPaths;
        this.metaData = metaData;
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

    public abstract Type type();

    public abstract InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext);

    @Override
    public final void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeStringArray(bucketsPaths);
        out.writeMap(metaData);
        doWriteTo(out);
    }

    protected abstract void doWriteTo(StreamOutput out) throws IOException;

    @Override
    public final void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        bucketsPaths = in.readStringArray();
        metaData = in.readMap();
        doReadFrom(in);
    }

    protected abstract void doReadFrom(StreamInput in) throws IOException;
}
