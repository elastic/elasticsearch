/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
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

package org.elasticsearch.search.facet.query;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;

/**
 *
 */
public class InternalQueryFacet implements QueryFacet, InternalFacet {

    private static final String STREAM_TYPE = "query";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readQueryFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    private String name;

    private long count;

    private InternalQueryFacet() {

    }

    public InternalQueryFacet(String name, long count) {
        this.name = name;
        this.count = count;
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    /**
     * The "logical" name of the facet.
     */
    public String name() {
        return name;
    }

    @Override
    public String getName() {
        return name();
    }

    /**
     * The count of the facet.
     */
    public long count() {
        return count;
    }

    /**
     * The count of the facet.
     */
    public long getCount() {
        return count;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, QueryFacet.TYPE);
        builder.field(Fields.COUNT, count);
        builder.endObject();
        return builder;
    }

    public static QueryFacet readQueryFacet(StreamInput in) throws IOException {
        InternalQueryFacet result = new InternalQueryFacet();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        count = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVLong(count);
    }
}