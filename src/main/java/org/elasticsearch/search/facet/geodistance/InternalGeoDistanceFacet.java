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

package org.elasticsearch.search.facet.geodistance;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 *
 */
public class InternalGeoDistanceFacet implements GeoDistanceFacet, InternalFacet {

    private static final String STREAM_TYPE = "geoDistance";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readGeoDistanceFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    private String name;

    Entry[] entries;

    InternalGeoDistanceFacet() {
    }

    public InternalGeoDistanceFacet(String name, Entry[] entries) {
        this.name = name;
        this.entries = entries;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String getName() {
        return name();
    }

    @Override
    public String type() {
        return TYPE;
    }

    @Override
    public String getType() {
        return type();
    }

    @Override
    public List<Entry> entries() {
        return ImmutableList.copyOf(entries);
    }

    @Override
    public List<Entry> getEntries() {
        return entries();
    }

    @Override
    public Iterator<Entry> iterator() {
        return entries().iterator();
    }

    public static InternalGeoDistanceFacet readGeoDistanceFacet(StreamInput in) throws IOException {
        InternalGeoDistanceFacet facet = new InternalGeoDistanceFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = new Entry(in.readDouble(), in.readDouble(), in.readVLong(), in.readVLong(), in.readDouble(), in.readDouble(), in.readDouble());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeVInt(entries.length);
        for (Entry entry : entries) {
            out.writeDouble(entry.from);
            out.writeDouble(entry.to);
            out.writeVLong(entry.count);
            out.writeVLong(entry.totalCount);
            out.writeDouble(entry.total);
            out.writeDouble(entry.min);
            out.writeDouble(entry.max);
        }
    }


    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString RANGES = new XContentBuilderString("ranges");
        static final XContentBuilderString FROM = new XContentBuilderString("from");
        static final XContentBuilderString TO = new XContentBuilderString("to");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, GeoDistanceFacet.TYPE);
        builder.startArray(Fields.RANGES);
        for (Entry entry : entries) {
            builder.startObject();
            if (!Double.isInfinite(entry.from)) {
                builder.field(Fields.FROM, entry.from);
            }
            if (!Double.isInfinite(entry.to)) {
                builder.field(Fields.TO, entry.to);
            }
            builder.field(Fields.MIN, entry.min());
            builder.field(Fields.MAX, entry.max());
            builder.field(Fields.TOTAL_COUNT, entry.totalCount());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
