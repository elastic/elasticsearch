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

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
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
public class InternalGeoDistanceFacet extends InternalFacet implements GeoDistanceFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("geoDistance"));

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readGeoDistanceFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }

    Entry[] entries;

    InternalGeoDistanceFacet() {
    }

    public InternalGeoDistanceFacet(String name, Entry[] entries) {
        super(name);
        this.entries = entries;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public List<Entry> getEntries() {
        return ImmutableList.copyOf(entries);
    }

    @Override
    public Iterator<Entry> iterator() {
        return getEntries().iterator();
    }

    public Facet reduce(ReduceContext context) {
        List<Facet> facets = context.facets();
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalGeoDistanceFacet agg = (InternalGeoDistanceFacet) facets.get(0);
        for (int i = 1; i < facets.size(); i++) {
            InternalGeoDistanceFacet geoDistanceFacet = (InternalGeoDistanceFacet) facets.get(i);
            for (int j = 0; j < geoDistanceFacet.entries.length; j++) {
                GeoDistanceFacet.Entry aggEntry = agg.entries[j];
                GeoDistanceFacet.Entry currentEntry = geoDistanceFacet.entries[j];
                aggEntry.count += currentEntry.count;
                aggEntry.totalCount += currentEntry.totalCount;
                aggEntry.total += currentEntry.total;
                if (currentEntry.min < aggEntry.min) {
                    aggEntry.min = currentEntry.min;
                }
                if (currentEntry.max > aggEntry.max) {
                    aggEntry.max = currentEntry.max;
                }
            }
        }
        return agg;
    }

    public static InternalGeoDistanceFacet readGeoDistanceFacet(StreamInput in) throws IOException {
        InternalGeoDistanceFacet facet = new InternalGeoDistanceFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        entries = new Entry[in.readVInt()];
        for (int i = 0; i < entries.length; i++) {
            entries[i] = new Entry(in.readDouble(), in.readDouble(), in.readVLong(), in.readVLong(), in.readDouble(), in.readDouble(), in.readDouble());
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
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
        builder.startObject(getName());
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
            builder.field(Fields.COUNT, entry.getCount());
            builder.field(Fields.MIN, entry.getMin());
            builder.field(Fields.MAX, entry.getMax());
            builder.field(Fields.TOTAL_COUNT, entry.getTotalCount());
            builder.field(Fields.TOTAL, entry.getTotal());
            builder.field(Fields.MEAN, entry.getMean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
