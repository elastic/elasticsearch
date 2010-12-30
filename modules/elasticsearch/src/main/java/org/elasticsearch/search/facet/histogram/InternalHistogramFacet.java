/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
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

package org.elasticsearch.search.facet.histogram;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.TLongDoubleHashMap;
import org.elasticsearch.common.trove.TLongLongHashMap;
import org.elasticsearch.common.trove.TLongLongIterator;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.InternalFacet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * @author kimchy (shay.banon)
 */
public class InternalHistogramFacet implements HistogramFacet, InternalFacet {

    private static final String STREAM_TYPE = "histogram";

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override public Facet readFacet(String type, StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    @Override public String streamType() {
        return STREAM_TYPE;
    }

    private String name;

    private String keyFieldName;
    private String valueFieldName;

    private long interval;

    private ComparatorType comparatorType;

    TLongLongHashMap counts;

    TLongDoubleHashMap totals;

    Collection<Entry> entries = null;

    private InternalHistogramFacet() {
    }

    public InternalHistogramFacet(String name, String keyFieldName, String valueFieldName, long interval, ComparatorType comparatorType, TLongLongHashMap counts, TLongDoubleHashMap totals) {
        this.name = name;
        this.keyFieldName = keyFieldName;
        this.valueFieldName = valueFieldName;
        this.interval = interval;
        this.comparatorType = comparatorType;
        this.counts = counts;
        this.totals = totals;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return name();
    }

    @Override public String keyFieldName() {
        return this.keyFieldName;
    }

    @Override public String getKeyFieldName() {
        return keyFieldName();
    }

    @Override public String valueFieldName() {
        return this.valueFieldName;
    }

    @Override public String getValueFieldName() {
        return valueFieldName();
    }

    @Override public String type() {
        return TYPE;
    }

    @Override public String getType() {
        return type();
    }

    @Override public List<Entry> entries() {
        computeEntries();
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<Entry>) entries;
    }

    @Override public List<Entry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return computeEntries().iterator();
    }

    private Collection<Entry> computeEntries() {
        if (entries != null) {
            return entries;
        }
        TreeSet<Entry> set = new TreeSet<Entry>(comparatorType.comparator());
        for (TLongLongIterator it = counts.iterator(); it.hasNext();) {
            it.advance();
            set.add(new Entry(it.key(), it.value(), totals.get(it.key())));
        }
        entries = set;
        return entries;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _KEY_FIELD = new XContentBuilderString("_key_field");
        static final XContentBuilderString _VALUE_FIELD = new XContentBuilderString("_value_field");
        static final XContentBuilderString _COMPARATOR = new XContentBuilderString("_comparator");
        static final XContentBuilderString _INTERVAL = new XContentBuilderString("_interval");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString KEY = new XContentBuilderString("key");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, HistogramFacet.TYPE);
        builder.field(Fields._KEY_FIELD, keyFieldName);
        builder.field(Fields._VALUE_FIELD, valueFieldName);
        builder.field(Fields._COMPARATOR, comparatorType.description());
        builder.field(Fields._INTERVAL, interval);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : computeEntries()) {
            builder.startObject();
            builder.field(Fields.KEY, entry.key());
            builder.field(Fields.COUNT, entry.count());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    public static InternalHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalHistogramFacet facet = new InternalHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        keyFieldName = in.readUTF();
        valueFieldName = in.readUTF();
        interval = in.readVLong();
        comparatorType = ComparatorType.fromId(in.readByte());

        int size = in.readVInt();
        if (size == 0) {
            counts = EMPTY_LONG_LONG_MAP;
            totals = EMPTY_LONG_DOUBLE_MAP;
        } else {
            counts = new TLongLongHashMap(size);
            totals = new TLongDoubleHashMap(size);
            for (int i = 0; i < size; i++) {
                long key = in.readLong();
                counts.put(key, in.readVLong());
                totals.put(key, in.readDouble());
            }
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(keyFieldName);
        out.writeUTF(valueFieldName);
        out.writeVLong(interval);
        out.writeByte(comparatorType.id());
        // optimize the write, since we know we have the same buckets as keys
        out.writeVInt(counts.size());
        for (TLongLongIterator it = counts.iterator(); it.hasNext();) {
            it.advance();
            out.writeLong(it.key());
            out.writeVLong(it.value());
            out.writeDouble(totals.get(it.key()));
        }
    }

    static final TLongLongHashMap EMPTY_LONG_LONG_MAP = new TLongLongHashMap();
    static final TLongDoubleHashMap EMPTY_LONG_DOUBLE_MAP = new TLongDoubleHashMap();
}