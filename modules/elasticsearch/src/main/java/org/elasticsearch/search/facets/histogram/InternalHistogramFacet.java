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

package org.elasticsearch.search.facets.histogram;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.TLongDoubleHashMap;
import org.elasticsearch.common.trove.TLongDoubleIterator;
import org.elasticsearch.common.trove.TLongLongHashMap;
import org.elasticsearch.common.trove.TLongLongIterator;
import org.elasticsearch.common.xcontent.builder.XContentBuilder;
import org.elasticsearch.search.facets.Facet;
import org.elasticsearch.search.facets.internal.InternalFacet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * @author kimchy (shay.banon)
 */
public class InternalHistogramFacet implements HistogramFacet, InternalFacet {

    private static final TLongLongHashMap EMPTY_LONG_LONG_MAP = new TLongLongHashMap();
    private static final TLongDoubleHashMap EMPTY_LONG_DOUBLE_MAP = new TLongDoubleHashMap();

    private String name;

    private String keyFieldName;
    private String valueFieldName;

    private long interval;

    private ComparatorType comparatorType;

    private TLongLongHashMap counts;

    private TLongDoubleHashMap totals;

    private Collection<Entry> entries = null;

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

    @Override public Type type() {
        return Type.HISTOGRAM;
    }

    @Override public Type getType() {
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

    @Override public Facet aggregate(Iterable<Facet> facets) {
        TLongLongHashMap counts = null;
        TLongDoubleHashMap totals = null;

        for (Facet facet : facets) {
            if (!facet.name().equals(name)) {
                continue;
            }
            InternalHistogramFacet histoFacet = (InternalHistogramFacet) facet;
            if (!histoFacet.counts.isEmpty()) {
                if (counts == null) {
                    counts = histoFacet.counts;
                } else {
                    for (TLongLongIterator it = histoFacet.counts.iterator(); it.hasNext();) {
                        it.advance();
                        counts.adjustOrPutValue(it.key(), it.value(), it.value());
                    }
                }
            }

            if (!histoFacet.totals.isEmpty()) {
                if (totals == null) {
                    totals = histoFacet.totals;
                } else {
                    for (TLongDoubleIterator it = histoFacet.totals.iterator(); it.hasNext();) {
                        it.advance();
                        totals.adjustOrPutValue(it.key(), it.value(), it.value());
                    }
                }
            }
        }
        if (counts == null) {
            counts = EMPTY_LONG_LONG_MAP;
        }
        if (totals == null) {
            totals = EMPTY_LONG_DOUBLE_MAP;
        }

        return new InternalHistogramFacet(name, keyFieldName, valueFieldName, interval, comparatorType, counts, totals);
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field("_type", "histogram");
        builder.field("_key_field", keyFieldName);
        builder.field("_value_field", valueFieldName);
        builder.field("_comparator", comparatorType.description());
        builder.field("_interval", interval);
        builder.startArray("entries");
        for (Entry entry : computeEntries()) {
            builder.startObject();
            builder.field("key", entry.key());
            builder.field("count", entry.count());
            builder.field("total", entry.total());
            builder.field("mean", entry.mean());
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
}