/*
 * Licensed to Elastic Search and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Elastic Search licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.facet.datehistogram;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.TLongDoubleHashMap;
import org.elasticsearch.common.trove.TLongDoubleIterator;
import org.elasticsearch.common.trove.TLongLongHashMap;
import org.elasticsearch.common.trove.TLongLongIterator;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * @author kimchy (shay.banon)
 */
public class InternalCountAndTotalDateHistogramFacet extends InternalDateHistogramFacet {

    private static final String STREAM_TYPE = "ctdHistogram";

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


    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     */
    public class CountAndTotalEntry implements Entry {
        private final long time;
        private final long count;
        private final double total;

        public CountAndTotalEntry(long time, long count, double total) {
            this.time = time;
            this.count = count;
            this.total = total;
        }

        public long time() {
            return time;
        }

        public long getTime() {
            return time();
        }

        public long count() {
            return count;
        }

        public long getCount() {
            return count();
        }

        public double total() {
            return total;
        }

        public double getTotal() {
            return total();
        }

        public double mean() {
            return total / count;
        }

        public double getMean() {
            return mean();
        }
    }

    private String name;

    private ComparatorType comparatorType;

    TLongLongHashMap counts;

    TLongDoubleHashMap totals;

    Collection<CountAndTotalEntry> entries = null;

    private InternalCountAndTotalDateHistogramFacet() {
    }

    public InternalCountAndTotalDateHistogramFacet(String name, ComparatorType comparatorType, TLongLongHashMap counts, TLongDoubleHashMap totals) {
        this.name = name;
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

    @Override public String type() {
        return TYPE;
    }

    @Override public String getType() {
        return type();
    }

    @Override public List<CountAndTotalEntry> entries() {
        computeEntries();
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<CountAndTotalEntry>) entries;
    }

    @Override public List<CountAndTotalEntry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return (Iterator) computeEntries().iterator();
    }

    private Collection<CountAndTotalEntry> computeEntries() {
        if (entries != null) {
            return entries;
        }
        TreeSet<CountAndTotalEntry> set = new TreeSet<CountAndTotalEntry>(comparatorType.comparator());
        for (TLongLongIterator it = counts.iterator(); it.hasNext();) {
            it.advance();
            set.add(new CountAndTotalEntry(it.key(), it.value(), totals.get(it.key())));
        }
        entries = set;
        return entries;
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        TLongLongHashMap counts = null;
        TLongDoubleHashMap totals = null;

        InternalCountAndTotalDateHistogramFacet firstHistoFacet = (InternalCountAndTotalDateHistogramFacet) facets.get(0);
        for (Facet facet : facets) {
            InternalCountAndTotalDateHistogramFacet histoFacet = (InternalCountAndTotalDateHistogramFacet) facet;
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
            counts = InternalCountAndTotalDateHistogramFacet.EMPTY_LONG_LONG_MAP;
        }
        if (totals == null) {
            totals = InternalCountAndTotalDateHistogramFacet.EMPTY_LONG_DOUBLE_MAP;
        }
        firstHistoFacet.counts = counts;
        firstHistoFacet.totals = totals;

        return firstHistoFacet;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, HistogramFacet.TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : computeEntries()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.time());
            builder.field(Fields.COUNT, entry.count());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalCountAndTotalDateHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalCountAndTotalDateHistogramFacet facet = new InternalCountAndTotalDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
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