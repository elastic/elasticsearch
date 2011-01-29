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

package org.elasticsearch.search.facet.histogram;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.TLongLongHashMap;
import org.elasticsearch.common.trove.TLongLongIterator;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * @author kimchy (shay.banon)
 */
public class InternalCountHistogramFacet extends InternalHistogramFacet {

    private static final String STREAM_TYPE = "cHistogram";

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
    public class CountEntry implements Entry {
        private final long key;
        private final long count;

        public CountEntry(long key, long count) {
            this.key = key;
            this.count = count;
        }

        /**
         * The key value of the histogram.
         */
        public long key() {
            return key;
        }

        /**
         * The key value of the histogram.
         */
        public long getKey() {
            return key();
        }

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        public long count() {
            return count;
        }

        /**
         * The number of hits that fall within that key "range" or "interval".
         */
        public long getCount() {
            return count();
        }

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        public double total() {
            return -1;
        }

        /**
         * The sum / total of the value field that fall within this key "interval".
         */
        public double getTotal() {
            return total();
        }

        /**
         * The mean of this facet interval.
         */
        public double mean() {
            return -1;
        }

        /**
         * The mean of this facet interval.
         */
        public double getMean() {
            return mean();
        }
    }

    private String name;

    ComparatorType comparatorType;

    TLongLongHashMap counts;

    Collection<CountEntry> entries = null;

    private InternalCountHistogramFacet() {
    }

    public InternalCountHistogramFacet(String name, ComparatorType comparatorType, TLongLongHashMap counts) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.counts = counts;
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

    @Override public List<CountEntry> entries() {
        computeEntries();
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<CountEntry>) entries;
    }

    @Override public List<CountEntry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return (Iterator) computeEntries().iterator();
    }

    private Collection<CountEntry> computeEntries() {
        if (entries != null) {
            return entries;
        }
        TreeSet<CountEntry> set = new TreeSet<CountEntry>(comparatorType.comparator());
        for (TLongLongIterator it = counts.iterator(); it.hasNext();) {
            it.advance();
            set.add(new CountEntry(it.key(), it.value()));
        }
        entries = set;
        return entries;
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        TLongLongHashMap counts = null;

        InternalCountHistogramFacet firstHistoFacet = (InternalCountHistogramFacet) facets.get(0);
        for (Facet facet : facets) {
            InternalCountHistogramFacet histoFacet = (InternalCountHistogramFacet) facet;
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
        }
        if (counts == null) {
            counts = InternalCountHistogramFacet.EMPTY_LONG_LONG_MAP;
        }
        firstHistoFacet.counts = counts;

        return firstHistoFacet;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString KEY = new XContentBuilderString("key");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, HistogramFacet.TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : computeEntries()) {
            builder.startObject();
            builder.field(Fields.KEY, entry.key());
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalCountHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalCountHistogramFacet facet = new InternalCountHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());

        int size = in.readVInt();
        if (size == 0) {
            counts = EMPTY_LONG_LONG_MAP;
        } else {
            counts = new TLongLongHashMap(size);
            for (int i = 0; i < size; i++) {
                long key = in.readLong();
                counts.put(key, in.readVLong());
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
        }
    }

    static final TLongLongHashMap EMPTY_LONG_LONG_MAP = new TLongLongHashMap();
}