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

package org.elasticsearch.search.facet.datehistogram;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.iterator.TLongLongIterator;
import org.elasticsearch.common.trove.map.hash.TLongLongHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class InternalCountDateHistogramFacet extends InternalDateHistogramFacet {

    private static final String STREAM_TYPE = "cdHistogram";

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
    public static class CountEntry implements Entry {
        private final long time;
        private final long count;

        public CountEntry(long time, long count) {
            this.time = time;
            this.count = count;
        }

        @Override public long time() {
            return time;
        }

        @Override public long getTime() {
            return time();
        }

        @Override public long count() {
            return count;
        }

        @Override public long getCount() {
            return count();
        }

        @Override public long totalCount() {
            return 0;
        }

        @Override public long getTotalCount() {
            return 0;
        }

        @Override public double total() {
            return Double.NaN;
        }

        @Override public double getTotal() {
            return total();
        }

        @Override public double mean() {
            return Double.NaN;
        }

        @Override public double getMean() {
            return mean();
        }

        @Override public double min() {
            return Double.NaN;
        }

        @Override public double getMin() {
            return Double.NaN;
        }

        @Override public double max() {
            return Double.NaN;
        }

        @Override public double getMax() {
            return Double.NaN;
        }
    }

    private String name;

    private ComparatorType comparatorType;

    TLongLongHashMap counts;
    boolean cachedCounts;

    CountEntry[] entries = null;

    private InternalCountDateHistogramFacet() {
    }

    public InternalCountDateHistogramFacet(String name, ComparatorType comparatorType, TLongLongHashMap counts, boolean cachedCounts) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.counts = counts;
        this.cachedCounts = cachedCounts;
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
        return Arrays.asList(computeEntries());
    }

    @Override public List<CountEntry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return (Iterator) entries().iterator();
    }

    void releaseCache() {
        if (cachedCounts) {
            CacheRecycler.pushLongLongMap(counts);
            cachedCounts = false;
            counts = null;
        }
    }

    private CountEntry[] computeEntries() {
        if (entries != null) {
            return entries;
        }
        entries = new CountEntry[counts.size()];
        int i = 0;
        for (TLongLongIterator it = counts.iterator(); it.hasNext();) {
            it.advance();
            entries[i++] = new CountEntry(it.key(), it.value());
        }
        releaseCache();
        Arrays.sort(entries, comparatorType.comparator());
        return entries;
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        TLongLongHashMap counts = CacheRecycler.popLongLongMap();

        for (Facet facet : facets) {
            InternalCountDateHistogramFacet histoFacet = (InternalCountDateHistogramFacet) facet;
            for (TLongLongIterator it = histoFacet.counts.iterator(); it.hasNext();) {
                it.advance();
                counts.adjustOrPutValue(it.key(), it.value(), it.value());
            }
            histoFacet.releaseCache();

        }

        return new InternalCountDateHistogramFacet(name, comparatorType, counts, true);
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, HistogramFacet.TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : computeEntries()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.time());
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalCountDateHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalCountDateHistogramFacet facet = new InternalCountDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());

        int size = in.readVInt();
        counts = CacheRecycler.popLongLongMap();
        cachedCounts = true;
        for (int i = 0; i < size; i++) {
            long key = in.readLong();
            counts.put(key, in.readVLong());
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(counts.size());
        for (TLongLongIterator it = counts.iterator(); it.hasNext();) {
            it.advance();
            out.writeLong(it.key());
            out.writeVLong(it.value());
        }
        releaseCache();
    }
}