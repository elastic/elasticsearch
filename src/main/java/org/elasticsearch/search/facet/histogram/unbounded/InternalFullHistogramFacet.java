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

package org.elasticsearch.search.facet.histogram.unbounded;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.InternalHistogramFacet;

import java.io.IOException;
import java.util.*;

/**
 * @author kimchy (shay.banon)
 */
public class InternalFullHistogramFacet extends InternalHistogramFacet {

    private static final String STREAM_TYPE = "fHistogram";

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
    public static class FullEntry implements Entry {
        long key;
        long count;
        long totalCount;
        double total;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        public FullEntry(long key, long count, double min, double max, long totalCount, double total) {
            this.key = key;
            this.count = count;
            this.min = min;
            this.max = max;
            this.totalCount = totalCount;
            this.total = total;
        }

        @Override public long key() {
            return key;
        }

        @Override public long getKey() {
            return key();
        }

        @Override public long count() {
            return count;
        }

        @Override public long getCount() {
            return count();
        }

        @Override public double total() {
            return total;
        }

        @Override public double getTotal() {
            return total();
        }

        @Override public long totalCount() {
            return totalCount;
        }

        @Override public long getTotalCount() {
            return this.totalCount;
        }

        @Override public double mean() {
            return total / totalCount;
        }

        @Override public double getMean() {
            return total / totalCount;
        }

        @Override public double min() {
            return this.min;
        }

        @Override public double getMin() {
            return this.min;
        }

        @Override public double max() {
            return this.max;
        }

        @Override public double getMax() {
            return this.max;
        }
    }

    private String name;

    private ComparatorType comparatorType;

    ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> tEntries;
    boolean cachedEntries;
    Collection<FullEntry> entries;

    private InternalFullHistogramFacet() {
    }

    public InternalFullHistogramFacet(String name, ComparatorType comparatorType, ExtTLongObjectHashMap<InternalFullHistogramFacet.FullEntry> entries, boolean cachedEntries) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.tEntries = entries;
        this.cachedEntries = cachedEntries;
        this.entries = entries.valueCollection();
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

    @Override public List<FullEntry> entries() {
        if (!(entries instanceof List)) {
            entries = new ArrayList<FullEntry>(entries);
        }
        return (List<FullEntry>) entries;
    }

    @Override public List<FullEntry> getEntries() {
        return entries();
    }

    @Override public Iterator<Entry> iterator() {
        return (Iterator) entries().iterator();
    }

    void releaseCache() {
        if (cachedEntries) {
            CacheRecycler.pushLongObjectMap(tEntries);
            cachedEntries = false;
            tEntries = null;
        }
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            // we need to sort it
            InternalFullHistogramFacet internalFacet = (InternalFullHistogramFacet) facets.get(0);
            List<FullEntry> entries = internalFacet.entries();
            Collections.sort(entries, comparatorType.comparator());
            internalFacet.releaseCache();
            return internalFacet;
        }

        ExtTLongObjectHashMap<FullEntry> map = CacheRecycler.popLongObjectMap();

        for (Facet facet : facets) {
            InternalFullHistogramFacet histoFacet = (InternalFullHistogramFacet) facet;
            for (FullEntry fullEntry : histoFacet.entries) {
                FullEntry current = map.get(fullEntry.key);
                if (current != null) {
                    current.count += fullEntry.count;
                    current.total += fullEntry.total;
                    current.totalCount += fullEntry.totalCount;
                    if (fullEntry.min < current.min) {
                        current.min = fullEntry.min;
                    }
                    if (fullEntry.max > current.max) {
                        current.max = fullEntry.max;
                    }
                } else {
                    map.put(fullEntry.key, fullEntry);
                }
            }
            histoFacet.releaseCache();
        }

        // sort
        Object[] values = map.internalValues();
        Arrays.sort(values, (Comparator) comparatorType.comparator());
        List<FullEntry> ordered = new ArrayList<FullEntry>(map.size());
        for (int i = 0; i < map.size(); i++) {
            FullEntry value = (FullEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        CacheRecycler.pushLongObjectMap(map);

        // just initialize it as already ordered facet
        InternalFullHistogramFacet ret = new InternalFullHistogramFacet();
        ret.name = name;
        ret.comparatorType = comparatorType;
        ret.entries = ordered;
        return ret;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString KEY = new XContentBuilderString("key");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, HistogramFacet.TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : entries) {
            builder.startObject();
            builder.field(Fields.KEY, entry.key());
            builder.field(Fields.COUNT, entry.count());
            builder.field(Fields.MIN, entry.min());
            builder.field(Fields.MAX, entry.max());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.TOTAL_COUNT, entry.totalCount());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalFullHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalFullHistogramFacet facet = new InternalFullHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());

        cachedEntries = false;
        int size = in.readVInt();
        entries = new ArrayList<FullEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new FullEntry(in.readLong(), in.readVLong(), in.readDouble(), in.readDouble(), in.readVLong(), in.readDouble()));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(entries.size());
        for (FullEntry entry : entries) {
            out.writeLong(entry.key);
            out.writeVLong(entry.count);
            out.writeDouble(entry.min);
            out.writeDouble(entry.max);
            out.writeVLong(entry.totalCount);
            out.writeDouble(entry.total);
        }
        releaseCache();
    }
}