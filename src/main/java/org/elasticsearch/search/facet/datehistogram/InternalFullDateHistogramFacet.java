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

package org.elasticsearch.search.facet.datehistogram;

import com.carrotsearch.hppc.LongObjectOpenHashMap;
import org.apache.lucene.util.CollectionUtil;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;

import java.io.IOException;
import java.util.*;

/**
 *
 */
public class InternalFullDateHistogramFacet extends InternalDateHistogramFacet {

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("fdHistogram"));

    public static void registerStreams() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readHistogramFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
        return STREAM_TYPE;
    }


    /**
     * A histogram entry representing a single entry within the result of a histogram facet.
     */
    public static class FullEntry implements Entry {
        private final long time;
        long count;
        long totalCount;
        double total;
        double min = Double.POSITIVE_INFINITY;
        double max = Double.NEGATIVE_INFINITY;

        public FullEntry(long time, long count, double min, double max, long totalCount, double total) {
            this.time = time;
            this.count = count;
            this.min = min;
            this.max = max;
            this.totalCount = totalCount;
            this.total = total;
        }

        @Override
        public long getTime() {
            return time;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public double getTotal() {
            return total;
        }

        @Override
        public long getTotalCount() {
            return totalCount;
        }

        @Override
        public double getMean() {
            if (totalCount == 0) {
                return totalCount;
            }
            return total / totalCount;
        }

        @Override
        public double getMin() {
            return this.min;
        }

        @Override
        public double getMax() {
            return this.max;
        }
    }

    private ComparatorType comparatorType;
    List<FullEntry> entries;

    InternalFullDateHistogramFacet() {
    }

    InternalFullDateHistogramFacet(String name) {
        super(name);
    }

    public InternalFullDateHistogramFacet(String name, ComparatorType comparatorType, List<FullEntry> entries) {
        super(name);
        this.comparatorType = comparatorType;
        this.entries = entries;
    }

    @Override
    public List<FullEntry> getEntries() {
        return entries;
    }

    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) getEntries().iterator();
    }

    @Override
    public Facet reduce(ReduceContext context) {
        List<Facet> facets = context.facets();
        if (facets.size() == 1) {
            // we need to sort it
            InternalFullDateHistogramFacet internalFacet = (InternalFullDateHistogramFacet) facets.get(0);
            List<FullEntry> entries = internalFacet.getEntries();
            CollectionUtil.timSort(entries, comparatorType.comparator());
            return internalFacet;
        }

        Recycler.V<LongObjectOpenHashMap<FullEntry>> map = context.cacheRecycler().longObjectMap(-1);

        for (Facet facet : facets) {
            InternalFullDateHistogramFacet histoFacet = (InternalFullDateHistogramFacet) facet;
            for (FullEntry fullEntry : histoFacet.entries) {
                FullEntry current = map.v().get(fullEntry.time);
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
                    map.v().put(fullEntry.time, fullEntry);
                }
            }
        }

        // sort
        // TODO: hppc - not happy with toArray
        Object[] values = map.v().values().toArray();
        Arrays.sort(values, (Comparator) comparatorType.comparator());
        List<FullEntry> ordered = new ArrayList<>(map.v().size());
        for (int i = 0; i < map.v().size(); i++) {
            FullEntry value = (FullEntry) values[i];
            if (value == null) {
                break;
            }
            ordered.add(value);
        }

        map.close();

        // just initialize it as already ordered facet
        InternalFullDateHistogramFacet ret = new InternalFullDateHistogramFacet(getName());
        ret.comparatorType = comparatorType;
        ret.entries = ordered;
        return ret;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString ENTRIES = new XContentBuilderString("entries");
        static final XContentBuilderString TIME = new XContentBuilderString("time");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, TYPE);
        builder.startArray(Fields.ENTRIES);
        for (Entry entry : getEntries()) {
            builder.startObject();
            builder.field(Fields.TIME, entry.getTime());
            builder.field(Fields.COUNT, entry.getCount());
            builder.field(Fields.MIN, entry.getMin());
            builder.field(Fields.MAX, entry.getMax());
            builder.field(Fields.TOTAL, entry.getTotal());
            builder.field(Fields.TOTAL_COUNT, entry.getTotalCount());
            builder.field(Fields.MEAN, entry.getMean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalFullDateHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalFullDateHistogramFacet facet = new InternalFullDateHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        comparatorType = ComparatorType.fromId(in.readByte());
        int size = in.readVInt();
        entries = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new FullEntry(in.readLong(), in.readVLong(), in.readDouble(), in.readDouble(), in.readVLong(), in.readDouble()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeByte(comparatorType.id());
        out.writeVInt(entries.size());
        for (FullEntry entry : entries) {
            out.writeLong(entry.time);
            out.writeVLong(entry.count);
            out.writeDouble(entry.min);
            out.writeDouble(entry.max);
            out.writeVLong(entry.totalCount);
            out.writeDouble(entry.total);
        }
    }
}