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

package org.elasticsearch.search.facet.histogram.bounded;

import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.histogram.HistogramFacet;
import org.elasticsearch.search.facet.histogram.InternalHistogramFacet;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class InternalBoundedCountHistogramFacet extends InternalHistogramFacet {

    private static final String STREAM_TYPE = "cBdHistogram";

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
        private final long key;
        private final long count;

        public CountEntry(long key, long count) {
            this.key = key;
            this.count = count;
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
            return Double.NaN;
        }

        @Override public double getTotal() {
            return total();
        }

        @Override public long totalCount() {
            return 0;
        }

        @Override public long getTotalCount() {
            return 0;
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

    ComparatorType comparatorType;

    boolean cachedCounts;
    int[] counts;
    int size;
    long interval;
    long offset;

    CountEntry[] entries = null;

    private InternalBoundedCountHistogramFacet() {
    }

    public InternalBoundedCountHistogramFacet(String name, ComparatorType comparatorType, long interval, long offset, int size, int[] counts, boolean cachedCounts) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.interval = interval;
        this.offset = offset;
        this.counts = counts;
        this.size = size;
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

    private CountEntry[] computeEntries() {
        if (entries != null) {
            return entries;
        }
        entries = new CountEntry[size];
        for (int i = 0; i < size; i++) {
            entries[i] = new CountEntry((i * interval) + offset, counts[i]);
        }
        releaseCache();
        return entries;
    }

    void releaseCache() {
        if (cachedCounts) {
            cachedCounts = false;
            CacheRecycler.pushIntArray(counts);
            counts = null;
        }
    }

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            InternalBoundedCountHistogramFacet firstHistoFacet = (InternalBoundedCountHistogramFacet) facets.get(0);
            if (comparatorType != ComparatorType.KEY) {
                Arrays.sort(firstHistoFacet.entries, comparatorType.comparator());
            }
            return facets.get(0);
        }
        InternalBoundedCountHistogramFacet firstHistoFacet = (InternalBoundedCountHistogramFacet) facets.get(0);
        for (int i = 1; i < facets.size(); i++) {
            InternalBoundedCountHistogramFacet histoFacet = (InternalBoundedCountHistogramFacet) facets.get(i);
            for (int j = 0; j < firstHistoFacet.size; j++) {
                firstHistoFacet.counts[j] += histoFacet.counts[j];
            }
            histoFacet.releaseCache();
        }
        if (comparatorType != ComparatorType.KEY) {
            Arrays.sort(firstHistoFacet.entries, comparatorType.comparator());
        }

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
        for (int i = 0; i < size; i++) {
            builder.startObject();
            builder.field(Fields.KEY, (i * interval) + offset);
            builder.field(Fields.COUNT, counts[i]);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        releaseCache();
        return builder;
    }

    public static InternalBoundedCountHistogramFacet readHistogramFacet(StreamInput in) throws IOException {
        InternalBoundedCountHistogramFacet facet = new InternalBoundedCountHistogramFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());
        offset = in.readLong();
        interval = in.readVLong();
        size = in.readVInt();
        counts = CacheRecycler.popIntArray(size);
        cachedCounts = true;
        for (int i = 0; i < size; i++) {
            counts[i] = in.readVInt();
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        out.writeLong(offset);
        out.writeVLong(interval);
        out.writeVInt(size);
        for (int i = 0; i < size; i++) {
            out.writeVInt(counts[i]);
        }
        releaseCache();
    }
}