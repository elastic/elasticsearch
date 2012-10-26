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

package org.elasticsearch.search.facet.termsstats.doubles;

import com.google.common.collect.ImmutableList;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.trove.ExtTDoubleObjectHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.termsstats.InternalTermsStatsFacet;

import java.io.IOException;
import java.util.*;

public class InternalTermsStatsDoubleFacet extends InternalTermsStatsFacet {

    private static final String STREAM_TYPE = "dTS";

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readTermsStatsFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    public InternalTermsStatsDoubleFacet() {
    }

    public static class DoubleEntry implements Entry {

        double term;
        long count;
        long totalCount;
        double total;
        double min;
        double max;

        public DoubleEntry(double term, long count, long totalCount, double total, double min, double max) {
            this.term = term;
            this.count = count;
            this.total = total;
            this.totalCount = totalCount;
            this.min = min;
            this.max = max;
        }

        @Override
        public Text term() {
            return new StringText(Double.toString(term));
        }

        @Override
        public Text getTerm() {
            return term();
        }

        @Override
        public Number termAsNumber() {
            return term;
        }

        @Override
        public Number getTermAsNumber() {
            return termAsNumber();
        }

        @Override
        public long count() {
            return count;
        }

        @Override
        public long getCount() {
            return count();
        }

        @Override
        public long totalCount() {
            return this.totalCount;
        }

        @Override
        public long getTotalCount() {
            return this.totalCount;
        }

        @Override
        public double min() {
            return this.min;
        }

        @Override
        public double getMin() {
            return min();
        }

        @Override
        public double max() {
            return max;
        }

        @Override
        public double getMax() {
            return max();
        }

        @Override
        public double total() {
            return total;
        }

        @Override
        public double getTotal() {
            return total();
        }

        @Override
        public double mean() {
            if (totalCount == 0) {
                return 0;
            }
            return total / totalCount;
        }

        @Override
        public double getMean() {
            return mean();
        }

        @Override
        public int compareTo(Entry o) {
            DoubleEntry other = (DoubleEntry) o;
            return (term < other.term ? -1 : (term == other.term ? 0 : 1));
        }
    }

    private String name;

    int requiredSize;

    long missing;

    Collection<DoubleEntry> entries = ImmutableList.of();

    ComparatorType comparatorType;

    public InternalTermsStatsDoubleFacet(String name, ComparatorType comparatorType, int requiredSize, Collection<DoubleEntry> entries, long missing) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.requiredSize = requiredSize;
        this.entries = entries;
        this.missing = missing;
    }

    @Override
    public String name() {
        return this.name;
    }

    @Override
    public String getName() {
        return this.name;
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
    public List<DoubleEntry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<DoubleEntry>) entries;
    }

    List<DoubleEntry> mutableList() {
        if (!(entries instanceof List)) {
            entries = new ArrayList<DoubleEntry>(entries);
        }
        return (List<DoubleEntry>) entries;
    }

    @Override
    public List<DoubleEntry> getEntries() {
        return entries();
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) entries.iterator();
    }

    @Override
    public long missingCount() {
        return this.missing;
    }

    @Override
    public long getMissingCount() {
        return missingCount();
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            if (requiredSize == 0) {
                // we need to sort it here!
                InternalTermsStatsDoubleFacet tsFacet = (InternalTermsStatsDoubleFacet) facets.get(0);
                if (!tsFacet.entries.isEmpty()) {
                    List<DoubleEntry> entries = tsFacet.mutableList();
                    Collections.sort(entries, comparatorType.comparator());
                }
            }
            return facets.get(0);
        }
        int missing = 0;
        ExtTDoubleObjectHashMap<DoubleEntry> map = CacheRecycler.popDoubleObjectMap();
        for (Facet facet : facets) {
            InternalTermsStatsDoubleFacet tsFacet = (InternalTermsStatsDoubleFacet) facet;
            missing += tsFacet.missing;
            for (Entry entry : tsFacet) {
                DoubleEntry doubleEntry = (DoubleEntry) entry;
                DoubleEntry current = map.get(doubleEntry.term);
                if (current != null) {
                    current.count += doubleEntry.count;
                    current.totalCount += doubleEntry.totalCount;
                    current.total += doubleEntry.total;
                    if (doubleEntry.min < current.min) {
                        current.min = doubleEntry.min;
                    }
                    if (doubleEntry.max > current.max) {
                        current.max = doubleEntry.max;
                    }
                } else {
                    map.put(doubleEntry.term, doubleEntry);
                }
            }
        }

        // sort
        if (requiredSize == 0) { // all terms
            DoubleEntry[] entries1 = map.values(new DoubleEntry[map.size()]);
            Arrays.sort(entries1, comparatorType.comparator());
            CacheRecycler.pushDoubleObjectMap(map);
            return new InternalTermsStatsDoubleFacet(name, comparatorType, requiredSize, Arrays.asList(entries1), missing);
        } else {
            Object[] values = map.internalValues();
            Arrays.sort(values, (Comparator) comparatorType.comparator());
            List<DoubleEntry> ordered = new ArrayList<DoubleEntry>(map.size());
            for (int i = 0; i < requiredSize; i++) {
                DoubleEntry value = (DoubleEntry) values[i];
                if (value == null) {
                    break;
                }
                ordered.add(value);
            }
            CacheRecycler.pushDoubleObjectMap(map);
            return new InternalTermsStatsDoubleFacet(name, comparatorType, requiredSize, ordered, missing);
        }
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString MISSING = new XContentBuilderString("missing");
        static final XContentBuilderString TERMS = new XContentBuilderString("terms");
        static final XContentBuilderString TERM = new XContentBuilderString("term");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL_COUNT = new XContentBuilderString("total_count");
        static final XContentBuilderString MIN = new XContentBuilderString("min");
        static final XContentBuilderString MAX = new XContentBuilderString("max");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, InternalTermsStatsFacet.TYPE);
        builder.field(Fields.MISSING, missing);
        builder.startArray(Fields.TERMS);
        for (Entry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, ((DoubleEntry) entry).term);
            builder.field(Fields.COUNT, entry.count());
            builder.field(Fields.TOTAL_COUNT, entry.totalCount());
            builder.field(Fields.MIN, entry.min());
            builder.field(Fields.MAX, entry.max());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalTermsStatsDoubleFacet readTermsStatsFacet(StreamInput in) throws IOException {
        InternalTermsStatsDoubleFacet facet = new InternalTermsStatsDoubleFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();
        missing = in.readVLong();

        int size = in.readVInt();
        entries = new ArrayList<DoubleEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new DoubleEntry(in.readDouble(), in.readVLong(), in.readVLong(), in.readDouble(), in.readDouble(), in.readDouble()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(requiredSize);
        out.writeVLong(missing);

        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            out.writeDouble(((DoubleEntry) entry).term);
            out.writeVLong(entry.count());
            out.writeVLong(entry.totalCount());
            out.writeDouble(entry.total());
            out.writeDouble(entry.min());
            out.writeDouble(entry.max());
        }
    }
}