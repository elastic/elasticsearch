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

package org.elasticsearch.search.facet.termsstats.longs;

import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.termsstats.InternalTermsStatsFacet;

import java.io.IOException;
import java.util.*;

public class InternalTermsStatsLongFacet extends InternalTermsStatsFacet {

    private static final String STREAM_TYPE = "lTS";

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override public Facet readFacet(String type, StreamInput in) throws IOException {
            return readTermsStatsFacet(in);
        }
    };

    @Override public String streamType() {
        return STREAM_TYPE;
    }

    public InternalTermsStatsLongFacet() {
    }

    public static class LongEntry implements Entry {

        long term;
        int count;
        double total;

        public LongEntry(long term, int count, double total) {
            this.term = term;
            this.count = count;
            this.total = total;
        }

        @Override public String term() {
            return Long.toString(term);
        }

        @Override public String getTerm() {
            return term();
        }

        @Override public Number termAsNumber() {
            return term;
        }

        @Override public Number getTermAsNumber() {
            return termAsNumber();
        }

        @Override public int count() {
            return count;
        }

        @Override public int getCount() {
            return count();
        }

        @Override public double total() {
            return total;
        }

        @Override public double getTotal() {
            return total();
        }

        @Override public double mean() {
            return total / count;
        }

        @Override public double getMean() {
            return mean();
        }

        @Override public int compareTo(Entry o) {
            LongEntry other = (LongEntry) o;
            return (term < other.term ? -1 : (term == other.term ? 0 : 1));
        }
    }

    private String name;

    int requiredSize;

    long missing;

    Collection<LongEntry> entries = ImmutableList.of();

    ComparatorType comparatorType;

    public InternalTermsStatsLongFacet(String name, ComparatorType comparatorType, int requiredSize, Collection<LongEntry> entries, long missing) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.requiredSize = requiredSize;
        this.entries = entries;
        this.missing = missing;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return this.name;
    }

    @Override public String type() {
        return TYPE;
    }

    @Override public String getType() {
        return type();
    }

    @Override public List<LongEntry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<LongEntry>) entries;
    }

    List<LongEntry> mutableList() {
        if (!(entries instanceof List)) {
            entries = new ArrayList<LongEntry>(entries);
        }
        return (List<LongEntry>) entries;
    }

    @Override public List<LongEntry> getEntries() {
        return entries();
    }

    @SuppressWarnings({"unchecked"}) @Override public Iterator<Entry> iterator() {
        return (Iterator) entries.iterator();
    }

    @Override public long missingCount() {
        return this.missing;
    }

    @Override public long getMissingCount() {
        return missingCount();
    }

    private static ThreadLocal<ThreadLocals.CleanableValue<ExtTLongObjectHashMap<LongEntry>>> aggregateCache = new ThreadLocal<ThreadLocals.CleanableValue<ExtTLongObjectHashMap<LongEntry>>>() {
        @Override protected ThreadLocals.CleanableValue<ExtTLongObjectHashMap<LongEntry>> initialValue() {
            return new ThreadLocals.CleanableValue<ExtTLongObjectHashMap<LongEntry>>(new ExtTLongObjectHashMap<LongEntry>());
        }
    };

    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            if (requiredSize == 0) {
                // we need to sort it here!
                InternalTermsStatsLongFacet tsFacet = (InternalTermsStatsLongFacet) facets.get(0);
                if (!tsFacet.entries.isEmpty()) {
                    List<LongEntry> entries = tsFacet.mutableList();
                    Collections.sort(entries, comparatorType.comparator());
                }
            }
            return facets.get(0);
        }
        int missing = 0;
        ExtTLongObjectHashMap<LongEntry> map = aggregateCache.get().get();
        map.clear();
        for (Facet facet : facets) {
            InternalTermsStatsLongFacet tsFacet = (InternalTermsStatsLongFacet) facet;
            missing += tsFacet.missing;
            for (Entry entry : tsFacet) {
                LongEntry longEntry = (LongEntry) entry;
                LongEntry current = map.get(longEntry.term);
                if (current != null) {
                    current.count += longEntry.count;
                    current.total += longEntry.total;
                } else {
                    map.put(longEntry.term, longEntry);
                }
            }
        }

        // sort
        if (requiredSize == 0) { // all terms
            LongEntry[] entries1 = map.values(new LongEntry[map.size()]);
            Arrays.sort(entries1, comparatorType.comparator());
            return new InternalTermsStatsLongFacet(name, comparatorType, requiredSize, Arrays.asList(entries1), missing);
        } else {
            Object[] values = map.internalValues();
            Arrays.sort(values, (Comparator) comparatorType.comparator());
            List<LongEntry> ordered = new ArrayList<LongEntry>();
            for (int i = 0; i < requiredSize; i++) {
                LongEntry value = (LongEntry) values[i];
                if (value == null) {
                    break;
                }
                ordered.add(value);
            }
            return new InternalTermsStatsLongFacet(name, comparatorType, requiredSize, ordered, missing);
        }
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString MISSING = new XContentBuilderString("missing");
        static final XContentBuilderString TERMS = new XContentBuilderString("terms");
        static final XContentBuilderString TERM = new XContentBuilderString("term");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString MEAN = new XContentBuilderString("mean");
    }

    @Override public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, InternalTermsStatsFacet.TYPE);
        builder.field(Fields.MISSING, missing);
        builder.startArray(Fields.TERMS);
        for (Entry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, ((LongEntry) entry).term);
            builder.field(Fields.COUNT, entry.count());
            builder.field(Fields.TOTAL, entry.total());
            builder.field(Fields.MEAN, entry.mean());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalTermsStatsLongFacet readTermsStatsFacet(StreamInput in) throws IOException {
        InternalTermsStatsLongFacet facet = new InternalTermsStatsLongFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();
        missing = in.readVLong();

        int size = in.readVInt();
        entries = new ArrayList<LongEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new LongEntry(in.readLong(), in.readVInt(), in.readDouble()));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(requiredSize);
        out.writeVLong(missing);

        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            out.writeLong(((LongEntry) entry).term);
            out.writeVInt(entry.count());
            out.writeDouble(entry.total());
        }
    }
}