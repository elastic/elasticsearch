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

package org.elasticsearch.search.facet.terms.doubles;

import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.TDoubleIntHashMap;
import org.elasticsearch.common.trove.TDoubleIntIterator;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.facet.Facet;
import org.elasticsearch.search.facet.terms.InternalTermsFacet;
import org.elasticsearch.search.facet.terms.TermsFacet;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author kimchy (shay.banon)
 */
public class InternalDoubleTermsFacet extends InternalTermsFacet {

    private static final String STREAM_TYPE = "dTerms";

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override public Facet readFacet(String type, StreamInput in) throws IOException {
            return readTermsFacet(in);
        }
    };

    @Override public String streamType() {
        return STREAM_TYPE;
    }

    public static class DoubleEntry implements Entry {

        double term;
        int count;

        public DoubleEntry(double term, int count) {
            this.term = term;
            this.count = count;
        }

        public String term() {
            return Double.toString(term);
        }

        public String getTerm() {
            return term();
        }

        @Override public Number termAsNumber() {
            return term;
        }

        @Override public Number getTermAsNumber() {
            return termAsNumber();
        }

        public int count() {
            return count;
        }

        public int getCount() {
            return count();
        }

        @Override public int compareTo(Entry o) {
            double anotherVal = ((DoubleEntry) o).term;
            if (term < anotherVal) {
                return -1;
            }
            if (term == anotherVal) {
                int i = count - o.count();
                if (i == 0) {
                    i = System.identityHashCode(this) - System.identityHashCode(o);
                }
                return i;
            }
            return 1;
        }
    }

    private String name;

    private String fieldName;

    int requiredSize;

    Collection<DoubleEntry> entries = ImmutableList.of();

    private ComparatorType comparatorType;

    InternalDoubleTermsFacet() {
    }

    public InternalDoubleTermsFacet(String name, String fieldName, ComparatorType comparatorType, int requiredSize, Collection<DoubleEntry> entries) {
        this.name = name;
        this.fieldName = fieldName;
        this.comparatorType = comparatorType;
        this.requiredSize = requiredSize;
        this.entries = entries;
    }

    @Override public String name() {
        return this.name;
    }

    @Override public String getName() {
        return this.name;
    }

    @Override public String fieldName() {
        return this.fieldName;
    }

    @Override public String getFieldName() {
        return fieldName();
    }

    @Override public String type() {
        return TYPE;
    }

    @Override public String getType() {
        return type();
    }

    @Override public ComparatorType comparatorType() {
        return comparatorType;
    }

    @Override public ComparatorType getComparatorType() {
        return comparatorType();
    }

    @Override public List<DoubleEntry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<DoubleEntry>) entries;
    }

    @Override public List<DoubleEntry> getEntries() {
        return entries();
    }

    @SuppressWarnings({"unchecked"}) @Override public Iterator<Entry> iterator() {
        return (Iterator) entries.iterator();
    }


    private static ThreadLocal<ThreadLocals.CleanableValue<TDoubleIntHashMap>> aggregateCache = new ThreadLocal<ThreadLocals.CleanableValue<TDoubleIntHashMap>>() {
        @Override protected ThreadLocals.CleanableValue<TDoubleIntHashMap> initialValue() {
            return new ThreadLocals.CleanableValue<TDoubleIntHashMap>(new TDoubleIntHashMap());
        }
    };


    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalDoubleTermsFacet first = (InternalDoubleTermsFacet) facets.get(0);
        TDoubleIntHashMap aggregated = aggregateCache.get().get();
        aggregated.clear();

        for (Facet facet : facets) {
            InternalDoubleTermsFacet mFacet = (InternalDoubleTermsFacet) facet;
            for (DoubleEntry entry : mFacet.entries) {
                aggregated.adjustOrPutValue(entry.term, entry.count(), entry.count());
            }
        }

        BoundedTreeSet<DoubleEntry> ordered = new BoundedTreeSet<DoubleEntry>(first.comparatorType().comparator(), first.requiredSize);
        for (TDoubleIntIterator it = aggregated.iterator(); it.hasNext();) {
            it.advance();
            ordered.add(new DoubleEntry(it.key(), it.value()));
        }
        first.entries = ordered;
        return first;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString _FIELD = new XContentBuilderString("_field");
        static final XContentBuilderString TERMS = new XContentBuilderString("terms");
        static final XContentBuilderString TERM = new XContentBuilderString("term");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override public void toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, TermsFacet.TYPE);
        builder.field(Fields._FIELD, fieldName);
        builder.startArray(Fields.TERMS);
        for (DoubleEntry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, entry.term);
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    public static InternalDoubleTermsFacet readTermsFacet(StreamInput in) throws IOException {
        InternalDoubleTermsFacet facet = new InternalDoubleTermsFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        fieldName = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();

        int size = in.readVInt();
        entries = new ArrayList<DoubleEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new DoubleEntry(in.readDouble(), in.readVInt()));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(fieldName);
        out.writeByte(comparatorType.id());

        out.writeVInt(requiredSize);

        out.writeVInt(entries.size());
        for (DoubleEntry entry : entries) {
            out.writeDouble(entry.term);
            out.writeVInt(entry.count());
        }
    }
}