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

package org.elasticsearch.search.facet.terms.shorts;

import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.thread.ThreadLocals;
import org.elasticsearch.common.trove.TShortIntHashMap;
import org.elasticsearch.common.trove.TShortIntIterator;
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
public class InternalShortTermsFacet extends InternalTermsFacet {

    private static final String STREAM_TYPE = "sTerms";

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

    public static class ShortEntry implements Entry {

        short term;
        int count;

        public ShortEntry(short term, int count) {
            this.term = term;
            this.count = count;
        }

        public String term() {
            return Short.toString(term);
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
            short anotherVal = ((ShortEntry) o).term;
            int i = term - anotherVal;
            if (i == 0) {
                i = count - o.count();
                if (i == 0) {
                    i = System.identityHashCode(this) - System.identityHashCode(o);
                }
            }
            return i;
        }
    }

    private String name;

    private String fieldName;

    int requiredSize;

    Collection<ShortEntry> entries = ImmutableList.of();

    private ComparatorType comparatorType;

    InternalShortTermsFacet() {
    }

    public InternalShortTermsFacet(String name, String fieldName, ComparatorType comparatorType, int requiredSize, Collection<ShortEntry> entries) {
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

    @Override public List<ShortEntry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<ShortEntry>) entries;
    }

    @Override public List<ShortEntry> getEntries() {
        return entries();
    }

    @SuppressWarnings({"unchecked"}) @Override public Iterator<Entry> iterator() {
        return (Iterator) entries.iterator();
    }


    private static ThreadLocal<ThreadLocals.CleanableValue<TShortIntHashMap>> aggregateCache = new ThreadLocal<ThreadLocals.CleanableValue<TShortIntHashMap>>() {
        @Override protected ThreadLocals.CleanableValue<TShortIntHashMap> initialValue() {
            return new ThreadLocals.CleanableValue<TShortIntHashMap>(new TShortIntHashMap());
        }
    };


    @Override public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalShortTermsFacet first = (InternalShortTermsFacet) facets.get(0);
        TShortIntHashMap aggregated = aggregateCache.get().get();
        aggregated.clear();

        for (Facet facet : facets) {
            InternalShortTermsFacet mFacet = (InternalShortTermsFacet) facet;
            for (ShortEntry entry : mFacet.entries) {
                aggregated.adjustOrPutValue(entry.term, entry.count(), entry.count());
            }
        }

        BoundedTreeSet<ShortEntry> ordered = new BoundedTreeSet<ShortEntry>(first.comparatorType().comparator(), first.requiredSize);
        for (TShortIntIterator it = aggregated.iterator(); it.hasNext();) {
            it.advance();
            ordered.add(new ShortEntry(it.key(), it.value()));
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
        for (ShortEntry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, entry.term);
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
    }

    public static InternalShortTermsFacet readTermsFacet(StreamInput in) throws IOException {
        InternalShortTermsFacet facet = new InternalShortTermsFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override public void readFrom(StreamInput in) throws IOException {
        name = in.readUTF();
        fieldName = in.readUTF();
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();

        int size = in.readVInt();
        entries = new ArrayList<ShortEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new ShortEntry(in.readShort(), in.readVInt()));
        }
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeUTF(name);
        out.writeUTF(fieldName);
        out.writeByte(comparatorType.id());

        out.writeVInt(requiredSize);

        out.writeVInt(entries.size());
        for (ShortEntry entry : entries) {
            out.writeShort(entry.term);
            out.writeVInt(entry.count());
        }
    }
}