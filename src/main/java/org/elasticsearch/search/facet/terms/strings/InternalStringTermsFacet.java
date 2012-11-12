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

package org.elasticsearch.search.facet.terms.strings;

import com.google.common.collect.ImmutableList;
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.text.BytesText;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.text.Text;
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
 *
 */
public class InternalStringTermsFacet extends InternalTermsFacet {

    private static final String STREAM_TYPE = "tTerms";

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(String type, StreamInput in) throws IOException {
            return readTermsFacet(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    public static class TermEntry implements Entry {

        private Text term;
        private int count;

        public TermEntry(String term, int count) {
            this.term = new StringText(term);
            this.count = count;
        }

        public TermEntry(BytesRef term, int count) {
            this.term = new BytesText(new BytesArray(term));
            this.count = count;
        }

        public TermEntry(Text term, int count) {
            this.term = term;
            this.count = count;
        }

        public Text term() {
            return term;
        }

        public Text getTerm() {
            return term;
        }

        @Override
        public Number termAsNumber() {
            // LUCENE 4 UPGRADE: better way?
            return Double.parseDouble(term.string());
        }

        @Override
        public Number getTermAsNumber() {
            return termAsNumber();
        }

        public int count() {
            return count;
        }

        public int getCount() {
            return count();
        }

        @Override
        public int compareTo(Entry o) {
            int i = this.term.compareTo(o.term());
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

    int requiredSize;

    long missing;

    long total;

    Collection<TermEntry> entries = ImmutableList.of();

    ComparatorType comparatorType;

    InternalStringTermsFacet() {
    }

    public InternalStringTermsFacet(String name, ComparatorType comparatorType, int requiredSize, Collection<TermEntry> entries, long missing, long total) {
        this.name = name;
        this.comparatorType = comparatorType;
        this.requiredSize = requiredSize;
        this.entries = entries;
        this.missing = missing;
        this.total = total;
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
    public List<TermEntry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<TermEntry>) entries;
    }

    @Override
    public List<TermEntry> getEntries() {
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
    public long totalCount() {
        return this.total;
    }

    @Override
    public long getTotalCount() {
        return totalCount();
    }

    @Override
    public long otherCount() {
        long other = total;
        for (Entry entry : entries) {
            other -= entry.count();
        }
        return other;
    }

    @Override
    public long getOtherCount() {
        return otherCount();
    }

    @Override
    public Facet reduce(String name, List<Facet> facets) {
        if (facets.size() == 1) {
            return facets.get(0);
        }
        InternalStringTermsFacet first = (InternalStringTermsFacet) facets.get(0);
        TObjectIntHashMap<Text> aggregated = CacheRecycler.popObjectIntMap();
        long missing = 0;
        long total = 0;
        for (Facet facet : facets) {
            InternalStringTermsFacet mFacet = (InternalStringTermsFacet) facet;
            missing += mFacet.missingCount();
            total += mFacet.totalCount();
            for (TermEntry entry : mFacet.entries) {
                aggregated.adjustOrPutValue(entry.term(), entry.count(), entry.count());
            }
        }

        BoundedTreeSet<TermEntry> ordered = new BoundedTreeSet<TermEntry>(first.comparatorType.comparator(), first.requiredSize);
        for (TObjectIntIterator<Text> it = aggregated.iterator(); it.hasNext(); ) {
            it.advance();
            ordered.add(new TermEntry(it.key(), it.value()));
        }
        first.entries = ordered;
        first.missing = missing;
        first.total = total;

        CacheRecycler.pushObjectIntMap(aggregated);

        return first;
    }

    static final class Fields {
        static final XContentBuilderString _TYPE = new XContentBuilderString("_type");
        static final XContentBuilderString MISSING = new XContentBuilderString("missing");
        static final XContentBuilderString TOTAL = new XContentBuilderString("total");
        static final XContentBuilderString OTHER = new XContentBuilderString("other");
        static final XContentBuilderString TERMS = new XContentBuilderString("terms");
        static final XContentBuilderString TERM = new XContentBuilderString("term");
        static final XContentBuilderString COUNT = new XContentBuilderString("count");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name);
        builder.field(Fields._TYPE, TermsFacet.TYPE);
        builder.field(Fields.MISSING, missing);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.OTHER, otherCount());
        builder.startArray(Fields.TERMS);
        for (Entry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, entry.term());
            builder.field(Fields.COUNT, entry.count());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalStringTermsFacet readTermsFacet(StreamInput in) throws IOException {
        InternalStringTermsFacet facet = new InternalStringTermsFacet();
        facet.readFrom(in);
        return facet;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        name = in.readString();
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();
        missing = in.readVLong();
        total = in.readVLong();

        int size = in.readVInt();
        entries = new ArrayList<TermEntry>(size);
        for (int i = 0; i < size; i++) {
            entries.add(new TermEntry(in.readText(), in.readVInt()));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(name);
        out.writeByte(comparatorType.id());
        out.writeVInt(requiredSize);
        out.writeVLong(missing);
        out.writeVLong(total);

        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            out.writeText(entry.term());
            out.writeVInt(entry.count());
        }
    }
}