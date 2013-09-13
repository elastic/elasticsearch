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

import com.carrotsearch.hppc.ObjectIntOpenHashMap;
import com.google.common.collect.ImmutableList;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.bytes.HashedBytesArray;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.recycler.Recycler;
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

    private static final BytesReference STREAM_TYPE = new HashedBytesArray(Strings.toUTF8Bytes("tTerms"));

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Facet readFacet(StreamInput in) throws IOException {
            return readTermsFacet(in);
        }
    };

    @Override
    public BytesReference streamType() {
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

        @Override
        public Text getTerm() {
            return term;
        }

        @Override
        public Number getTermAsNumber() {
            return Double.parseDouble(term.string());
        }

        @Override
        public int getCount() {
            return count;
        }

        @Override
        public int compareTo(Entry o) {
            int i = this.term.compareTo(o.getTerm());
            if (i == 0) {
                i = count - o.getCount();
                if (i == 0) {
                    i = System.identityHashCode(this) - System.identityHashCode(o);
                }
            }
            return i;
        }
    }

    int requiredSize;
    long missing;
    long total;
    Collection<TermEntry> entries = ImmutableList.of();
    ComparatorType comparatorType;

    InternalStringTermsFacet() {
    }

    public InternalStringTermsFacet(String name, ComparatorType comparatorType, int requiredSize, Collection<TermEntry> entries, long missing, long total) {
        super(name);
        this.comparatorType = comparatorType;
        this.requiredSize = requiredSize;
        this.entries = entries;
        this.missing = missing;
        this.total = total;
    }

    @Override
    public List<TermEntry> getEntries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<TermEntry>) entries;
    }

    @SuppressWarnings({"unchecked"})
    @Override
    public Iterator<Entry> iterator() {
        return (Iterator) entries.iterator();
    }

    @Override
    public long getMissingCount() {
        return this.missing;
    }

    @Override
    public long getTotalCount() {
        return this.total;
    }

    @Override
    public long getOtherCount() {
        long other = total;
        for (Entry entry : entries) {
            other -= entry.getCount();
        }
        return other;
    }

    @Override
    public Facet reduce(ReduceContext context) {
        List<Facet> facets = context.facets();
        if (facets.size() == 1) {
            InternalStringTermsFacet facet = (InternalStringTermsFacet) facets.get(0);
            facet.trimExcessEntries();
            return facet;
        }

        InternalStringTermsFacet first = null;

        Recycler.V<ObjectIntOpenHashMap<Text>> aggregated = context.cacheRecycler().objectIntMap(-1);
        long missing = 0;
        long total = 0;
        for (Facet facet : facets) {
            InternalTermsFacet termsFacet = (InternalTermsFacet) facet;
            missing += termsFacet.getMissingCount();
            total += termsFacet.getTotalCount();

            if (!(termsFacet instanceof InternalStringTermsFacet)) {
                // the assumption is that if one of the facets is of different type, it should do the
                // reduction (all the facets we iterated so far most likely represent unmapped fields, if not
                // class cast exception will be thrown)
                return termsFacet.reduce(context);
            }

            if (first == null) {
                first = (InternalStringTermsFacet) termsFacet;
            }

            for (Entry entry : termsFacet.getEntries()) {
                aggregated.v().addTo(entry.getTerm(), entry.getCount());
            }
        }

        BoundedTreeSet<TermEntry> ordered = new BoundedTreeSet<TermEntry>(first.comparatorType.comparator(), first.requiredSize);
        ObjectIntOpenHashMap<Text> aggregatedEntries = aggregated.v();

        final boolean[] states = aggregatedEntries.allocated;
        Object[] keys = aggregatedEntries.keys;
        int[] values = aggregatedEntries.values;
        for (int i = 0; i < aggregatedEntries.allocated.length; i++) {
            if (states[i]) {
                Text key = (Text) keys[i];
                ordered.add(new TermEntry(key, values[i]));
            }
        }
        first.entries = ordered;
        first.missing = missing;
        first.total = total;

        aggregated.release();

        return first;
    }

    private void trimExcessEntries() {
        if (requiredSize >= entries.size()) {
            return;
        }

        if (entries instanceof List) {
            entries = ((List) entries).subList(0, requiredSize);
            return;
        }

        int i = 0;
        for (Iterator<TermEntry> iter  = entries.iterator(); iter.hasNext();) {
            iter.next();
            if (i++ >= requiredSize) {
                iter.remove();
            }
        }
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
        builder.startObject(getName());
        builder.field(Fields._TYPE, TermsFacet.TYPE);
        builder.field(Fields.MISSING, missing);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.OTHER, getOtherCount());
        builder.startArray(Fields.TERMS);
        for (Entry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, entry.getTerm());
            builder.field(Fields.COUNT, entry.getCount());
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
        super.readFrom(in);
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
        super.writeTo(out);
        out.writeByte(comparatorType.id());
        out.writeVInt(requiredSize);
        out.writeVLong(missing);
        out.writeVLong(total);

        out.writeVInt(entries.size());
        for (Entry entry : entries) {
            out.writeText(entry.getTerm());
            out.writeVInt(entry.getCount());
        }
    }
}