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

package org.elasticsearch.search.group.terms.strings;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.collect.BoundedTreeSet;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;
import org.elasticsearch.search.group.Group;
import org.elasticsearch.search.group.terms.InternalTermsGroup;
import org.elasticsearch.search.group.terms.TermsGroup;

/**
 *
 */
public class InternalStringTermsGroup extends InternalTermsGroup {

    private static final String STREAM_TYPE = "tGroups";

    public static void registerStream() {
        Streams.registerStream(STREAM, STREAM_TYPE);
    }

    static Stream STREAM = new Stream() {
        @Override
        public Group readGroup(String type, StreamInput in) throws IOException {
            return readTermsGroup(in);
        }
    };

    @Override
    public String streamType() {
        return STREAM_TYPE;
    }

    public static class ScoreDocsEntry implements Entry {

        private String term;
        private int count;
        private List<ScoreDoc> docs;

        public ScoreDocsEntry(String term, int count, List<ScoreDoc> docs) {
            this.term = term;
            this.count = count;
            this.docs = docs;
        }

        public String term() {
            return term;
        }

        public String getTerm() {
            return term;
        }

        @Override
        public Number termAsNumber() {
            return Double.parseDouble(term);
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

        public List<ScoreDoc> docs() {
            return docs;
        }

        public List<ScoreDoc> getDocs() {
            return docs();
        }

        @Override
        public int compareTo(Entry o) {
            int i = term.compareTo(o.term());
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

    Collection<ScoreDocsEntry> entries = ImmutableList.of();

    ComparatorType comparatorType;

    InternalStringTermsGroup() {
    }

    public InternalStringTermsGroup(String name, ComparatorType comparatorType, int requiredSize,
            Collection<ScoreDocsEntry> entries, long missing, long total)
    {
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
    public List<ScoreDocsEntry> entries() {
        if (!(entries instanceof List)) {
            entries = ImmutableList.copyOf(entries);
        }
        return (List<ScoreDocsEntry>) entries;
    }

    @Override
    public List<ScoreDocsEntry> getEntries() {
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
    public Group reduce(String name, List<Group> groups) {
        if (groups.size() == 1) {
            return groups.get(0);
        }
        InternalStringTermsGroup first = (InternalStringTermsGroup) groups.get(0);
        // TODO
        TObjectIntHashMap<String> aggregated = CacheRecycler.popObjectIntMap();
        long missing = 0;
        long total = 0;
        for (Group group : groups) {
            InternalStringTermsGroup mGroup = (InternalStringTermsGroup) group;
            missing += mGroup.missingCount();
            total += mGroup.totalCount();
            for (InternalStringTermsGroup.ScoreDocsEntry entry : mGroup.entries) {
                
                aggregated.adjustOrPutValue(entry.term(), entry.count(), entry.count());
            }
        }

        BoundedTreeSet<ScoreDocsEntry> ordered = new BoundedTreeSet<ScoreDocsEntry>(first.comparatorType.comparator(), first.requiredSize);
//        for (TObjectIntIterator<String> it = aggregated.iterator(); it.hasNext(); ) {
//            it.advance();
//            ordered.add(new StringEntry(it.key(), it.value()));
//        }
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
        static final XContentBuilderString HITS = new XContentBuilderString("hits");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(getName());
        builder.field(Fields._TYPE, TermsGroup.TYPE);
        builder.field(Fields.MISSING, missing);
        builder.field(Fields.TOTAL, total);
        builder.field(Fields.OTHER, otherCount());
        builder.startArray(Fields.TERMS);
        for (ScoreDocsEntry entry : entries) {
            builder.startObject();
            builder.field(Fields.TERM, entry.term());
            builder.field(Fields.COUNT, entry.count());
            // TODO: At some point there should be internalHits
            builder.array(Fields.HITS, entry.docs);
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public static InternalStringTermsGroup readTermsGroup(StreamInput in) throws IOException {
        InternalStringTermsGroup group = new InternalStringTermsGroup();
        group.readFrom(in);
        return group;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        comparatorType = ComparatorType.fromId(in.readByte());
        requiredSize = in.readVInt();
        missing = in.readVLong();
        total = in.readVLong();

        int size = in.readVInt();
        entries = Lists.newArrayListWithCapacity(size);
        for (int i = 0; i < size; i++) {
            String term = in.readString();
            int count = in.readVInt();
            int docsSize = in.readVInt();
            List<ScoreDoc> docs = Lists.newArrayListWithCapacity(docsSize);
            for (int j = 0; j < docsSize; j++) {
                docs.add(new ScoreDoc(in.readVInt(), in.readFloat(), in.readVInt()));
            }
            entries.add(new ScoreDocsEntry(term, count, docs));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeByte(comparatorType.id());
        out.writeVInt(requiredSize);
        out.writeVLong(missing);
        out.writeVLong(total);

        out.writeVInt(entries.size());
        for (ScoreDocsEntry entry : entries) {
            out.writeString(entry.term());
            out.writeVInt(entry.count());
            out.writeVInt(entry.docs().size());
            for (ScoreDoc doc : entry.docs()) {
                out.writeVInt(doc.doc);
                out.writeFloat(doc.score);
                out.writeVInt(doc.shardIndex);
            }
        }
    }

}