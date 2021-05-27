/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;


import com.carrotsearch.hppc.ObjectObjectHashMap;
import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;

public class AggregatedDfs implements Writeable {

    private ObjectObjectHashMap<Term, TermStatistics> termStatistics;
    private ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics;
    private long maxDoc;

    public AggregatedDfs(StreamInput in) throws IOException {
        int size = in.readVInt();
        termStatistics = HppcMaps.newMap(size);
        for (int i = 0; i < size; i++) {
            Term term = new Term(in.readString(), in.readBytesRef());
            TermStatistics stats = new TermStatistics(in.readBytesRef(),
                in.readVLong(),
                DfsSearchResult.subOne(in.readVLong()));
            termStatistics.put(term, stats);
        }
        fieldStatistics = DfsSearchResult.readFieldStats(in);
        maxDoc = in.readVLong();
    }

    public AggregatedDfs(ObjectObjectHashMap<Term, TermStatistics> termStatistics,
            ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics, long maxDoc) {
        this.termStatistics = termStatistics;
        this.fieldStatistics = fieldStatistics;
        this.maxDoc = maxDoc;
    }

    public ObjectObjectHashMap<Term, TermStatistics> termStatistics() {
        return termStatistics;
    }

    public ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    public long maxDoc() {
        return maxDoc;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(termStatistics.size());

        for (ObjectObjectCursor<Term, TermStatistics> c : termStatistics()) {
            Term term = c.key;
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
            TermStatistics stats = c.value;
            out.writeBytesRef(stats.term());
            out.writeVLong(stats.docFreq());
            out.writeVLong(DfsSearchResult.addOne(stats.totalTermFreq()));
        }

        DfsSearchResult.writeFieldStats(out, fieldStatistics);
        out.writeVLong(maxDoc);
    }
}
