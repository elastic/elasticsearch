/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class AggregatedDfs implements Writeable {

    private final Map<Term, TermStatistics> termStatistics;
    private final Map<String, CollectionStatistics> fieldStatistics;
    private final long maxDoc;

    public AggregatedDfs(StreamInput in) throws IOException {
        int size = in.readVInt();
        termStatistics = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            // term constructor copies the bytes so we can work with a slice
            Term term = new Term(in.readString(), in.readSlicedBytesReference().toBytesRef());
            TermStatistics stats = new TermStatistics(in.readBytesRef(), in.readVLong(), DfsSearchResult.subOne(in.readVLong()));
            termStatistics.put(term, stats);
        }
        fieldStatistics = DfsSearchResult.readFieldStats(in);
        maxDoc = in.readVLong();
    }

    public AggregatedDfs(Map<Term, TermStatistics> termStatistics, Map<String, CollectionStatistics> fieldStatistics, long maxDoc) {
        this.termStatistics = termStatistics;
        this.fieldStatistics = fieldStatistics;
        this.maxDoc = maxDoc;
    }

    public Map<Term, TermStatistics> termStatistics() {
        return termStatistics;
    }

    public Map<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeMap(termStatistics, (o, k) -> {
            o.writeString(k.field());
            o.writeBytesRef(k.bytes());
        }, (o, v) -> {
            o.writeBytesRef(v.term());
            o.writeVLong(v.docFreq());
            o.writeVLong(DfsSearchResult.addOne(v.totalTermFreq()));
        });
        DfsSearchResult.writeFieldStats(out, fieldStatistics);
        out.writeVLong(maxDoc);
    }
}
