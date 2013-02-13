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

package org.elasticsearch.search.dfs;

import gnu.trove.impl.Constants;
import gnu.trove.map.TMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.trove.ExtTHashMap;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class AggregatedDfs implements Streamable {

    private TMap<Term, TermStatistics> termStatistics;
    private TMap<String, CollectionStatistics> fieldStatistics;
    private long maxDoc;

    private AggregatedDfs() {

    }

    public AggregatedDfs(TMap<Term, TermStatistics> termStatistics, TMap<String, CollectionStatistics> fieldStatistics, long maxDoc) {
        this.termStatistics = termStatistics;
        this.fieldStatistics = fieldStatistics;
        this.maxDoc = maxDoc;
    }

    public TMap<Term, TermStatistics> termStatistics() {
        return termStatistics;
    }

    public TMap<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    public long maxDoc() {
        return maxDoc;
    }

    public static AggregatedDfs readAggregatedDfs(StreamInput in) throws IOException {
        AggregatedDfs result = new AggregatedDfs();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        int size = in.readVInt();
        termStatistics = new ExtTHashMap<Term, TermStatistics>(size, Constants.DEFAULT_LOAD_FACTOR);
        for (int i = 0; i < size; i++) {
            Term term = new Term(in.readString(), in.readBytesRef());
            TermStatistics stats = new TermStatistics(in.readBytesRef(), in.readVLong(), in.readVLong());
            termStatistics.put(term, stats);
        }
        size = in.readVInt();
        fieldStatistics = new ExtTHashMap<String, CollectionStatistics>(size, Constants.DEFAULT_LOAD_FACTOR);
        for (int i = 0; i < size; i++) {
            String field = in.readString();
            CollectionStatistics stats = new CollectionStatistics(field, in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
            fieldStatistics.put(field, stats);
        }
        maxDoc = in.readVLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(termStatistics.size());
        for (Map.Entry<Term, TermStatistics> termTermStatisticsEntry : termStatistics.entrySet()) {
            Term term = termTermStatisticsEntry.getKey();
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
            TermStatistics stats = termTermStatisticsEntry.getValue();
            out.writeBytesRef(stats.term());
            out.writeVLong(stats.docFreq());
            out.writeVLong(stats.totalTermFreq());
        }

        out.writeVInt(fieldStatistics.size());
        for (Map.Entry<String, CollectionStatistics> entry : fieldStatistics.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVLong(entry.getValue().maxDoc());
            out.writeVLong(entry.getValue().docCount());
            out.writeVLong(entry.getValue().sumTotalTermFreq());
            out.writeVLong(entry.getValue().sumDocFreq());
        }

        out.writeVLong(maxDoc);
    }
}
