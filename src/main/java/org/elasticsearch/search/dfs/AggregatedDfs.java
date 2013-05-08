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


import java.io.IOException;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.collect.XMaps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;

public class AggregatedDfs implements Streamable {

    private Map<Term, TermStatistics> termStatistics;
    private Map<String, CollectionStatistics> fieldStatistics;
    private long maxDoc;

    private AggregatedDfs() {
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
        termStatistics = XMaps.newMap(size);
        for (int i = 0; i < size; i++) {
            Term term = new Term(in.readString(), in.readBytesRef());
            TermStatistics stats = new TermStatistics(in.readBytesRef(), 
                    in.readVLong(), 
                    DfsSearchResult.toNotAvailable(in.readVLong()));
            termStatistics.put(term, stats);
        }
        fieldStatistics = DfsSearchResult.readFieldStats(in);
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
            out.writeVLong(DfsSearchResult.plusOne(stats.totalTermFreq()));
        }
        DfsSearchResult.writeFieldStats(out, fieldStatistics);
        out.writeVLong(maxDoc);
    }
}
