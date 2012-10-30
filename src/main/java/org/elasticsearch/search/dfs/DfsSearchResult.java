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

import gnu.trove.map.TMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class DfsSearchResult extends TransportResponse implements SearchPhaseResult {

    private static final Term[] EMPTY_TERMS = new Term[0];
    private static final TermStatistics[] EMPTY_TERM_STATS = new TermStatistics[0];

    private SearchShardTarget shardTarget;
    private long id;
    private Term[] terms;
    private TermStatistics[] termStatistics;
    private TMap<String, CollectionStatistics> fieldStatistics = new ExtTHashMap<String, CollectionStatistics>();
    private int maxDoc;

    public DfsSearchResult() {

    }

    public DfsSearchResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    public long id() {
        return this.id;
    }

    public SearchShardTarget shardTarget() {
        return shardTarget;
    }

    @Override
    public void shardTarget(SearchShardTarget shardTarget) {
        this.shardTarget = shardTarget;
    }

    public DfsSearchResult maxDoc(int maxDoc) {
        this.maxDoc = maxDoc;
        return this;
    }

    public int maxDoc() {
        return maxDoc;
    }

    public DfsSearchResult termsStatistics(Term[] terms, TermStatistics[] termStatistics) {
        this.terms = terms;
        this.termStatistics = termStatistics;
        return this;
    }

    public DfsSearchResult fieldStatistics(TMap<String, CollectionStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
        return this;
    }

    public Term[] terms() {
        return terms;
    }

    public TermStatistics[] termStatistics() {
        return termStatistics;
    }

    public TMap<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    public static DfsSearchResult readDfsSearchResult(StreamInput in) throws IOException, ClassNotFoundException {
        DfsSearchResult result = new DfsSearchResult();
        result.readFrom(in);
        return result;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);
        id = in.readLong();
//        shardTarget = readSearchShardTarget(in);
        int termsSize = in.readVInt();
        if (termsSize == 0) {
            terms = EMPTY_TERMS;
        } else {
            terms = new Term[termsSize];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = new Term(in.readString(), in.readBytesRef());
            }
        }
        int termsStatsSize = in.readVInt();
        if (termsStatsSize == 0) {
            termStatistics = EMPTY_TERM_STATS;
        } else {
            termStatistics = new TermStatistics[termsStatsSize];
            for (int i = 0; i < termStatistics.length; i++) {
                BytesRef term = terms[i].bytes();
                long docFreq = in.readVLong();
                long totalTermFreq = in.readVLong();
                termStatistics[i] = new TermStatistics(term, docFreq, totalTermFreq);
            }
        }
        int numFieldStatistics = in.readVInt();
        for (int i = 0; i < numFieldStatistics; i++) {
            String field = in.readString();
            CollectionStatistics stats = new CollectionStatistics(field, in.readVLong(), in.readVLong(), in.readVLong(), in.readVLong());
            fieldStatistics.put(field, stats);
        }

        maxDoc = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
//        shardTarget.writeTo(out);
        out.writeVInt(terms.length);
        for (Term term : terms) {
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
        }
        out.writeVInt(termStatistics.length);
        for (TermStatistics termStatistic : termStatistics) {
            out.writeVLong(termStatistic.docFreq());
            out.writeVLong(termStatistic.totalTermFreq());
        }
        out.writeVInt(fieldStatistics.size());
        for (Map.Entry<String, CollectionStatistics> entry : fieldStatistics.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVLong(entry.getValue().maxDoc());
            out.writeVLong(entry.getValue().docCount());
            out.writeVLong(entry.getValue().sumTotalTermFreq());
            out.writeVLong(entry.getValue().sumDocFreq());
        }
        out.writeVInt(maxDoc);
    }

}
