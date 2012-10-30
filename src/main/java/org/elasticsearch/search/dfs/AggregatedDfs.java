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
import gnu.trove.iterator.TObjectIntIterator;
import gnu.trove.map.TMap;
import gnu.trove.map.hash.TObjectIntHashMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.TermStatistics;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.trove.ExtTHashMap;
import org.elasticsearch.common.trove.ExtTObjectIntHasMap;

import java.io.IOException;
import java.util.Map;

/**
 *
 */
public class AggregatedDfs implements Streamable {

    private TMap<Term, TermStatistics> dfMap;

    private long maxDoc;

    private AggregatedDfs() {

    }

    public AggregatedDfs(TMap<Term, TermStatistics> dfMap, long maxDoc) {
        this.dfMap = dfMap;
        this.maxDoc = maxDoc;
    }

    public TMap<Term, TermStatistics> dfMap() {
        return dfMap;
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
        dfMap = new ExtTHashMap<Term, TermStatistics>(size, Constants.DEFAULT_LOAD_FACTOR);
        for (int i = 0; i < size; i++) {
            Term term = new Term(in.readString(), in.readBytesRef());
            TermStatistics stats = new TermStatistics(in.readBytesRef(), in.readVLong(), in.readVLong());
            dfMap.put(term, stats);
        }
        maxDoc = in.readVLong();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(dfMap.size());

        for (Map.Entry<Term, TermStatistics> termTermStatisticsEntry : dfMap.entrySet()) {
            Term term = termTermStatisticsEntry.getKey();
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
            TermStatistics stats = termTermStatisticsEntry.getValue();
            out.writeBytesRef(stats.term());
            out.writeVLong(stats.docFreq());
            out.writeVLong(stats.totalTermFreq());
        }

        out.writeVLong(maxDoc);
    }
}
