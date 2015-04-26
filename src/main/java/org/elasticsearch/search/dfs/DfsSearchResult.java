/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.transport.TransportResponse;

import java.io.IOException;

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
    private ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
    private int maxDoc;

    public DfsSearchResult() {

    }

    public DfsSearchResult(long id, SearchShardTarget shardTarget) {
        this.id = id;
        this.shardTarget = shardTarget;
    }

    @Override
    public long id() {
        return this.id;
    }

    @Override
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

    public DfsSearchResult fieldStatistics(ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
        return this;
    }

    public Term[] terms() {
        return terms;
    }

    public TermStatistics[] termStatistics() {
        return termStatistics;
    }

    public ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics() {
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
        int termsSize = in.readVInt();
        if (termsSize == 0) {
            terms = EMPTY_TERMS;
        } else {
            terms = new Term[termsSize];
            for (int i = 0; i < terms.length; i++) {
                terms[i] = new Term(in.readString(), in.readBytesRef());
            }
        }
        this.termStatistics = readTermStats(in, terms);
        readFieldStats(in, fieldStatistics);
        

        maxDoc = in.readVInt();
    }

 
  @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(id);
        out.writeVInt(terms.length);
        for (Term term : terms) {
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
        }
        writeTermStats(out, termStatistics);
        writeFieldStats(out, fieldStatistics);
        out.writeVInt(maxDoc);
    }
    
    public static void writeFieldStats(StreamOutput out, ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics) throws IOException {
        out.writeVInt(fieldStatistics.size());
        final boolean[] states = fieldStatistics.allocated;
        Object[] keys = fieldStatistics.keys;
        Object[] values = fieldStatistics.values;
        for (int i = 0; i < states.length; i++) {
            if (states[i]) {
                out.writeString((String) keys[i]);
                CollectionStatistics statistics = (CollectionStatistics) values[i];
                assert statistics.maxDoc() >= 0;
                out.writeVLong(statistics.maxDoc());
                out.writeVLong(addOne(statistics.docCount()));
                out.writeVLong(addOne(statistics.sumTotalTermFreq()));
                out.writeVLong(addOne(statistics.sumDocFreq()));
            }
        }
    }
    
    public static void writeTermStats(StreamOutput out, TermStatistics[] termStatistics) throws IOException {
        out.writeVInt(termStatistics.length);
        for (TermStatistics termStatistic : termStatistics) {
            writeSingleTermStats(out, termStatistic);
        }
    }
    
    public  static void writeSingleTermStats(StreamOutput out, TermStatistics termStatistic) throws IOException {
        assert termStatistic.docFreq() >= 0;
        out.writeVLong(termStatistic.docFreq());
        out.writeVLong(addOne(termStatistic.totalTermFreq()));        
    }
    
    public static ObjectObjectOpenHashMap<String, CollectionStatistics> readFieldStats(StreamInput in) throws IOException {
        return readFieldStats(in, null);
    }

    public static ObjectObjectOpenHashMap<String, CollectionStatistics> readFieldStats(StreamInput in, ObjectObjectOpenHashMap<String, CollectionStatistics> fieldStatistics) throws IOException {
        final int numFieldStatistics = in.readVInt();
        if (fieldStatistics == null) {
            fieldStatistics = HppcMaps.newNoNullKeysMap(numFieldStatistics);
        }
        for (int i = 0; i < numFieldStatistics; i++) {
            final String field = in.readString();
            assert field != null;
            final long maxDoc = in.readVLong();
            final long docCount = subOne(in.readVLong());
            final long sumTotalTermFreq = subOne(in.readVLong());
            final long sumDocFreq = subOne(in.readVLong());
            CollectionStatistics stats = new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
            fieldStatistics.put(field, stats);
        }
        return fieldStatistics;
    }

    public static TermStatistics[] readTermStats(StreamInput in, Term[] terms) throws IOException {
        int termsStatsSize = in.readVInt();
        final TermStatistics[] termStatistics;
        if (termsStatsSize == 0) {
            termStatistics = EMPTY_TERM_STATS;
        } else {
            termStatistics = new TermStatistics[termsStatsSize];
            assert terms.length == termsStatsSize;
            for (int i = 0; i < termStatistics.length; i++) {
                BytesRef term = terms[i].bytes();
                final long docFreq = in.readVLong();
                assert docFreq >= 0;
                final long totalTermFreq = subOne(in.readVLong());
                termStatistics[i] = new TermStatistics(term, docFreq, totalTermFreq);
            }
        }
        return termStatistics;
    }

    
    /*
     * optional statistics are set to -1 in lucene by default.
     * Since we are using var longs to encode values we add one to each value
     * to ensure we don't waste space and don't add negative values.
     */
    public static long addOne(long value) {
        assert value + 1 >= 0;
        return value + 1;
    }
    
    
    /*
     * See #addOne this just subtracting one and asserts that the actual value
     * is positive.
     */
    public static long subOne(long value) {
        assert value >= 0;
        return value - 1;
    }

}
