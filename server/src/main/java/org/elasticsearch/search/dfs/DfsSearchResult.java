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
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.Version;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;

import java.io.IOException;

public class DfsSearchResult extends SearchPhaseResult {

    private static final Term[] EMPTY_TERMS = new Term[0];
    private static final TermStatistics[] EMPTY_TERM_STATS = new TermStatistics[0];
    private Term[] terms;
    private TermStatistics[] termStatistics;
    private ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
    private int maxDoc;

    public DfsSearchResult(StreamInput in) throws IOException {
        super(in);
        contextId = new ShardSearchContextId(in);
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
        fieldStatistics = readFieldStats(in);

        maxDoc = in.readVInt();
        if (in.getVersion().onOrAfter(Version.V_7_10_0)) {
            setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
        }
    }

    public DfsSearchResult(ShardSearchContextId contextId, SearchShardTarget shardTarget, ShardSearchRequest shardSearchRequest) {
        this.setSearchShardTarget(shardTarget);
        this.contextId = contextId;
        setShardSearchRequest(shardSearchRequest);
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

    public DfsSearchResult fieldStatistics(ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
        return this;
    }

    public Term[] terms() {
        return terms;
    }

    public TermStatistics[] termStatistics() {
        return termStatistics;
    }

    public ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        contextId.writeTo(out);
        out.writeVInt(terms.length);
        for (Term term : terms) {
            out.writeString(term.field());
            out.writeBytesRef(term.bytes());
        }
        writeTermStats(out, termStatistics);
        writeFieldStats(out, fieldStatistics);
        out.writeVInt(maxDoc);
        if (out.getVersion().onOrAfter(Version.V_7_10_0)) {
            out.writeOptionalWriteable(getShardSearchRequest());
        }
    }

    public static void writeFieldStats(StreamOutput out, ObjectObjectHashMap<String,
            CollectionStatistics> fieldStatistics) throws IOException {
        out.writeVInt(fieldStatistics.size());

        for (ObjectObjectCursor<String, CollectionStatistics> c : fieldStatistics) {
            out.writeString(c.key);
            CollectionStatistics statistics = c.value;
            assert statistics.maxDoc() > 0;
            out.writeVLong(statistics.maxDoc());
            if (out.getVersion().onOrAfter(Version.V_7_0_0)) {
                // stats are always positive numbers
                out.writeVLong(statistics.docCount());
                out.writeVLong(statistics.sumTotalTermFreq());
                out.writeVLong(statistics.sumDocFreq());
            } else {
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
        if (termStatistic != null) {
            assert termStatistic.docFreq() > 0;
            out.writeVLong(termStatistic.docFreq());
            out.writeVLong(addOne(termStatistic.totalTermFreq()));
        } else {
            out.writeVLong(0);
            out.writeVLong(0);
        }
    }

    static ObjectObjectHashMap<String, CollectionStatistics> readFieldStats(StreamInput in) throws IOException {
        final int numFieldStatistics = in.readVInt();
        ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap(numFieldStatistics);
        for (int i = 0; i < numFieldStatistics; i++) {
            final String field = in.readString();
            assert field != null;
            long maxDoc = in.readVLong();
            final long docCount;
            long sumTotalTermFreq;
            final long sumDocFreq;
            if (in.getVersion().onOrAfter(Version.V_7_0_0)) {
                // stats are always positive numbers
                docCount = in.readVLong();
                sumTotalTermFreq = in.readVLong();
                sumDocFreq = in.readVLong();
            } else {
                docCount = subOne(in.readVLong());
                sumTotalTermFreq = subOne(in.readVLong());
                sumDocFreq = subOne(in.readVLong());
                if (sumTotalTermFreq == -1L) {
                    // fallback used by Lucene in ES 6 (LUCENE-8007)
                    sumTotalTermFreq = sumDocFreq;
                }
                if (maxDoc == 0L) {
                    // fallback used by Lucene in ES 6 (LUCENE-8007)
                    maxDoc = docCount;
                }
                if (docCount == 0L) {
                    // empty stats object (LUCENE-8020)
                    assert maxDoc == 0 && docCount == 0 && sumTotalTermFreq == 0 && sumDocFreq == 0:
                        " maxDoc:" + maxDoc +
                        " docCount:" + docCount +
                        " sumTotalTermFreq:" + sumTotalTermFreq +
                        " sumDocFreq:" + sumDocFreq;
                    continue;
                }
            }
            CollectionStatistics stats = new CollectionStatistics(field, maxDoc, docCount, sumTotalTermFreq, sumDocFreq);
            fieldStatistics.put(field, stats);
        }
        return fieldStatistics;
    }

    static TermStatistics[] readTermStats(StreamInput in, Term[] terms) throws IOException {
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
                long totalTermFreq = subOne(in.readVLong());
                if (docFreq == 0) {
                    continue;
                }
                if (in.getVersion().before(Version.V_7_0_0)) {
                    if (totalTermFreq == -1L) {
                        // fallback used by Lucene in ES 6
                        totalTermFreq = docFreq;
                    }
                }
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
