/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.dfs;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.profile.SearchProfileDfsPhaseResult;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DfsSearchResult extends SearchPhaseResult {

    private static final Term[] EMPTY_TERMS = new Term[0];
    private static final TermStatistics[] EMPTY_TERM_STATS = new TermStatistics[0];
    private Term[] terms;
    private TermStatistics[] termStatistics;
    private Map<String, CollectionStatistics> fieldStatistics = new HashMap<>();
    private List<DfsKnnResults> knnResults;
    private int maxDoc;
    private SearchProfileDfsPhaseResult searchProfileDfsPhaseResult;

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
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            setShardSearchRequest(in.readOptionalWriteable(ShardSearchRequest::new));
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                knnResults = in.readOptionalList(DfsKnnResults::new);
            } else {
                DfsKnnResults results = in.readOptionalWriteable(DfsKnnResults::new);
                knnResults = results != null ? List.of(results) : List.of();
            }
        }
        if (in.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            searchProfileDfsPhaseResult = in.readOptionalWriteable(SearchProfileDfsPhaseResult::new);
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

    public DfsSearchResult fieldStatistics(Map<String, CollectionStatistics> fieldStatistics) {
        this.fieldStatistics = fieldStatistics;
        return this;
    }

    public DfsSearchResult knnResults(List<DfsKnnResults> knnResults) {
        this.knnResults = knnResults;
        return this;
    }

    public DfsSearchResult profileResult(SearchProfileDfsPhaseResult searchProfileDfsPhaseResult) {
        this.searchProfileDfsPhaseResult = searchProfileDfsPhaseResult;
        return this;
    }

    public Term[] terms() {
        return terms;
    }

    public TermStatistics[] termStatistics() {
        return termStatistics;
    }

    public Map<String, CollectionStatistics> fieldStatistics() {
        return fieldStatistics;
    }

    public List<DfsKnnResults> knnResults() {
        return knnResults;
    }

    public SearchProfileDfsPhaseResult searchProfileDfsPhaseResult() {
        return searchProfileDfsPhaseResult;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        contextId.writeTo(out);
        out.writeArray((o, term) -> {
            o.writeString(term.field());
            o.writeBytesRef(term.bytes());
        }, terms);
        writeTermStats(out, termStatistics);
        writeFieldStats(out, fieldStatistics);
        out.writeVInt(maxDoc);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_7_10_0)) {
            out.writeOptionalWriteable(getShardSearchRequest());
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_4_0)) {
            if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_7_0)) {
                out.writeOptionalCollection(knnResults);
            } else {
                if (knnResults != null && knnResults.size() > 1) {
                    throw new IllegalArgumentException(
                        "Cannot serialize multiple KNN results to nodes using previous transport version ["
                            + out.getTransportVersion()
                            + "], minimum required transport version is [8070099]"
                    );
                }
                out.writeOptionalWriteable(knnResults == null || knnResults.isEmpty() ? null : knnResults.get(0));
            }
        }
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_6_0)) {
            out.writeOptionalWriteable(searchProfileDfsPhaseResult);
        }
    }

    public static void writeFieldStats(StreamOutput out, Map<String, CollectionStatistics> fieldStatistics) throws IOException {
        out.writeMap(fieldStatistics, StreamOutput::writeString, (o, statistics) -> {
            assert statistics.maxDoc() >= 0;
            o.writeVLong(statistics.maxDoc());
            // stats are always positive numbers
            o.writeVLong(statistics.docCount());
            o.writeVLong(statistics.sumTotalTermFreq());
            o.writeVLong(statistics.sumDocFreq());
        });
    }

    public static void writeTermStats(StreamOutput out, TermStatistics[] termStatistics) throws IOException {
        out.writeArray(DfsSearchResult::writeSingleTermStats, termStatistics);
    }

    public static void writeSingleTermStats(StreamOutput out, TermStatistics termStatistic) throws IOException {
        if (termStatistic != null) {
            assert termStatistic.docFreq() > 0;
            out.writeVLong(termStatistic.docFreq());
            out.writeVLong(addOne(termStatistic.totalTermFreq()));
        } else {
            out.writeVLong(0);
            out.writeVLong(0);
        }
    }

    static Map<String, CollectionStatistics> readFieldStats(StreamInput in) throws IOException {
        final int numFieldStatistics = in.readVInt();
        Map<String, CollectionStatistics> fieldStatistics = new HashMap<>(numFieldStatistics);
        for (int i = 0; i < numFieldStatistics; i++) {
            final String field = in.readString();
            assert field != null;
            final long maxDoc = in.readVLong();
            // stats are always positive numbers
            final long docCount = in.readVLong();
            final long sumTotalTermFreq = in.readVLong();
            final long sumDocFreq = in.readVLong();
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
                final long totalTermFreq = subOne(in.readVLong());
                if (docFreq == 0) {
                    continue;
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
