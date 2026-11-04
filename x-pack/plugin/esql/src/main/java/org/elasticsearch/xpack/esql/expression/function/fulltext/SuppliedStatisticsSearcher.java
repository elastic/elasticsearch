/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.fulltext;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.similarities.Similarity;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.similarity.SimilarityService;

/**
 * An {@link IndexSearcher} over the one-row {@code MemoryIndex} of a scored row. It answers Lucene's statistics
 * questions with the statistics of the whole BM25 statistics group, not with the one row it holds, so Lucene scores
 * the row as an index of all the statistics rows would. Query DSL feeds DFS statistics into Lucene the same way.
 * <p>
 * {@code ContextIndexSearcher.setAggregatedDfs} is not reused: it falls back to the local statistics for a term it
 * has none for, which would hide a missing term instead of failing.
 * <p>
 * Lucene asks for term statistics only for terms present in the row being scored, and every scored row was also a
 * statistics row. So a df of 0, a df above N, or a summed length below N can only mean the data changed between the
 * passes. The searcher clamps such values to the nearest valid ones, which Lucene requires, and records that it did.
 */
final class SuppliedStatisticsSearcher extends IndexSearcher {

    /** The similarity of an indexed {@code text} field with default settings. */
    static final Similarity SIMILARITY = SimilarityService.BUILT_IN.get(SimilarityService.DEFAULT_SIMILARITY)
        .apply(Settings.EMPTY, IndexVersion.current(), null);

    private final Bm25Statistics statistics;
    private boolean clampFired;

    SuppliedStatisticsSearcher(IndexReader reader, Bm25Statistics statistics) {
        super(reader);
        assert statistics.isEmpty() == false : "empty statistics cannot back a CollectionStatistics";
        this.statistics = statistics;
        setSimilarity(SIMILARITY);
        setQueryCache(null);
    }

    /**
     * Returns the group's df for {@code term}, clamped to [1, N]. BM25 does not read {@code totalTermFreq}, so df
     * stands in for it.
     */
    @Override
    public TermStatistics termStatistics(Term term, int docFreq, long totalTermFreq) {
        Long supplied = statistics.docFreq(term.bytes());
        // The data-node analyzer check guarantees every query term has statistics.
        assert supplied != null : "no statistics for term [" + term + "]";
        long raw = supplied == null ? 0 : supplied;
        long df = Math.clamp(raw, 1, statistics.docCount());
        clampFired |= df != raw;
        return new TermStatistics(term.bytes(), df, df);
    }

    /**
     * Returns N and the summed length, the latter clamped to at least N. BM25 does not read {@code sumDocFreq}, so N
     * stands in for it and satisfies Lucene's checks.
     */
    @Override
    public CollectionStatistics collectionStatistics(String field) {
        long docCount = statistics.docCount();
        long summedLength = Math.max(statistics.summedLength(), docCount);
        clampFired |= summedLength != statistics.summedLength();
        return new CollectionStatistics(field, docCount, docCount, summedLength, docCount);
    }

    /** Whether a statistic was clamped while scoring with this searcher. */
    boolean clampFired() {
        return clampFired;
    }
}
