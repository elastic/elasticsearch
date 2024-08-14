/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.TermStatistics;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Access the term statistics of the children query of a script_score query.
 */
public class ScriptTermStats {
    private final Supplier<Integer> docIdSupplier;
    private final Term[] terms;
    private final Reader termStatsReader;
    private final StatsAccumulator statsAccumulator = new StatsAccumulator();
    private volatile PostingsEnum[] postings;
    private volatile TermStatistics[] termStatistics;

    public ScriptTermStats(IndexSearcher searcher, LeafReaderContext leafReaderContext, Supplier<Integer> docIdSupplier, Set<Term> terms) {
        this(new Reader(searcher, leafReaderContext), docIdSupplier, terms);
    }

    ScriptTermStats(Reader termStatsReader, Supplier<Integer> docIdSupplier, Set<Term> terms) {
        this.docIdSupplier = docIdSupplier;
        this.terms = terms.toArray(new Term[0]);
        this.termStatsReader = termStatsReader;
    }

    /**
     * Number of unique terms in the query.
     *
     * @return the number of unique terms
     */
    public long uniqueTermsCount() {
        return terms.length;
    }

    /**
     * Number of terms that are matched im the query.
     *
     * @return the number of matched terms
     */
    public long matchedTermsCount() {
        final int docId = docIdSupplier.get();
        int matchedTerms = 0;

        try {
            for (PostingsEnum postingsEnum : postings()) {
                if (postingsEnum != null && postingsEnum.advance(docId) == docId && postingsEnum.freq() > 0) {
                    matchedTerms++;
                }
            }
            return matchedTerms;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Collect docFreq (number of documents a term occurs in) for the terms of the query and returns statistics for them.
     *
     * @return statistics on docFreq for the terms of the query.
     */
    public StatsAccumulator docFreq() {
        statsAccumulator.reset();

        try {
            for (TermStatistics termStats : termStatistics()) {
                statsAccumulator.accept(termStats != null ? termStats.docFreq() : 0L);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return statsAccumulator;
    }

    /**
     * Collect totalTermFreq (total number of occurrence of a term in the index) for the terms of the query and returns statistics for them.
     *
     * @return statistics on totalTermFreq for the terms of the query.
     */
    public StatsAccumulator totalTermFreq() {
        statsAccumulator.reset();

        try {
            for (TermStatistics termStats : termStatistics()) {
                statsAccumulator.accept(termStats != null ? termStats.totalTermFreq() : 0L);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return statsAccumulator;
    }

    /**
     * Collect totalFreq (number of occurrence of a term in the current doc for the terms of the query and returns statistics for them.
     *
     * @return statistics on totalTermFreq for the terms of the query in the current dac
     */
    public StatsAccumulator termFreq() {
        statsAccumulator.reset();
        final int docId = docIdSupplier.get();

        try {
            for (PostingsEnum postingsEnum : postings()) {
                if (postingsEnum == null || postingsEnum.advance(docId) != docId) {
                    statsAccumulator.accept(0);
                } else {
                    statsAccumulator.accept(postingsEnum.freq());
                }
            }

            return statsAccumulator;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Collect termPositions (positions of a term in the current document) for the terms of the query and returns statistics for them.
     *
     * @return statistics on termPositions for the terms of the query in the current dac
     */
    public StatsAccumulator termPositions() {
        try {
            statsAccumulator.reset();
            int docId = docIdSupplier.get();

            for (PostingsEnum postingsEnum : postings()) {
                if (postingsEnum == null || postingsEnum.advance(docId) != docId) {
                    continue;
                }
                for (int i = 0; i < postingsEnum.freq(); i++) {
                    statsAccumulator.accept(postingsEnum.nextPosition() + 1);
                }
            }

            return statsAccumulator;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PostingsEnum[] postings() throws IOException {
        if (postings != null) {
            return postings;
        }

        postings = new PostingsEnum[terms.length];

        for (int i = 0; i < terms.length; i++) {
            postings[i] = termStatsReader.postings(terms[i]);
        }

        return postings;
    }

    private TermStatistics[] termStatistics() throws IOException {
        if (termStatistics != null) {
            return termStatistics;
        }

        termStatistics = new TermStatistics[terms.length];

        for (int i = 0; i < terms.length; i++) {
            termStatistics[i] = termStatsReader.termStatistics(terms[i]);
        }

        return termStatistics;
    }

    /**
     * Reader class encapsulating all the logic to read term statistics from the index.
     */
    static class Reader {
        private final IndexSearcher searcher;
        private final LeafReaderContext leafReaderContext;

        private Reader(IndexSearcher searcher, LeafReaderContext leafReaderContext) {
            this.searcher = searcher;
            this.leafReaderContext = leafReaderContext;
        }

        PostingsEnum postings(Term term) throws IOException {
            TermStates termStates = TermStates.build(searcher, term, true);

            if (termStates.docFreq() == 0) {
                return null;
            }

            TermState state = termStates.get(leafReaderContext);
            if (state == null) {
                return null;
            }

            TermsEnum termsEnum = leafReaderContext.reader().terms(term.field()).iterator();
            termsEnum.seekExact(term.bytes(), state);

            return termsEnum.postings(null, PostingsEnum.ALL);
        }

        TermStatistics termStatistics(Term term) throws IOException {
            try {
                TermStates termStates = TermStates.build(searcher, term, true);

                // Using the searcher to get term statistics. If search_type is dfs, this is how the stats from the DFS phase are read.
                return searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
            } catch (IllegalArgumentException e) {
                return null;
            }
        }
    }
}
