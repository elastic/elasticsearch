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
import java.util.DoubleSummaryStatistics;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Access the term statistics of the children query of a script_score query.
 */
public class ScriptTermStats {
    private static final DoubleSummaryStatistics EMPTY_STATS = new DoubleSummaryStatistics(1, 0, 0, 0);
    private final Supplier<Integer> docIdSupplier;
    private final Set<Term> terms;
    private final Reader termStatsReader;

    public ScriptTermStats(IndexSearcher searcher, LeafReaderContext leafReaderContext, Supplier<Integer> docIdSupplier, Set<Term> terms) {
        this(new Reader(searcher, leafReaderContext), docIdSupplier, terms);
    }

    ScriptTermStats(Reader termStatsReader, Supplier<Integer> docIdSupplier, Set<Term> terms) {
        this.docIdSupplier = docIdSupplier;
        this.terms = terms;
        this.termStatsReader = termStatsReader;
    }

    /**
     * Number of unique terms in the query.
     *
     * @return the number of unique terms
     */
    public long uniqueTermsCount() {
        return terms.size();
    }

    /**
     * Number of terms that are matched im the query.
     *
     * @return the number of matched terms
     */
    public long matchedTermsCount() {
        return terms.stream().filter(term -> {
            try {
                PostingsEnum postingsEnum = termStatsReader.postings(term);
                int docId = docIdSupplier.get();
                return postingsEnum != null && postingsEnum.advance(docId) == docId && postingsEnum.freq() > 0;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).count();
    }

    /**
     * Collect docFreq (number of documents a term occurs in) for the terms of the query and returns statistics for them.
     *
     * @return statistics on docFreq for the terms of the query.
     */
    public DoubleSummaryStatistics docFreq() {
        DoubleSummaryStatistics docFreqStatistics = new DoubleSummaryStatistics();

        for (Term term : terms) {
            TermStatistics termStats = termStatsReader.termStatistics(term);
            docFreqStatistics.accept(termStats != null ? termStats.docFreq() : 0L);
        }

        return docFreqStatistics.getCount() > 0 ? docFreqStatistics : EMPTY_STATS;
    }

    /**
     * Collect totalTermFreq (total number of occurrence of a term in the index) for the terms of the query and returns statistics for them.
     *
     * @return statistics on totalTermFreq for the terms of the query.
     */
    public DoubleSummaryStatistics totalTermFreq() {
        DoubleSummaryStatistics totalTermFreqStatistics = new DoubleSummaryStatistics();

        for (Term term : terms) {
            TermStatistics termStats = termStatsReader.termStatistics(term);
            totalTermFreqStatistics.accept(termStats != null ? termStats.totalTermFreq() : 0L);
        }

        return totalTermFreqStatistics.getCount() > 0 ? totalTermFreqStatistics : EMPTY_STATS;
    }

    /**
     * Collect totalFreq (number of occurrence of a term in the current doc for the terms of the query and returns statistics for them.
     *
     * @return statistics on totalTermFreq for the terms of the query in the current dac
     */
    public DoubleSummaryStatistics termFreq() {
        DoubleSummaryStatistics termFreqStatistics = new DoubleSummaryStatistics();

        for (Term term : terms) {
            try {
                PostingsEnum postingsEnum = termStatsReader.postings(term);
                int docId = docIdSupplier.get();
                if (postingsEnum == null || postingsEnum.advance(docId) != docId) {
                    termFreqStatistics.accept(0);
                } else {
                    termFreqStatistics.accept(postingsEnum.freq());
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return termFreqStatistics.getCount() > 0 ? termFreqStatistics : EMPTY_STATS;
    }

    /**
     * Collect termPositions (positions of a term in the current document) for the terms of the query and returns statistics for them.
     *
     * @return statistics on termPositions for the terms of the query in the current dac
     */
    public DoubleSummaryStatistics termPositions() {
        DoubleSummaryStatistics termPositionsStatistics = new DoubleSummaryStatistics();

        for (Term term : terms) {
            try {
                PostingsEnum postingsEnum = termStatsReader.postings(term);
                int docId = docIdSupplier.get();
                if (postingsEnum == null || postingsEnum.advance(docId) != docId) {
                    continue;
                }
                for (int i = 0; i < postingsEnum.freq(); i++) {
                    termPositionsStatistics.accept(postingsEnum.nextPosition() + 1);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return termPositionsStatistics.getCount() > 0 ? termPositionsStatistics : EMPTY_STATS;
    }

    /**
     * Reader class encapsulating all the logic to read term statistics from the index.
     */
    static class Reader {
        private final IndexSearcher searcher;
        private final LeafReaderContext leafReaderContext;
        private final Map<Term, TermStates> termContexts = new HashMap<>();
        private final Map<Term, PostingsEnum> postings = new HashMap<>();

        private Reader(IndexSearcher searcher, LeafReaderContext leafReaderContext) {
            this.searcher = searcher;
            this.leafReaderContext = leafReaderContext;
        }

        PostingsEnum postings(Term term) {
            return postings.computeIfAbsent(term, t -> {
                try {
                    TermStates termStates = termStates(term);

                    if (termStates == null || termStates.docFreq() == 0) {
                        return null;
                    }

                    TermState state = termStates.get(leafReaderContext);
                    if (state == null) {
                        return null;
                    }

                    TermsEnum termsEnum = leafReaderContext.reader().terms(term.field()).iterator();
                    termsEnum.seekExact(term.bytes(), state);
                    return termsEnum.postings(null, PostingsEnum.ALL);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }

        TermStatistics termStatistics(Term term) {
            try {
                TermStates termStates = termStates(term);

                if (termStates != null && termStates.docFreq() > 0) {
                    // Using the searcher to get term statistics. If search_type is dfs, this is how the stats from the DFS phase are read.
                    return searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
                }

                // TermStates is null or docFreq is 0, but we still want try to load term stats using the searcher, so we can retrieve
                // terms statistics from the DFS phase, even if the term is not present on this shard.
                return searcher.termStatistics(term, 0, 0);
            } catch (IllegalArgumentException e) {
                return null;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        private TermStates termStates(Term term) {
            return termContexts.computeIfAbsent(term, t -> {
                try {
                    return TermStates.build(searcher, t, true);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            });
        }
    }
}
