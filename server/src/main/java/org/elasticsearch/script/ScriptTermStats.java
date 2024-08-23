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
import org.elasticsearch.common.util.CachedSupplier;

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
    private final IndexSearcher searcher;
    private final LeafReaderContext leafReaderContext;
    private final StatsAccumulator statsAccumulator = new StatsAccumulator();
    private final Supplier<TermStates[]> termContextsSupplier;
    private final Supplier<PostingsEnum[]> postingsSupplier;
    private final Supplier<StatsAccumulator> docFreqSupplier;
    private final Supplier<StatsAccumulator> totalTermFreqSupplier;

    public ScriptTermStats(IndexSearcher searcher, LeafReaderContext leafReaderContext, Supplier<Integer> docIdSupplier, Set<Term> terms) {
        this.searcher = searcher;
        this.leafReaderContext = leafReaderContext;
        this.docIdSupplier = docIdSupplier;
        this.terms = terms.toArray(new Term[0]);
        this.termContextsSupplier = CachedSupplier.wrap(this::loadTermContexts);
        this.postingsSupplier = CachedSupplier.wrap(this::loadPostings);
        this.docFreqSupplier = CachedSupplier.wrap(this::loadDocFreq);
        this.totalTermFreqSupplier = CachedSupplier.wrap(this::loadTotalTermFreq);
    }

    /**
     * Number of unique terms in the query.
     *
     * @return the number of unique terms
     */
    public int uniqueTermsCount() {
        return terms.length;
    }

    /**
     * Number of terms that are matched im the query.
     *
     * @return the number of matched terms
     */
    public int matchedTermsCount() {
        final int docId = docIdSupplier.get();
        int matchedTerms = 0;

        try {
            for (PostingsEnum postingsEnum : postingsSupplier.get()) {
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
        return docFreqSupplier.get();
    }

    private StatsAccumulator loadDocFreq() {
        StatsAccumulator docFreqStats = new StatsAccumulator();
        TermStates[] termContexts = termContextsSupplier.get();

        try {
            for (int i = 0; i < termContexts.length; i++) {
                try {
                    docFreqStats.accept(termStatistics(terms[i], termContexts[i]).docFreq());
                } catch (IllegalArgumentException e) {
                    docFreqStats.accept(termContexts[i].docFreq());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return docFreqStats;
    }

    /**
     * Collect totalTermFreq (total number of occurrence of a term in the index) for the terms of the query and returns statistics for them.
     *
     * @return statistics on totalTermFreq for the terms of the query.
     */
    public StatsAccumulator totalTermFreq() {
        return this.totalTermFreqSupplier.get();
    }

    private StatsAccumulator loadTotalTermFreq() {
        StatsAccumulator totalTermFreqStats = new StatsAccumulator();
        TermStates[] termContexts = termContextsSupplier.get();

        try {
            for (int i = 0; i < termContexts.length; i++) {
                try {
                    totalTermFreqStats.accept(termStatistics(terms[i], termContexts[i]).totalTermFreq());
                } catch (IllegalArgumentException e) {
                    totalTermFreqStats.accept(termContexts[i].totalTermFreq());
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return totalTermFreqStats;
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
            for (PostingsEnum postingsEnum : postingsSupplier.get()) {
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

            for (PostingsEnum postingsEnum : postingsSupplier.get()) {
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

    private TermStates[] loadTermContexts() {
        try {
            TermStates[] termContexts = new TermStates[terms.length];

            for (int i = 0; i < terms.length; i++) {
                termContexts[i] = TermStates.build(searcher, terms[i], true);
            }

            return termContexts;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private TermStatistics termStatistics(Term term, TermStates termStates) throws IOException {
        return searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
    }

    private PostingsEnum[] loadPostings() {
        try {
            PostingsEnum[] postings = new PostingsEnum[terms.length];
            TermStates[] contexts = termContextsSupplier.get();

            for (int i = 0; i < terms.length; i++) {
                TermStates termStates = contexts[i];
                if (termStates.docFreq() == 0) {
                    postings[i] = null;
                    continue;
                }

                TermState state = termStates.get(leafReaderContext);
                if (state == null) {
                    postings[i] = null;
                    continue;
                }

                TermsEnum termsEnum = leafReaderContext.reader().terms(terms[i].field()).iterator();
                termsEnum.seekExact(terms[i].bytes(), state);

                postings[i] = termsEnum.postings(null, PostingsEnum.ALL);
            }

            return postings;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
