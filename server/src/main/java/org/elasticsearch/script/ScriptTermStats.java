/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.IndexSearcher;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.search.internal.ContextIndexSearcher;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Set;
import java.util.function.IntSupplier;
import java.util.function.Supplier;

/**
 * Access the term statistics of the children query of a script_score query.
 */
public class ScriptTermStats {

    private final IntSupplier docIdSupplier;
    private final Term[] terms;
    private final IndexSearcher searcher;
    private final LeafReaderContext leafReaderContext;
    private final StatsSummary statsSummary = new StatsSummary();
    private final Supplier<TermStates[]> termContextsSupplier;
    private final Supplier<PostingsEnum[]> postingsSupplier;
    private final Supplier<StatsSummary> docFreqSupplier;
    private final Supplier<StatsSummary> totalTermFreqSupplier;

    public ScriptTermStats(IndexSearcher searcher, LeafReaderContext leafReaderContext, IntSupplier docIdSupplier, Set<Term> terms) {
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
        final int docId = docIdSupplier.getAsInt();
        int matchedTerms = 0;
        advancePostings(docId);

        for (PostingsEnum postingsEnum : postingsSupplier.get()) {
            if (postingsEnum != null && postingsEnum.docID() == docId) {
                matchedTerms++;
            }
        }

        return matchedTerms;
    }

    /**
     * Collect docFreq (number of documents a term occurs in) for the terms of the query and returns statistics for them.
     *
     * @return statistics on docFreq for the terms of the query.
     */
    public StatsSummary docFreq() {
        return docFreqSupplier.get();
    }

    private StatsSummary loadDocFreq() {
        StatsSummary docFreqStats = new StatsSummary();
        TermStates[] termContexts = termContextsSupplier.get();

        try {
            for (int i = 0; i < termContexts.length; i++) {
                if (searcher instanceof ContextIndexSearcher contextIndexSearcher) {
                    docFreqStats.accept(contextIndexSearcher.docFreq(terms[i], termContexts[i].docFreq()));
                } else {
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
    public StatsSummary totalTermFreq() {
        return this.totalTermFreqSupplier.get();
    }

    private StatsSummary loadTotalTermFreq() {
        StatsSummary totalTermFreqStats = new StatsSummary();
        TermStates[] termContexts = termContextsSupplier.get();

        try {
            for (int i = 0; i < termContexts.length; i++) {
                if (searcher instanceof ContextIndexSearcher contextIndexSearcher) {
                    totalTermFreqStats.accept(contextIndexSearcher.totalTermFreq(terms[i], termContexts[i].totalTermFreq()));
                } else {
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
    public StatsSummary termFreq() {
        statsSummary.reset();
        final int docId = docIdSupplier.getAsInt();

        try {
            advancePostings(docId);
            for (PostingsEnum postingsEnum : postingsSupplier.get()) {
                if (postingsEnum == null || postingsEnum.docID() != docId) {
                    statsSummary.accept(0);
                } else {
                    statsSummary.accept(postingsEnum.freq());
                }
            }

            return statsSummary;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * Collect termPositions (positions of a term in the current document) for the terms of the query and returns statistics for them.
     *
     * @return statistics on termPositions for the terms of the query in the current dac
     */
    public StatsSummary termPositions() {
        statsSummary.reset();
        int docId = docIdSupplier.getAsInt();

        try {
            advancePostings(docId);
            for (PostingsEnum postingsEnum : postingsSupplier.get()) {
                if (postingsEnum == null || postingsEnum.docID() != docId) {
                    continue;
                }
                for (int i = 0; i < postingsEnum.freq(); i++) {
                    statsSummary.accept(postingsEnum.nextPosition() + 1);
                }
            }

            return statsSummary;
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

    private PostingsEnum[] loadPostings() {
        try {
            PostingsEnum[] postings = new PostingsEnum[terms.length];

            for (int i = 0; i < terms.length; i++) {
                postings[i] = leafReaderContext.reader().postings(terms[i], PostingsEnum.POSITIONS);
            }

            return postings;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void advancePostings(int targetDocId) {
        try {
            for (PostingsEnum posting : postingsSupplier.get()) {
                if (posting != null && posting.docID() < targetDocId && posting.docID() != DocIdSetIterator.NO_MORE_DOCS) {
                    posting.advance(targetDocId);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
