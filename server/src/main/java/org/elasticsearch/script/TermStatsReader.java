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

public class TermStatsReader {
    private final IndexSearcher searcher;
    private final LeafReaderContext leafReaderContext;
    private final Supplier<Integer> docIdSupplier;
    private final Map<Term, TermStates> termContexts = new HashMap<>();
    private final Map<Term, PostingsEnum> postings = new HashMap<>();

    private Set<Term> terms;

    public TermStatsReader(IndexSearcher searcher, Supplier<Integer> docIdSupplier, LeafReaderContext leafReaderContext) {
        this.searcher = searcher;
        this.docIdSupplier = docIdSupplier;
        this.leafReaderContext = leafReaderContext;
    }

    public void _setTerms(Set<Term> terms) {
        this.terms = terms;
    }

    public Set<Term> terms() {
        return terms;
    }

    public long uniqueTermsCount() {
        return terms.size();
    }

    public long matchedTermsCount() {
        return terms().stream().filter(term -> {
            try {
            PostingsEnum postingsEnum = postings(term);
            int docId = docIdSupplier.get();
                return postingsEnum != null && postingsEnum.advance(docId) == docId;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).count();
    }

    public DoubleSummaryStatistics docFreq() {
        DoubleSummaryStatistics docFreqStatistics = new DoubleSummaryStatistics();
        for (Term term : terms()) {
            TermStatistics termStats = termStatistics(term);
            docFreqStatistics.accept(termStats != null ? termStats.docFreq() : 0);
        }
        return docFreqStatistics;
    }

    public DoubleSummaryStatistics totalTermFreq() {
        DoubleSummaryStatistics totalTermFreqStatistics = new DoubleSummaryStatistics();
        for (Term term : terms()) {
            TermStatistics termStats = termStatistics(term);
            totalTermFreqStatistics.accept(termStats != null ? termStats.totalTermFreq() : 0);
        }
        return totalTermFreqStatistics;
    }

    public DoubleSummaryStatistics termFreq() {
        DoubleSummaryStatistics termFreqStatistics = new DoubleSummaryStatistics();

        for (Term term: terms()) {
            try {
                PostingsEnum postingsEnum = postings(term);
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

        return termFreqStatistics;
    }

    public DoubleSummaryStatistics termPositions() {
        DoubleSummaryStatistics termPositionsStatistics = new DoubleSummaryStatistics();

        for (Term term : terms()) {
            try {
                PostingsEnum postingsEnum = postings(term);
                int docId = docIdSupplier.get();
                if (postingsEnum == null || postingsEnum.advance(docId) != docId) {
                    continue;
                }
                for (int i=0; i < postingsEnum.freq(); i++) {
                    termPositionsStatistics.accept(postingsEnum.nextPosition() + 1);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        return termPositionsStatistics;
    }

    private TermStatistics termStatistics(Term term) {
        try {
            TermStates termStates = termStates(term);

            if (termStates != null && termStates.docFreq() > 0) {
                return searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
            }
            return searcher.termStatistics(term, 0, 0);
        } catch (IllegalArgumentException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private PostingsEnum postings(Term term) {
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
