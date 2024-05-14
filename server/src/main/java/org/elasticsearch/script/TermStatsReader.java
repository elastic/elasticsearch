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
import java.util.Map;
import java.util.Set;

public class TermStatsReader {
    private final IndexSearcher searcher;
    private final Set<Term> terms;
    private final Map<Term, TermStates> termContexts;

    public TermStatsReader(IndexSearcher searcher, Set<Term> terms, Map<Term, TermStates> termContexts) {
        this.searcher = searcher;
        this.terms = terms;
        this.termContexts = termContexts;
    }

    public Set<Term> terms() {
        return terms;
    }

    public TermStatistics termStatistics(Term term) {
        try {
            if (termContexts.containsKey(term) == false) {
                return searcher.termStatistics(term, 0, 0);
            }

            return searcher.termStatistics(term, termContexts.get(term).docFreq(), termContexts.get(term).totalTermFreq());
        } catch (IllegalArgumentException e) {
            return null;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public PostingsEnum postings(LeafReaderContext leafReaderContext, Term term, int flags) {
        if (termContexts.containsKey(term) == false) {
            return null;
        }

        try {
            TermStates termContext = termContexts.get(term);
            TermState state = termContext.get(leafReaderContext);
            if (state == null || termContext.docFreq() == 0) {
                return null;
            }

            TermsEnum termsEnum = leafReaderContext.reader().terms(term.field()).iterator();
            termsEnum.seekExact(term.bytes(), state);
            return termsEnum.postings(null, flags);

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
