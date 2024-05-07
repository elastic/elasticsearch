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
import java.util.HashMap;
import java.util.Map;

public class TermsStatsReader {
    private final LeafReaderContext leafReaderContext;
    private final IndexSearcher searcher;
    private final Map<Term, TermStates> termContexts = new HashMap<>();
    private final Map<Term, PostingsEnum> postingsEnums = new HashMap<>();

    public TermsStatsReader(LeafReaderContext leafReaderContext, IndexSearcher searcher) {
        this.leafReaderContext = leafReaderContext;
        this.searcher = searcher;
    }

    private TermStates termStates(Term term) throws IOException {
        if (termContexts.containsKey(term) == false) {
            TermStates termStates = TermStates.build(searcher, term, true);

            if (termStates != null && termStates.docFreq() > 0) {
                searcher.collectionStatistics(term.field());
                searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
            }

            termContexts.put(term, termStates);
        }

        return termContexts.get(term);
    }

    public TermStatistics termStatistics(Term term) throws IOException {
        TermStates termStates = termStates(term);
        try {
            return searcher.termStatistics(term, termStates.docFreq(), termStates.totalTermFreq());
        } catch (IllegalArgumentException e) {
            return null;
        }
    }

    public PostingsEnum postingsEnum(Term term) throws IOException {
        return postingsEnums.computeIfAbsent(term, t -> {
            try {
                TermStates termStates = termStates(t);
                TermState state = termStates.get(leafReaderContext);

                if (state == null || termStates.docFreq() == 0) {
                    return null;
                }

                TermsEnum termsEnum = leafReaderContext.reader().terms(t.field()).iterator();
                termsEnum.seekExact(t.bytes(), state);
                return termsEnum.postings(null, PostingsEnum.ALL);
            } catch (IOException e) {
                return null;
            }
        });
    }

    public Map<Term, TermStates> collectedTermStates() {
        return termContexts;
    }
}
