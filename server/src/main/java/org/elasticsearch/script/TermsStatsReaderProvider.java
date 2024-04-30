/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.TermToBytesRefAttribute;
import org.apache.lucene.index.IndexReaderContext;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.ReaderUtil;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermState;
import org.apache.lucene.index.TermStates;
import org.apache.lucene.index.TermsEnum;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class TermsStatsReaderProvider {

    private final SearchLookup searchLookup;

    private final LeafReaderContext leafReaderContext;

    private final Map<CacheKey, TermsStatsReader> termsStatsReaderCache = new HashMap<>();

    public TermsStatsReaderProvider(SearchLookup searchLookup, LeafReaderContext leafReaderContext) {
        this.searchLookup = searchLookup;
        this.leafReaderContext = leafReaderContext;
    }

    public TermsStatsReader termStatsReader(String fieldName, String query) throws IOException {
        CacheKey cacheKey = new CacheKey(fieldName, query);

        if (termsStatsReaderCache.containsKey(cacheKey) == false) {
            termsStatsReaderCache.put(cacheKey, buildTermStatsReader(fieldName, query));
        }

        return termsStatsReaderCache.get(cacheKey);
    }

    private TermsStatsReader buildTermStatsReader(String fieldName, String query) throws IOException {

        IndexReaderContext topLevelContext = ReaderUtil.getTopLevelContext(leafReaderContext);

        final Map<Term, TermStates> termStates = new HashMap<>();
        final Map<Term, PostingsEnum> postingsEnums = new HashMap<>();

        try(
            Analyzer analyzer = searchLookup.fieldType(fieldName).getTextSearchInfo().searchAnalyzer();
            TokenStream ts = analyzer.tokenStream(fieldName, query);
        ) {
            TermToBytesRefAttribute termAttr = ts.getAttribute(TermToBytesRefAttribute.class);
            ts.reset();
            while (ts.incrementToken()) {
                Term term = new Term(fieldName, termAttr.getBytesRef());
                TermStates termContext = searchLookup.getTermStates(term);
                termStates.put(term, termContext);

                TermState state = termContext.get(leafReaderContext);

                if (state == null || termContext.docFreq() == 0) {
                    continue;
                }

                TermsEnum termsEnum = leafReaderContext.reader().terms(term.field()).iterator();
                termsEnum.seekExact(term.bytes(), state);
                postingsEnums.put(term, termsEnum.postings(null, PostingsEnum.ALL));
            }
        }

        return new TermsStatsReader(termStates, postingsEnums);
    }

    private record CacheKey(String fieldName, String query) { }
}
