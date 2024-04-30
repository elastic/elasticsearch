/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.apache.lucene.index.PostingsEnum;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.TermStates;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TermsStatsReader {
    private final Map<Term, TermStates> termStates;

    private final Map<Term, PostingsEnum> postingsEnums;

    public TermsStatsReader(Map<Term, TermStates> termStates, Map<Term, PostingsEnum> postingsEnums) {
        this.termStates = termStates;
        this.postingsEnums = postingsEnums;
    }

    public Map<Term, Integer> docFrequencies() {
        return termStates.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().docFreq()));
    }

    public Map<Term, Integer> termFrequencies(int docId) throws IOException {
        Map<Term, Integer> termFreqs = new HashMap<>();

        for (Term term: postingsEnums.keySet()) {
            PostingsEnum postingsEnum = postingsEnums.get(term);
            if (postingsEnum.advance(docId) == docId){
                termFreqs.put(term, postingsEnum.freq());
            } else {
                termFreqs.put(term, 0);
            }
        }

        return termFreqs;
    }
}
