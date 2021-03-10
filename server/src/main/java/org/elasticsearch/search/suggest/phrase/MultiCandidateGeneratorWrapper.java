/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.codecs.TermStats;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

import java.io.IOException;
import java.util.Arrays;
//TODO public for tests
public final class MultiCandidateGeneratorWrapper extends CandidateGenerator {


    private final CandidateGenerator[] candidateGenerator;
    private int numCandidates ;

    public MultiCandidateGeneratorWrapper(int numCandidates, CandidateGenerator...candidateGenerators) {
        this.candidateGenerator = candidateGenerators;
        this.numCandidates = numCandidates;
    }
    @Override
    public boolean isKnownWord(BytesRef term) throws IOException {
        return candidateGenerator[0].isKnownWord(term);
    }

    @Override
    public TermStats termStats(BytesRef term) throws IOException {
        return candidateGenerator[0].termStats(term);
    }

    @Override
    public CandidateSet drawCandidates(CandidateSet set) throws IOException {
        for (CandidateGenerator generator : candidateGenerator) {
            generator.drawCandidates(set);
        }
        return reduce(set, numCandidates);
    }

    private CandidateSet reduce(CandidateSet set, int numCandidates) {
        if (set.candidates.length > numCandidates) {
            Candidate[] candidates = set.candidates;
            Arrays.sort(candidates, (left, right) -> Double.compare(right.score, left.score));
            Candidate[] newSet = new Candidate[numCandidates];
            System.arraycopy(candidates, 0, newSet, 0, numCandidates);
            set.candidates = newSet;
        }

        return set;
    }
    @Override
    public Candidate createCandidate(BytesRef term, TermStats termStats, double channelScore, boolean userInput) throws IOException {
        return candidateGenerator[0].createCandidate(term, termStats, channelScore, userInput);
    }

}
