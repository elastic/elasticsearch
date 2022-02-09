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

//TODO public for tests
public abstract class CandidateGenerator {

    public abstract boolean isKnownWord(BytesRef term) throws IOException;

    public abstract TermStats termStats(BytesRef term) throws IOException;

    public CandidateSet drawCandidates(BytesRef term) throws IOException {
        CandidateSet set = new CandidateSet(Candidate.EMPTY, createCandidate(term, true));
        return drawCandidates(set);
    }

    public Candidate createCandidate(BytesRef term, boolean userInput) throws IOException {
        return createCandidate(term, termStats(term), 1.0, userInput);
    }

    public Candidate createCandidate(BytesRef term, TermStats termStats, double channelScore) throws IOException {
        return createCandidate(term, termStats, channelScore, false);
    }

    public abstract Candidate createCandidate(BytesRef term, TermStats termStats, double channelScore, boolean userInput)
        throws IOException;

    public abstract CandidateSet drawCandidates(CandidateSet set) throws IOException;
}
