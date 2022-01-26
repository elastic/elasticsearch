/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;

import java.util.Arrays;

//TODO public for tests
public final class Correction implements Comparable<Correction> {

    public static final Correction[] EMPTY = new Correction[0];
    public double score;
    public final Candidate[] candidates;

    public Correction(double score, Candidate[] candidates) {
        this.score = score;
        this.candidates = candidates;
    }

    @Override
    public String toString() {
        return "Correction [score=" + score + ", candidates=" + Arrays.toString(candidates) + "]";
    }

    public BytesRef join(BytesRef separator) {
        return join(separator, null, null);
    }

    public BytesRef join(BytesRef separator, BytesRef preTag, BytesRef postTag) {
        return join(separator, new BytesRefBuilder(), preTag, postTag);
    }

    public BytesRef join(BytesRef separator, BytesRefBuilder result, BytesRef preTag, BytesRef postTag) {
        BytesRef[] toJoin = new BytesRef[this.candidates.length];
        int len = separator.length * this.candidates.length - 1;
        for (int i = 0; i < toJoin.length; i++) {
            Candidate candidate = candidates[i];
            if (preTag == null || candidate.userInput) {
                toJoin[i] = candidate.term;
            } else {
                final int maxLen = preTag.length + postTag.length + candidate.term.length;
                final BytesRefBuilder highlighted = new BytesRefBuilder();// just allocate once
                highlighted.grow(maxLen);
                if (i == 0 || candidates[i - 1].userInput) {
                    highlighted.append(preTag);
                }
                highlighted.append(candidate.term);
                if (toJoin.length == i + 1 || candidates[i + 1].userInput) {
                    highlighted.append(postTag);
                }
                toJoin[i] = highlighted.get();
            }
            len += toJoin[i].length;
        }
        result.grow(len);
        return WordScorer.join(separator, result, toJoin);
    }

    /** Lower scores sorts first; if scores are equal,
     *  than later terms (zzz) sort first .*/
    @Override
    public int compareTo(Correction other) {
        return compareTo(other.score, other.candidates);
    }

    int compareTo(double otherScore, Candidate[] otherCandidates) {
        if (score == otherScore) {
            int limit = Math.min(candidates.length, otherCandidates.length);
            for (int i = 0; i < limit; i++) {
                int cmp = candidates[i].term.compareTo(otherCandidates[i].term);
                if (cmp != 0) {
                    // Later (zzz) terms sort before (are weaker than) earlier (aaa) terms:
                    return -cmp;
                }
            }

            return candidates.length - otherCandidates.length;
        } else {
            return Double.compare(score, otherScore);
        }
    }
}
