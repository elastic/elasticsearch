/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.suggest.phrase;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.BytesRefBuilder;
import org.elasticsearch.search.suggest.SuggestUtils;
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
                if (i == 0 || candidates[i-1].userInput) {
                    highlighted.append(preTag);
                }
                highlighted.append(candidate.term);
                if (toJoin.length == i + 1 || candidates[i+1].userInput) {
                    highlighted.append(postTag);
                }
                toJoin[i] = highlighted.get();
            }
            len += toJoin[i].length;
        }
        result.grow(len);
        return SuggestUtils.join(separator, result, toJoin);
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
            for (int i=0;i<limit;i++) {
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
