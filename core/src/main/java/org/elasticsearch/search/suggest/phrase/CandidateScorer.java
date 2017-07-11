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

import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.Candidate;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGenerator.CandidateSet;

import java.io.IOException;

final class CandidateScorer {
    private final WordScorer scorer;
    private final int maxNumCorrections;
    private final int gramSize;

    CandidateScorer(WordScorer scorer, int maxNumCorrections, int gramSize) {
        this.scorer = scorer;
        this.maxNumCorrections = maxNumCorrections;
        this.gramSize = gramSize;
    }


    public Correction[] findBestCandiates(CandidateSet[] sets, float errorFraction, double cutoffScore) throws IOException {
        if (sets.length == 0) {
            return Correction.EMPTY;
        }
        PriorityQueue<Correction> corrections = new PriorityQueue<Correction>(maxNumCorrections) {
            @Override
            protected boolean lessThan(Correction a, Correction b) {
                return a.compareTo(b) < 0;
            }
        };
        int numMissspellings = 1;
        if (errorFraction >= 1.0) {
            numMissspellings = (int) errorFraction;
        } else {
            numMissspellings = Math.round(errorFraction * sets.length);
        }
        findCandidates(sets, new Candidate[sets.length], 0, Math.max(1, numMissspellings), corrections, cutoffScore, 0.0);
        Correction[] result = new Correction[corrections.size()];
        for (int i = result.length - 1; i >= 0; i--) {
            result[i] = corrections.pop();
        }
        assert corrections.size() == 0;
        return result;

    }

    public void findCandidates(CandidateSet[] candidates, Candidate[] path, int ord, int numMissspellingsLeft,
            PriorityQueue<Correction> corrections, double cutoffScore, final double pathScore) throws IOException {
        CandidateSet current = candidates[ord];
        if (ord == candidates.length - 1) {
            path[ord] = current.originalTerm;
            updateTop(candidates, path, corrections, cutoffScore, pathScore + scorer.score(path, candidates, ord, gramSize));
            if (numMissspellingsLeft > 0) {
                for (int i = 0; i < current.candidates.length; i++) {
                    path[ord] = current.candidates[i];
                    updateTop(candidates, path, corrections, cutoffScore, pathScore + scorer.score(path, candidates, ord, gramSize));
                }
            }
        } else {
            if (numMissspellingsLeft > 0) {
                path[ord] = current.originalTerm;
                findCandidates(candidates, path, ord + 1, numMissspellingsLeft, corrections, cutoffScore, pathScore + scorer.score(path, candidates, ord, gramSize));
                for (int i = 0; i < current.candidates.length; i++) {
                    path[ord] = current.candidates[i];
                    findCandidates(candidates, path, ord + 1, numMissspellingsLeft - 1, corrections, cutoffScore, pathScore + scorer.score(path, candidates, ord, gramSize));
                }
            } else {
                path[ord] = current.originalTerm;
                findCandidates(candidates, path, ord + 1, 0, corrections, cutoffScore, pathScore + scorer.score(path, candidates, ord, gramSize));
            }
        }

    }

    private void updateTop(CandidateSet[] candidates, Candidate[] path, PriorityQueue<Correction> corrections, double cutoffScore, double score)
            throws IOException {
        score = Math.exp(score);
        assert Math.abs(score - score(path, candidates)) < 0.00001;
        if (score > cutoffScore) {
            if (corrections.size() < maxNumCorrections) {
                Candidate[] c = new Candidate[candidates.length];
                System.arraycopy(path, 0, c, 0, path.length);
                corrections.add(new Correction(score, c));
            } else if (corrections.top().compareTo(score, path) < 0) {
                Correction top = corrections.top();
                System.arraycopy(path, 0, top.candidates, 0, path.length);
                top.score = score;
                corrections.updateTop();
            }
        }
    }

    public double score(Candidate[] path, CandidateSet[] candidates) throws IOException {
        double score = 0.0d;
        for (int i = 0; i < candidates.length; i++) {
           score += scorer.score(path, candidates, i, gramSize);
        }
        return Math.exp(score);
    }
}
