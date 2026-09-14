/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.knneval;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Scores one query: the candidate's top-k against the baseline's, plus the per-query detail. */
final class KnnEvalRecall {

    private KnnEvalRecall() {}

    /**
     * A query's reference result, carried from the baseline pass into every candidate pass. {@code keys} is built while the live hits
     * still expose their index name, since the overlap is keyed on {@code _index}/{@code _id} and not on {@code _id} alone.
     */
    record BaselineResult(List<KnnEvalDetails.Hit> hits, Set<String> keys) {}

    /** The live hits do not outlive the msearch callback, so the reference is captured here. */
    static BaselineResult baselineOf(SearchHit[] baselineHits) {
        Set<String> keys = Sets.newHashSetWithExpectedSize(baselineHits.length);
        for (SearchHit hit : baselineHits) {
            keys.add(key(hit));
        }
        return new BaselineResult(toHits(baselineHits), keys);
    }

    private static String key(SearchHit hit) {
        return hit.getIndex() + "/" + hit.getId();
    }

    /**
     * Scores a candidate run against the stored baseline and annotates each hit with its baseline rank, which is what turns a bare
     * recall number into something actionable. {@code relevant} is the baseline hit count rather than {@code k}, so a shard that cannot
     * return {@code k} documents yields 1.0 rather than a misleading fraction.
     */
    static KnnEvalDetails.QueryDetail recallOf(
        String queryId,
        SearchHit[] candidateHits,
        BaselineResult baseline,
        @Nullable KnnEvalFidelity fidelity,
        int k,
        double valueTolerance
    ) {
        int relevant = baseline.keys().size();
        int relevantRetrieved = 0;
        for (SearchHit hit : candidateHits) {
            if (baseline.keys().contains(key(hit))) {
                relevantRetrieved++;
            }
        }
        double recall = relevant > 0 ? (double) relevantRetrieved / relevant : 0.0;
        Map<String, Integer> baselineRanks = Maps.newMapWithExpectedSize(baseline.hits().size());
        for (int rank = 0; rank < baseline.hits().size(); rank++) {
            baselineRanks.putIfAbsent(baseline.hits().get(rank).id(), rank);
        }
        List<KnnEvalDetails.RankedHit> annotatedHits = new ArrayList<>(candidateHits.length);
        Set<String> returnedIds = Sets.newHashSetWithExpectedSize(candidateHits.length);
        for (SearchHit hit : candidateHits) {
            annotatedHits.add(new KnnEvalDetails.RankedHit(hit.getId(), hit.getScore(), baselineRanks.get(hit.getId())));
            returnedIds.add(hit.getId());
        }
        List<KnnEvalDetails.RankedHit> missed = new ArrayList<>();
        for (int rank = 0; rank < baseline.hits().size(); rank++) {
            KnnEvalDetails.Hit baselineHit = baseline.hits().get(rank);
            if (returnedIds.contains(baselineHit.id()) == false) {
                missed.add(new KnnEvalDetails.RankedHit(baselineHit.id(), baselineHit.score(), rank));
            }
        }
        assert relevant - relevantRetrieved == missed.size()
            : "missed [" + missed.size() + "] does not account for " + relevant + " - " + relevantRetrieved;

        // both lists are score-ordered, so rank i is comparable with rank i without matching ids
        List<Double> epsilonProfile = List.of();
        boolean incomplete = false;
        if (fidelity != null && fidelity.isSkipped() == false) {
            List<Double> profile = new ArrayList<>(k);
            for (int rank = 0; rank < k; rank++) {
                if (rank >= baseline.hits().size()) {
                    // the reference never reached this rank, so there is nothing to have lost
                    profile.add(null);
                } else if (rank >= candidateHits.length) {
                    profile.add(null);
                    incomplete = true;
                } else {
                    Double epsilon = fidelity.epsilonAtRank(baseline.hits().get(rank).score(), candidateHits[rank].getScore());
                    profile.add(epsilon);
                    incomplete |= epsilon == null;
                }
            }
            epsilonProfile = profile;
        }
        // shares the id recall's denominator, so the gap between the two is meaningful
        Double recallValue = null;
        long valueMatches = 0;
        if (fidelity != null && fidelity.isSkipped() == false && baseline.hits().isEmpty() == false) {
            float baselineWorstScore = baseline.hits().get(baseline.hits().size() - 1).score();
            for (SearchHit hit : candidateHits) {
                if (fidelity.isValueMatch(baselineWorstScore, hit.getScore(), valueTolerance)) {
                    valueMatches++;
                }
            }
            recallValue = (double) Math.min(valueMatches, baseline.hits().size()) / baseline.hits().size();
        }
        return new KnnEvalDetails.QueryDetail(
            recall,
            relevantRetrieved,
            relevant,
            annotatedHits,
            missed,
            epsilonProfile,
            incomplete,
            recallValue,
            valueMatches
        );
    }

    private static List<KnnEvalDetails.Hit> toHits(SearchHit[] hits) {
        List<KnnEvalDetails.Hit> result = new ArrayList<>(hits.length);
        for (SearchHit hit : hits) {
            result.add(new KnnEvalDetails.Hit(hit.getId(), hit.getScore()));
        }
        return result;
    }

    /** Both runs are trimmed identically, so the overlap is over comparable windows. */
    static SearchHit[] topKExcluding(SearchHit[] hits, @Nullable String excludedId, int k) {
        List<SearchHit> kept = new ArrayList<>(Math.min(hits.length, k));
        for (SearchHit hit : hits) {
            if (excludedId != null && excludedId.equals(hit.getId())) {
                continue;
            }
            kept.add(hit);
            if (kept.size() == k) {
                break;
            }
        }
        return kept.toArray(new SearchHit[0]);
    }
}
