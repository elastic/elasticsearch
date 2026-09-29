/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.knneval;

import org.elasticsearch.common.util.set.Sets;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.search.SearchHit;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/** Scores one query: the candidate's top-k against the baseline's. */
final class KnnEvalRecall {

    private KnnEvalRecall() {}

    /** A query's reference document keys and lowest top-k score, copied before the pooled search hits are released. */
    record BaselineResult(Set<String> keys, float lowestScore) {}

    record RecallResult(@Nullable Double recall, int baselineMissedBetter, long tookMs, long vectorOps) {}

    static BaselineResult baselineOf(SearchHit[] baselineHits) {
        Set<String> keys = Sets.newHashSetWithExpectedSize(baselineHits.length);
        float lowestScore = Float.POSITIVE_INFINITY;
        for (SearchHit hit : baselineHits) {
            keys.add(key(hit));
            lowestScore = Math.min(lowestScore, hit.getScore());
        }
        return new BaselineResult(keys, baselineHits.length == 0 ? Float.NEGATIVE_INFINITY : lowestScore);
    }

    static String key(SearchHit hit) {
        return hit.getIndex() + "/" + hit.getId();
    }

    static RecallResult recallOf(SearchHit[] candidateHits, BaselineResult baseline, long tookMs, long vectorOps) {
        int relevant = baseline.keys().size();
        int relevantRetrieved = 0;
        int baselineMissedBetter = 0;
        for (SearchHit hit : candidateHits) {
            if (baseline.keys().contains(key(hit))) {
                relevantRetrieved++;
            } else if (hit.getScore() > baseline.lowestScore()) {
                baselineMissedBetter++;
            }
        }
        return new RecallResult(
            baselineMissedBetter == 0 ? (relevant == 0 ? 0.0 : (double) relevantRetrieved / relevant) : null,
            baselineMissedBetter,
            tookMs,
            vectorOps
        );
    }

    /** Both runs are trimmed identically, so the overlap is over comparable windows. */
    static SearchHit[] topKExcluding(SearchHit[] hits, @Nullable String excludedKey, int k) {
        List<SearchHit> kept = new ArrayList<>(Math.min(hits.length, k));
        for (SearchHit hit : hits) {
            if (excludedKey != null && excludedKey.equals(key(hit))) {
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
