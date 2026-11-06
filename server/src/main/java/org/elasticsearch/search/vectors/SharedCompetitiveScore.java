/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License, v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.vectors;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.elasticsearch.search.vectors.AbstractMaxScoreKnnCollector.LEAST_COMPETITIVE;

/**
 * Accumulates retained candidate counts and their least scores across leaves. The score remains non-competitive until
 * {@code requiredObservations} candidates support it.
 */
public final class SharedCompetitiveScore {
    // This is the cross-leaf copy. Trackers access it concurrently, so updates must be atomic.
    private final AtomicLong sharedMinCompetitiveDocScore = new AtomicLong(LEAST_COMPETITIVE);
    private final int requiredObservations;
    private final AtomicInteger supportingCandidateCount = new AtomicInteger();
    private final AtomicLong leastSupportingScore = new AtomicLong(Long.MAX_VALUE);
    private volatile boolean sharedScoreEstablished;

    SharedCompetitiveScore(int requiredObservations) {
        if (requiredObservations <= 0) {
            throw new IllegalArgumentException("required observations must be positive");
        }
        this.requiredObservations = requiredObservations;
    }

    long get() {
        return sharedMinCompetitiveDocScore.get();
    }

    int requiredObservations() {
        return requiredObservations;
    }

    Tracker newTracker() {
        return new Tracker();
    }

    /** Tracks the observations contributed by one collector. */
    final class Tracker {
        private int accountedCandidates;

        boolean hasEstablishedScore() {
            return sharedScoreEstablished;
        }

        void sync(AbstractMaxScoreKnnCollector collector) {
            int retainedCandidates = collector.numCollected();
            assert retainedCandidates >= accountedCandidates : "the number of retained candidates must not decrease during search";

            // Account for each newly occupied queue slot once. Replacements add no support.
            int newCandidates = retainedCandidates - accountedCandidates;
            accountedCandidates = retainedCandidates;

            if (retainedCandidates >= requiredObservations) {
                // This collector independently supports its bound, so no cumulative contribution is needed.
                raiseTo(collector.collectedScoreLowerBound());
                sharedScoreEstablished = true;
            } else if (newCandidates > 0) {
                contribute(newCandidates, collector.collectedScoreLowerBound());
            }
            collector.mergeMinCompetitiveDocScore(sharedMinCompetitiveDocScore.get());
        }
    }

    private void contribute(int count, long leastScore) {
        if (sharedScoreEstablished) {
            return;
        }
        // Publish the supporting score before the count so the thread crossing the threshold observes every prior minimum.
        lowerLeastSupportingScore(leastScore);
        if (supportingCandidateCount.addAndGet(count) >= requiredObservations) {
            raiseTo(leastSupportingScore.get());
            // Another thread may have established a higher score meanwhile; raiseTo never lowers it.
            sharedScoreEstablished = true;
        }
    }

    private void raiseTo(long score) {
        long current = sharedMinCompetitiveDocScore.get();
        while (score > current && sharedMinCompetitiveDocScore.compareAndSet(current, score) == false) {
            current = sharedMinCompetitiveDocScore.get();
        }
    }

    private void lowerLeastSupportingScore(long score) {
        long current = leastSupportingScore.get();
        while (score < current && leastSupportingScore.compareAndSet(current, score) == false) {
            current = leastSupportingScore.get();
        }
    }
}
