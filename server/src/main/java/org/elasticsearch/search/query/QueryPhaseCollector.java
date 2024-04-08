/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.FilterLeafCollector;
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.search.internal.TwoPhaseCollector;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Top-level collector used in the query phase to perform top hits collection as well as aggs collection.
 * Inspired by {@link org.apache.lucene.search.MultiCollector} but specialized for wrapping two collectors and filtering collected
 * documents as follows:
 * - through an optional <code>post_filter</code> that is applied to the top hits collection
 * - through an optional <code>min_score</code> threshold, which is applied to both the top hits as well as aggs.
 * Supports also terminating the collection after a certain number of documents have been collected (<code>terminate_after</code>).
 *
 * When top docs as well as aggs are collected (because both collectors were provided), skipping low scoring hits via
 * {@link Scorable#setMinCompetitiveScore(float)} is not supported for either of the collectors.
 */
public final class QueryPhaseCollector implements TwoPhaseCollector {
    private final Collector aggsCollector;
    private final Collector topDocsCollector;
    private final TerminateAfterChecker terminateAfterChecker;
    private final Weight postFilterWeight;
    private final Float minScore;
    private final boolean cacheScores;
    private boolean terminatedAfter = false;

    QueryPhaseCollector(
        Collector topDocsCollector,
        Weight postFilterWeight,
        TerminateAfterChecker terminateAfterChecker,
        Collector aggsCollector,
        Float minScore
    ) {
        this.topDocsCollector = Objects.requireNonNull(topDocsCollector);
        this.postFilterWeight = postFilterWeight;
        this.terminateAfterChecker = terminateAfterChecker;
        this.aggsCollector = aggsCollector;
        this.minScore = minScore;
        this.cacheScores = aggsCollector != null && topDocsCollector.scoreMode().needsScores() && aggsCollector.scoreMode().needsScores();
    }

    Collector getTopDocsCollector() {
        return topDocsCollector;
    }

    Collector getAggsCollector() {
        return aggsCollector;
    }

    @Override
    public void setWeight(Weight weight) {
        if (postFilterWeight == null && minScore == null) {
            // propagate the weight when we do no additional filtering over the docs that are collected
            // when post_filter or min_score are provided, the collection cannot be shortcut via Weight#count
            topDocsCollector.setWeight(weight);
        }
        if (aggsCollector != null && minScore == null) {
            // min_score is applied to aggs collection as well as top docs collection, though BucketCollectorWrapper does not override it.
            aggsCollector.setWeight(weight);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        ScoreMode scoreMode;
        if (aggsCollector == null) {
            scoreMode = topDocsCollector.scoreMode();
        } else {
            assert aggsCollector.scoreMode() != ScoreMode.TOP_SCORES : "aggs never rely on setMinCompetitiveScore";
            if (topDocsCollector.scoreMode() == aggsCollector.scoreMode()) {
                scoreMode = topDocsCollector.scoreMode();
            } else if (topDocsCollector.scoreMode().needsScores() || aggsCollector.scoreMode().needsScores()) {
                scoreMode = ScoreMode.COMPLETE;
            } else {
                scoreMode = ScoreMode.COMPLETE_NO_SCORES;
            }
            // TODO for aggs that return TOP_DOCS, score mode becomes exhaustive unless top docs collector agrees on the score mode
        }
        if (minScore != null) {
            // TODO if we had TOP_DOCS, shouldn't we return TOP_DOCS_WITH_SCORES instead of COMPLETE?
            scoreMode = scoreMode == ScoreMode.TOP_SCORES ? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE;
        }
        return scoreMode;
    }

    /**
     * @return whether the collection was terminated based on the provided <code>terminate_after</code> value
     */
    boolean isTerminatedAfter() {
        return terminatedAfter;
    }

    private boolean shouldCollectTopDocs(int doc, Scorable scorer, Bits postFilterBits) throws IOException {
        return isDocWithinMinScore(scorer) && (postFilterBits == null || postFilterBits.get(doc));
    }

    private boolean isDocWithinMinScore(Scorable scorer) throws IOException {
        return minScore == null || scorer.score() >= minScore;
    }

    private void earlyTerminate() {
        terminatedAfter = true;
        throw new CollectionTerminatedException();
    }

    private Bits getPostFilterBits(LeafReaderContext context) throws IOException {
        if (postFilterWeight == null) {
            return null;
        }
        final ScorerSupplier filterScorerSupplier = postFilterWeight.scorerSupplier(context);
        return Lucene.asSequentialAccessBits(context.reader().maxDoc(), filterScorerSupplier);
    }

    @Override
    public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
        if (terminateAfterChecker.isThresholdReached()) {
            earlyTerminate();
        }
        Bits postFilterBits = getPostFilterBits(context);

        if (aggsCollector == null) {
            final LeafCollector topDocsLeafCollector = topDocsCollector.getLeafCollector(context);
            if (postFilterBits == null && terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER && minScore == null) {
                // no need to wrap if we just need to collect unfiltered docs through leaf collector.
                // aggs collector was not originally provided so the overall score mode is that of the top docs collector
                return topDocsLeafCollector;
            }
            return new TopDocsLeafCollector(postFilterBits, topDocsLeafCollector);
        }

        LeafCollector tdlc = null;
        try {
            tdlc = topDocsCollector.getLeafCollector(context);
        } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
            // top docs collector does not need this segment, but the aggs collector does.
        }
        final LeafCollector topDocsLeafCollector = tdlc;

        LeafCollector alf = null;
        try {
            alf = aggsCollector.getLeafCollector(context);
        } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
            // aggs collector does not need this segment, but the top docs collector may.
            if (topDocsLeafCollector == null) {
                throw e;
            }
        }
        final LeafCollector aggsLeafCollector = alf;

        if (topDocsLeafCollector == null && minScore == null) {
            // top docs collector early terminated, we can avoid wrapping as long as we don't need to apply min_score.
            // post_filter and terminate_after do not matter because they not applied to aggs collection anyways.
            // aggs don't support skipping low scoring hits, so we can rely on setMinCompetitiveScore being a no-op already.
            return aggsLeafCollector;
        }

        // if that the aggs collector early terminates while the top docs collector does not, we still need to wrap the leaf collector
        // to enforce that setMinCompetitiveScore is a no-op. Otherwise we may allow the top docs collector to skip non competitive
        // hits despite the score mode of the Collector did not allow it (because aggs don't support TOP_SCORES).
        if (aggsLeafCollector == null
            && postFilterBits == null
            && terminateAfterChecker == NO_OP_TERMINATE_AFTER_CHECKER
            && minScore == null) {
            // special case for early terminated aggs
            return new FilterLeafCollector(topDocsLeafCollector) {
                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(new FilterScorable(scorer) {
                        @Override
                        public void setMinCompetitiveScore(float minScore) {
                            // Ignore calls to setMinCompetitiveScore. The top docs collector may try to skip low
                            // scoring hits, but the overall score_mode won't allow it because an aggs collector
                            // was originally provided which never supports TOP_SCORES is not supported for aggs
                        }
                    });
                }

                @Override
                public DocIdSetIterator competitiveIterator() throws IOException {
                    return topDocsLeafCollector.competitiveIterator();
                }
            };
        }
        return new CompositeLeafCollector(postFilterBits, topDocsLeafCollector, aggsLeafCollector);
    }

    private class TopDocsLeafCollector implements LeafCollector {
        private final Bits postFilterBits;
        private final LeafCollector topDocsLeafCollector;
        private Scorable scorer;

        TopDocsLeafCollector(Bits postFilterBits, LeafCollector topDocsLeafCollector) {
            assert topDocsLeafCollector != null;
            assert postFilterBits != null || terminateAfterChecker != NO_OP_TERMINATE_AFTER_CHECKER || minScore != null;
            this.postFilterBits = postFilterBits;
            this.topDocsLeafCollector = topDocsLeafCollector;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            topDocsLeafCollector.setScorer(scorer);
            this.scorer = scorer;
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            return topDocsLeafCollector.competitiveIterator();
        }

        @Override
        public void collect(int doc) throws IOException {
            if (shouldCollectTopDocs(doc, scorer, postFilterBits)) {
                // terminate_after is purposely applied after post_filter, and terminates aggs collection based on number of filtered
                // top hits that have been collected. Strange feature, but that has been behaviour for a long time.
                if (terminateAfterChecker.incrementHitCountAndCheckThreshold()) {
                    earlyTerminate();
                }
                topDocsLeafCollector.collect(doc);
            }
        }
    }

    private class CompositeLeafCollector implements LeafCollector {
        private final Bits postFilterBits;
        private LeafCollector topDocsLeafCollector;
        private LeafCollector aggsLeafCollector;
        private Scorable scorer;

        CompositeLeafCollector(Bits postFilterBits, LeafCollector topDocsLeafCollector, LeafCollector aggsLeafCollector) {
            assert topDocsLeafCollector != null || aggsLeafCollector != null;
            this.postFilterBits = postFilterBits;
            this.topDocsLeafCollector = topDocsLeafCollector;
            this.aggsLeafCollector = aggsLeafCollector;
        }

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            if (cacheScores && topDocsLeafCollector != null && aggsLeafCollector != null) {
                scorer = ScoreCachingWrappingScorer.wrap(scorer);
            }
            scorer = new FilterScorable(scorer) {
                @Override
                public void setMinCompetitiveScore(float minScore) {
                    // Ignore calls to setMinCompetitiveScore so that if the top docs collector
                    // wants to skip low-scoring hits, the aggs collector still sees all hits.
                    // this is important also for terminate_after in case used when total hits tracking is early terminated.
                }
            };
            if (topDocsLeafCollector != null) {
                topDocsLeafCollector.setScorer(scorer);
            }
            if (aggsLeafCollector != null) {
                aggsLeafCollector.setScorer(scorer);
            }
            this.scorer = scorer;
        }

        @Override
        public void collect(int doc) throws IOException {
            if (shouldCollectTopDocs(doc, scorer, postFilterBits)) {
                // we keep on counting and checking the terminate_after threshold so that we can terminate aggs collection
                // even if top docs collection early terminated
                if (terminateAfterChecker.incrementHitCountAndCheckThreshold()) {
                    earlyTerminate();
                }
                if (topDocsLeafCollector != null) {
                    try {
                        topDocsLeafCollector.collect(doc);
                    } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                        topDocsLeafCollector = null;
                        // top docs collector does not need this segment, but the aggs collector may.
                        if (aggsLeafCollector == null) {
                            throw e;
                        }
                    }
                }
            }
            if (aggsLeafCollector != null) {
                // min_score is applied to aggs as well as top hits
                if (isDocWithinMinScore(scorer)) {
                    try {
                        aggsLeafCollector.collect(doc);
                    } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                        aggsLeafCollector = null;
                        // aggs collector does not need this segment, but the top docs collector may.
                        if (topDocsLeafCollector == null) {
                            throw e;
                        }
                    }
                }
            }
        }

        @Override
        public DocIdSetIterator competitiveIterator() throws IOException {
            // TODO we expose the competitive iterator only when one of the two sub-leaf collectors has early terminated,
            // it could be a good idea to expose a disjunction of the two when both are not null
            if (topDocsLeafCollector == null) {
                return aggsLeafCollector.competitiveIterator();
            }
            if (aggsLeafCollector == null) {
                return topDocsLeafCollector.competitiveIterator();
            }
            return null;
        }
    }

    static TerminateAfterChecker resolveTerminateAfterChecker(int terminateAfter) {
        if (terminateAfter < 0) {
            throw new IllegalArgumentException("terminateAfter must be greater than or equal to 0");
        }
        return terminateAfter == 0 ? NO_OP_TERMINATE_AFTER_CHECKER : new GlobalTerminateAfterChecker(terminateAfter);
    }

    abstract static class TerminateAfterChecker {
        abstract boolean isThresholdReached();

        abstract boolean incrementHitCountAndCheckThreshold();
    }

    private static final class GlobalTerminateAfterChecker extends TerminateAfterChecker {
        private final int terminateAfter;
        private final AtomicInteger numCollected = new AtomicInteger();

        GlobalTerminateAfterChecker(int terminateAfter) {
            assert terminateAfter > 0;
            this.terminateAfter = terminateAfter;
        }

        boolean isThresholdReached() {
            return numCollected.getAcquire() >= terminateAfter;
        }

        boolean incrementHitCountAndCheckThreshold() {
            return numCollected.incrementAndGet() > terminateAfter;
        }
    }

    // no needless counting when terminate_after is not set
    private static final TerminateAfterChecker NO_OP_TERMINATE_AFTER_CHECKER = new TerminateAfterChecker() {
        @Override
        boolean isThresholdReached() {
            return false;
        }

        @Override
        boolean incrementHitCountAndCheckThreshold() {
            return false;
        }
    };

    @Override
    public void doPostCollection() throws IOException {
        if (aggsCollector instanceof TwoPhaseCollector twoPhaseCollector) {
            twoPhaseCollector.doPostCollection();
        }
    }
}
