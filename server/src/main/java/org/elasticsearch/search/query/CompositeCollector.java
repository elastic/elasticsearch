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
import org.apache.lucene.search.FilterScorable;
import org.apache.lucene.search.LeafCollector;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreCachingWrappingScorer;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.Bits;
import org.elasticsearch.common.lucene.Lucene;

import java.io.IOException;

public class CompositeCollector implements Collector {
    private final Collector aggsCollector;
    private final Collector topDocsCollector;
    private final int terminateAfter;
    private final Weight postFilterWeight;
    private final Float minScore;
    private final boolean cacheScores;

    private int numCollected;
    private boolean terminatedAfter = false;

    public CompositeCollector(
        Collector topDocsCollector,
        Weight postFilterWeight,
        int terminateAfter,
        Collector aggsCollector,
        Float minScore
    ) {
        this.topDocsCollector = topDocsCollector;
        this.postFilterWeight = postFilterWeight;
        this.terminateAfter = terminateAfter;
        this.aggsCollector = aggsCollector;
        this.minScore = minScore;
        if (aggsCollector == null) {
            // TODO do we still need to cache scores now that min score is no longer in a separate collector?
            this.cacheScores = minScore != null;
        } else {
            this.cacheScores = (topDocsCollector.scoreMode().needsScores() && aggsCollector.scoreMode().needsScores()) || minScore != null;
        }
    }

    public boolean isTerminatedAfter() {
        return terminatedAfter;
    }

    private boolean shouldCollectTopDocs(int doc, Scorable scorer, Bits postFilterBits) throws IOException {
        if (minScore == null || scorer.score() >= minScore) {
            if (postFilterBits == null || postFilterBits.get(doc)) {
                if (terminateAfter > 0 && ++numCollected > terminateAfter) {
                    terminatedAfter = true;
                    throw new CollectionTerminatedException();
                }
                return true;
            }
        }
        return false;
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
        if (terminateAfter > 0 && numCollected >= terminateAfter) {
            terminatedAfter = true;
            throw new CollectionTerminatedException();
        }
        Bits postFilterBits = getPostFilterBits(context);

        if (aggsCollector == null) {
            LeafCollector leafCollector;
            try {
                leafCollector = topDocsCollector.getLeafCollector(context);
            } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf collector).
                // The reason is only to set the early terminated flag to the QueryResult like some tests expect. This needs fixing.
                if (terminateAfter == 0) {
                    throw e;
                }
                leafCollector = null;
            }
            final LeafCollector topDocsLeafCollector = leafCollector;
            return new LeafCollector() {
                LeafCollector tdlc = topDocsLeafCollector;
                Scorable scorer;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    scorer = new FilterScorable(scorer) {
                        @Override
                        public void setMinCompetitiveScore(float minScore) throws IOException {
                            if (terminateAfter == 0) {
                                // Ignore calls to setMinCompetitiveScore when terminate_after is used, otherwise early termination
                                // of total hits tracking makes it impossible to terminate after.
                                // TODO the reason is only to set the early terminated flag to the QueryResult like some tests expect.
                                in.setMinCompetitiveScore(minScore);
                            }
                        }
                    };
                    if (tdlc != null) {
                        tdlc.setScorer(scorer);
                    }
                    this.scorer = scorer;
                }

                @Override
                public DocIdSetIterator competitiveIterator() throws IOException {
                    if (tdlc != null) {
                        return tdlc.competitiveIterator();
                    }
                    return null;
                }

                @Override
                public void collect(int doc) throws IOException {
                    if (shouldCollectTopDocs(doc, scorer, postFilterBits)) {
                        if (tdlc != null) {
                            try {
                                tdlc.collect(doc);
                            } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                                // TODO we keep on collecting although we have nothing to collect (there is no top docs nor aggs leaf
                                // collector).
                                // The reason is only to set the early terminated flag to the QueryResult like some tests expect.
                                if (terminateAfter == 0) {
                                    throw e;
                                }
                                tdlc = null;
                            }
                        }
                    }
                }
            };
        }

        LeafCollector leafCollector;
        try {
            leafCollector = topDocsCollector.getLeafCollector(context);
        } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
            // top docs collector does not need this segment, but the aggs collector does.
            leafCollector = null;
        }

        final LeafCollector topDocsLeafCollector = leafCollector;
        final LeafCollector aggsLeafCollector = aggsCollector.getLeafCollector(context);
        return new LeafCollector() {
            LeafCollector tdlc = topDocsLeafCollector;
            Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                if (cacheScores) {
                    scorer = ScoreCachingWrappingScorer.wrap(scorer);
                }
                scorer = new FilterScorable(scorer) {
                    // TODO aggs can also skip non competitive hits
                    @Override
                    public void setMinCompetitiveScore(float minScore) {
                        // Ignore calls to setMinCompetitiveScore so that if the top docs collector
                        // wants to skip low-scoring hits, the aggs collector still sees all hits.
                        // this is important also for terminate_after in case used when total hits tracking is early terminated.
                    }
                };
                if (tdlc != null) {
                    tdlc.setScorer(scorer);
                }
                aggsLeafCollector.setScorer(scorer);
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc) throws IOException {
                if (shouldCollectTopDocs(doc, scorer, postFilterBits)) {
                    if (tdlc != null) {
                        try {
                            tdlc.collect(doc);
                        } catch (@SuppressWarnings("unused") CollectionTerminatedException e) {
                            // top docs collector does not need this segment, but the aggs collector does.
                            // Don't collect further from top docs, but keep on counting so aggs can be terminated after.
                            tdlc = null;
                        }
                    }
                }
                if (minScore == null || scorer.score() >= minScore) {
                    aggsLeafCollector.collect(doc);
                }
            }

            @Override
            public DocIdSetIterator competitiveIterator() throws IOException {
                if (tdlc == null) {
                    return aggsLeafCollector.competitiveIterator();
                }
                // TODO MultiLeafCollector does not override this, should we return a disjunction of the two?
                return LeafCollector.super.competitiveIterator();
            }
        };
    }

    @Override
    public void setWeight(Weight weight) {
        if (postFilterWeight == null && minScore == null) {
            // propagate the weight when we do no additional filtering over the docs that are collected
            topDocsCollector.setWeight(weight);
        }
        if (aggsCollector != null && minScore == null) {
            // min_score is applied to aggs collection as well as top docs collection
            aggsCollector.setWeight(weight);
        }
    }

    @Override
    public ScoreMode scoreMode() {
        ScoreMode scoreMode;
        if (aggsCollector == null) {
            scoreMode = topDocsCollector.scoreMode();
        } else if (topDocsCollector.scoreMode() == aggsCollector.scoreMode()) {
            scoreMode = topDocsCollector.scoreMode();
        } else if (topDocsCollector.scoreMode().needsScores() || aggsCollector.scoreMode().needsScores()) {
            scoreMode = ScoreMode.COMPLETE;
        } else {
            scoreMode = ScoreMode.COMPLETE_NO_SCORES;
        }
        // TODO should we also look at isExhaustive? MultiCollector does not.
        if (minScore != null) {
            scoreMode = scoreMode == ScoreMode.TOP_SCORES ? ScoreMode.TOP_SCORES : ScoreMode.COMPLETE;
        }
        return scoreMode;
    }
}
