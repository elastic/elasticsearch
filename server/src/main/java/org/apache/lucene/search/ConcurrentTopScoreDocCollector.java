/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.apache.lucene.search;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.MaxScoreAccumulator.DocAndScore;

import java.io.IOException;
import java.util.Collection;

/**
 * This is a modified version of {@link TopScoreDocCollector} that doesn't keep any leaf-specific state in
 * the collector and therefore its leaf collectors can be used concurrently.
 *
 * TODO: Contribute back to Lucene
 *
 * A {@link Collector} implementation that collects the top-scoring hits, returning them as a {@link
 * TopDocs}. This is used by {@link IndexSearcher} to implement {@link TopDocs}-based search. Hits
 * are sorted by score descending and then (when the scores are tied) docID ascending. When you
 * create an instance of this collector you should know in advance whether documents are going to be
 * collected in doc Id order or not.
 *
 * <p><b>NOTE</b>: The values {@link Float#NaN} and {@link Float#NEGATIVE_INFINITY} are not valid
 * scores. This collector will not properly collect hits with such scores.
 */
public abstract class ConcurrentTopScoreDocCollector extends TopDocsCollector<ScoreDoc> {

    /** Scorable leaf collector */
    public abstract static class ScorerLeafCollector implements LeafCollector {

        protected Scorable scorer;

        @Override
        public void setScorer(Scorable scorer) throws IOException {
            this.scorer = scorer;
        }
    }

    private static class SimpleTopScoreDocCollector extends ConcurrentTopScoreDocCollector {

        SimpleTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
            super(numHits, hitsThresholdChecker, minScoreAcc);
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            // reset the minimum competitive score

            return new ScorerLeafCollector() {
                final int docBase = context.docBase;
                float minCompetitiveScore = 0f;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    if (minScoreAcc == null) {
                        minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                    } else {
                        minCompetitiveScore = updateGlobalMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                    }
                }

                @Override
                public void collect(int doc) throws IOException {
                    float score = scorer.score();

                    // This collector relies on the fact that scorers produce positive values:
                    assert score >= 0; // NOTE: false for NaN

                    totalHits++;
                    hitsThresholdChecker.incrementHitCount();

                    if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
                        minCompetitiveScore = updateGlobalMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                    }

                    if (score <= pqTop.score) {
                        if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                            // we just reached totalHitsThreshold, we can start setting the min
                            // competitive score now
                            minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                        }
                        // Since docs are returned in-order (i.e., increasing doc Id), a document
                        // with equal score to pqTop.score cannot compete since HitQueue favors
                        // documents with lower doc Ids. Therefore reject those docs too.
                        return;
                    }
                    pqTop.doc = doc + docBase;
                    pqTop.score = score;
                    pqTop = pq.updateTop();
                    minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                }
            };
        }
    }

    private static class PagingTopScoreDocCollector extends ConcurrentTopScoreDocCollector {

        private final ScoreDoc after;
        private int collectedHits;

        PagingTopScoreDocCollector(
            int numHits,
            ScoreDoc after,
            HitsThresholdChecker hitsThresholdChecker,
            MaxScoreAccumulator minScoreAcc
        ) {
            super(numHits, hitsThresholdChecker, minScoreAcc);
            this.after = after;
            this.collectedHits = 0;
        }

        @Override
        protected int topDocsSize() {
            return collectedHits < pq.size() ? collectedHits : pq.size();
        }

        @Override
        protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
            return results == null
                ? new TopDocs(new TotalHits(totalHits, totalHitsRelation), new ScoreDoc[0])
                : new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
        }

        @Override
        public LeafCollector getLeafCollector(LeafReaderContext context) throws IOException {
            final int afterDoc = after.doc - context.docBase;

            return new ScorerLeafCollector() {
                final int docBase = context.docBase;
                float minCompetitiveScore = 0f;

                @Override
                public void setScorer(Scorable scorer) throws IOException {
                    super.setScorer(scorer);
                    if (minScoreAcc == null) {
                        minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                    } else {
                        minCompetitiveScore = updateGlobalMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                    }
                }

                @Override
                public void collect(int doc) throws IOException {
                    float score = scorer.score();

                    // This collector relies on the fact that scorers produce positive values:
                    assert score >= 0; // NOTE: false for NaN

                    totalHits++;
                    hitsThresholdChecker.incrementHitCount();

                    if (minScoreAcc != null && (totalHits & minScoreAcc.modInterval) == 0) {
                        minCompetitiveScore = updateGlobalMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                    }

                    if (score > after.score || (score == after.score && doc <= afterDoc)) {
                        // hit was collected on a previous page
                        if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                            // we just reached totalHitsThreshold, we can start setting the min
                            // competitive score now
                            minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                        }
                        return;
                    }

                    if (score <= pqTop.score) {
                        if (totalHitsRelation == TotalHits.Relation.EQUAL_TO) {
                            // we just reached totalHitsThreshold, we can start setting the min
                            // competitive score now
                            minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                        }

                        // Since docs are returned in-order (i.e., increasing doc Id), a document
                        // with equal score to pqTop.score cannot compete since HitQueue favors
                        // documents with lower doc Ids. Therefore reject those docs too.
                        return;
                    }
                    collectedHits++;
                    pqTop.doc = doc + docBase;
                    pqTop.score = score;
                    pqTop = pq.updateTop();
                    minCompetitiveScore = updateMinCompetitiveScore(docBase, minCompetitiveScore, scorer);
                }
            };
        }
    }

    /**
     * Creates a new {@link ConcurrentTopScoreDocCollector} given the number of hits to collect and the number
     * of hits to count accurately.
     *
     * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
     * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
     * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
     * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
     * but will also likely make query processing slower.
     *
     * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
     * <code>numHits</code>, and fill the array with sentinel objects.
     */
    public static ConcurrentTopScoreDocCollector create(int numHits, int totalHitsThreshold) {
        return create(numHits, null, totalHitsThreshold);
    }

    /**
     * Creates a new {@link ConcurrentTopScoreDocCollector} given the number of hits to collect, the bottom of
     * the previous page, and the number of hits to count accurately.
     *
     * <p><b>NOTE</b>: If the total hit count of the top docs is less than or exactly {@code
     * totalHitsThreshold} then this value is accurate. On the other hand, if the {@link
     * TopDocs#totalHits} value is greater than {@code totalHitsThreshold} then its value is a lower
     * bound of the hit count. A value of {@link Integer#MAX_VALUE} will make the hit count accurate
     * but will also likely make query processing slower.
     *
     * <p><b>NOTE</b>: The instances returned by this method pre-allocate a full array of length
     * <code>numHits</code>, and fill the array with sentinel objects.
     */
    public static ConcurrentTopScoreDocCollector create(int numHits, ScoreDoc after, int totalHitsThreshold) {
        return create(numHits, after, HitsThresholdChecker.create(Math.max(totalHitsThreshold, numHits)), null);
    }

    static ConcurrentTopScoreDocCollector create(
        int numHits,
        ScoreDoc after,
        HitsThresholdChecker hitsThresholdChecker,
        MaxScoreAccumulator minScoreAcc
    ) {

        if (numHits <= 0) {
            throw new IllegalArgumentException(
                "numHits must be > 0; please use TotalHitCountCollector if you just need the total hit count"
            );
        }

        if (hitsThresholdChecker == null) {
            throw new IllegalArgumentException("hitsThresholdChecker must be non null");
        }

        if (after == null) {
            return new SimpleTopScoreDocCollector(numHits, hitsThresholdChecker, minScoreAcc);
        } else {
            return new PagingTopScoreDocCollector(numHits, after, hitsThresholdChecker, minScoreAcc);
        }
    }

    /**
     * Create a CollectorManager which uses a shared hit counter to maintain number of hits and a
     * shared {@link MaxScoreAccumulator} to propagate the minimum score accross segments
     */
    public static CollectorManager<ConcurrentTopScoreDocCollector, TopDocs> createSharedManager(
        int numHits,
        ScoreDoc after,
        int totalHitsThreshold
    ) {
        return new CollectorManager<>() {

            private final HitsThresholdChecker hitsThresholdChecker = HitsThresholdChecker.createShared(
                Math.max(totalHitsThreshold, numHits)
            );
            private final MaxScoreAccumulator minScoreAcc = new MaxScoreAccumulator();

            @Override
            public ConcurrentTopScoreDocCollector newCollector() throws IOException {
                return ConcurrentTopScoreDocCollector.create(numHits, after, hitsThresholdChecker, minScoreAcc);
            }

            @Override
            public TopDocs reduce(Collection<ConcurrentTopScoreDocCollector> collectors) throws IOException {
                final TopDocs[] topDocs = new TopDocs[collectors.size()];
                int i = 0;
                for (ConcurrentTopScoreDocCollector collector : collectors) {
                    topDocs[i++] = collector.topDocs();
                }
                return TopDocs.merge(0, numHits, topDocs);
            }
        };
    }

    ScoreDoc pqTop;
    final HitsThresholdChecker hitsThresholdChecker;
    final MaxScoreAccumulator minScoreAcc;

    // prevents instantiation
    ConcurrentTopScoreDocCollector(int numHits, HitsThresholdChecker hitsThresholdChecker, MaxScoreAccumulator minScoreAcc) {
        super(new HitQueue(numHits, true));
        assert hitsThresholdChecker != null;

        // HitQueue implements getSentinelObject to return a ScoreDoc, so we know
        // that at this point top() is already initialized.
        pqTop = pq.top();
        this.hitsThresholdChecker = hitsThresholdChecker;
        this.minScoreAcc = minScoreAcc;
    }

    @Override
    protected TopDocs newTopDocs(ScoreDoc[] results, int start) {
        if (results == null) {
            return EMPTY_TOPDOCS;
        }

        return new TopDocs(new TotalHits(totalHits, totalHitsRelation), results);
    }

    @Override
    public ScoreMode scoreMode() {
        return hitsThresholdChecker.scoreMode();
    }

    protected float updateGlobalMinCompetitiveScore(int docBase, float minCompetitiveScore, Scorable scorer) throws IOException {
        assert minScoreAcc != null;
        DocAndScore maxMinScore = minScoreAcc.get();
        if (maxMinScore != null) {
            // since we tie-break on doc id and collect in doc id order we can require
            // the next float if the global minimum score is set on a document id that is
            // smaller than the ids in the current leaf
            float score = docBase >= maxMinScore.docBase ? Math.nextUp(maxMinScore.score) : maxMinScore.score;
            if (score > minCompetitiveScore) {
                assert hitsThresholdChecker.isThresholdReached();
                scorer.setMinCompetitiveScore(score);
                minCompetitiveScore = score;
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
            }
        }
        return minCompetitiveScore;
    }

    protected float updateMinCompetitiveScore(int docBase, float minCompetitiveScore, Scorable scorer) throws IOException {
        if (hitsThresholdChecker.isThresholdReached() && pqTop != null && pqTop.score != Float.NEGATIVE_INFINITY) { // -Infinity is the
                                                                                                                    // score of sentinels
            // since we tie-break on doc id and collect in doc id order, we can require
            // the next float
            float localMinScore = Math.nextUp(pqTop.score);
            if (localMinScore > minCompetitiveScore) {
                scorer.setMinCompetitiveScore(localMinScore);
                totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                minCompetitiveScore = localMinScore;
                if (minScoreAcc != null) {
                    // we don't use the next float but we register the document
                    // id so that other leaves can require it if they are after
                    // the current maximum
                    minScoreAcc.accumulate(docBase, pqTop.score);
                }
            }
        }
        return minCompetitiveScore;
    }
}
