/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopDocsCollector;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollector;
import org.apache.lucene.search.TotalHitCountCollector;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.MaxScoreCollector;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollector;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;

import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_COUNT;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_TOP_HITS;

/**
 * A {@link QueryCollectorContext} that creates top docs collector
 */
abstract class TopDocsCollectorContext extends QueryCollectorContext {
    protected final int numHits;

    TopDocsCollectorContext(String profilerName, int numHits) {
        super(profilerName);
        this.numHits = numHits;
    }

    /**
     * Returns true if the top docs should be re-scored after initial search
     */
    boolean shouldRescore() {
        return false;
    }

    static class EmptyTopDocsCollectorContext extends TopDocsCollectorContext {
        private final Sort sort;
        private final Collector collector;
        private final Supplier<TotalHits> hitCountSupplier;

        private EmptyTopDocsCollectorContext(@Nullable SortAndFormats sortAndFormats, int trackTotalHitsUpTo) {
            super(REASON_SEARCH_COUNT, 0);
            this.sort = sortAndFormats == null ? null : sortAndFormats.sort;
            if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                this.collector = new EarlyTerminatingCollector(new TotalHitCountCollector(), 0, false);
                // for bwc hit count is set to 0, it will be converted to -1 by the coordinating node
                this.hitCountSupplier = () -> new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            } else {
                TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
                // implicit total hit counts are valid only when there is no filter collector in the chain
                if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                    this.collector = hitCountCollector;
                    this.hitCountSupplier = () -> new TotalHits(hitCountCollector.getTotalHits(), TotalHits.Relation.EQUAL_TO);
                } else {
                    EarlyTerminatingCollector col = new EarlyTerminatingCollector(hitCountCollector, trackTotalHitsUpTo, false);
                    this.collector = col;
                    this.hitCountSupplier = () -> new TotalHits(
                        hitCountCollector.getTotalHits(),
                        col.hasEarlyTerminated() ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO
                    );
                }
            }
        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        @Override
        void postProcess(QuerySearchResult result) {
            final TotalHits totalHitCount = hitCountSupplier.get();
            final TopDocs topDocs;
            if (sort != null) {
                topDocs = new TopFieldDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS, sort.getSort());
            } else {
                topDocs = new TopDocs(totalHitCount, Lucene.EMPTY_SCORE_DOCS);
            }
            result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), null);
        }
    }

    static class CollapsingTopDocsCollectorContext extends TopDocsCollectorContext {
        private final DocValueFormat[] sortFmt;
        private final SinglePassGroupingCollector<?> topDocsCollector;
        private final Supplier<Float> maxScoreSupplier;

        /**
         * Ctr
         * @param collapseContext The collapsing context
         * @param sortAndFormats The query sort
         * @param numHits The number of collapsed top hits to retrieve.
         * @param trackMaxScore True if max score should be tracked
         */
        private CollapsingTopDocsCollectorContext(
            CollapseContext collapseContext,
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            boolean trackMaxScore,
            @Nullable FieldDoc after
        ) {
            super(REASON_SEARCH_TOP_HITS, numHits);
            assert numHits > 0;
            assert collapseContext != null;
            Sort sort = sortAndFormats == null ? Sort.RELEVANCE : sortAndFormats.sort;
            this.sortFmt = sortAndFormats == null ? new DocValueFormat[] { DocValueFormat.RAW } : sortAndFormats.formats;
            this.topDocsCollector = collapseContext.createTopDocs(sort, numHits, after);

            MaxScoreCollector maxScoreCollector;
            if (trackMaxScore) {
                maxScoreCollector = new MaxScoreCollector();
                maxScoreSupplier = maxScoreCollector::getMaxScore;
            } else {
                maxScoreSupplier = () -> Float.NaN;
            }
        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return topDocsCollector;
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
            TopFieldGroups topDocs = topDocsCollector.getTopGroups(0);
            result.topDocs(new TopDocsAndMaxScore(topDocs, maxScoreSupplier.get()), sortFmt);
        }
    }

    abstract static class SimpleTopDocsCollectorContext extends TopDocsCollectorContext {

        private static TopDocsCollector<?> createCollector(
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            @Nullable ScoreDoc searchAfter,
            int hitCountThreshold
        ) {
            if (sortAndFormats == null) {
                return TopScoreDocCollector.create(numHits, searchAfter, hitCountThreshold);
            } else {
                return TopFieldCollector.create(sortAndFormats.sort, numHits, (FieldDoc) searchAfter, hitCountThreshold);
            }
        }

        protected final @Nullable SortAndFormats sortAndFormats;
        private final Collector collector;
        private final Supplier<TotalHits> totalHitsSupplier;
        private final Supplier<TopDocs> topDocsSupplier;
        private final Supplier<Float> maxScoreSupplier;

        /**
         * Ctr
         * @param query The Lucene query
         * @param sortAndFormats The query sort
         * @param numHits The number of top hits to retrieve
         * @param searchAfter The doc this request should "search after"
         * @param trackMaxScore True if max score should be tracked
         * @param trackTotalHitsUpTo True if the total number of hits should be tracked
         */
        private SimpleTopDocsCollectorContext(
            Query query,
            @Nullable SortAndFormats sortAndFormats,
            @Nullable ScoreDoc searchAfter,
            int numHits,
            boolean trackMaxScore,
            int trackTotalHitsUpTo
        ) {
            super(REASON_SEARCH_TOP_HITS, numHits);
            this.sortAndFormats = sortAndFormats;

            final TopDocsCollector<?> topDocsCollector;

            if ((sortAndFormats == null || SortField.FIELD_SCORE.equals(sortAndFormats.sort.getSort()[0])) && hasInfMaxScore(query)) {
                // disable max score optimization since we have a mandatory clause
                // that doesn't track the maximum score
                topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, Integer.MAX_VALUE);
                topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                totalHitsSupplier = () -> topDocsSupplier.get().totalHits;
            } else if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                // don't compute hit counts via the collector
                topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, 1);
                topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                totalHitsSupplier = () -> new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            } else {
                topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, trackTotalHitsUpTo);
                topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                totalHitsSupplier = () -> topDocsSupplier.get().totalHits;
            }
            MaxScoreCollector maxScoreCollector = null;
            if (sortAndFormats == null) {
                maxScoreSupplier = () -> {
                    TopDocs topDocs = topDocsSupplier.get();
                    if (topDocs.scoreDocs.length == 0) {
                        return Float.NaN;
                    } else {
                        return topDocs.scoreDocs[0].score;
                    }
                };
            } else if (trackMaxScore) {
                maxScoreCollector = new MaxScoreCollector();
                maxScoreSupplier = maxScoreCollector::getMaxScore;
            } else {
                maxScoreSupplier = () -> Float.NaN;
            }

            this.collector = MultiCollector.wrap(topDocsCollector, maxScoreCollector);

        }

        @Override
        Collector create(Collector in) {
            assert in == null;
            return collector;
        }

        TopDocsAndMaxScore newTopDocs() {
            TopDocs in = topDocsSupplier.get();
            float maxScore = maxScoreSupplier.get();
            final TopDocs newTopDocs;
            if (in instanceof TopFieldDocs fieldDocs) {
                newTopDocs = new TopFieldDocs(totalHitsSupplier.get(), fieldDocs.scoreDocs, fieldDocs.fields);
            } else {
                newTopDocs = new TopDocs(totalHitsSupplier.get(), in.scoreDocs);
            }
            return new TopDocsAndMaxScore(newTopDocs, maxScore);
        }

        @Override
        void postProcess(QuerySearchResult result) {
            final TopDocsAndMaxScore topDocs = newTopDocs();
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    static class ScrollingTopDocsCollectorContext extends SimpleTopDocsCollectorContext {
        private final ScrollContext scrollContext;
        private final int numberOfShards;

        private ScrollingTopDocsCollectorContext(
            Query query,
            ScrollContext scrollContext,
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            boolean trackMaxScore,
            int numberOfShards,
            int trackTotalHitsUpTo
        ) {
            super(query, sortAndFormats, scrollContext.lastEmittedDoc, numHits, trackMaxScore, trackTotalHitsUpTo);
            this.scrollContext = Objects.requireNonNull(scrollContext);
            this.numberOfShards = numberOfShards;
        }

        @Override
        void postProcess(QuerySearchResult result) {
            final TopDocsAndMaxScore topDocs = newTopDocs();
            if (scrollContext.totalHits == null) {
                // first round
                scrollContext.totalHits = topDocs.topDocs.totalHits;
                scrollContext.maxScore = topDocs.maxScore;
            } else {
                // subsequent round: the total number of hits and
                // the maximum score were computed on the first round
                topDocs.topDocs.totalHits = scrollContext.totalHits;
                topDocs.maxScore = scrollContext.maxScore;
            }
            if (numberOfShards == 1) {
                // if we fetch the document in the same roundtrip, we already know the last emitted doc
                if (topDocs.topDocs.scoreDocs.length > 0) {
                    // set the last emitted doc
                    scrollContext.lastEmittedDoc = topDocs.topDocs.scoreDocs[topDocs.topDocs.scoreDocs.length - 1];
                }
            }
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    /**
     * Creates a {@link TopDocsCollectorContext} from the provided <code>searchContext</code>.
     */
    static TopDocsCollectorContext createTopDocsCollectorContext(SearchContext searchContext) {
        if (searchContext.size() == 0) {
            // no matter what the value of from is
            return new EmptyTopDocsCollectorContext(searchContext.sort(), searchContext.trackTotalHitsUpTo());
        } else {
            final IndexReader reader = searchContext.searcher().getIndexReader();
            final Query query = searchContext.rewrittenQuery();
            // top collectors don't like a size of 0
            final int totalNumDocs = Math.max(1, reader.numDocs());
            if (searchContext.scrollContext() != null) {
                // we can disable the tracking of total hits after the initial scroll query
                // since the total hits is preserved in the scroll context.
                int trackTotalHitsUpTo = searchContext.scrollContext().totalHits != null
                    ? SearchContext.TRACK_TOTAL_HITS_DISABLED
                    : SearchContext.TRACK_TOTAL_HITS_ACCURATE;
                // no matter what the value of from is
                int numDocs = Math.min(searchContext.size(), totalNumDocs);
                return new ScrollingTopDocsCollectorContext(
                    query,
                    searchContext.scrollContext(),
                    searchContext.sort(),
                    numDocs,
                    searchContext.trackScores(),
                    searchContext.numberOfShards(),
                    trackTotalHitsUpTo
                );
            } else if (searchContext.collapse() != null) {
                boolean trackScores = searchContext.sort() == null ? true : searchContext.trackScores();
                int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
                return new CollapsingTopDocsCollectorContext(
                    searchContext.collapse(),
                    searchContext.sort(),
                    numDocs,
                    trackScores,
                    searchContext.searchAfter()
                );
            } else {
                int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
                final boolean rescore = searchContext.rescore().isEmpty() == false;
                if (rescore) {
                    assert searchContext.sort() == null;
                    for (RescoreContext rescoreContext : searchContext.rescore()) {
                        numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
                    }
                }
                return new SimpleTopDocsCollectorContext(
                    query,
                    searchContext.sort(),
                    searchContext.searchAfter(),
                    numDocs,
                    searchContext.trackScores(),
                    searchContext.trackTotalHitsUpTo()
                ) {
                    @Override
                    boolean shouldRescore() {
                        return rescore;
                    }
                };
            }
        }
    }

    /**
     * Return true if the provided query contains a mandatory clauses (MUST)
     * that doesn't track the maximum scores per block
     */
    static boolean hasInfMaxScore(Query query) {
        MaxScoreQueryVisitor visitor = new MaxScoreQueryVisitor();
        query.visit(visitor);
        return visitor.hasInfMaxScore;
    }

    private static class MaxScoreQueryVisitor extends QueryVisitor {
        private boolean hasInfMaxScore;

        @Override
        public void visitLeaf(Query query) {
            checkMaxScoreInfo(query);
        }

        @Override
        public QueryVisitor getSubVisitor(BooleanClause.Occur occur, Query parent) {
            if (occur != BooleanClause.Occur.MUST) {
                // boolean queries can skip documents even if they have some should
                // clauses that don't track maximum scores
                return QueryVisitor.EMPTY_VISITOR;
            }
            checkMaxScoreInfo(parent);
            return this;
        }

        void checkMaxScoreInfo(Query query) {
            if (query instanceof FunctionScoreQuery || query instanceof ScriptScoreQuery || query instanceof SpanQuery) {
                hasInfMaxScore = true;
            } else if (query instanceof ESToParentBlockJoinQuery q) {
                hasInfMaxScore |= (q.getScoreMode() != org.apache.lucene.search.join.ScoreMode.None);
            }
        }
    }
}
