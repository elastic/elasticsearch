/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.Terms;
import org.apache.lucene.queries.spans.SpanQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.FieldExistsQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MultiCollector;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermQuery;
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
     * Returns the number of top docs to retrieve
     */
    final int numHits() {
        return numHits;
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

        /**
         * Ctr
         * @param reader The index reader
         * @param query The query to execute
         * @param trackTotalHitsUpTo True if the total number of hits should be tracked
         * @param hasFilterCollector True if the collector chain contains a filter
         */
        private EmptyTopDocsCollectorContext(
            IndexReader reader,
            Query query,
            @Nullable SortAndFormats sortAndFormats,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
            super(REASON_SEARCH_COUNT, 0);
            this.sort = sortAndFormats == null ? null : sortAndFormats.sort;
            if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                this.collector = new EarlyTerminatingCollector(new TotalHitCountCollector(), 0, false);
                // for bwc hit count is set to 0, it will be converted to -1 by the coordinating node
                this.hitCountSupplier = () -> new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            } else {
                TotalHitCountCollector hitCountCollector = new TotalHitCountCollector();
                // implicit total hit counts are valid only when there is no filter collector in the chain
                int hitCount = hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                if (hitCount == -1) {
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
                } else {
                    this.collector = new EarlyTerminatingCollector(hitCountCollector, 0, false);
                    this.hitCountSupplier = () -> new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO);
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
        Collector create(Collector in) throws IOException {
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
         * @param reader The index reader
         * @param query The Lucene query
         * @param sortAndFormats The query sort
         * @param numHits The number of top hits to retrieve
         * @param searchAfter The doc this request should "search after"
         * @param trackMaxScore True if max score should be tracked
         * @param trackTotalHitsUpTo True if the total number of hits should be tracked
         * @param hasFilterCollector True if the collector chain contains at least one collector that can filters document
         */
        private SimpleTopDocsCollectorContext(
            IndexReader reader,
            Query query,
            @Nullable SortAndFormats sortAndFormats,
            @Nullable ScoreDoc searchAfter,
            int numHits,
            boolean trackMaxScore,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
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
                // implicit total hit counts are valid only when there is no filter collector in the chain
                final int hitCount = hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                if (hitCount == -1) {
                    topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, trackTotalHitsUpTo);
                    topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                    totalHitsSupplier = () -> topDocsSupplier.get().totalHits;
                } else {
                    // don't compute hit counts via the collector
                    topDocsCollector = createCollector(sortAndFormats, numHits, searchAfter, 1);
                    topDocsSupplier = new CachedSupplier<>(topDocsCollector::topDocs);
                    totalHitsSupplier = () -> new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO);
                }
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
        void postProcess(QuerySearchResult result) throws IOException {
            final TopDocsAndMaxScore topDocs = newTopDocs();
            result.topDocs(topDocs, sortAndFormats == null ? null : sortAndFormats.formats);
        }
    }

    static class ScrollingTopDocsCollectorContext extends SimpleTopDocsCollectorContext {
        private final ScrollContext scrollContext;
        private final int numberOfShards;

        private ScrollingTopDocsCollectorContext(
            IndexReader reader,
            Query query,
            ScrollContext scrollContext,
            @Nullable SortAndFormats sortAndFormats,
            int numHits,
            boolean trackMaxScore,
            int numberOfShards,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
            super(
                reader,
                query,
                sortAndFormats,
                scrollContext.lastEmittedDoc,
                numHits,
                trackMaxScore,
                trackTotalHitsUpTo,
                hasFilterCollector
            );
            this.scrollContext = Objects.requireNonNull(scrollContext);
            this.numberOfShards = numberOfShards;
        }

        @Override
        void postProcess(QuerySearchResult result) throws IOException {
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
     * Returns query total hit count if the <code>query</code> is a {@link MatchAllDocsQuery}
     * or a {@link TermQuery} and the <code>reader</code> has no deletions,
     * -1 otherwise.
     */
    static int shortcutTotalHitCount(IndexReader reader, Query query) throws IOException {
        while (true) {
            // remove wrappers that don't matter for counts
            // this is necessary so that we don't only optimize match_all
            // queries but also match_all queries that are nested in
            // a constant_score query
            if (query instanceof ConstantScoreQuery) {
                query = ((ConstantScoreQuery) query).getQuery();
            } else if (query instanceof BoostQuery) {
                query = ((BoostQuery) query).getQuery();
            } else {
                break;
            }
        }
        if (query.getClass() == MatchAllDocsQuery.class) {
            return reader.numDocs();
        } else if (query.getClass() == TermQuery.class && reader.hasDeletions() == false) {
            final Term term = ((TermQuery) query).getTerm();
            int count = 0;
            for (LeafReaderContext context : reader.leaves()) {
                count += context.reader().docFreq(term);
            }
            return count;
        } else if (query.getClass() == FieldExistsQuery.class && reader.hasDeletions() == false) {
            final String field = ((FieldExistsQuery) query).getField();
            int count = 0;
            for (LeafReaderContext context : reader.leaves()) {
                FieldInfos fieldInfos = context.reader().getFieldInfos();
                FieldInfo fieldInfo = fieldInfos.fieldInfo(field);
                if (fieldInfo != null) {
                    if (fieldInfo.getPointIndexDimensionCount() > 0) {
                        PointValues points = context.reader().getPointValues(field);
                        if (points != null) {
                            count += points.getDocCount();
                        }
                    } else if (fieldInfo.getIndexOptions() != IndexOptions.NONE) {
                        Terms terms = context.reader().terms(field);
                        if (terms != null) {
                            count += terms.getDocCount();
                        }
                    } else {
                        return -1; // no shortcut possible for fields that are not indexed
                    }
                }
            }
            return count;
        } else {
            return -1;
        }
    }

    /**
     * Creates a {@link TopDocsCollectorContext} from the provided <code>searchContext</code>.
     * @param hasFilterCollector True if the collector chain contains at least one collector that can filters document.
     */
    static TopDocsCollectorContext createTopDocsCollectorContext(SearchContext searchContext, boolean hasFilterCollector)
        throws IOException {
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final Query query = searchContext.rewrittenQuery();
        // top collectors don't like a size of 0
        final int totalNumDocs = Math.max(1, reader.numDocs());
        if (searchContext.size() == 0) {
            // no matter what the value of from is
            return new EmptyTopDocsCollectorContext(
                reader,
                query,
                searchContext.sort(),
                searchContext.trackTotalHitsUpTo(),
                hasFilterCollector
            );
        } else if (searchContext.scrollContext() != null) {
            // we can disable the tracking of total hits after the initial scroll query
            // since the total hits is preserved in the scroll context.
            int trackTotalHitsUpTo = searchContext.scrollContext().totalHits != null
                ? SearchContext.TRACK_TOTAL_HITS_DISABLED
                : SearchContext.TRACK_TOTAL_HITS_ACCURATE;
            // no matter what the value of from is
            int numDocs = Math.min(searchContext.size(), totalNumDocs);
            return new ScrollingTopDocsCollectorContext(
                reader,
                query,
                searchContext.scrollContext(),
                searchContext.sort(),
                numDocs,
                searchContext.trackScores(),
                searchContext.numberOfShards(),
                trackTotalHitsUpTo,
                hasFilterCollector
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
                reader,
                query,
                searchContext.sort(),
                searchContext.searchAfter(),
                numDocs,
                searchContext.trackScores(),
                searchContext.trackTotalHitsUpTo(),
                hasFilterCollector
            ) {
                @Override
                boolean shouldRescore() {
                    return rescore;
                }
            };
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
