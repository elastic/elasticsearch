/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.search.query;

import org.apache.lucene.index.DocValuesType;
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
import org.apache.lucene.search.CollectorManager;
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
import org.apache.lucene.search.TopFieldCollectorManager;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TopScoreDocCollectorManager;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.search.MaxScoreCollector;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.common.lucene.search.function.ScriptScoreQuery;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.search.ESToParentBlockJoinQuery;
import org.elasticsearch.lucene.grouping.SinglePassGroupingCollector;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregatorCollector;
import org.elasticsearch.search.collapse.CollapseContext;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.search.rescore.RescoreContext;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.SortAndFormats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.search.profile.query.CollectorResult.REASON_AGGREGATION;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_QUERY_PHASE;
import static org.elasticsearch.search.profile.query.CollectorResult.REASON_SEARCH_TOP_HITS;

/**
 * Base collector manager that creates and reduces {@link QueryPhaseCollector}s used in the query phase.
 * It is aware of profiling, in that it applies the proper wrapping as well as reduction of collectors when profiling is enabled.
 * The different subclasses plug in their specific behaviour that revolves around top docs collection.
 */
abstract class QueryPhaseCollectorManager implements CollectorManager<Collector, QueryPhaseResult> {
    private final Weight postFilterWeight;
    private final QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker;
    private final CollectorManager<AggregatorCollector, Void> aggsCollectorManager;
    private final Float minScore;
    private final boolean profile;

    QueryPhaseCollectorManager(
        Weight postFilterWeight,
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
        CollectorManager<AggregatorCollector, Void> aggsCollectorManager,
        Float minScore,
        boolean profile
    ) {
        this.postFilterWeight = postFilterWeight;
        this.terminateAfterChecker = terminateAfterChecker;
        this.aggsCollectorManager = aggsCollectorManager;
        this.minScore = minScore;
        this.profile = profile;
    }

    /**
     * Returns the reason to set to the profile output when profiling is enabled
     */
    String getTopDocsProfilerReason() {
        return REASON_SEARCH_TOP_HITS;
    }

    /**
     * Returns the fields that sorting was applied on, to be returned as part of the search response
     */
    abstract DocValueFormat[] getSortValueFormats();

    @Override
    public final Collector newCollector() throws IOException {
        if (profile) {
            InternalProfileCollector topDocsProfileCollector = new InternalProfileCollector(
                newTopDocsCollector(),
                getTopDocsProfilerReason()
            );
            if (aggsCollectorManager == null) {
                return new InternalProfileCollector(
                    new QueryPhaseCollector(topDocsProfileCollector, postFilterWeight, terminateAfterChecker, null, minScore),
                    REASON_SEARCH_QUERY_PHASE,
                    topDocsProfileCollector
                );
            }
            InternalProfileCollector aggsProfileCollector = new InternalProfileCollector(
                aggsCollectorManager.newCollector(),
                REASON_AGGREGATION
            );
            return new InternalProfileCollector(
                new QueryPhaseCollector(topDocsProfileCollector, postFilterWeight, terminateAfterChecker, aggsProfileCollector, minScore),
                REASON_SEARCH_QUERY_PHASE,
                topDocsProfileCollector,
                aggsProfileCollector
            );
        }
        return new QueryPhaseCollector(
            newTopDocsCollector(),
            postFilterWeight,
            terminateAfterChecker,
            aggsCollectorManager == null ? null : aggsCollectorManager.newCollector(),
            minScore
        );
    }

    protected abstract Collector newTopDocsCollector() throws IOException;

    @Override
    public final QueryPhaseResult reduce(Collection<Collector> collectors) throws IOException {
        boolean terminatedAfter = false;
        CollectorResult collectorResult = null;
        List<Collector> topDocsCollectors = new ArrayList<>();
        List<AggregatorCollector> aggsCollectors = new ArrayList<>();
        if (profile) {
            List<CollectorResult> resultsPerProfiler = new ArrayList<>();
            List<CollectorResult> topDocsCollectorResults = new ArrayList<>();
            List<CollectorResult> aggsCollectorResults = new ArrayList<>();
            for (Collector collector : collectors) {
                InternalProfileCollector profileCollector = (InternalProfileCollector) collector;
                resultsPerProfiler.add(profileCollector.getCollectorTree());
                QueryPhaseCollector queryPhaseCollector = (QueryPhaseCollector) profileCollector.getWrappedCollector();
                if (queryPhaseCollector.isTerminatedAfter()) {
                    terminatedAfter = true;
                }
                InternalProfileCollector profileTopDocsCollector = (InternalProfileCollector) queryPhaseCollector.getTopDocsCollector();
                topDocsCollectorResults.add(profileTopDocsCollector.getCollectorTree());
                topDocsCollectors.add(profileTopDocsCollector.getWrappedCollector());
                if (aggsCollectorManager != null) {
                    InternalProfileCollector profileAggsCollector = (InternalProfileCollector) queryPhaseCollector.getAggsCollector();
                    aggsCollectorResults.add(profileAggsCollector.getCollectorTree());
                    aggsCollectors.add((AggregatorCollector) profileAggsCollector.getWrappedCollector());
                }
            }
            List<CollectorResult> childrenResults = new ArrayList<>();
            childrenResults.add(reduceCollectorResults(topDocsCollectorResults, Collections.emptyList()));
            if (aggsCollectorManager != null) {
                childrenResults.add(reduceCollectorResults(aggsCollectorResults, Collections.emptyList()));
            }
            collectorResult = reduceCollectorResults(resultsPerProfiler, Collections.unmodifiableList(childrenResults));
        } else {
            for (Collector collector : collectors) {
                QueryPhaseCollector queryPhaseCollector = (QueryPhaseCollector) collector;
                topDocsCollectors.add(queryPhaseCollector.getTopDocsCollector());
                aggsCollectors.add((AggregatorCollector) queryPhaseCollector.getAggsCollector());
                if (queryPhaseCollector.isTerminatedAfter()) {
                    terminatedAfter = true;
                }
            }
        }
        if (aggsCollectorManager != null) {
            aggsCollectorManager.reduce(aggsCollectors);
        }
        TopDocsAndMaxScore topDocsAndMaxScore = reduceTopDocsCollectors(topDocsCollectors);
        return new QueryPhaseResult(topDocsAndMaxScore, getSortValueFormats(), terminatedAfter, collectorResult);
    }

    abstract TopDocsAndMaxScore reduceTopDocsCollectors(Collection<Collector> collectors) throws IOException;

    private static CollectorResult reduceCollectorResults(Collection<CollectorResult> collectorResults, List<CollectorResult> children) {
        long totalTime = collectorResults.stream().map(CollectorResult::getTime).reduce(0L, Long::sum);
        String collectorName = collectorResults.iterator().next().getName();
        String reason = collectorResults.iterator().next().getReason();
        return new CollectorResult(collectorName, reason, totalTime, children);
    }

    /**
     * Creates a {@link QueryPhaseCollectorManager} from the provided <code>searchContext</code>.
     *
     * @param postFilterWeight the weight for the post_filter provided with the search request
     * @param aggsCollectorManager the collector manager for aggregations
     * @param searchContext the search context
     * @param hasFilterCollector True if the collector chain contains at least one collector that can filter documents.
     */
    static CollectorManager<Collector, QueryPhaseResult> createQueryPhaseCollectorManager(
        Weight postFilterWeight,
        CollectorManager<AggregatorCollector, Void> aggsCollectorManager,
        SearchContext searchContext,
        boolean hasFilterCollector
    ) throws IOException {
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker = QueryPhaseCollector.resolveTerminateAfterChecker(
            searchContext.terminateAfter()
        );
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final Query query = searchContext.rewrittenQuery();
        if (searchContext.size() == 0) {
            return new EmptyHits(
                postFilterWeight,
                terminateAfterChecker,
                aggsCollectorManager,
                searchContext.minimumScore(),
                searchContext.getProfilers() != null,
                searchContext.sort(),
                searchContext.trackTotalHitsUpTo()
            );
        }
        // top collectors don't like a size of 0
        final int totalNumDocs = Math.max(1, reader.numDocs());
        if (searchContext.scrollContext() == null) {
            int numDocs = Math.min(searchContext.from() + searchContext.size(), totalNumDocs);
            final boolean rescore = searchContext.rescore().isEmpty() == false;
            if (rescore) {
                assert RescorePhase.validateSort(searchContext.sort());
                for (RescoreContext rescoreContext : searchContext.rescore()) {
                    numDocs = Math.max(numDocs, rescoreContext.getWindowSize());
                }
            }
            if (searchContext.collapse() == null) {
                return new WithHits(
                    postFilterWeight,
                    terminateAfterChecker,
                    aggsCollectorManager,
                    searchContext.minimumScore(),
                    searchContext.getProfilers() != null,
                    reader,
                    query,
                    searchContext.sort(),
                    searchContext.searchAfter(),
                    numDocs,
                    searchContext.trackScores(),
                    searchContext.trackTotalHitsUpTo(),
                    hasFilterCollector
                );
            } else {
                boolean trackScores = searchContext.sort() == null || searchContext.trackScores();
                return forCollapsing(
                    postFilterWeight,
                    terminateAfterChecker,
                    aggsCollectorManager,
                    searchContext.minimumScore(),
                    searchContext.getProfilers() != null,
                    searchContext.collapse(),
                    searchContext.sort(),
                    numDocs,
                    trackScores,
                    searchContext.searchAfter()
                );
            }
        }
        // we can disable the tracking of total hits after the initial scroll query
        // since the total hits is preserved in the scroll context.
        int trackTotalHitsUpTo = searchContext.scrollContext().totalHits != null
            ? SearchContext.TRACK_TOTAL_HITS_DISABLED
            : SearchContext.TRACK_TOTAL_HITS_ACCURATE;
        // no matter what the value of from is
        int numDocs = Math.min(searchContext.size(), totalNumDocs);
        return forScroll(
            postFilterWeight,
            terminateAfterChecker,
            aggsCollectorManager,
            searchContext.minimumScore(),
            searchContext.getProfilers() != null,
            reader,
            query,
            searchContext.sort(),
            numDocs,
            searchContext.trackScores(),
            trackTotalHitsUpTo,
            hasFilterCollector,
            searchContext.scrollContext(),
            searchContext.numberOfShards()
        );
    }

    /**
     * Collector manager used when size is set to 0, hence there are no hits to collect. Top docs collection in
     * this case takes care of retrieving the total hit count.
     */
    private static final class EmptyHits extends QueryPhaseCollectorManager {
        private final PartialHitCountCollector.HitsThresholdChecker hitsThresholdChecker;
        private final SortAndFormats sortAndFormats;

        EmptyHits(
            Weight postFilterWeight,
            QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
            CollectorManager<AggregatorCollector, Void> aggsCollectorManager,
            Float minScore,
            boolean profile,
            @Nullable SortAndFormats sortAndFormats,
            int trackTotalHitsUpTo
        ) {
            super(postFilterWeight, terminateAfterChecker, aggsCollectorManager, minScore, profile);
            this.sortAndFormats = sortAndFormats;
            this.hitsThresholdChecker = new PartialHitCountCollector.HitsThresholdChecker(
                trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED ? 0 : trackTotalHitsUpTo
            );
        }

        @Override
        protected PartialHitCountCollector newTopDocsCollector() {
            return new PartialHitCountCollector(hitsThresholdChecker);
        }

        @Override
        protected TopDocsAndMaxScore reduceTopDocsCollectors(Collection<Collector> collectors) {
            int totalHitCount = 0;
            boolean earlyTerminated = false;
            for (Collector collector : collectors) {
                PartialHitCountCollector partialHitCountCollector = (PartialHitCountCollector) collector;
                totalHitCount += partialHitCountCollector.getTotalHits();
                if (partialHitCountCollector.hasEarlyTerminated()) {
                    earlyTerminated = true;
                }
            }
            final TotalHits totalHits = new TotalHits(
                totalHitCount,
                earlyTerminated ? TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO : TotalHits.Relation.EQUAL_TO
            );
            final TopDocs topDocs;
            if (sortAndFormats != null) {
                topDocs = new TopFieldDocs(totalHits, Lucene.EMPTY_SCORE_DOCS, sortAndFormats.sort.getSort());
            } else {
                topDocs = new TopDocs(totalHits, Lucene.EMPTY_SCORE_DOCS);
            }
            return new TopDocsAndMaxScore(topDocs, Float.NaN);
        }

        @Override
        protected String getTopDocsProfilerReason() {
            return CollectorResult.REASON_SEARCH_COUNT;
        }

        @Override
        protected DocValueFormat[] getSortValueFormats() {
            return null;
        }
    }

    /**
     * Collector manager used when size is greater than zero, meaning hits need to be collected.
     */
    private static class WithHits extends QueryPhaseCollectorManager {
        private final SortAndFormats sortAndFormats;
        private final boolean trackMaxScore;
        private final TotalHits shortcutTotalHits;
        private final CollectorManager<? extends TopDocsCollector<?>, ? extends TopDocs> topDocsManager;

        WithHits(
            Weight postFilterWeight,
            QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
            CollectorManager<AggregatorCollector, Void> aggsCollectorManager,
            Float minScore,
            boolean profile,
            IndexReader reader,
            Query query,
            @Nullable SortAndFormats sortAndFormats,
            @Nullable ScoreDoc searchAfter,
            int numHits,
            boolean trackMaxScore,
            int trackTotalHitsUpTo,
            boolean hasFilterCollector
        ) throws IOException {
            super(postFilterWeight, terminateAfterChecker, aggsCollectorManager, minScore, profile);
            this.sortAndFormats = sortAndFormats;
            this.trackMaxScore = trackMaxScore;

            final int hitCountThreshold;
            if ((sortAndFormats == null || SortField.FIELD_SCORE.equals(sortAndFormats.sort.getSort()[0])) && hasInfMaxScore(query)) {
                // disable max score optimization since we have a mandatory clause
                // that doesn't track the maximum score
                hitCountThreshold = Integer.MAX_VALUE;
                shortcutTotalHits = null;
            } else if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                // don't compute hit counts via the collector
                hitCountThreshold = 1;
                shortcutTotalHits = Lucene.TOTAL_HITS_GREATER_OR_EQUAL_TO_ZERO;
            } else {
                // implicit total hit counts are valid only when there is no filter collector in the chain
                final int hitCount = hasFilterCollector ? -1 : shortcutTotalHitCount(reader, query);
                if (hitCount == -1) {
                    hitCountThreshold = trackTotalHitsUpTo;
                    shortcutTotalHits = null;
                } else {
                    // don't compute hit counts via the collector
                    hitCountThreshold = 1;
                    shortcutTotalHits = new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO);
                }
            }
            if (sortAndFormats == null) {
                this.topDocsManager = new TopScoreDocCollectorManager(numHits, searchAfter, hitCountThreshold);
            } else {
                this.topDocsManager = new TopFieldCollectorManager(sortAndFormats.sort, numHits, (FieldDoc) searchAfter, hitCountThreshold);
            }
        }

        @Override
        protected Collector newTopDocsCollector() throws IOException {
            if (trackMaxScore) {
                return MultiCollector.wrap(topDocsManager.newCollector(), new MaxScoreCollector());
            }
            return topDocsManager.newCollector();
        }

        @Override
        protected TopDocsAndMaxScore reduceTopDocsCollectors(Collection<Collector> collectors) throws IOException {
            final Collection<TopDocsCollector<?>> topDocsCollectors = new ArrayList<>();
            final Collection<MaxScoreCollector> maxScoreCollectors;
            if (trackMaxScore) {
                maxScoreCollectors = new ArrayList<>();
                for (Collector collector : collectors) {
                    MultiCollector mc = (MultiCollector) collector;
                    topDocsCollectors.add((TopDocsCollector<?>) mc.getCollectors()[0]);
                    maxScoreCollectors.add((MaxScoreCollector) mc.getCollectors()[1]);
                }
            } else {
                maxScoreCollectors = null;
                for (Collector collector : collectors) {
                    topDocsCollectors.add((TopDocsCollector<?>) collector);
                }
            }

            @SuppressWarnings("unchecked")
            CollectorManager<TopDocsCollector<?>, ? extends TopDocs> tdcm = (CollectorManager<
                TopDocsCollector<?>,
                ? extends TopDocs>) topDocsManager;
            TopDocs topDocs = tdcm.reduce(topDocsCollectors);
            if (shortcutTotalHits != null) {
                if (topDocs instanceof TopFieldDocs fieldDocs) {
                    topDocs = new TopFieldDocs(shortcutTotalHits, fieldDocs.scoreDocs, fieldDocs.fields);
                } else {
                    topDocs = new TopDocs(shortcutTotalHits, topDocs.scoreDocs);
                }
            }
            final float maxScore = getMaxScore(topDocs, sortAndFormats, trackMaxScore, maxScoreCollectors);
            return new TopDocsAndMaxScore(topDocs, maxScore);
        }

        @Override
        protected final DocValueFormat[] getSortValueFormats() {
            return sortAndFormats == null ? null : sortAndFormats.formats;
        }
    }

    /**
     * Returns the collector manager used for scroll requests. It is an extension of the {@link WithHits} implementation above but
     * with some customizations applied to the reduction of top docs.
     */
    private static WithHits forScroll(
        Weight postFilterWeight,
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
        CollectorManager<AggregatorCollector, Void> aggsCollectorManager,
        Float minScore,
        boolean profile,
        IndexReader reader,
        Query query,
        @Nullable SortAndFormats sortAndFormats,
        int numHits,
        boolean trackMaxScore,
        int trackTotalHitsUpTo,
        boolean hasFilterCollector,
        ScrollContext scrollContext,
        int numberOfShards
    ) throws IOException {
        return new WithHits(
            postFilterWeight,
            terminateAfterChecker,
            aggsCollectorManager,
            minScore,
            profile,
            reader,
            query,
            sortAndFormats,
            scrollContext.lastEmittedDoc,
            numHits,
            trackMaxScore,
            trackTotalHitsUpTo,
            hasFilterCollector
        ) {
            @Override
            public TopDocsAndMaxScore reduceTopDocsCollectors(Collection<Collector> collectors) throws IOException {
                TopDocsAndMaxScore topDocs = super.reduceTopDocsCollectors(collectors);
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
                return topDocs;
            }
        };
    }

    /**
     * Builds a {@link CollectorManager} to be used when collapse is used in a search request.
     * Note: does not support concurrency, and enforces that it is used only with single sliced searches.
     */
    private static QueryPhaseCollectorManager forCollapsing(
        Weight postFilterWeight,
        QueryPhaseCollector.TerminateAfterChecker terminateAfterChecker,
        CollectorManager<AggregatorCollector, Void> aggsCollectorManager,
        Float minScore,
        boolean profile,
        CollapseContext collapseContext,
        @Nullable SortAndFormats sortAndFormats,
        int numHits,
        boolean trackMaxScore,
        @Nullable FieldDoc after
    ) {
        assert numHits > 0;
        assert collapseContext != null;
        Sort sort = sortAndFormats == null ? Sort.RELEVANCE : sortAndFormats.sort;
        final SinglePassGroupingCollector<?> topDocsCollector = collapseContext.createTopDocs(sort, numHits, after);
        MaxScoreCollector maxScoreCollector = trackMaxScore ? new MaxScoreCollector() : null;
        return new QueryPhaseCollectorManager(postFilterWeight, terminateAfterChecker, aggsCollectorManager, minScore, profile) {
            boolean newCollectorCalled = false;

            @Override
            protected Collector newTopDocsCollector() {
                assert newCollectorCalled == false : "Field collapsing does not support concurrent execution";
                newCollectorCalled = true;
                return MultiCollector.wrap(topDocsCollector, maxScoreCollector);
            }

            @Override
            protected TopDocsAndMaxScore reduceTopDocsCollectors(Collection<Collector> collectors) throws IOException {
                assert collectors.size() == 1 : "Field collapsing does not support concurrent execution";
                TopFieldGroups topDocs = topDocsCollector.getTopGroups(0);
                float maxScore = getMaxScore(topDocs, sortAndFormats, trackMaxScore, Collections.singletonList(maxScoreCollector));
                return new TopDocsAndMaxScore(topDocs, maxScore);
            }

            @Override
            protected DocValueFormat[] getSortValueFormats() {
                return sortAndFormats == null ? new DocValueFormat[] { DocValueFormat.RAW } : sortAndFormats.formats;
            }
        };
    }

    private static float getMaxScore(
        TopDocs topDocs,
        SortAndFormats sortAndFormats,
        boolean trackMaxScore,
        Collection<MaxScoreCollector> maxScoreCollectors
    ) {
        if (sortAndFormats == null) {
            return topDocs.scoreDocs.length == 0 ? Float.NaN : topDocs.scoreDocs[0].score;
        }
        if (trackMaxScore) {
            return maxScoreCollectors.stream().map(MaxScoreCollector::getMaxScore).reduce(Float.NEGATIVE_INFINITY, (f1, f2) -> {
                if (Float.isNaN(f1)) {
                    return f2;
                }
                if (Float.isNaN(f2)) {
                    return f1;
                }
                return Math.max(f1, f2);
            });
        }
        return Float.NaN;
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
                    if (fieldInfo.getDocValuesType() == DocValuesType.NONE) {
                        // no shortcut possible: it's a text field, empty values are counted as no value.
                        return -1;
                    }
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
