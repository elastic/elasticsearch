/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.PointValues;
import org.elasticsearch.lucene.queries.MinDocQuery;
import org.elasticsearch.lucene.queries.SearchAfterSortedDocQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DocValuesFieldExistsQuery;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldCollector;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.Weight;
import org.elasticsearch.action.search.SearchShardTask;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.EWMATrackingEsThreadPoolExecutor;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.core.Booleans;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.index.IndexSortConfig;
import org.elasticsearch.index.mapper.DateFieldMapper.DateFieldType;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.index.query.SearchExecutionContext;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchContextSourcePrinter;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestPhase;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;

import static org.elasticsearch.search.query.QueryCollectorContext.createEarlyTerminationCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createFilteredCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createMinScoreCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createMultiCollectorContext;
import static org.elasticsearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;
import static org.elasticsearch.search.query.TopDocsCollectorContext.shortcutTotalHitCount;


/**
 * Query phase of a search request, used to run the query and get back from each shard information about the matching documents
 * (document ids and score or sort criteria) so that matches can be reduced on the coordinating node
 */
public class QueryPhase {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhase.class);
    // TODO: remove this property in 8.0
    public static final boolean SYS_PROP_REWRITE_SORT = Booleans.parseBoolean(System.getProperty("es.search.rewrite_sort", "true"));

    private final AggregationPhase aggregationPhase;
    private final SuggestPhase suggestPhase;
    private final RescorePhase rescorePhase;

    public QueryPhase() {
        this.aggregationPhase = new AggregationPhase();
        this.suggestPhase = new SuggestPhase();
        this.rescorePhase = new RescorePhase();
    }

    public void preProcess(SearchContext context) {
        final Runnable cancellation;
        if (context.lowLevelCancellation()) {
            cancellation = context.searcher().addQueryCancellation(() -> {
                SearchShardTask task = context.getTask();
                if (task != null) {
                    task.ensureNotCancelled();
                }
            });
        } else {
            cancellation = null;
        }
        try {
            context.preProcess(true);
        } finally {
            if (cancellation != null) {
                context.searcher().removeQueryCancellation(cancellation);
            }
        }
    }

    public void execute(SearchContext searchContext) throws QueryPhaseExecutionException {
        if (searchContext.hasOnlySuggest()) {
            suggestPhase.execute(searchContext);
            searchContext.queryResult().topDocs(new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS), Float.NaN),
                new DocValueFormat[0]);
            return;
        }

        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace("{}", new SearchContextSourcePrinter(searchContext));
        }

        // Pre-process aggregations as late as possible. In the case of a DFS_Q_T_F
        // request, preProcess is called on the DFS phase phase, this is why we pre-process them
        // here to make sure it happens during the QUERY phase
        aggregationPhase.preProcess(searchContext);
        boolean rescore = executeInternal(searchContext);

        if (rescore) { // only if we do a regular search
            rescorePhase.execute(searchContext);
        }
        suggestPhase.execute(searchContext);
        aggregationPhase.execute(searchContext);

        if (searchContext.getProfilers() != null) {
            ProfileShardResult shardResults = SearchProfileShardResults
                .buildShardResults(searchContext.getProfilers());
            searchContext.queryResult().profileResults(shardResults);
        }
    }

    /**
     * In a package-private method so that it can be tested without having to
     * wire everything (mapperService, etc.)
     * @return whether the rescoring phase should be executed
     */
    static boolean executeInternal(SearchContext searchContext) throws QueryPhaseExecutionException {
        final ContextIndexSearcher searcher = searchContext.searcher();
        SortAndFormats sortAndFormatsForRewrittenNumericSort = null;
        final IndexReader reader = searcher.getIndexReader();
        QuerySearchResult queryResult = searchContext.queryResult();
        queryResult.searchTimedOut(false);
        try {
            queryResult.from(searchContext.from());
            queryResult.size(searchContext.size());
            Query query = searchContext.query();
            assert query == searcher.rewrite(query); // already rewritten

            final ScrollContext scrollContext = searchContext.scrollContext();
            if (scrollContext != null) {
                if (scrollContext.totalHits == null) {
                    // first round
                    assert scrollContext.lastEmittedDoc == null;
                    // there is not much that we can optimize here since we want to collect all
                    // documents in order to get the total number of hits

                } else {
                    final ScoreDoc after = scrollContext.lastEmittedDoc;
                    if (returnsDocsInOrder(query, searchContext.sort())) {
                        // now this gets interesting: since we sort in index-order, we can directly
                        // skip to the desired doc
                        if (after != null) {
                            query = new BooleanQuery.Builder()
                                .add(query, BooleanClause.Occur.MUST)
                                .add(new MinDocQuery(after.doc + 1), BooleanClause.Occur.FILTER)
                                .build();
                        }
                        // ... and stop collecting after ${size} matches
                        searchContext.terminateAfter(searchContext.size());
                    } else if (canEarlyTerminate(reader, searchContext.sort())) {
                        // now this gets interesting: since the search sort is a prefix of the index sort, we can directly
                        // skip to the desired doc
                        if (after != null) {
                            query = new BooleanQuery.Builder()
                                .add(query, BooleanClause.Occur.MUST)
                                .add(new SearchAfterSortedDocQuery(searchContext.sort().sort, (FieldDoc) after), BooleanClause.Occur.FILTER)
                                .build();
                        }
                    }
                }
            }

            final LinkedList<QueryCollectorContext> collectors = new LinkedList<>();
            // whether the chain contains a collector that filters documents
            boolean hasFilterCollector = false;
            if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER) {
                // add terminate_after before the filter collectors
                // it will only be applied on documents accepted by these filter collectors
                collectors.add(createEarlyTerminationCollectorContext(searchContext.terminateAfter()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }
            if (searchContext.parsedPostFilter() != null) {
                // add post filters before aggregations
                // it will only be applied to top hits
                collectors.add(createFilteredCollectorContext(searcher, searchContext.parsedPostFilter().query()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }
            if (searchContext.queryCollectors().isEmpty() == false) {
                // plug in additional collectors, like aggregations
                collectors.add(createMultiCollectorContext(searchContext.queryCollectors().values()));
            }
            if (searchContext.minimumScore() != null) {
                // apply the minimum score after multi collector so we filter aggs as well
                collectors.add(createMinScoreCollectorContext(searchContext.minimumScore()));
                // this collector can filter documents during the collection
                hasFilterCollector = true;
            }

            CheckedConsumer<List<LeafReaderContext>, IOException> leafSorter = l -> {};
            // try to rewrite numeric or date sort to the optimized distanceFeatureQuery
            if ((searchContext.sort() != null) && SYS_PROP_REWRITE_SORT) {
                Query rewrittenQuery = tryRewriteLongSort(searchContext, searcher.getIndexReader(), query, hasFilterCollector);
                if (rewrittenQuery != null) {
                    query = rewrittenQuery;
                    // modify sorts: add sort on _score as 1st sort, and move the sort on the original field as the 2nd sort
                    SortField[] oldSortFields = searchContext.sort().sort.getSort();
                    DocValueFormat[] oldFormats = searchContext.sort().formats;
                    SortField[] newSortFields = new SortField[oldSortFields.length + 1];
                    DocValueFormat[] newFormats = new DocValueFormat[oldSortFields.length + 1];
                    newSortFields[0] = SortField.FIELD_SCORE;
                    newFormats[0] = DocValueFormat.RAW;
                    System.arraycopy(oldSortFields, 0, newSortFields, 1, oldSortFields.length);
                    System.arraycopy(oldFormats, 0, newFormats, 1, oldFormats.length);
                    sortAndFormatsForRewrittenNumericSort = searchContext.sort(); // stash SortAndFormats to restore it later
                    searchContext.sort(new SortAndFormats(new Sort(newSortFields), newFormats));
                    leafSorter = createLeafSorter(oldSortFields[0]);
                }
            }

            boolean timeoutSet = scrollContext == null && searchContext.timeout() != null &&
                searchContext.timeout().equals(SearchService.NO_TIMEOUT) == false;

            final Runnable timeoutRunnable;
            if (timeoutSet) {
                final long startTime = searchContext.getRelativeTimeInMillis();
                final long timeout = searchContext.timeout().millis();
                final long maxTime = startTime + timeout;
                timeoutRunnable = searcher.addQueryCancellation(() -> {
                    final long time = searchContext.getRelativeTimeInMillis();
                    if (time > maxTime) {
                        throw new TimeExceededException();
                    }
                });
            } else {
                timeoutRunnable = null;
            }

            if (searchContext.lowLevelCancellation()) {
                searcher.addQueryCancellation(() -> {
                    SearchShardTask task = searchContext.getTask();
                    if (task != null) {
                        task.ensureNotCancelled();
                    }
                });
            }

            try {
                boolean shouldRescore;
                // if we are optimizing sort and there are no other collectors
                if (sortAndFormatsForRewrittenNumericSort != null && collectors.size() == 0 && searchContext.getProfilers() == null) {
                    shouldRescore = searchWithCollectorManager(searchContext, searcher, query, leafSorter, timeoutSet);
                } else {
                    shouldRescore = searchWithCollector(searchContext, searcher, query, collectors, hasFilterCollector, timeoutSet);
                }

                // if we rewrote numeric long or date sort, restore fieldDocs based on the original sort
                if (sortAndFormatsForRewrittenNumericSort != null) {
                    searchContext.sort(sortAndFormatsForRewrittenNumericSort); // restore SortAndFormats
                    restoreTopFieldDocs(queryResult, sortAndFormatsForRewrittenNumericSort);
                }

                ExecutorService executor = searchContext.indexShard().getThreadPool().executor(ThreadPool.Names.SEARCH);
                assert executor instanceof EWMATrackingEsThreadPoolExecutor ||
                    (executor instanceof EsThreadPoolExecutor == false /* in case thread pool is mocked out in tests */) :
                    "SEARCH threadpool should have an executor that exposes EWMA metrics, but is of type " + executor.getClass();
                if (executor instanceof EWMATrackingEsThreadPoolExecutor) {
                    EWMATrackingEsThreadPoolExecutor rExecutor = (EWMATrackingEsThreadPoolExecutor) executor;
                    queryResult.nodeQueueSize(rExecutor.getCurrentQueueSize());
                    queryResult.serviceTimeEWMA((long) rExecutor.getTaskExecutionEWMA());
                }

                return shouldRescore;
            } finally {
                // Search phase has finished, no longer need to check for timeout
                // otherwise aggregation phase might get cancelled.
                if (timeoutRunnable != null) {
                   searcher.removeQueryCancellation(timeoutRunnable);
                }
            }
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Failed to execute main query", e);
        }
    }

    private static boolean searchWithCollector(SearchContext searchContext, ContextIndexSearcher searcher, Query query,
            LinkedList<QueryCollectorContext> collectors, boolean hasFilterCollector, boolean timeoutSet) throws IOException {
        // create the top docs collector last when the other collectors are known
        final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, hasFilterCollector);
        // add the top docs collector, the first collector context in the chain
        collectors.addFirst(topDocsFactory);

        final Collector queryCollector;
        if (searchContext.getProfilers() != null) {
            InternalProfileCollector profileCollector = QueryCollectorContext.createQueryCollectorWithProfiler(collectors);
            searchContext.getProfilers().getCurrentQueryProfiler().setCollector(profileCollector);
            queryCollector = profileCollector;
        } else {
            queryCollector = QueryCollectorContext.createQueryCollector(collectors);
        }
        QuerySearchResult queryResult = searchContext.queryResult();
        try {
            searcher.search(query, queryCollector);
        } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
            queryResult.terminatedEarly(true);
        } catch (TimeExceededException e) {
            assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
            if (searchContext.request().allowPartialSearchResults() == false) {
                // Can't rethrow TimeExceededException because not serializable
                throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
            }
            queryResult.searchTimedOut(true);
        }
        if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER && queryResult.terminatedEarly() == null) {
            queryResult.terminatedEarly(false);
        }
        for (QueryCollectorContext ctx : collectors) {
            ctx.postProcess(queryResult);
        }
        return topDocsFactory.shouldRescore();
    }


    /*
     * We use collectorManager during sort optimization, where
     * we have already checked that there are no other collectors, no filters,
     * no search after, no scroll, no collapse, no track scores.
     * Absence of all other collectors and parameters allows us to use TopFieldCollector directly.
     */
    private static boolean searchWithCollectorManager(SearchContext searchContext,
                                                      ContextIndexSearcher searcher,
                                                      Query query,
                                                      CheckedConsumer<List<LeafReaderContext>, IOException> leafSorter,
                                                      boolean timeoutSet) throws IOException {
        final QuerySearchResult queryResult = searchContext.queryResult();
        final IndexReader reader = searchContext.searcher().getIndexReader();
        final int numHits = Math.min(searchContext.from() + searchContext.size(), Math.max(1, reader.numDocs()));
        final SortAndFormats sortAndFormats = searchContext.sort();

        int totalHitsThreshold;
        final TotalHits totalHits;
        if (searchContext.trackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
            totalHitsThreshold = 1;
            totalHits = new TotalHits(0, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
        } else {
            int hitCount = shortcutTotalHitCount(reader, query);
            if (hitCount == -1) {
                totalHitsThreshold = searchContext.trackTotalHitsUpTo();
                totalHits = null; // will be computed via the collector
            } else {
                totalHitsThreshold = 1;
                totalHits = new TotalHits(hitCount, TotalHits.Relation.EQUAL_TO); // don't compute hit counts via the collector
            }
        }

        CollectorManager<TopFieldCollector, TopFieldDocs> sharedManager = TopFieldCollector.createSharedManager(
            sortAndFormats.sort, numHits, null, totalHitsThreshold);

        List<LeafReaderContext> leaves = new ArrayList<>(searcher.getIndexReader().leaves());
        leafSorter.accept(leaves);

        final Weight weight = searcher.createWeight(searcher.rewrite(query), ScoreMode.TOP_SCORES, 1f);
        final List<TopFieldCollector> collectors = new ArrayList<>(leaves.size());

        try {
            for (LeafReaderContext ctx : leaves) {
                final TopFieldCollector collector = sharedManager.newCollector();
                collectors.add(collector);
                searcher.search(Collections.singletonList(ctx), weight, collector);
            }
        } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
            queryResult.terminatedEarly(true);
        } catch (TimeExceededException e) {
            assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";
            if (searchContext.request().allowPartialSearchResults() == false) {
                // Can't rethrow TimeExceededException because not serializable
                throw new QueryPhaseExecutionException(searchContext.shardTarget(), "Time exceeded");
            }
            queryResult.searchTimedOut(true);
        }

        TopFieldDocs mergedTopDocs = sharedManager.reduce(collectors);
        // Lucene sets shards indexes during merging of topDocs from different collectors
        // We need to reset shard index; ES will set shard index later during reduce stage
        for (ScoreDoc scoreDoc : mergedTopDocs.scoreDocs) {
            scoreDoc.shardIndex = -1;
        }
        if (totalHits != null) { // we have already precalculated totalHits for the whole index
            mergedTopDocs = new TopFieldDocs(totalHits, mergedTopDocs.scoreDocs, mergedTopDocs.fields);
        }
        queryResult.topDocs(new TopDocsAndMaxScore(mergedTopDocs, Float.NaN), sortAndFormats.formats);
        return false; // no rescoring when sorting by field
    }

    private static Query tryRewriteLongSort(SearchContext searchContext, IndexReader reader,
                                            Query query, boolean hasFilterCollector) throws IOException {
        if ((searchContext.from() + searchContext.size()) <= 0) return null;
        if (searchContext.searchAfter() != null) return null; //TODO: handle sort optimization with search after
        if (searchContext.scrollContext() != null) return null;
        if (searchContext.collapse() != null) return null;
        if (searchContext.trackScores()) return null;
        if (searchContext.aggregations() != null) return null;
        if (canEarlyTerminate(reader, searchContext.sort())) {
            // disable this optimization if index sorting matches the query sort since it's already optimized by index searcher
            return null;
        }
        Sort sort = searchContext.sort().sort;
        SortField sortField = sort.getSort()[0];
        if (SortField.Type.LONG.equals(IndexSortConfig.getSortFieldType(sortField)) == false) return null;

        // check if this is a field of type Long or Date, that is indexed and has doc values
        String fieldName = sortField.getField();
        SearchExecutionContext searchExecutionContext = searchContext.getSearchExecutionContext();
        if (fieldName == null) return null; // happens when _score or _doc is the 1st sort field
        final MappedFieldType fieldType = searchExecutionContext.getFieldType(fieldName);
        if (fieldType == null) return null; // for unmapped fields, default behaviour depending on "unmapped_type" flag
        if ((fieldType.typeName().equals("long") == false) && (fieldType instanceof DateFieldType == false)) return null;
        if (fieldType.isSearchable() == false) return null;
        if (fieldType.hasDocValues() == false) return null;


        // check that all sorts are actual document fields or _doc
        for (int i = 1; i < sort.getSort().length; i++) {
            SortField sField = sort.getSort()[i];
            String sFieldName = sField.getField();
            if (sFieldName == null) {
                if (SortField.FIELD_DOC.equals(sField) == false) {
                    return null;
                }
            } else if (FieldSortBuilder.SHARD_DOC_FIELD_NAME.equals(sFieldName) == false) {
                //TODO: find out how to cover _script sort that don't use _score
                if (searchExecutionContext.getFieldType(sFieldName) == null) {
                    return null; // could be _script sort that uses _score
                }
            }
        }

        // check that setting of missing values allows optimization
        if (sortField.getMissingValue() == null) return null;
        Long missingValue = (Long) sortField.getMissingValue();
        boolean missingValuesAccordingToSort = (sortField.getReverse() && (missingValue == Long.MIN_VALUE)) ||
            ((sortField.getReverse() == false) && (missingValue == Long.MAX_VALUE));
        if (missingValuesAccordingToSort == false) return null;

        int docCount = PointValues.getDocCount(reader, fieldName);
        // is not worth to run optimization on small index
        if (docCount <= 512) return null;

        // check for multiple values
        if (PointValues.size(reader, fieldName) != docCount) return null; //TODO: handle multiple values

        // check if the optimization makes sense with the track_total_hits setting
        if (searchContext.trackTotalHitsUpTo() == Integer.MAX_VALUE) {
            // with filter, we can't pre-calculate hitsCount, we need to explicitly calculate them => optimization does't make sense
            if (hasFilterCollector) return null;
            // if we can't pre-calculate hitsCount based on the query type, optimization does't make sense
            if (shortcutTotalHitCount(reader, query) == -1) return null;
        }

        byte[] minValueBytes = PointValues.getMinPackedValue(reader, fieldName);
        byte[] maxValueBytes = PointValues.getMaxPackedValue(reader, fieldName);
        if ((maxValueBytes == null) || (minValueBytes == null)) return null;
        long minValue = LongPoint.decodeDimension(minValueBytes, 0);
        long maxValue = LongPoint.decodeDimension(maxValueBytes, 0);

        Query rewrittenQuery;
        if (minValue == maxValue) {
            rewrittenQuery = new DocValuesFieldExistsQuery(fieldName);
        } else {
            if (indexFieldHasDuplicateData(reader, fieldName)) return null;
            long origin = (sortField.getReverse()) ? maxValue : minValue;
            long pivotDistance = (maxValue - minValue) >>> 1; // division by 2 on the unsigned representation to avoid overflow
            if (pivotDistance == 0) { // 0 if maxValue = (minValue + 1)
                pivotDistance = 1;
            }
            rewrittenQuery = LongPoint.newDistanceFeatureQuery(sortField.getField(), 1, origin, pivotDistance);
        }
        rewrittenQuery = new BooleanQuery.Builder()
            .add(query, BooleanClause.Occur.FILTER) // filter for original query
            .add(rewrittenQuery, BooleanClause.Occur.SHOULD) //should for rewrittenQuery
            .build();
        return rewrittenQuery;
    }

    /**
     * Creates a sorter of {@link LeafReaderContext} that orders leaves depending on the minimum
     * value and the sort order of the provided <code>sortField</code>.
     */
    static CheckedConsumer<List<LeafReaderContext>, IOException> createLeafSorter(SortField sortField) {
        return leaves -> {
            long[] sortValues = new long[leaves.size()];
            long missingValue = (long) sortField.getMissingValue();
            for (LeafReaderContext ctx : leaves) {
                PointValues values = ctx.reader().getPointValues(sortField.getField());
                if (values == null) {
                    sortValues[ctx.ord] = missingValue;
                } else {
                    byte[] sortValue = sortField.getReverse() ? values.getMaxPackedValue(): values.getMinPackedValue();
                    sortValues[ctx.ord] = sortValue == null ? missingValue : LongPoint.decodeDimension(sortValue, 0);
                }
            }
            Comparator<LeafReaderContext> comparator = Comparator.comparingLong(l -> sortValues[l.ord]);
            if (sortField.getReverse()) {
                comparator = comparator.reversed();
            }
            Collections.sort(leaves, comparator);
        };
    }

    /**
     * Restore fieldsDocs to remove the first _score
     */
    private static void restoreTopFieldDocs(QuerySearchResult result, SortAndFormats originalSortAndFormats) {
        TopDocs topDocs = result.topDocs().topDocs;
        for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
            FieldDoc fieldDoc = (FieldDoc) scoreDoc;
            fieldDoc.fields = Arrays.copyOfRange(fieldDoc.fields, 1, fieldDoc.fields.length);
        }
        TopFieldDocs newTopDocs = new TopFieldDocs(topDocs.totalHits, topDocs.scoreDocs, originalSortAndFormats.sort.getSort());
        result.topDocs(new TopDocsAndMaxScore(newTopDocs, Float.NaN), originalSortAndFormats.formats);
    }

    /**
     * Returns true if the provided <code>query</code> returns docs in index order (internal doc ids).
     * @param query The query to execute
     * @param sf The query sort
     */
    private static boolean returnsDocsInOrder(Query query, SortAndFormats sf) {
        if (sf == null || Sort.RELEVANCE.equals(sf.sort)) {
            // sort by score
            // queries that return constant scores will return docs in index
            // order since Lucene tie-breaks on the doc id
            return query.getClass() == ConstantScoreQuery.class
                || query.getClass() == MatchAllDocsQuery.class;
        } else {
            return Sort.INDEXORDER.equals(sf.sort);
        }
    }

    /**
     * Returns whether collection within the provided <code>reader</code> can be early-terminated if it sorts
     * with <code>sortAndFormats</code>.
     **/
    private static boolean canEarlyTerminate(IndexReader reader, SortAndFormats sortAndFormats) {
        if (sortAndFormats == null || sortAndFormats.sort == null) {
            return false;
        }
        final Sort sort = sortAndFormats.sort;
        for (LeafReaderContext ctx : reader.leaves()) {
            Sort indexSort = ctx.reader().getMetaData().getSort();
            if (indexSort == null || Lucene.canEarlyTerminate(sort, indexSort) == false) {
                return false;
            }
        }
        return true;
    }

    /**
     * Returns true if more than 50% of data in the index have the same value
     * The evaluation is approximation based on finding the median value and estimating its count
     */
    private static boolean indexFieldHasDuplicateData(IndexReader reader, String field) throws IOException {
        long docsNoDupl = 0; // number of docs in segments with NO duplicate data that would benefit optimization
        long docsDupl = 0; // number of docs in segments with duplicate data that would NOT benefit optimization
        for (LeafReaderContext lrc : reader.leaves()) {
            PointValues pointValues = lrc.reader().getPointValues(field);
            if (pointValues == null) continue;
            int docCount = pointValues.getDocCount();
            if (docCount <= 512) { // skipping small segments as estimateMedianCount doesn't work well on them
                continue;
            }
            assert(pointValues.size() == docCount); // TODO: modify the code to handle multiple values
            int duplDocCount = docCount/2; // expected doc count of duplicate data
            if (pointsHaveDuplicateData(pointValues, duplDocCount)) {
                docsDupl += docCount;
            } else {
                docsNoDupl += docCount;
            }
        }
        return (docsDupl > docsNoDupl);
    }

    static boolean pointsHaveDuplicateData(PointValues pointValues, int duplDocCount) throws IOException {
        long minValue = LongPoint.decodeDimension(pointValues.getMinPackedValue(), 0);
        long maxValue = LongPoint.decodeDimension(pointValues.getMaxPackedValue(), 0);
        boolean hasDuplicateData = true;
        while ((minValue < maxValue) && hasDuplicateData) {
            long midValue = Math.floorDiv(minValue, 2) + Math.floorDiv(maxValue, 2); // to avoid overflow first divide each value by 2
            long countLeft = estimatePointCount(pointValues, minValue, midValue);
            long countRight = estimatePointCount(pointValues, midValue + 1, maxValue);
            if ((countLeft >= countRight) && (countLeft > duplDocCount) ) {
                maxValue = midValue;
            } else if ((countRight > countLeft) && (countRight > duplDocCount)) {
                minValue = midValue + 1;
            } else {
                hasDuplicateData = false;
            }
        }
        return hasDuplicateData;
    }


    private static long estimatePointCount(PointValues pointValues, long minValue, long maxValue) {
        final byte[] minValueAsBytes = new byte[Long.BYTES];
        LongPoint.encodeDimension(minValue, minValueAsBytes, 0);
        final byte[] maxValueAsBytes = new byte[Long.BYTES];
        LongPoint.encodeDimension(maxValue, maxValueAsBytes, 0);

        PointValues.IntersectVisitor visitor = new PointValues.IntersectVisitor() {
            @Override
            public void grow(int count) {}

            @Override
            public void visit(int docID) {}

            @Override
            public void visit(int docID, byte[] packedValue) {}

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0 ||
                    Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, minValueAsBytes, 0, Long.BYTES) < 0 ||
                    Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, maxValueAsBytes, 0, Long.BYTES) > 0) {
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
                return PointValues.Relation.CELL_INSIDE_QUERY;
            }
        };
        return pointValues.estimatePointCount(visitor);
    }

    static class TimeExceededException extends RuntimeException {}
}
