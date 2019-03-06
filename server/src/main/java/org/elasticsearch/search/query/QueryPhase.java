/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.query;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.IndexOptions;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.MultiDocValues;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.queries.MinDocQuery;
import org.apache.lucene.queries.SearchAfterSortedDocQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.FieldComparatorSource;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.util.InPlaceMergeSorter;
import org.elasticsearch.action.search.SearchTask;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.concurrent.QueueResizingEsThreadPoolExecutor;
import org.elasticsearch.index.fielddata.NumericDoubleValues;
import org.elasticsearch.index.fielddata.fieldcomparator.LongValuesComparatorSource;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhase;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationPhase;
import org.elasticsearch.search.internal.ContextIndexSearcher;
import org.elasticsearch.search.internal.ScrollContext;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.profile.query.InternalProfileCollector;
import org.elasticsearch.search.rescore.RescorePhase;
import org.elasticsearch.search.sort.SortAndFormats;
import org.elasticsearch.search.suggest.SuggestPhase;
import org.elasticsearch.tasks.TaskCancelledException;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.LinkedList;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

import static org.elasticsearch.search.query.QueryCollectorContext.createCancellableCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createEarlyTerminationCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createFilteredCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createMinScoreCollectorContext;
import static org.elasticsearch.search.query.QueryCollectorContext.createMultiCollectorContext;
import static org.elasticsearch.search.query.TopDocsCollectorContext.createTopDocsCollectorContext;


/**
 * Query phase of a search request, used to run the query and get back from each shard information about the matching documents
 * (document ids and score or sort criteria) so that matches can be reduced on the coordinating node
 */
public class QueryPhase implements SearchPhase {
    private static final Logger LOGGER = LogManager.getLogger(QueryPhase.class);

    private final AggregationPhase aggregationPhase;
    private final SuggestPhase suggestPhase;
    private final RescorePhase rescorePhase;

    public QueryPhase() {
        this.aggregationPhase = new AggregationPhase();
        this.suggestPhase = new SuggestPhase();
        this.rescorePhase = new RescorePhase();
    }

    @Override
    public void preProcess(SearchContext context) {
        context.preProcess(true);
    }

    @Override
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
        final ContextIndexSearcher searcher = searchContext.searcher();
        boolean rescore = execute(searchContext, searchContext.searcher(), searcher::setCheckCancelled);

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
    static boolean execute(SearchContext searchContext,
                           final IndexSearcher searcher,
                           Consumer<Runnable> checkCancellationSetter) throws QueryPhaseExecutionException {
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

            // rewrite numeric sort to the optimized distanceFeatureQuery
            if (searchContext.sort() != null) {
                try {
                    Query distanceFeatureQuery = tryRewriteNumericLongSort(searchContext, searcher.getIndexReader());
                    if (distanceFeatureQuery != null) {
                        query = new BooleanQuery.Builder()
                            .add(query, BooleanClause.Occur.FILTER) // filter for original query
                            .add(distanceFeatureQuery, BooleanClause.Occur.SHOULD) //should for DistanceFeatureQuery
                            .build();
                        sortAndFormatsForRewrittenNumericSort = searchContext.sort(); //stash SortAndFormats to restore it later
                        searchContext.sort(null);
                    }
                } finally {
                    // in case of errors do nothing - keep the same query
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

            boolean timeoutSet = scrollContext == null && searchContext.timeout() != null &&
                searchContext.timeout().equals(SearchService.NO_TIMEOUT) == false;

            final Runnable timeoutRunnable;
            if (timeoutSet) {
                final long startTime = searchContext.getRelativeTimeInMillis();
                final long timeout = searchContext.timeout().millis();
                final long maxTime = startTime + timeout;
                timeoutRunnable = () -> {
                    final long time = searchContext.getRelativeTimeInMillis();
                    if (time > maxTime) {
                        throw new TimeExceededException();
                    }
                };
            } else {
                timeoutRunnable = null;
            }

            final Runnable cancellationRunnable;
            if (searchContext.lowLevelCancellation()) {
                SearchTask task = searchContext.getTask();
                cancellationRunnable = () -> { if (task.isCancelled()) throw new TaskCancelledException("cancelled"); };
            } else {
                cancellationRunnable = null;
            }

            final Runnable checkCancelled;
            if (timeoutRunnable != null && cancellationRunnable != null) {
                checkCancelled = () -> {
                    timeoutRunnable.run();
                    cancellationRunnable.run();
                };
            } else if (timeoutRunnable != null) {
                checkCancelled = timeoutRunnable;
            } else if (cancellationRunnable != null) {
                checkCancelled = cancellationRunnable;
            } else {
                checkCancelled = null;
            }

            checkCancellationSetter.accept(checkCancelled);

            // add cancellable
            // this only performs segment-level cancellation, which is cheap and checked regardless of
            // searchContext.lowLevelCancellation()
            collectors.add(createCancellableCollectorContext(searchContext.getTask()::isCancelled));

            final boolean doProfile = searchContext.getProfilers() != null;
            // create the top docs collector last when the other collectors are known
            final TopDocsCollectorContext topDocsFactory = createTopDocsCollectorContext(searchContext, reader, hasFilterCollector);
            // add the top docs collector, the first collector context in the chain
            collectors.addFirst(topDocsFactory);

            final Collector queryCollector;
            if (doProfile) {
                InternalProfileCollector profileCollector = QueryCollectorContext.createQueryCollectorWithProfiler(collectors);
                searchContext.getProfilers().getCurrentQueryProfiler().setCollector(profileCollector);
                queryCollector = profileCollector;
            } else {
               queryCollector = QueryCollectorContext.createQueryCollector(collectors);
            }

            try {
                searcher.search(query, queryCollector);
            } catch (EarlyTerminatingCollector.EarlyTerminationException e) {
                queryResult.terminatedEarly(true);
            } catch (TimeExceededException e) {
                assert timeoutSet : "TimeExceededException thrown even though timeout wasn't set";

                if (searchContext.request().allowPartialSearchResults() == false) {
                    // Can't rethrow TimeExceededException because not serializable
                    throw new QueryPhaseExecutionException(searchContext, "Time exceeded");
                }
                queryResult.searchTimedOut(true);
            } finally {
                searchContext.clearReleasables(SearchContext.Lifetime.COLLECTION);
            }
            if (searchContext.terminateAfter() != SearchContext.DEFAULT_TERMINATE_AFTER
                    && queryResult.terminatedEarly() == null) {
                queryResult.terminatedEarly(false);
            }

            final QuerySearchResult result = searchContext.queryResult();
            for (QueryCollectorContext ctx : collectors) {
                ctx.postProcess(result);
            }

            // if we rewrote numeric sort, rewrite the result back to fieldDocs
            if (sortAndFormatsForRewrittenNumericSort != null) {
                searchContext.sort(sortAndFormatsForRewrittenNumericSort); // restore SortAndFormats
                TopDocs topDocs = convertToTopFieldDocs(searchContext.sort().sort.getSort(), reader, result);
                result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), searchContext.sort().formats);
            }

            ExecutorService executor = searchContext.indexShard().getThreadPool().executor(ThreadPool.Names.SEARCH);
            if (executor instanceof QueueResizingEsThreadPoolExecutor) {
                QueueResizingEsThreadPoolExecutor rExecutor = (QueueResizingEsThreadPoolExecutor) executor;
                queryResult.nodeQueueSize(rExecutor.getCurrentQueueSize());
                queryResult.serviceTimeEWMA((long) rExecutor.getTaskExecutionEWMA());
            }
            if (searchContext.getProfilers() != null) {
                ProfileShardResult shardResults = SearchProfileShardResults.buildShardResults(searchContext.getProfilers());
                result.profileResults(shardResults);
            }
            return topDocsFactory.shouldRescore();
        } catch (Exception e) {
            throw new QueryPhaseExecutionException(searchContext, "Failed to execute main query", e);
        }
    }

     private static Query tryRewriteNumericLongSort(SearchContext searchContext, IndexReader reader) throws IOException {
        // check sort
        Sort sort = searchContext.sort().sort;
        if (Sort.RELEVANCE.equals(sort)) return null;
        if (Sort.INDEXORDER.equals(sort)) return null;
        if (sort.getSort().length > 1) return null; // we need only a single sort
        SortField sortField = sort.getSort()[0];

        // check if this is a field of type Long, that is indexed and has doc values
        String fieldName = sortField.getField();
        final MappedFieldType fieldType = searchContext.mapperService().fullName(fieldName);
        if (fieldType == null) return  null;
        if (fieldType.typeName() != "long") return null;
        if (fieldType.indexOptions() == IndexOptions.NONE) return null;
        if (fieldType.hasDocValues() == false) return null;
        byte[] minValueBytes =  PointValues.getMinPackedValue(reader, fieldName);
        byte[] maxValueBytes =  PointValues.getMaxPackedValue(reader, fieldName);
        if ((maxValueBytes == null) || (minValueBytes == null)) return null; // no values on this shard

        // finally all checks done, query can be rewritten, rewrite
        long minValue = LongPoint.decodeDimension(minValueBytes, 0);
        long maxValue = LongPoint.decodeDimension(maxValueBytes, 0);
        final long origin = (sortField.getReverse()) ? maxValue : minValue;
        final long pivotDistance = maxValue == minValue ? maxValue/2 : (maxValue - minValue)/2;
        return LongPoint.newDistanceFeatureQuery(sortField.getField(), 1, origin, pivotDistance);
    }

    static TopDocs convertToTopFieldDocs(SortField[] sortFields, IndexReader reader, QuerySearchResult result) throws IOException {
        // 1. sort docIds, so DocValues iterator can iterate over docIds in ascending order
        // keep their original positions in positions
        TopDocsAndMaxScore topDocsAndMaxScore = result.topDocs();
        ScoreDoc[] oldScoreDocs = topDocsAndMaxScore.topDocs.scoreDocs;
        int[] docIds = new int[oldScoreDocs.length];
        int[] positions = new int[oldScoreDocs.length];
        for (int i = 0; i < oldScoreDocs.length; i++) {
            docIds[i] = oldScoreDocs[i].doc;
            positions[i] = i;
        }
        new InPlaceMergeSorter() {
            @Override
            public int compare(int i, int j) {
                return Integer.compare(docIds[i], docIds[j]);
            }
            @Override
            public void swap(int i, int j) {
                int tempDocId = docIds[i];
                docIds[i] = docIds[j];
                docIds[j] = tempDocId;

                int pos = positions[i];
                positions[i] = positions[j];
                positions[j] = pos;
            }
        }.sort(0, docIds.length);

        // 2. Iterate over DocValues and fill fieldDocs
        Long missingValue = (Long) sortFields[0].getMissingValue(); // for rewritten numeric sort, when we had a single sort on Long field
        SortedNumericDocValues sdvs = MultiDocValues.getSortedNumericValues(reader, sortFields[0].getField());
        FieldDoc[] newFieldDocs = new FieldDoc[oldScoreDocs.length];
        for (int i = 0; i < docIds.length; i++) {
            Long[] fieldValues = {sdvs.advanceExact(docIds[i]) ? sdvs.nextValue() : missingValue};
            newFieldDocs[positions[i]] = new FieldDoc(docIds[i], Float.NaN, fieldValues, oldScoreDocs[i].shardIndex);
        }
        return new TopFieldDocs(topDocsAndMaxScore.topDocs.totalHits, newFieldDocs, sortFields);
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

    private static class TimeExceededException extends RuntimeException {}
}
