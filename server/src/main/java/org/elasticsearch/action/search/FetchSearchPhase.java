/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.util.concurrent.AbstractRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.RescoreDocIds;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.internal.ShardSearchRequest;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.transport.Transport;

import java.util.List;
import java.util.function.BiFunction;

/**
 * This search phase merges the query results from the previous phase together and calculates the topN hits for this search.
 * Then it reaches out to all relevant shards to fetch the topN hits.
 */
final class FetchSearchPhase extends SearchPhase {
    private final ArraySearchPhaseResults<FetchSearchResult> fetchResults;
    private final AtomicArray<SearchPhaseResult> queryResults;
    private final BiFunction<InternalSearchResponse, AtomicArray<SearchPhaseResult>, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final Logger logger;
    private final SearchPhaseResults<SearchPhaseResult> resultConsumer;
    private final SearchProgressListener progressListener;
    private final AggregatedDfs aggregatedDfs;

    FetchSearchPhase(SearchPhaseResults<SearchPhaseResult> resultConsumer, AggregatedDfs aggregatedDfs, SearchPhaseContext context) {
        this(
            resultConsumer,
            aggregatedDfs,
            context,
            (response, queryPhaseResults) -> new ExpandSearchPhase(
                context,
                response,
                () -> new FetchLookupFieldsPhase(context, response, queryPhaseResults)
            )
        );
    }

    FetchSearchPhase(
        SearchPhaseResults<SearchPhaseResult> resultConsumer,
        AggregatedDfs aggregatedDfs,
        SearchPhaseContext context,
        BiFunction<InternalSearchResponse, AtomicArray<SearchPhaseResult>, SearchPhase> nextPhaseFactory
    ) {
        super("fetch");
        if (context.getNumShards() != resultConsumer.getNumShards()) {
            throw new IllegalStateException(
                "number of shards must match the length of the query results but doesn't:"
                    + context.getNumShards()
                    + "!="
                    + resultConsumer.getNumShards()
            );
        }
        this.fetchResults = new ArraySearchPhaseResults<>(resultConsumer.getNumShards());
        this.queryResults = resultConsumer.getAtomicArray();
        this.aggregatedDfs = aggregatedDfs;
        this.nextPhaseFactory = nextPhaseFactory;
        this.context = context;
        this.logger = context.getLogger();
        this.resultConsumer = resultConsumer;
        this.progressListener = context.getTask().getProgressListener();
    }

    @Override
    public void run() {
        context.execute(new AbstractRunnable() {
            @Override
            protected void doRun() throws Exception {
                // we do the heavy lifting in this inner run method where we reduce aggs etc. that's why we fork this phase
                // off immediately instead of forking when we send back the response to the user since there we only need
                // to merge together the fetched results which is a linear operation.
                innerRun();
            }

            @Override
            public void onFailure(Exception e) {
                context.onPhaseFailure(FetchSearchPhase.this, "", e);
            }
        });
    }

    private void innerRun() throws Exception {
        final int numShards = context.getNumShards();
        final boolean isScrollSearch = context.getRequest().scroll() != null;
        final List<SearchPhaseResult> phaseResults = queryResults.asList();
        final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = resultConsumer.reduce();
        final boolean queryAndFetchOptimization = queryResults.length() == 1;
        final Runnable finishPhase = () -> moveToNextPhase(
            queryResults,
            reducedQueryPhase,
            queryAndFetchOptimization ? queryResults : fetchResults.getAtomicArray()
        );
        if (queryAndFetchOptimization) {
            assert phaseResults.isEmpty() || phaseResults.get(0).fetchResult() != null
                : "phaseResults empty [" + phaseResults.isEmpty() + "], single result: " + phaseResults.get(0).fetchResult();
            // query AND fetch optimization
            finishPhase.run();
        } else {
            ScoreDoc[] scoreDocs = reducedQueryPhase.sortedTopDocs().scoreDocs();
            final IntArrayList[] docIdsToLoad = SearchPhaseController.fillDocIdsToLoad(numShards, scoreDocs);
            // no docs to fetch -- sidestep everything and return
            if (scoreDocs.length == 0) {
                // we have to release contexts here to free up resources
                phaseResults.stream().map(SearchPhaseResult::queryResult).forEach(this::releaseIrrelevantSearchContext);
                finishPhase.run();
            } else {
                final ScoreDoc[] lastEmittedDocPerShard = isScrollSearch
                    ? SearchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, numShards)
                    : null;
                final CountedCollector<FetchSearchResult> counter = new CountedCollector<>(
                    fetchResults,
                    docIdsToLoad.length, // we count down every shard in the result no matter if we got any results or not
                    finishPhase,
                    context
                );
                for (int i = 0; i < docIdsToLoad.length; i++) {
                    IntArrayList entry = docIdsToLoad[i];
                    SearchPhaseResult queryResult = queryResults.get(i);
                    if (entry == null) { // no results for this shard ID
                        if (queryResult != null) {
                            // if we got some hits from this shard we have to release the context there
                            // we do this as we go since it will free up resources and passing on the request on the
                            // transport layer is cheap.
                            releaseIrrelevantSearchContext(queryResult.queryResult());
                            progressListener.notifyFetchResult(i);
                        }
                        // in any case we count down this result since we don't talk to this shard anymore
                        counter.countDown();
                    } else {
                        SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
                        Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
                        ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(
                            queryResult.queryResult().getContextId(),
                            i,
                            entry,
                            lastEmittedDocPerShard,
                            context.getOriginalIndices(queryResult.getShardIndex()),
                            queryResult.getShardSearchRequest(),
                            queryResult.getRescoreDocIds()
                        );
                        executeFetch(
                            queryResult.getShardIndex(),
                            shardTarget,
                            counter,
                            fetchSearchRequest,
                            queryResult.queryResult(),
                            connection
                        );
                    }
                }
            }
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(
        ShardSearchContextId contextId,
        int index,
        IntArrayList entry,
        ScoreDoc[] lastEmittedDocPerShard,
        OriginalIndices originalIndices,
        ShardSearchRequest shardSearchRequest,
        RescoreDocIds rescoreDocIds
    ) {
        final ScoreDoc lastEmittedDoc = (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[index] : null;
        return new ShardFetchSearchRequest(
            originalIndices,
            contextId,
            shardSearchRequest,
            entry,
            lastEmittedDoc,
            rescoreDocIds,
            aggregatedDfs
        );
    }

    private void executeFetch(
        final int shardIndex,
        final SearchShardTarget shardTarget,
        final CountedCollector<FetchSearchResult> counter,
        final ShardFetchSearchRequest fetchSearchRequest,
        final QuerySearchResult querySearchResult,
        final Transport.Connection connection
    ) {
        context.getSearchTransport()
            .sendExecuteFetch(
                connection,
                fetchSearchRequest,
                context.getTask(),
                new SearchActionListener<FetchSearchResult>(shardTarget, shardIndex) {
                    @Override
                    public void innerOnResponse(FetchSearchResult result) {
                        try {
                            progressListener.notifyFetchResult(shardIndex);
                            counter.onResult(result);
                        } catch (Exception e) {
                            context.onPhaseFailure(FetchSearchPhase.this, "", e);
                        }
                    }

                    @Override
                    public void onFailure(Exception e) {
                        try {
                            logger.debug(
                                () -> new ParameterizedMessage("[{}] Failed to execute fetch phase", fetchSearchRequest.contextId()),
                                e
                            );
                            progressListener.notifyFetchFailure(shardIndex, shardTarget, e);
                            counter.onFailure(shardIndex, shardTarget, e);
                        } finally {
                            // the search context might not be cleared on the node where the fetch was executed for example
                            // because the action was rejected by the thread pool. in this case we need to send a dedicated
                            // request to clear the search context.
                            releaseIrrelevantSearchContext(querySearchResult);
                        }
                    }
                }
            );
    }

    /**
     * Releases shard targets that are not used in the docsIdsToLoad.
     */
    private void releaseIrrelevantSearchContext(QuerySearchResult queryResult) {
        // we only release search context that we did not fetch from, if we are not scrolling
        // or using a PIT and if it has at least one hit that didn't make it to the global topDocs
        if (queryResult.hasSearchContext()
            && context.getRequest().scroll() == null
            && (context.isPartOfPointInTime(queryResult.getContextId()) == false)) {
            try {
                SearchShardTarget shardTarget = queryResult.getSearchShardTarget();
                Transport.Connection connection = context.getConnection(shardTarget.getClusterAlias(), shardTarget.getNodeId());
                context.sendReleaseSearchContext(
                    queryResult.getContextId(),
                    connection,
                    context.getOriginalIndices(queryResult.getShardIndex())
                );
            } catch (Exception e) {
                context.getLogger().trace("failed to release context", e);
            }
        }
    }

    private void moveToNextPhase(
        AtomicArray<SearchPhaseResult> queryPhaseResults,
        SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
        AtomicArray<? extends SearchPhaseResult> fetchResultsArr
    ) {
        final InternalSearchResponse internalResponse = SearchPhaseController.merge(
            context.getRequest().scroll() != null,
            reducedQueryPhase,
            fetchResultsArr.asList(),
            fetchResultsArr::get
        );
        context.executeNextPhase(this, nextPhaseFactory.apply(internalResponse, queryPhaseResults));
    }
}
