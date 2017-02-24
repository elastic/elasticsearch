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
package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.logging.log4j.util.Supplier;
import org.apache.lucene.search.ScoreDoc;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRunnable;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.ShardFetchSearchRequest;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.transport.Transport;

import java.io.IOException;
import java.util.List;
import java.util.function.Function;

/**
 * This search phase merges the query results from the previous phase together and calculates the topN hits for this search.
 * Then it reaches out to all relevant shards to fetch the topN hits.
 */
final class FetchSearchPhase extends SearchPhase {
    private final AtomicArray<FetchSearchResult> fetchResults;
    private final SearchPhaseController searchPhaseController;
    private final AtomicArray<QuerySearchResultProvider> queryResults;
    private final Function<SearchResponse, SearchPhase> nextPhaseFactory;
    private final SearchPhaseContext context;
    private final Logger logger;
    private final InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> resultConsumer;

    FetchSearchPhase(InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> resultConsumer,
                     SearchPhaseController searchPhaseController,
                     SearchPhaseContext context) {
        this(resultConsumer, searchPhaseController, context,
            (response) -> new ExpandSearchPhase(context, response, // collapse only happens if the request has inner hits
                (finalResponse) -> sendResponsePhase(finalResponse, context)));
    }

    FetchSearchPhase(InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> resultConsumer,
                     SearchPhaseController searchPhaseController,
                     SearchPhaseContext context, Function<SearchResponse, SearchPhase> nextPhaseFactory) {
        super("fetch");
        if (context.getNumShards() != resultConsumer.getNumShards()) {
            throw new IllegalStateException("number of shards must match the length of the query results but doesn't:"
                + context.getNumShards() + "!=" + resultConsumer.getNumShards());
        }
        this.fetchResults = new AtomicArray<>(resultConsumer.getNumShards());
        this.searchPhaseController = searchPhaseController;
        this.queryResults = resultConsumer.results;
        this.nextPhaseFactory =  nextPhaseFactory;
        this.context = context;
        this.logger = context.getLogger();
        this.resultConsumer = resultConsumer;

    }

    @Override
    public void run() throws IOException {
        context.execute(new ActionRunnable<SearchResponse>(context) {
            @Override
            public void doRun() throws IOException {
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

    private void innerRun() throws IOException {
        final int numShards = context.getNumShards();
        final boolean isScrollSearch = context.getRequest().scroll() != null;
        ScoreDoc[] sortedShardDocs = searchPhaseController.sortDocs(isScrollSearch, queryResults);
        String scrollId = isScrollSearch ? TransportSearchHelper.buildScrollId(queryResults) : null;
        List<AtomicArray.Entry<QuerySearchResultProvider>> queryResultsAsList = queryResults.asList();
        final SearchPhaseController.ReducedQueryPhase reducedQueryPhase = resultConsumer.reduce();
        final boolean queryAndFetchOptimization = queryResults.length() == 1;
        final Runnable finishPhase = ()
            -> moveToNextPhase(searchPhaseController, sortedShardDocs, scrollId, reducedQueryPhase, queryAndFetchOptimization ?
            queryResults : fetchResults);
        if (queryAndFetchOptimization) {
            assert queryResults.get(0) == null || queryResults.get(0).fetchResult() != null;
            // query AND fetch optimization
            finishPhase.run();
        } else {
            final IntArrayList[] docIdsToLoad = searchPhaseController.fillDocIdsToLoad(numShards, sortedShardDocs);
            if (sortedShardDocs.length == 0) { // no docs to fetch -- sidestep everything and return
                queryResultsAsList.stream()
                    .map(e -> e.value.queryResult())
                    .forEach(this::releaseIrrelevantSearchContext); // we have to release contexts here to free up resources
                finishPhase.run();
            } else {
                final ScoreDoc[] lastEmittedDocPerShard = isScrollSearch ?
                    searchPhaseController.getLastEmittedDocPerShard(reducedQueryPhase, sortedShardDocs, numShards)
                    : null;
                final CountedCollector<FetchSearchResult> counter = new CountedCollector<>(fetchResults::set,
                    docIdsToLoad.length, // we count down every shard in the result no matter if we got any results or not
                    finishPhase, context);
                for (int i = 0; i < docIdsToLoad.length; i++) {
                    IntArrayList entry = docIdsToLoad[i];
                    QuerySearchResultProvider queryResult = queryResults.get(i);
                    if (entry == null) { // no results for this shard ID
                        if (queryResult != null) {
                            // if we got some hits from this shard we have to release the context there
                            // we do this as we go since it will free up resources and passing on the request on the
                            // transport layer is cheap.
                            releaseIrrelevantSearchContext(queryResult.queryResult());
                        }
                        // in any case we count down this result since we don't talk to this shard anymore
                        counter.countDown();
                    } else {
                        Transport.Connection connection = context.getConnection(queryResult.shardTarget().getNodeId());
                        ShardFetchSearchRequest fetchSearchRequest = createFetchRequest(queryResult.queryResult().id(), i, entry,
                            lastEmittedDocPerShard);
                        executeFetch(i, queryResult.shardTarget(), counter, fetchSearchRequest, queryResult.queryResult(),
                            connection);
                    }
                }
            }
        }
    }

    protected ShardFetchSearchRequest createFetchRequest(long queryId, int index, IntArrayList entry,
                                                               ScoreDoc[] lastEmittedDocPerShard) {
        final ScoreDoc lastEmittedDoc = (lastEmittedDocPerShard != null) ? lastEmittedDocPerShard[index] : null;
        return new ShardFetchSearchRequest(context.getRequest(), queryId, entry, lastEmittedDoc);
    }

    private void executeFetch(final int shardIndex, final SearchShardTarget shardTarget,
                              final CountedCollector<FetchSearchResult> counter,
                              final ShardFetchSearchRequest fetchSearchRequest, final QuerySearchResult querySearchResult,
                              final Transport.Connection connection) {
        context.getSearchTransport().sendExecuteFetch(connection, fetchSearchRequest, context.getTask(),
            new ActionListener<FetchSearchResult>() {
                @Override
                public void onResponse(FetchSearchResult result) {
                    counter.onResult(shardIndex, result, shardTarget);
                }

                @Override
                public void onFailure(Exception e) {
                    try {
                        if (logger.isDebugEnabled()) {
                            logger.debug((Supplier<?>) () -> new ParameterizedMessage("[{}] Failed to execute fetch phase",
                                fetchSearchRequest.id()), e);
                        }
                        counter.onFailure(shardIndex, shardTarget, e);
                    } finally {
                        // the search context might not be cleared on the node where the fetch was executed for example
                        // because the action was rejected by the thread pool. in this case we need to send a dedicated
                        // request to clear the search context.
                        releaseIrrelevantSearchContext(querySearchResult);
                    }
                }
            });
    }

    /**
     * Releases shard targets that are not used in the docsIdsToLoad.
     */
    private void releaseIrrelevantSearchContext(QuerySearchResult queryResult) {
        // we only release search context that we did not fetch from if we are not scrolling
        // and if it has at lease one hit that didn't make it to the global topDocs
        if (context.getRequest().scroll() == null && queryResult.hasHits()) {
            try {
                Transport.Connection connection = context.getConnection(queryResult.shardTarget().getNodeId());
                context.sendReleaseSearchContext(queryResult.id(), connection);
            } catch (Exception e) {
                context.getLogger().trace("failed to release context", e);
            }
        }
    }

    private void moveToNextPhase(SearchPhaseController searchPhaseController, ScoreDoc[] sortedDocs,
                                 String scrollId, SearchPhaseController.ReducedQueryPhase reducedQueryPhase,
                                 AtomicArray<? extends QuerySearchResultProvider> fetchResultsArr) {
        final InternalSearchResponse internalResponse = searchPhaseController.merge(context.getRequest().scroll() != null,
            sortedDocs, reducedQueryPhase, fetchResultsArr);
        context.executeNextPhase(this, nextPhaseFactory.apply(context.buildSearchResponse(internalResponse, scrollId)));
    }

    private static SearchPhase sendResponsePhase(SearchResponse response, SearchPhaseContext context) {
        return new SearchPhase("response") {
            @Override
            public void run() throws IOException {
                context.onResponse(response);
            }
        };
    }
}
