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
import com.carrotsearch.hppc.ObjectObjectHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.io.stream.DelayableWriteable;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public final class SearchPhaseController {
    private static final Logger logger = LogManager.getLogger(SearchPhaseController.class);
    private static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder;
    private final ThreadPool threadPool;

    public SearchPhaseController(NamedWriteableRegistry namedWriteableRegistry,
            Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder,
            ThreadPool threadPool) {
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.requestToAggReduceContextBuilder = requestToAggReduceContextBuilder;
        this.threadPool = threadPool;
    }

    public AggregatedDfs aggregateDfs(Collection<DfsSearchResult> results) {
        ObjectObjectHashMap<Term, TermStatistics> termStatistics = HppcMaps.newNoNullKeysMap();
        ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
        long aggMaxDoc = 0;
        for (DfsSearchResult lEntry : results) {
            final Term[] terms = lEntry.terms();
            final TermStatistics[] stats = lEntry.termStatistics();
            assert terms.length == stats.length;
            for (int i = 0; i < terms.length; i++) {
                assert terms[i] != null;
                if (stats[i] == null) {
                    continue;
                }
                TermStatistics existing = termStatistics.get(terms[i]);
                if (existing != null) {
                    assert terms[i].bytes().equals(existing.term());
                    termStatistics.put(terms[i], new TermStatistics(existing.term(),
                        existing.docFreq() + stats[i].docFreq(),
                        existing.totalTermFreq() + stats[i].totalTermFreq()));
                } else {
                    termStatistics.put(terms[i], stats[i]);
                }

            }

            assert !lEntry.fieldStatistics().containsKey(null);
            final Object[] keys = lEntry.fieldStatistics().keys;
            final Object[] values = lEntry.fieldStatistics().values;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != null) {
                    String key = (String) keys[i];
                    CollectionStatistics value = (CollectionStatistics) values[i];
                    if (value == null) {
                        continue;
                    }
                    assert key != null;
                    CollectionStatistics existing = fieldStatistics.get(key);
                    if (existing != null) {
                        CollectionStatistics merged = new CollectionStatistics(key,
                            existing.maxDoc() + value.maxDoc(),
                            existing.docCount() + value.docCount(),
                            existing.sumTotalTermFreq() + value.sumTotalTermFreq(),
                            existing.sumDocFreq() + value.sumDocFreq()
                        );
                        fieldStatistics.put(key, merged);
                    } else {
                        fieldStatistics.put(key, value);
                    }
                }
            }
            aggMaxDoc += lEntry.maxDoc();
        }
        return new AggregatedDfs(termStatistics, fieldStatistics, aggMaxDoc);
    }

    /**
     * Returns a score doc array of top N search docs across all shards, followed by top suggest docs for each
     * named completion suggestion across all shards. If more than one named completion suggestion is specified in the
     * request, the suggest docs for a named suggestion are ordered by the suggestion name.
     *
     * Note: The order of the sorted score docs depends on the shard index in the result array if the merge process needs to disambiguate
     * the result. In oder to obtain stable results the shard index (index of the result in the result array) must be the same.
     *
     * @param ignoreFrom Whether to ignore the from and sort all hits in each shard result.
     *                   Enabled only for scroll search, because that only retrieves hits of length 'size' in the query phase.
     * @param results the search phase results to obtain the sort docs from
     * @param bufferedTopDocs the pre-consumed buffered top docs
     * @param topDocsStats the top docs stats to fill
     * @param from the offset into the search results top docs
     * @param size the number of hits to return from the merged top docs
     */
    static SortedTopDocs sortDocs(boolean ignoreFrom, Collection<? extends SearchPhaseResult> results,
                                  final Collection<TopDocs> bufferedTopDocs, final TopDocsStats topDocsStats, int from, int size,
                                  List<CompletionSuggestion> reducedCompletionSuggestions) {
        if (results.isEmpty()) {
            return SortedTopDocs.EMPTY;
        }
        final Collection<TopDocs> topDocs = bufferedTopDocs == null ? new ArrayList<>() : bufferedTopDocs;
        for (SearchPhaseResult sortedResult : results) { // TODO we can move this loop into the reduce call to only loop over this once
            /* We loop over all results once, group together the completion suggestions if there are any and collect relevant
             * top docs results. Each top docs gets it's shard index set on all top docs to simplify top docs merging down the road
             * this allowed to remove a single shared optimization code here since now we don't materialized a dense array of
             * top docs anymore but instead only pass relevant results / top docs to the merge method*/
            QuerySearchResult queryResult = sortedResult.queryResult();
            if (queryResult.hasConsumedTopDocs() == false) { // already consumed?
                final TopDocsAndMaxScore td = queryResult.consumeTopDocs();
                assert td != null;
                topDocsStats.add(td, queryResult.searchTimedOut(), queryResult.terminatedEarly());
                // make sure we set the shard index before we add it - the consumer didn't do that yet
                if (td.topDocs.scoreDocs.length > 0) {
                    setShardIndex(td.topDocs, queryResult.getShardIndex());
                    topDocs.add(td.topDocs);
                }
            }
        }
        final boolean hasHits = (reducedCompletionSuggestions.isEmpty() && topDocs.isEmpty()) == false;
        if (hasHits) {
            final TopDocs mergedTopDocs = mergeTopDocs(topDocs, size, ignoreFrom ? 0 : from);
            final ScoreDoc[] mergedScoreDocs = mergedTopDocs == null ? EMPTY_DOCS : mergedTopDocs.scoreDocs;
            ScoreDoc[] scoreDocs = mergedScoreDocs;
            if (reducedCompletionSuggestions.isEmpty() == false) {
                int numSuggestDocs = 0;
                for (CompletionSuggestion completionSuggestion : reducedCompletionSuggestions) {
                    assert completionSuggestion != null;
                    numSuggestDocs += completionSuggestion.getOptions().size();
                }
                scoreDocs = new ScoreDoc[mergedScoreDocs.length + numSuggestDocs];
                System.arraycopy(mergedScoreDocs, 0, scoreDocs, 0, mergedScoreDocs.length);
                int offset = mergedScoreDocs.length;
                for (CompletionSuggestion completionSuggestion : reducedCompletionSuggestions) {
                    for (CompletionSuggestion.Entry.Option option : completionSuggestion.getOptions()) {
                        scoreDocs[offset++] = option.getDoc();
                    }
                }
            }
            boolean isSortedByField = false;
            SortField[] sortFields = null;
            String collapseField = null;
            Object[] collapseValues = null;
            if (mergedTopDocs instanceof TopFieldDocs) {
                TopFieldDocs fieldDocs = (TopFieldDocs) mergedTopDocs;
                sortFields = fieldDocs.fields;
                if (fieldDocs instanceof CollapseTopFieldDocs) {
                    isSortedByField = (fieldDocs.fields.length == 1 && fieldDocs.fields[0].getType() == SortField.Type.SCORE) == false;
                    CollapseTopFieldDocs collapseTopFieldDocs = (CollapseTopFieldDocs) fieldDocs;
                    collapseField = collapseTopFieldDocs.field;
                    collapseValues = collapseTopFieldDocs.collapseValues;
                } else {
                    isSortedByField = true;
                }
            }
            return new SortedTopDocs(scoreDocs, isSortedByField, sortFields, collapseField, collapseValues);
        } else {
            // no relevant docs
            return SortedTopDocs.EMPTY;
        }
    }

    static TopDocs mergeTopDocs(Collection<TopDocs> results, int topN, int from) {
        if (results.isEmpty()) {
            return null;
        }
        final boolean setShardIndex = false;
        final TopDocs topDocs = results.stream().findFirst().get();
        final TopDocs mergedTopDocs;
        final int numShards = results.size();
        if (numShards == 1 && from == 0) { // only one shard and no pagination we can just return the topDocs as we got them.
            return topDocs;
        } else if (topDocs instanceof CollapseTopFieldDocs) {
            CollapseTopFieldDocs firstTopDocs = (CollapseTopFieldDocs) topDocs;
            final Sort sort = new Sort(firstTopDocs.fields);
            final CollapseTopFieldDocs[] shardTopDocs = results.toArray(new CollapseTopFieldDocs[numShards]);
            mergedTopDocs = CollapseTopFieldDocs.merge(sort, from, topN, shardTopDocs, setShardIndex);
        } else if (topDocs instanceof TopFieldDocs) {
            TopFieldDocs firstTopDocs = (TopFieldDocs) topDocs;
            final Sort sort = new Sort(firstTopDocs.fields);
            final TopFieldDocs[] shardTopDocs = results.toArray(new TopFieldDocs[numShards]);
            mergedTopDocs = TopDocs.merge(sort, from, topN, shardTopDocs, setShardIndex);
        } else {
            final TopDocs[] shardTopDocs = results.toArray(new TopDocs[numShards]);
            mergedTopDocs = TopDocs.merge(from, topN, shardTopDocs, setShardIndex);
        }
        return mergedTopDocs;
    }

    private static void setShardIndex(TopDocs topDocs, int shardIndex) {
        assert topDocs.scoreDocs.length == 0 || topDocs.scoreDocs[0].shardIndex == -1 : "shardIndex is already set";
        for (ScoreDoc doc : topDocs.scoreDocs) {
            doc.shardIndex = shardIndex;
        }
    }

    public ScoreDoc[] getLastEmittedDocPerShard(ReducedQueryPhase reducedQueryPhase, int numShards) {
        final ScoreDoc[] lastEmittedDocPerShard = new ScoreDoc[numShards];
        if (reducedQueryPhase.isEmptyResult == false) {
            final ScoreDoc[] sortedScoreDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
            // from is always zero as when we use scroll, we ignore from
            long size = Math.min(reducedQueryPhase.fetchHits, reducedQueryPhase.size);
            // with collapsing we can have more hits than sorted docs
            size = Math.min(sortedScoreDocs.length, size);
            for (int sortedDocsIndex = 0; sortedDocsIndex < size; sortedDocsIndex++) {
                ScoreDoc scoreDoc = sortedScoreDocs[sortedDocsIndex];
                lastEmittedDocPerShard[scoreDoc.shardIndex] = scoreDoc;
            }
        }
        return lastEmittedDocPerShard;
    }

    /**
     * Builds an array, with potential null elements, with docs to load.
     */
    public IntArrayList[] fillDocIdsToLoad(int numShards, ScoreDoc[] shardDocs) {
        IntArrayList[] docIdsToLoad = new IntArrayList[numShards];
        for (ScoreDoc shardDoc : shardDocs) {
            IntArrayList shardDocIdsToLoad = docIdsToLoad[shardDoc.shardIndex];
            if (shardDocIdsToLoad == null) {
                shardDocIdsToLoad = docIdsToLoad[shardDoc.shardIndex] = new IntArrayList();
            }
            shardDocIdsToLoad.add(shardDoc.doc);
        }
        return docIdsToLoad;
    }

    /**
     * Enriches search hits and completion suggestion hits from <code>sortedDocs</code> using <code>fetchResultsArr</code>,
     * merges suggestions, aggregations and profile results
     *
     * Expects sortedDocs to have top search docs across all shards, optionally followed by top suggest docs for each named
     * completion suggestion ordered by suggestion name
     */
    public InternalSearchResponse merge(boolean ignoreFrom, ReducedQueryPhase reducedQueryPhase,
                                        Collection<? extends SearchPhaseResult> fetchResults,
                                        IntFunction<SearchPhaseResult> resultsLookup) {
        if (reducedQueryPhase.isEmptyResult) {
            return InternalSearchResponse.empty();
        }
        ScoreDoc[] sortedDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
        SearchHits hits = getHits(reducedQueryPhase, ignoreFrom, fetchResults, resultsLookup);
        if (reducedQueryPhase.suggest != null) {
            if (!fetchResults.isEmpty()) {
                int currentOffset = hits.getHits().length;
                for (CompletionSuggestion suggestion : reducedQueryPhase.suggest.filter(CompletionSuggestion.class)) {
                    final List<CompletionSuggestion.Entry.Option> suggestionOptions = suggestion.getOptions();
                    for (int scoreDocIndex = currentOffset; scoreDocIndex < currentOffset + suggestionOptions.size(); scoreDocIndex++) {
                        ScoreDoc shardDoc = sortedDocs[scoreDocIndex];
                        SearchPhaseResult searchResultProvider = resultsLookup.apply(shardDoc.shardIndex);
                        if (searchResultProvider == null) {
                            // this can happen if we are hitting a shard failure during the fetch phase
                            // in this case we referenced the shard result via the ScoreDoc but never got a
                            // result from fetch.
                            // TODO it would be nice to assert this in the future
                            continue;
                        }
                        FetchSearchResult fetchResult = searchResultProvider.fetchResult();
                        final int index = fetchResult.counterGetAndIncrement();
                        assert index < fetchResult.hits().getHits().length : "not enough hits fetched. index [" + index + "] length: "
                            + fetchResult.hits().getHits().length;
                        SearchHit hit = fetchResult.hits().getHits()[index];
                        CompletionSuggestion.Entry.Option suggestOption =
                            suggestionOptions.get(scoreDocIndex - currentOffset);
                        hit.score(shardDoc.score);
                        hit.shard(fetchResult.getSearchShardTarget());
                        suggestOption.setHit(hit);
                    }
                    currentOffset += suggestionOptions.size();
                }
                assert currentOffset == sortedDocs.length : "expected no more score doc slices";
            }
        }
        return reducedQueryPhase.buildResponse(hits);
    }

    private SearchHits getHits(ReducedQueryPhase reducedQueryPhase, boolean ignoreFrom,
                               Collection<? extends SearchPhaseResult> fetchResults, IntFunction<SearchPhaseResult> resultsLookup) {
        SortedTopDocs sortedTopDocs = reducedQueryPhase.sortedTopDocs;
        int sortScoreIndex = -1;
        if (sortedTopDocs.isSortedByField) {
            SortField[] sortFields = sortedTopDocs.sortFields;
            for (int i = 0; i < sortFields.length; i++) {
                if (sortFields[i].getType() == SortField.Type.SCORE) {
                    sortScoreIndex = i;
                }
            }
        }
        // clean the fetch counter
        for (SearchPhaseResult entry : fetchResults) {
            entry.fetchResult().initCounter();
        }
        int from = ignoreFrom ? 0 : reducedQueryPhase.from;
        int numSearchHits = (int) Math.min(reducedQueryPhase.fetchHits - from, reducedQueryPhase.size);
        // with collapsing we can have more fetch hits than sorted docs
        numSearchHits = Math.min(sortedTopDocs.scoreDocs.length, numSearchHits);
        // merge hits
        List<SearchHit> hits = new ArrayList<>();
        if (!fetchResults.isEmpty()) {
            for (int i = 0; i < numSearchHits; i++) {
                ScoreDoc shardDoc = sortedTopDocs.scoreDocs[i];
                SearchPhaseResult fetchResultProvider = resultsLookup.apply(shardDoc.shardIndex);
                if (fetchResultProvider == null) {
                    // this can happen if we are hitting a shard failure during the fetch phase
                    // in this case we referenced the shard result via the ScoreDoc but never got a
                    // result from fetch.
                    // TODO it would be nice to assert this in the future
                    continue;
                }
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                final int index = fetchResult.counterGetAndIncrement();
                assert index < fetchResult.hits().getHits().length : "not enough hits fetched. index [" + index + "] length: "
                    + fetchResult.hits().getHits().length;
                SearchHit searchHit = fetchResult.hits().getHits()[index];
                searchHit.shard(fetchResult.getSearchShardTarget());
                if (sortedTopDocs.isSortedByField) {
                    FieldDoc fieldDoc = (FieldDoc) shardDoc;
                    searchHit.sortValues(fieldDoc.fields, reducedQueryPhase.sortValueFormats);
                    if (sortScoreIndex != -1) {
                        searchHit.score(((Number) fieldDoc.fields[sortScoreIndex]).floatValue());
                    }
                } else {
                    searchHit.score(shardDoc.score);
                }
                hits.add(searchHit);
            }
        }
        return new SearchHits(hits.toArray(new SearchHit[0]), reducedQueryPhase.totalHits,
            reducedQueryPhase.maxScore, sortedTopDocs.sortFields, sortedTopDocs.collapseField, sortedTopDocs.collapseValues);
    }

    /**
     * Reduces the given query results and consumes all aggregations and profile results.
     * @param queryResults a list of non-null query shard results
     */
    ReducedQueryPhase reducedScrollQueryPhase(Collection<? extends SearchPhaseResult> queryResults) {
        InternalAggregation.ReduceContextBuilder aggReduceContextBuilder = new InternalAggregation.ReduceContextBuilder() {
            @Override
            public ReduceContext forPartialReduction() {
                throw new UnsupportedOperationException("Scroll requests don't have aggs");
            }

            @Override
            public ReduceContext forFinalReduction() {
                throw new UnsupportedOperationException("Scroll requests don't have aggs");
            }
        };
        return reducedQueryPhase(queryResults, true, SearchContext.TRACK_TOTAL_HITS_ACCURATE, aggReduceContextBuilder, true);
    }

    /**
     * Reduces the given query results and consumes all aggregations and profile results.
     * @param queryResults a list of non-null query shard results
     */
    public ReducedQueryPhase reducedQueryPhase(Collection<? extends SearchPhaseResult> queryResults,
                                               boolean isScrollRequest, int trackTotalHitsUpTo,
                                               InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                               boolean performFinalReduce) {
        return reducedQueryPhase(queryResults, null, new ArrayList<>(), new TopDocsStats(trackTotalHitsUpTo),
            0, isScrollRequest, aggReduceContextBuilder, performFinalReduce);
    }

    /**
     * Reduces the given query results and consumes all aggregations and profile results.
     * @param queryResults a list of non-null query shard results
     * @param bufferedAggs a list of pre-collected / buffered aggregations. if this list is non-null all aggregations have been consumed
     *                    from all non-null query results.
     * @param bufferedTopDocs a list of pre-collected / buffered top docs. if this list is non-null all top docs have been consumed
     *                    from all non-null query results.
     * @param numReducePhases the number of non-final reduce phases applied to the query results.
     * @see QuerySearchResult#consumeAggs()
     * @see QuerySearchResult#consumeProfileResult()
     */
    public ReducedQueryPhase reducedQueryPhase(Collection<? extends SearchPhaseResult> queryResults,
                                                List<DelayableWriteable<InternalAggregations>> bufferedAggs,
                                                List<TopDocs> bufferedTopDocs,
                                                TopDocsStats topDocsStats, int numReducePhases, boolean isScrollRequest,
                                                InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                                boolean performFinalReduce) {
        assert numReducePhases >= 0 : "num reduce phases must be >= 0 but was: " + numReducePhases;
        numReducePhases++; // increment for this phase
        if (queryResults.isEmpty()) { // early terminate we have nothing to reduce
            final TotalHits totalHits = topDocsStats.getTotalHits();
            return new ReducedQueryPhase(totalHits, topDocsStats.fetchHits, topDocsStats.getMaxScore(),
                false, null, null, null, null, SortedTopDocs.EMPTY, null, numReducePhases, 0, 0, true);
        }
        int total = queryResults.size();
        queryResults = queryResults.stream()
            .filter(res -> res.queryResult().isNull() == false)
            .collect(Collectors.toList());
        String errorMsg = "must have at least one non-empty search result, got 0 out of " + total;
        assert queryResults.isEmpty() == false : errorMsg;
        if (queryResults.isEmpty()) {
            throw new IllegalStateException(errorMsg);
        }
        final QuerySearchResult firstResult = queryResults.stream().findFirst().get().queryResult();
        final boolean hasSuggest = firstResult.suggest() != null;
        final boolean hasProfileResults = firstResult.hasProfileResults();
        final boolean consumeAggs;
        final List<DelayableWriteable<InternalAggregations>> aggregationsList;
        if (bufferedAggs != null) {
            consumeAggs = false;
            // we already have results from intermediate reduces and just need to perform the final reduce
            assert firstResult.hasAggs() : "firstResult has no aggs but we got non null buffered aggs?";
            aggregationsList = bufferedAggs;
        } else if (firstResult.hasAggs()) {
            // the number of shards was less than the buffer size so we reduce agg results directly
            aggregationsList = new ArrayList<>(queryResults.size());
            consumeAggs = true;
        } else {
            // no aggregations
            aggregationsList = Collections.emptyList();
            consumeAggs = false;
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        final Map<String, List<Suggestion>> groupedSuggestions = hasSuggest ? new HashMap<>() : Collections.emptyMap();
        final Map<String, ProfileShardResult> profileResults = hasProfileResults ? new HashMap<>(queryResults.size())
            : Collections.emptyMap();
        int from = 0;
        int size = 0;
        for (SearchPhaseResult entry : queryResults) {
            QuerySearchResult result = entry.queryResult();
            from = result.from();
            // sorted queries can set the size to 0 if they have enough competitive hits.
            size = Math.max(result.size(), size);
            if (hasSuggest) {
                assert result.suggest() != null;
                for (Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> suggestion : result.suggest()) {
                    List<Suggestion> suggestionList = groupedSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                    suggestionList.add(suggestion);
                    if (suggestion instanceof CompletionSuggestion) {
                        CompletionSuggestion completionSuggestion = (CompletionSuggestion) suggestion;
                        completionSuggestion.setShardIndex(result.getShardIndex());
                    }
                }
            }
            if (consumeAggs) {
                aggregationsList.add(result.consumeAggs());
            }
            if (hasProfileResults) {
                String key = result.getSearchShardTarget().toString();
                profileResults.put(key, result.consumeProfileResult());
            }
        }
        final Suggest reducedSuggest;
        final List<CompletionSuggestion> reducedCompletionSuggestions;
        if (groupedSuggestions.isEmpty()) {
            reducedSuggest = null;
            reducedCompletionSuggestions = Collections.emptyList();
        } else {
            reducedSuggest = new Suggest(Suggest.reduce(groupedSuggestions));
            reducedCompletionSuggestions = reducedSuggest.filter(CompletionSuggestion.class);
        }
        final InternalAggregations aggregations = reduceAggs(aggReduceContextBuilder, performFinalReduce, aggregationsList);
        final SearchProfileShardResults shardResults = profileResults.isEmpty() ? null : new SearchProfileShardResults(profileResults);
        final SortedTopDocs sortedTopDocs = sortDocs(isScrollRequest, queryResults, bufferedTopDocs, topDocsStats, from, size,
            reducedCompletionSuggestions);
        final TotalHits totalHits = topDocsStats.getTotalHits();
        return new ReducedQueryPhase(totalHits, topDocsStats.fetchHits, topDocsStats.getMaxScore(),
            topDocsStats.timedOut, topDocsStats.terminatedEarly, reducedSuggest, aggregations, shardResults, sortedTopDocs,
            firstResult.sortValueFormats(), numReducePhases, size, from, false);
    }

    private static InternalAggregations reduceAggs(
        InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
        boolean performFinalReduce,
        List<DelayableWriteable<InternalAggregations>> aggregationsList
    ) {
        /*
         * Parse the aggregations, clearing the list as we go so bits backing
         * the DelayedWriteable can be collected immediately.
         */
        List<InternalAggregations> toReduce = new ArrayList<>(aggregationsList.size());
        for (int i = 0; i < aggregationsList.size(); i++) {
            toReduce.add(aggregationsList.get(i).expand());
            aggregationsList.set(i, null);
        }
        return aggregationsList.isEmpty() ? null : InternalAggregations.topLevelReduce(toReduce,
            performFinalReduce ? aggReduceContextBuilder.forFinalReduction() : aggReduceContextBuilder.forPartialReduction());
    }

    /*
     * Returns the size of the requested top documents (from + size)
     */
    static int getTopDocsSize(SearchRequest request) {
        if (request.source() == null) {
            return SearchService.DEFAULT_SIZE;
        }
        SearchSourceBuilder source = request.source();
        return (source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size()) +
            (source.from() == -1 ? SearchService.DEFAULT_FROM : source.from());
    }

    public static final class ReducedQueryPhase {
        // the sum of all hits across all reduces shards
        final TotalHits totalHits;
        // the number of returned hits (doc IDs) across all reduces shards
        final long fetchHits;
        // the max score across all reduces hits or {@link Float#NaN} if no hits returned
        final float maxScore;
        // <code>true</code> if at least one reduced result timed out
        final boolean timedOut;
        // non null and true if at least one reduced result was terminated early
        final Boolean terminatedEarly;
        // the reduced suggest results
        final Suggest suggest;
        // the reduced internal aggregations
        final InternalAggregations aggregations;
        // the reduced profile results
        final SearchProfileShardResults shardResults;
        // the number of reduces phases
        final int numReducePhases;
        //encloses info about the merged top docs, the sort fields used to sort the score docs etc.
        final SortedTopDocs sortedTopDocs;
        // the size of the top hits to return
        final int size;
        // <code>true</code> iff the query phase had no results. Otherwise <code>false</code>
        final boolean isEmptyResult;
        // the offset into the merged top hits
        final int from;
        // sort value formats used to sort / format the result
        final DocValueFormat[] sortValueFormats;

        ReducedQueryPhase(TotalHits totalHits, long fetchHits, float maxScore, boolean timedOut, Boolean terminatedEarly, Suggest suggest,
                          InternalAggregations aggregations, SearchProfileShardResults shardResults, SortedTopDocs sortedTopDocs,
                          DocValueFormat[] sortValueFormats, int numReducePhases, int size, int from, boolean isEmptyResult) {
            if (numReducePhases <= 0) {
                throw new IllegalArgumentException("at least one reduce phase must have been applied but was: " + numReducePhases);
            }
            this.totalHits = totalHits;
            this.fetchHits = fetchHits;
            this.maxScore = maxScore;
            this.timedOut = timedOut;
            this.terminatedEarly = terminatedEarly;
            this.suggest = suggest;
            this.aggregations = aggregations;
            this.shardResults = shardResults;
            this.numReducePhases = numReducePhases;
            this.sortedTopDocs = sortedTopDocs;
            this.size = size;
            this.from = from;
            this.isEmptyResult = isEmptyResult;
            this.sortValueFormats = sortValueFormats;
        }

        /**
         * Creates a new search response from the given merged hits.
         * @see #merge(boolean, ReducedQueryPhase, Collection, IntFunction)
         */
        public InternalSearchResponse buildResponse(SearchHits hits) {
            return new InternalSearchResponse(hits, aggregations, suggest, shardResults, timedOut, terminatedEarly, numReducePhases);
        }
    }

    /**
     * A {@link ArraySearchPhaseResults} implementation
     * that incrementally reduces aggregation results as shard results are consumed.
     * This implementation can be configured to batch up a certain amount of results and only reduce them
     * iff the buffer is exhausted.
     */
    public static final class QueryPhaseResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> {
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final SearchShardTarget[] processedShards;
        private final DelayableWriteable.Serialized<InternalAggregations>[] aggsBuffer;
        private final TopDocs[] topDocsBuffer;
        private final boolean hasAggs;
        private final boolean hasTopDocs;
        private final int bufferSize;
        private int index;
        private final SearchPhaseController controller;
        private final SearchProgressListener progressListener;
        private int numReducePhases = 0;
        private final TopDocsStats topDocsStats;
        private final int topNSize;
        private final InternalAggregation.ReduceContextBuilder aggReduceContextBuilder;
        private final boolean performFinalReduce;
        private long aggsCurrentBufferSize;
        private long aggsMaxBufferSize;

        /**
         * Creates a new {@link QueryPhaseResultConsumer}
         * @param progressListener a progress listener to be notified when a successful response is received
         *                         and when a partial or final reduce has completed.
         * @param controller a controller instance to reduce the query response objects
         * @param expectedResultSize the expected number of query results. Corresponds to the number of shards queried
         * @param bufferSize the size of the reduce buffer. if the buffer size is smaller than the number of expected results
         *                   the buffer is used to incrementally reduce aggregation results before all shards responded.
         */
        public QueryPhaseResultConsumer(NamedWriteableRegistry namedWriteableRegistry, SearchProgressListener progressListener,
                                         SearchPhaseController controller,
                                         int expectedResultSize, int bufferSize, boolean hasTopDocs, boolean hasAggs,
                                         int trackTotalHitsUpTo, int topNSize,
                                         InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                         boolean performFinalReduce) {
            super(expectedResultSize);
            this.namedWriteableRegistry = namedWriteableRegistry;
            if (expectedResultSize != 1 && bufferSize < 2) {
                throw new IllegalArgumentException("buffer size must be >= 2 if there is more than one expected result");
            }
            if (expectedResultSize <= bufferSize) {
                throw new IllegalArgumentException("buffer size must be less than the expected result size");
            }
            if (hasAggs == false && hasTopDocs == false) {
                throw new IllegalArgumentException("either aggs or top docs must be present");
            }
            this.controller = controller;
            this.progressListener = progressListener;
            this.processedShards = new SearchShardTarget[expectedResultSize];
            // no need to buffer anything if we have less expected results. in this case we don't consume any results ahead of time.
            @SuppressWarnings("unchecked")
            DelayableWriteable.Serialized<InternalAggregations>[] aggsBuffer = new DelayableWriteable.Serialized[hasAggs ? bufferSize : 0];
            this.aggsBuffer = aggsBuffer;
            this.topDocsBuffer = new TopDocs[hasTopDocs ? bufferSize : 0];
            this.hasTopDocs = hasTopDocs;
            this.hasAggs = hasAggs;
            this.bufferSize = bufferSize;
            this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
            this.topNSize = topNSize;
            this.aggReduceContextBuilder = aggReduceContextBuilder;
            this.performFinalReduce = performFinalReduce;
        }

        @Override
        public void consumeResult(SearchPhaseResult result) {
            super.consumeResult(result);
            QuerySearchResult queryResult = result.queryResult();
            consumeInternal(queryResult);
            progressListener.notifyQueryResult(queryResult.getShardIndex());
        }

        private synchronized void consumeInternal(QuerySearchResult querySearchResult) {
            if (querySearchResult.isNull() == false) {
                if (index == bufferSize) {
                    DelayableWriteable.Serialized<InternalAggregations> reducedAggs = null;
                    if (hasAggs) {
                        List<InternalAggregations> aggs = new ArrayList<>(aggsBuffer.length);
                        for (int i = 0; i < aggsBuffer.length; i++) {
                            aggs.add(aggsBuffer[i].expand());
                            aggsBuffer[i] = null; // null the buffer so it can be GCed now.
                        }
                        InternalAggregations reduced =
                                InternalAggregations.topLevelReduce(aggs, aggReduceContextBuilder.forPartialReduction());
                        reducedAggs = aggsBuffer[0] = DelayableWriteable.referencing(reduced)
                                .asSerialized(InternalAggregations::new, namedWriteableRegistry);
                        long previousBufferSize = aggsCurrentBufferSize;
                        aggsMaxBufferSize = Math.max(aggsMaxBufferSize, aggsCurrentBufferSize);
                        aggsCurrentBufferSize = aggsBuffer[0].ramBytesUsed();
                        logger.trace("aggs partial reduction [{}->{}] max [{}]",
                                previousBufferSize, aggsCurrentBufferSize, aggsMaxBufferSize);
                    }
                    if (hasTopDocs) {
                        TopDocs reducedTopDocs = mergeTopDocs(Arrays.asList(topDocsBuffer),
                            // we have to merge here in the same way we collect on a shard
                            topNSize, 0);
                        Arrays.fill(topDocsBuffer, null);
                        topDocsBuffer[0] = reducedTopDocs;
                    }
                    numReducePhases++;
                    index = 1;
                    if (hasAggs || hasTopDocs) {
                        progressListener.notifyPartialReduce(SearchProgressListener.buildSearchShards(processedShards),
                            topDocsStats.getTotalHits(), reducedAggs, numReducePhases);
                    }
                }
                final int i = index++;
                if (hasAggs) {
                    aggsBuffer[i] = querySearchResult.consumeAggs().asSerialized(InternalAggregations::new, namedWriteableRegistry);
                    aggsCurrentBufferSize += aggsBuffer[i].ramBytesUsed();
                }
                if (hasTopDocs) {
                    final TopDocsAndMaxScore topDocs = querySearchResult.consumeTopDocs(); // can't be null
                    topDocsStats.add(topDocs, querySearchResult.searchTimedOut(), querySearchResult.terminatedEarly());
                    setShardIndex(topDocs.topDocs, querySearchResult.getShardIndex());
                    topDocsBuffer[i] = topDocs.topDocs;
                }
            }
            processedShards[querySearchResult.getShardIndex()] = querySearchResult.getSearchShardTarget();
        }

        private synchronized List<DelayableWriteable<InternalAggregations>> getRemainingAggs() {
            return hasAggs ? Arrays.asList((DelayableWriteable<InternalAggregations>[]) aggsBuffer).subList(0, index) : null;
        }

        private synchronized List<TopDocs> getRemainingTopDocs() {
            return hasTopDocs ? Arrays.asList(topDocsBuffer).subList(0, index) : null;
        }

        @Override
        public ReducedQueryPhase reduce() {
            aggsMaxBufferSize = Math.max(aggsMaxBufferSize, aggsCurrentBufferSize);
            logger.trace("aggs final reduction [{}] max [{}]", aggsCurrentBufferSize, aggsMaxBufferSize);
            ReducedQueryPhase reducePhase = controller.reducedQueryPhase(results.asList(), getRemainingAggs(), getRemainingTopDocs(),
                    topDocsStats, numReducePhases, false, aggReduceContextBuilder, performFinalReduce);
            progressListener.notifyFinalReduce(SearchProgressListener.buildSearchShards(results.asList()),
                reducePhase.totalHits, reducePhase.aggregations, reducePhase.numReducePhases);
            return reducePhase;
        }

        /**
         * Returns the number of buffered results
         */
        int getNumBuffered() {
            return index;
        }

        int getNumReducePhases() { return numReducePhases; }
    }

    /**
     * A {@link ArraySearchPhaseResults} implementation
     * that reduces aggregation results in parallel.
     */
    public static final class QueryPhaseParallelResultConsumer extends ArraySearchPhaseResults<SearchPhaseResult> {
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final SearchShardTarget[] processedShards;
        private final int numShards;
        private final SearchPhaseController controller;
        private final SearchProgressListener progressListener;
        private final Executor reduceExecutor;
        private final boolean hasAggs;
        private final boolean hasTopDocs;
        private final TopDocsStats topDocsStats;
        private final int topNSize;
        private final InternalAggregation.ReduceContextBuilder aggReduceContextBuilder;
        private final boolean performFinalReduce;
        private final AtomicLong aggsMaxSize;
        private final AtomicInteger numReducePhases;
        private final BlockingQueue<PartialReduceResult> intermediateReducedResultsQueue;
        private final AtomicInteger runningParallelReduceCount;
        private final AtomicInteger shardResultConsumeCount;
        private final CountDownLatch finalReduce = new CountDownLatch(1);
        private final List<PartialReduceResult> remainningResults;
        private int parallelBuffSize = 512;

        /**
         * Creates a new {@link QueryPhaseParallelResultConsumer}
         * @param progressListener a progress listener to be notified when a successful response is received
         *                         and when a partial or final reduce has completed.
         * @param controller a controller instance to reduce the query response objects
         */
        public QueryPhaseParallelResultConsumer(NamedWriteableRegistry namedWriteableRegistry, SearchProgressListener progressListener,
                                         SearchPhaseController controller,
                                         int numShards, boolean hasTopDocs, boolean hasAggs,
                                         int trackTotalHitsUpTo, int topNSize,
                                         InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                         boolean performFinalReduce, ThreadPool threadPool) {
            super(numShards);
            this.namedWriteableRegistry = namedWriteableRegistry;
            if (hasAggs == false && hasTopDocs == false) {
                throw new IllegalArgumentException("either aggs or top docs must be present in parallel result consumer");
            }
            this.processedShards = new SearchShardTarget[numShards];
            this.numShards = numShards;
            this.controller = controller;
            this.progressListener = progressListener;
            this.hasTopDocs = hasTopDocs;
            this.hasAggs = hasAggs;
            this.topDocsStats = new TopDocsStats(trackTotalHitsUpTo);
            this.topNSize = topNSize;
            this.aggReduceContextBuilder = aggReduceContextBuilder;
            this.performFinalReduce = performFinalReduce;
            this.aggsMaxSize = new AtomicLong(0);
            this.numReducePhases = new AtomicInteger(0);
            this.intermediateReducedResultsQueue = new ArrayBlockingQueue<>(numShards);
            this.reduceExecutor = threadPool.executor(ThreadPool.Names.REDUCE_PARTIAL_PARALLEL);
            this.runningParallelReduceCount = new AtomicInteger(0);
            this.shardResultConsumeCount = new AtomicInteger(0);
            this.remainningResults = new ArrayList<>();
        }

        @Override
        public void consumeResult(SearchPhaseResult result) {
            super.consumeResult(result);
            QuerySearchResult queryResult = result.queryResult();
            consumeParallel(queryResult);
            progressListener.notifyQueryResult(queryResult.getShardIndex());
        }

        private void consumeParallel(QuerySearchResult querySearchResult) {
            if (querySearchResult.isNull() == false) {
                DelayableWriteable.Serialized<InternalAggregations> partialResultAggs = null;
                if (hasAggs) {
                    partialResultAggs = querySearchResult.consumeAggs().asSerialized(InternalAggregations::new, namedWriteableRegistry);
                    aggsMaxSize.addAndGet(partialResultAggs.ramBytesUsed());
                }
                TopDocs partialResultTopDocs = null;
                if (hasTopDocs) {
                    final TopDocsAndMaxScore topDocs = querySearchResult.consumeTopDocs(); // can't be null
                    topDocsStats.add(topDocs, querySearchResult.searchTimedOut(), querySearchResult.terminatedEarly());
                    setShardIndex(topDocs.topDocs, querySearchResult.getShardIndex());
                    partialResultTopDocs = topDocs.topDocs;
                }
                // put in queue
                PartialReduceResult partialReduceResult = new PartialReduceResult(partialResultAggs, partialResultTopDocs);
                putInQueue(partialReduceResult);
                // check to start parallel reduce task
                if (intermediateReducedResultsQueue.size() > parallelBuffSize
                    || intermediateReducedResultsQueue.size() == numShards) {
                    // execute parallel reduce task when there are enough buff size results or all results have put in
                    // queue and have not executed once
                    reduceExecutor.execute(new PartialReduceTask());
                }
                if (numShards == 1) {
                    // no need to parallel reduce, execute final reduce directly
                    finalReduce.countDown();
                }
            }
            processedShards[querySearchResult.getShardIndex()] = querySearchResult.getSearchShardTarget();
            shardResultConsumeCount.incrementAndGet();
        }

        private void putInQueue(PartialReduceResult partialReduceResult) {
            try {
                intermediateReducedResultsQueue.put(partialReduceResult);
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted partial reduce put in queue");
            }
        }

        private boolean checkParitalReduceAggsDone() {
            return runningParallelReduceCount.get() == 0 &&
                   shardResultConsumeCount.get() == numShards;
        }

        private void waitForAllParallelsDone() {
            try {
                finalReduce.await();
            } catch (InterruptedException e) {
                throw new RuntimeException("interrupted when final reduce wait all partial reduces done.");
            }
        }

        private void drainFromIntermediateResultQueue() {
            intermediateReducedResultsQueue.drainTo(remainningResults);
        }

        private synchronized List<DelayableWriteable<InternalAggregations>> getRemainingAggs() {
            return hasAggs ? remainningResults.stream().map(PartialReduceResult::getAggs).collect(Collectors.toList()) : null;
        }

        private synchronized List<TopDocs> getRemainingTopDocs() {
            return hasTopDocs ? remainningResults.stream().map(PartialReduceResult::getTopDocs).collect(Collectors.toList()) : null;
        }

        @Override
        public ReducedQueryPhase reduce() {
            waitForAllParallelsDone();
            drainFromIntermediateResultQueue();
            logger.trace("aggs final reduction [{}] max [{}]", remainningResults.stream().mapToLong(PartialReduceResult::ramUsed).sum(), aggsMaxSize);
            ReducedQueryPhase reducePhase = controller.reducedQueryPhase(results.asList(), getRemainingAggs(), getRemainingTopDocs(),
                topDocsStats, numReducePhases.get(), false, aggReduceContextBuilder, performFinalReduce);
            progressListener.notifyFinalReduce(SearchProgressListener.buildSearchShards(results.asList()),
                reducePhase.totalHits, reducePhase.aggregations, reducePhase.numReducePhases);
            remainningResults.clear();
            return reducePhase;
        }

        public void setParallelBuffSize(int parallelBuffSize) {
            this.parallelBuffSize = parallelBuffSize;
        }

        public int getNumReducePhases() {
            return numReducePhases.get();
        }

        static class PartialReduceResult {
            private final DelayableWriteable.Serialized<InternalAggregations> aggs;
            private final TopDocs topDocs;
            public PartialReduceResult(DelayableWriteable.Serialized<InternalAggregations> aggs, TopDocs topDocs) {
                this.aggs = aggs;
                this.topDocs = topDocs;
            }

            public DelayableWriteable.Serialized<InternalAggregations> getAggs() {
                return aggs;
            }

            public TopDocs getTopDocs() {
                return topDocs;
            }

            public long ramUsed() {
                return aggs != null ? aggs.ramBytesUsed() : 0;
            }
        }

        class PartialReduceTask implements Runnable {
            private void endTask() {
                runningParallelReduceCount.decrementAndGet();
                // check to notify final reduce to begin
                if (checkParitalReduceAggsDone()) {
                    finalReduce.countDown();
                }
            }

            @Override
            public void run() {
                runningParallelReduceCount.incrementAndGet();
                int fetches = 0;
                List<DelayableWriteable.Serialized<InternalAggregations>> aggsFetches = new ArrayList<>();
                List<TopDocs> topDocsFetches = new ArrayList<>();
                synchronized (intermediateReducedResultsQueue) {
                    /*
                     * other tasks may fetch from queue to change the queue size when current task check queue size to
                     * fetch results, this may cause to some tasks misread the queue size before starting to reduce,
                     * so we synced this block for checking queue size to fetches
                     */
                    if (intermediateReducedResultsQueue.size() < parallelBuffSize) {
                        //  not enough results to reduce
                        endTask();
                        return;
                    } else {
                        // enough results to reduce
                        if (parallelBuffSize == 2) {
                            // avoid 2 + 1 reduction cause more reduce times than 3 reduction
                            if (intermediateReducedResultsQueue.size() == 2) {
                                fetches = 2;
                            } else {
                                fetches = 3;
                            }
                        } else {
                            fetches = parallelBuffSize;
                        }
                        for (int i = 0; i < fetches; i++) {
                            PartialReduceResult fetchOne = intermediateReducedResultsQueue.remove();
                            aggsFetches.add(fetchOne.aggs);
                            topDocsFetches.add(fetchOne.topDocs);
                        }
                    }
                }
                numReducePhases.incrementAndGet();
                DelayableWriteable.Serialized<InternalAggregations> reducedAggs = null;
                if (hasAggs) {
                    List<InternalAggregations> aggs = new ArrayList<>(fetches);
                    long previousBufferSize = 0;
                    for (DelayableWriteable.Serialized<InternalAggregations> aggFetch : aggsFetches) {
                        aggs.add(aggFetch.expand());
                        previousBufferSize += aggFetch.ramBytesUsed();
                    }
                    aggsFetches.clear();
                    InternalAggregations reduced =
                        InternalAggregations.topLevelReduce(aggs, aggReduceContextBuilder.forPartialReduction());
                    reducedAggs = DelayableWriteable.referencing(reduced)
                        .asSerialized(InternalAggregations::new, namedWriteableRegistry);
                    logger.trace("aggs parallel partial reduction [{}->{}] max [{}]",
                        previousBufferSize, reducedAggs.ramBytesUsed(), aggsMaxSize.get());
                }
                TopDocs reducedTopDocs = null;
                if (hasTopDocs) {
                    reducedTopDocs = mergeTopDocs(topDocsFetches, topNSize, 0);
                    topDocsFetches.clear();
                }
                progressListener.notifyPartialReduce(SearchProgressListener.buildSearchShards(processedShards),
                    topDocsStats.getTotalHits(), reducedAggs, numReducePhases.get());
                // re-enqueue
                PartialReduceResult reducedResult = new PartialReduceResult(reducedAggs, reducedTopDocs);
                putInQueue(reducedResult);
                endTask();
            }
        }
    }

    /**
     * Returns a new ArraySearchPhaseResults instance. This might return an instance that reduces search responses incrementally.
     */
    ArraySearchPhaseResults<SearchPhaseResult> newSearchPhaseResults(SearchProgressListener listener,
                                                                     SearchRequest request,
                                                                     int numShards) {
        SearchSourceBuilder source = request.source();
        boolean isScrollRequest = request.scroll() != null;
        final boolean hasAggs = source != null && source.aggregations() != null;
        final boolean hasTopDocs = source == null || source.size() != 0;
        final int trackTotalHitsUpTo = request.resolveTrackTotalHitsUpTo();
        InternalAggregation.ReduceContextBuilder aggReduceContextBuilder = requestToAggReduceContextBuilder.apply(request);
        if (isScrollRequest == false && (hasAggs || hasTopDocs)) {
            // no incremental reduce if scroll is used - we only hit a single shard or sometimes more...
            if (request.getParallelReduce()) {
                // execute parallel reduce instead of batched reduce
                int topNSize = getTopDocsSize(request);
                return new QueryPhaseParallelResultConsumer(namedWriteableRegistry, listener, this, numShards,
                    hasTopDocs, hasAggs, trackTotalHitsUpTo, topNSize, aggReduceContextBuilder, request.isFinalReduce(), threadPool);
            }
            if (request.getBatchedReduceSize() < numShards) {
                int topNSize = getTopDocsSize(request);
                // only use this if there are aggs and if there are more shards than we should reduce at once
                return new QueryPhaseResultConsumer(namedWriteableRegistry, listener, this, numShards, request.getBatchedReduceSize(),
                    hasTopDocs, hasAggs, trackTotalHitsUpTo, topNSize, aggReduceContextBuilder, request.isFinalReduce());
            }
        }
        return new ArraySearchPhaseResults<SearchPhaseResult>(numShards) {
            @Override
            public void consumeResult(SearchPhaseResult result) {
                super.consumeResult(result);
                listener.notifyQueryResult(result.queryResult().getShardIndex());
            }

            @Override
            public ReducedQueryPhase reduce() {
                List<SearchPhaseResult> resultList = results.asList();
                final ReducedQueryPhase reducePhase =
                    reducedQueryPhase(resultList, isScrollRequest, trackTotalHitsUpTo, aggReduceContextBuilder, request.isFinalReduce());
                listener.notifyFinalReduce(SearchProgressListener.buildSearchShards(resultList),
                    reducePhase.totalHits, reducePhase.aggregations, reducePhase.numReducePhases);
                return reducePhase;
            }
        };
    }

    public static final class TopDocsStats {
        final int trackTotalHitsUpTo;
        long totalHits;
        private TotalHits.Relation totalHitsRelation;
        long fetchHits;
        private float maxScore = Float.NEGATIVE_INFINITY;
        boolean timedOut;
        Boolean terminatedEarly;

        public TopDocsStats(int trackTotalHitsUpTo) {
            this.trackTotalHitsUpTo = trackTotalHitsUpTo;
            this.totalHits = 0;
            this.totalHitsRelation = Relation.EQUAL_TO;
        }

        float getMaxScore() {
            return Float.isInfinite(maxScore) ? Float.NaN : maxScore;
        }

        TotalHits getTotalHits() {
            if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                return null;
            } else if (trackTotalHitsUpTo == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
                assert totalHitsRelation == Relation.EQUAL_TO;
                return new TotalHits(totalHits, totalHitsRelation);
            } else {
                if (totalHits <= trackTotalHitsUpTo) {
                    return new TotalHits(totalHits, totalHitsRelation);
                } else {
                    /*
                     * The user requested to count the total hits up to <code>trackTotalHitsUpTo</code>
                     * so we return this lower bound when the total hits is greater than this value.
                     * This can happen when multiple shards are merged since the limit to track total hits
                     * is applied per shard.
                     */
                    return new TotalHits(trackTotalHitsUpTo, Relation.GREATER_THAN_OR_EQUAL_TO);
                }
            }
        }

        synchronized void add(TopDocsAndMaxScore topDocs, boolean timedOut, Boolean terminatedEarly) {
            if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                totalHits += topDocs.topDocs.totalHits.value;
                if (topDocs.topDocs.totalHits.relation == Relation.GREATER_THAN_OR_EQUAL_TO) {
                    totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                }
            }
            fetchHits += topDocs.topDocs.scoreDocs.length;
            if (!Float.isNaN(topDocs.maxScore)) {
                maxScore = Math.max(maxScore, topDocs.maxScore);
            }
            if (timedOut) {
                this.timedOut = true;
            }
            if (terminatedEarly != null) {
                if (this.terminatedEarly == null) {
                    this.terminatedEarly = terminatedEarly;
                } else if (terminatedEarly) {
                    this.terminatedEarly = true;
                }
            }
        }
    }

    static final class SortedTopDocs {
        static final SortedTopDocs EMPTY = new SortedTopDocs(EMPTY_DOCS, false, null, null, null);
        // the searches merged top docs
        final ScoreDoc[] scoreDocs;
        // <code>true</code> iff the result score docs is sorted by a field (not score), this implies that <code>sortField</code> is set.
        final boolean isSortedByField;
        // the top docs sort fields used to sort the score docs, <code>null</code> if the results are not sorted
        final SortField[] sortFields;
        final String collapseField;
        final Object[] collapseValues;

        SortedTopDocs(ScoreDoc[] scoreDocs, boolean isSortedByField, SortField[] sortFields,
                      String collapseField, Object[] collapseValues) {
            this.scoreDocs = scoreDocs;
            this.isSortedByField = isSortedByField;
            this.sortFields = sortFields;
            this.collapseField = collapseField;
            this.collapseValues = collapseValues;
        }
    }
}
