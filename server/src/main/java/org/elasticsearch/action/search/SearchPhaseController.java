/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsKnnResults;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileResultsBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankCoordinatorContext;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.IntFunction;
import java.util.function.Supplier;

public final class SearchPhaseController {
    private static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final BiFunction<
        Supplier<Boolean>,
        AggregatorFactories.Builder,
        AggregationReduceContext.Builder> requestToAggReduceContextBuilder;

    public SearchPhaseController(
        BiFunction<Supplier<Boolean>, AggregatorFactories.Builder, AggregationReduceContext.Builder> requestToAggReduceContextBuilder
    ) {
        this.requestToAggReduceContextBuilder = requestToAggReduceContextBuilder;
    }

    public static AggregatedDfs aggregateDfs(Collection<DfsSearchResult> results) {
        Map<Term, TermStatistics> termStatistics = new HashMap<>();
        Map<String, CollectionStatistics> fieldStatistics = new HashMap<>();
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
                    termStatistics.put(
                        terms[i],
                        new TermStatistics(
                            existing.term(),
                            existing.docFreq() + stats[i].docFreq(),
                            existing.totalTermFreq() + stats[i].totalTermFreq()
                        )
                    );
                } else {
                    termStatistics.put(terms[i], stats[i]);
                }

            }

            assert lEntry.fieldStatistics().containsKey(null) == false;
            for (var entry : lEntry.fieldStatistics().entrySet()) {
                String key = entry.getKey();
                CollectionStatistics value = entry.getValue();
                if (value == null) {
                    continue;
                }
                assert key != null;
                CollectionStatistics existing = fieldStatistics.get(key);
                if (existing != null) {
                    CollectionStatistics merged = new CollectionStatistics(
                        key,
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
            aggMaxDoc += lEntry.maxDoc();
        }
        return new AggregatedDfs(termStatistics, fieldStatistics, aggMaxDoc);
    }

    public static List<DfsKnnResults> mergeKnnResults(SearchRequest request, List<DfsSearchResult> dfsSearchResults) {
        if (request.hasKnnSearch() == false) {
            return null;
        }

        List<List<TopDocs>> topDocsLists = new ArrayList<>(request.source().knnSearch().size());
        for (int i = 0; i < request.source().knnSearch().size(); i++) {
            topDocsLists.add(new ArrayList<>());
        }

        for (DfsSearchResult dfsSearchResult : dfsSearchResults) {
            if (dfsSearchResult.knnResults() != null) {
                for (int i = 0; i < dfsSearchResult.knnResults().size(); i++) {
                    DfsKnnResults knnResults = dfsSearchResult.knnResults().get(i);
                    ScoreDoc[] scoreDocs = knnResults.scoreDocs();
                    TotalHits totalHits = new TotalHits(scoreDocs.length, Relation.EQUAL_TO);
                    TopDocs shardTopDocs = new TopDocs(totalHits, scoreDocs);
                    setShardIndex(shardTopDocs, dfsSearchResult.getShardIndex());
                    topDocsLists.get(i).add(shardTopDocs);
                }
            }
        }
        List<DfsKnnResults> mergedResults = new ArrayList<>(request.source().knnSearch().size());
        for (int i = 0; i < request.source().knnSearch().size(); i++) {
            TopDocs mergedTopDocs = TopDocs.merge(request.source().knnSearch().get(i).k(), topDocsLists.get(i).toArray(new TopDocs[0]));
            mergedResults.add(new DfsKnnResults(mergedTopDocs.scoreDocs));
        }
        return mergedResults;
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
     * @param topDocs the buffered top docs
     * @param from the offset into the search results top docs
     * @param size the number of hits to return from the merged top docs
     */
    static SortedTopDocs sortDocs(
        boolean ignoreFrom,
        final Collection<TopDocs> topDocs,
        int from,
        int size,
        List<CompletionSuggestion> reducedCompletionSuggestions
    ) {
        if (topDocs.isEmpty() && reducedCompletionSuggestions.isEmpty()) {
            return SortedTopDocs.EMPTY;
        }
        final TopDocs mergedTopDocs = mergeTopDocs(topDocs, size, ignoreFrom ? 0 : from);
        final ScoreDoc[] mergedScoreDocs = mergedTopDocs == null ? EMPTY_DOCS : mergedTopDocs.scoreDocs;
        ScoreDoc[] scoreDocs = mergedScoreDocs;
        int numSuggestDocs = 0;
        if (reducedCompletionSuggestions.isEmpty() == false) {
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
        String groupField = null;
        Object[] groupValues = null;
        if (mergedTopDocs instanceof TopFieldDocs fieldDocs) {
            sortFields = fieldDocs.fields;
            if (fieldDocs instanceof TopFieldGroups topFieldGroups) {
                isSortedByField = (fieldDocs.fields.length == 1 && fieldDocs.fields[0].getType() == SortField.Type.SCORE) == false;
                groupField = topFieldGroups.field;
                groupValues = topFieldGroups.groupValues;
            } else {
                isSortedByField = true;
            }
        }
        return new SortedTopDocs(scoreDocs, isSortedByField, sortFields, groupField, groupValues, numSuggestDocs);
    }

    static TopDocs mergeTopDocs(Collection<TopDocs> results, int topN, int from) {
        if (results.isEmpty()) {
            return null;
        }
        final TopDocs topDocs = results.stream().findFirst().get();
        final TopDocs mergedTopDocs;
        final int numShards = results.size();
        if (numShards == 1 && from == 0) { // only one shard and no pagination we can just return the topDocs as we got them.
            return topDocs;
        } else if (topDocs instanceof TopFieldGroups firstTopDocs) {
            final Sort topSort = new Sort(firstTopDocs.fields);
            final Sort collapseSort = new Sort(firstTopDocs.collapseSortFields);
            final TopFieldGroups[] shardTopDocs = results.toArray(new TopFieldGroups[numShards]);
            mergedTopDocs = TopFieldGroups.merge(topSort, collapseSort, from, topN, shardTopDocs, false);
        } else if (topDocs instanceof TopFieldDocs firstTopDocs) {
            checkSameSortTypes(results, firstTopDocs.fields);
            final Sort sort = new Sort(firstTopDocs.fields);
            final TopFieldDocs[] shardTopDocs = results.toArray(new TopFieldDocs[numShards]);
            mergedTopDocs = TopDocs.merge(sort, from, topN, shardTopDocs);
        } else {
            final TopDocs[] shardTopDocs = results.toArray(new TopDocs[numShards]);
            mergedTopDocs = TopDocs.merge(from, topN, shardTopDocs);
        }
        return mergedTopDocs;
    }

    private static void checkSameSortTypes(Collection<TopDocs> results, SortField[] firstSortFields) {
        if (results.size() < 2) return;

        SortField.Type[] firstTypes = new SortField.Type[firstSortFields.length];
        boolean isFirstResult = true;
        for (TopDocs topDocs : results) {
            SortField[] curSortFields = ((TopFieldDocs) topDocs).fields;
            if (isFirstResult) {
                for (int i = 0; i < curSortFields.length; i++) {
                    firstTypes[i] = getType(firstSortFields[i]);
                    if (firstTypes[i] == SortField.Type.CUSTOM) {
                        // for custom types that we can't resolve, we can't do the check
                        return;
                    }
                }
                isFirstResult = false;
            } else {
                for (int i = 0; i < curSortFields.length; i++) {
                    SortField.Type curType = getType(curSortFields[i]);
                    if (curType != firstTypes[i]) {
                        if (curType == SortField.Type.CUSTOM) {
                            // for custom types that we can't resolve, we can't do the check
                            return;
                        }
                        throw new IllegalArgumentException(
                            "Can't sort on field ["
                                + curSortFields[i].getField()
                                + "]; the field has incompatible sort types: ["
                                + firstTypes[i]
                                + "] and ["
                                + curType
                                + "] across shards!"
                        );
                    }
                }
            }
        }
    }

    private static SortField.Type getType(SortField sortField) {
        if (sortField instanceof SortedNumericSortField) {
            return ((SortedNumericSortField) sortField).getNumericType();
        }
        if (sortField instanceof SortedSetSortField) {
            return SortField.Type.STRING;
        } else {
            return sortField.getType();
        }
    }

    static void setShardIndex(TopDocs topDocs, int shardIndex) {
        assert topDocs.scoreDocs.length == 0 || topDocs.scoreDocs[0].shardIndex == -1 : "shardIndex is already set";
        for (ScoreDoc doc : topDocs.scoreDocs) {
            doc.shardIndex = shardIndex;
        }
    }

    public static ScoreDoc[] getLastEmittedDocPerShard(ReducedQueryPhase reducedQueryPhase, int numShards) {
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
    public static List<Integer>[] fillDocIdsToLoad(int numShards, ScoreDoc[] shardDocs) {
        @SuppressWarnings("unchecked")
        List<Integer>[] docIdsToLoad = (List<Integer>[]) new ArrayList<?>[numShards];
        for (ScoreDoc shardDoc : shardDocs) {
            List<Integer> shardDocIdsToLoad = docIdsToLoad[shardDoc.shardIndex];
            if (shardDocIdsToLoad == null) {
                shardDocIdsToLoad = docIdsToLoad[shardDoc.shardIndex] = new ArrayList<>();
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
    public static InternalSearchResponse merge(
        boolean ignoreFrom,
        ReducedQueryPhase reducedQueryPhase,
        Collection<? extends SearchPhaseResult> fetchResults,
        IntFunction<SearchPhaseResult> resultsLookup
    ) {
        if (reducedQueryPhase.isEmptyResult) {
            return InternalSearchResponse.EMPTY_WITH_TOTAL_HITS;
        }
        ScoreDoc[] sortedDocs = reducedQueryPhase.sortedTopDocs.scoreDocs;
        SearchHits hits = getHits(reducedQueryPhase, ignoreFrom, fetchResults, resultsLookup);
        if (reducedQueryPhase.suggest != null) {
            if (fetchResults.isEmpty() == false) {
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
                        assert index < fetchResult.hits().getHits().length
                            : "not enough hits fetched. index [" + index + "] length: " + fetchResult.hits().getHits().length;
                        SearchHit hit = fetchResult.hits().getHits()[index];
                        CompletionSuggestion.Entry.Option suggestOption = suggestionOptions.get(scoreDocIndex - currentOffset);
                        hit.score(shardDoc.score);
                        hit.shard(fetchResult.getSearchShardTarget());
                        suggestOption.setHit(hit);
                    }
                    currentOffset += suggestionOptions.size();
                }
                assert currentOffset == sortedDocs.length : "expected no more score doc slices";
            }
        }
        return reducedQueryPhase.buildResponse(hits, fetchResults);
    }

    private static SearchHits getHits(
        ReducedQueryPhase reducedQueryPhase,
        boolean ignoreFrom,
        Collection<? extends SearchPhaseResult> fetchResults,
        IntFunction<SearchPhaseResult> resultsLookup
    ) {
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
        // also we need to take into account that we potentially have completion suggestions stored in the scoreDocs array
        numSearchHits = Math.min(sortedTopDocs.scoreDocs.length - sortedTopDocs.numberOfCompletionsSuggestions, numSearchHits);
        // merge hits
        List<SearchHit> hits = new ArrayList<>();
        if (fetchResults.isEmpty() == false) {
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
                assert index < fetchResult.hits().getHits().length
                    : "not enough hits fetched. index [" + index + "] length: " + fetchResult.hits().getHits().length;
                SearchHit searchHit = fetchResult.hits().getHits()[index];
                searchHit.shard(fetchResult.getSearchShardTarget());
                if (reducedQueryPhase.rankCoordinatorContext != null) {
                    assert shardDoc instanceof RankDoc;
                    searchHit.setRank(((RankDoc) shardDoc).rank);
                } else if (sortedTopDocs.isSortedByField) {
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
        return new SearchHits(
            hits.toArray(new SearchHit[0]),
            reducedQueryPhase.totalHits,
            reducedQueryPhase.maxScore,
            sortedTopDocs.sortFields,
            sortedTopDocs.collapseField,
            sortedTopDocs.collapseValues
        );
    }

    /**
     * Reduces the given query results and consumes all aggregations and profile results.
     * @param queryResults a list of non-null query shard results
     */
    static ReducedQueryPhase reducedScrollQueryPhase(Collection<? extends SearchPhaseResult> queryResults) {
        AggregationReduceContext.Builder aggReduceContextBuilder = new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                throw new UnsupportedOperationException("Scroll requests don't have aggs");
            }

            @Override
            public AggregationReduceContext forFinalReduction() {
                throw new UnsupportedOperationException("Scroll requests don't have aggs");
            }
        };
        final TopDocsStats topDocsStats = new TopDocsStats(SearchContext.TRACK_TOTAL_HITS_ACCURATE);
        final List<TopDocs> topDocs = new ArrayList<>();
        for (SearchPhaseResult sortedResult : queryResults) {
            QuerySearchResult queryResult = sortedResult.queryResult();
            final TopDocsAndMaxScore td = queryResult.consumeTopDocs();
            assert td != null;
            topDocsStats.add(td, queryResult.searchTimedOut(), queryResult.terminatedEarly());
            // make sure we set the shard index before we add it - the consumer didn't do that yet
            if (td.topDocs.scoreDocs.length > 0) {
                setShardIndex(td.topDocs, queryResult.getShardIndex());
                topDocs.add(td.topDocs);
            }
        }
        return reducedQueryPhase(
            queryResults,
            Collections.emptyList(),
            topDocs,
            topDocsStats,
            0,
            true,
            aggReduceContextBuilder,
            null,
            true
        );
    }

    /**
     * Reduces the given query results and consumes all aggregations and profile results.
     * @param queryResults a list of non-null query shard results
     * @param bufferedAggs a list of pre-collected aggregations.
     * @param bufferedTopDocs a list of pre-collected top docs.
     * @param numReducePhases the number of non-final reduce phases applied to the query results.
     * @see QuerySearchResult#consumeAggs()
     * @see QuerySearchResult#consumeProfileResult()
     */
    static ReducedQueryPhase reducedQueryPhase(
        Collection<? extends SearchPhaseResult> queryResults,
        List<InternalAggregations> bufferedAggs,
        List<TopDocs> bufferedTopDocs,
        TopDocsStats topDocsStats,
        int numReducePhases,
        boolean isScrollRequest,
        AggregationReduceContext.Builder aggReduceContextBuilder,
        RankCoordinatorContext rankCoordinatorContext,
        boolean performFinalReduce
    ) {
        assert numReducePhases >= 0 : "num reduce phases must be >= 0 but was: " + numReducePhases;
        numReducePhases++; // increment for this phase
        if (queryResults.isEmpty()) { // early terminate we have nothing to reduce
            final TotalHits totalHits = topDocsStats.getTotalHits();
            return new ReducedQueryPhase(
                totalHits,
                topDocsStats.fetchHits,
                topDocsStats.getMaxScore(),
                false,
                null,
                null,
                null,
                null,
                SortedTopDocs.EMPTY,
                null,
                null,
                numReducePhases,
                0,
                0,
                true
            );
        }
        int total = queryResults.size();
        queryResults = queryResults.stream().filter(res -> res.queryResult().isNull() == false).toList();
        String errorMsg = "must have at least one non-empty search result, got 0 out of " + total;
        assert queryResults.isEmpty() == false : errorMsg;
        if (queryResults.isEmpty()) {
            throw new IllegalStateException(errorMsg);
        }
        validateMergeSortValueFormats(queryResults);
        final boolean hasSuggest = queryResults.stream().anyMatch(res -> res.queryResult().suggest() != null);
        final boolean hasProfileResults = queryResults.stream().anyMatch(res -> res.queryResult().hasProfileResults());

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        final Map<String, List<Suggestion<?>>> groupedSuggestions = hasSuggest ? new HashMap<>() : Collections.emptyMap();
        final Map<String, SearchProfileQueryPhaseResult> profileShardResults = hasProfileResults
            ? Maps.newMapWithExpectedSize(queryResults.size())
            : Collections.emptyMap();
        int from = 0;
        int size = 0;
        DocValueFormat[] sortValueFormats = null;
        for (SearchPhaseResult entry : queryResults) {
            QuerySearchResult result = entry.queryResult();
            from = result.from();
            // sorted queries can set the size to 0 if they have enough competitive hits.
            size = Math.max(result.size(), size);
            if (result.sortValueFormats() != null) {
                sortValueFormats = result.sortValueFormats();
            }

            if (hasSuggest) {
                assert result.suggest() != null;
                for (Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> suggestion : result.suggest()) {
                    List<Suggestion<?>> suggestionList = groupedSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                    suggestionList.add(suggestion);
                    if (suggestion instanceof CompletionSuggestion completionSuggestion) {
                        completionSuggestion.setShardIndex(result.getShardIndex());
                    }
                }
            }
            if (bufferedTopDocs.isEmpty() == false) {
                assert result.hasConsumedTopDocs() : "firstResult has no aggs but we got non null buffered aggs?";
            }
            if (hasProfileResults) {
                String key = result.getSearchShardTarget().toString();
                profileShardResults.put(key, result.consumeProfileResult());
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
        final InternalAggregations aggregations = reduceAggs(aggReduceContextBuilder, performFinalReduce, bufferedAggs);
        final SearchProfileResultsBuilder profileBuilder = profileShardResults.isEmpty()
            ? null
            : new SearchProfileResultsBuilder(profileShardResults);
        final SortedTopDocs sortedTopDocs = rankCoordinatorContext == null
            ? sortDocs(isScrollRequest, bufferedTopDocs, from, size, reducedCompletionSuggestions)
            : rankCoordinatorContext.rank(queryResults.stream().map(SearchPhaseResult::queryResult).toList(), topDocsStats);
        if (rankCoordinatorContext != null) {
            size = sortedTopDocs.scoreDocs.length;
        }
        final TotalHits totalHits = topDocsStats.getTotalHits();
        return new ReducedQueryPhase(
            totalHits,
            topDocsStats.fetchHits,
            topDocsStats.getMaxScore(),
            topDocsStats.timedOut,
            topDocsStats.terminatedEarly,
            reducedSuggest,
            aggregations,
            profileBuilder,
            sortedTopDocs,
            sortValueFormats,
            rankCoordinatorContext,
            numReducePhases,
            size,
            from,
            false
        );
    }

    private static InternalAggregations reduceAggs(
        AggregationReduceContext.Builder aggReduceContextBuilder,
        boolean performFinalReduce,
        List<InternalAggregations> toReduce
    ) {
        return toReduce.isEmpty()
            ? null
            : InternalAggregations.topLevelReduce(
                toReduce,
                performFinalReduce ? aggReduceContextBuilder.forFinalReduction() : aggReduceContextBuilder.forPartialReduction()
            );
    }

    /**
     * Checks that query results from all shards have consistent unsigned_long format.
     * Sort queries on a field that has long type in one index, and unsigned_long in another index
     * don't work correctly. Throw an error if this kind of sorting is detected.
     * //TODO: instead of throwing error, find a way to sort long and unsigned_long together
     */
    private static void validateMergeSortValueFormats(Collection<? extends SearchPhaseResult> queryResults) {
        boolean[] ulFormats = null;
        boolean firstResult = true;
        for (SearchPhaseResult entry : queryResults) {
            DocValueFormat[] formats = entry.queryResult().sortValueFormats();
            if (formats == null) return;
            if (firstResult) {
                firstResult = false;
                ulFormats = new boolean[formats.length];
                for (int i = 0; i < formats.length; i++) {
                    ulFormats[i] = formats[i] == DocValueFormat.UNSIGNED_LONG_SHIFTED ? true : false;
                }
            } else {
                for (int i = 0; i < formats.length; i++) {
                    // if the format is unsigned_long in one shard, and something different in another shard
                    if (ulFormats[i] ^ (formats[i] == DocValueFormat.UNSIGNED_LONG_SHIFTED)) {
                        throw new IllegalArgumentException(
                            "Can't do sort across indices, as a field has [unsigned_long] type "
                                + "in one index, and different type in another index!"
                        );
                    }
                }
            }
        }
    }

    /*
     * Returns the size of the requested top documents (from + size)
     */
    static int getTopDocsSize(SearchRequest request) {
        if (request.source() == null) {
            return SearchService.DEFAULT_SIZE;
        }
        SearchSourceBuilder source = request.source();
        return (source.size() == -1 ? SearchService.DEFAULT_SIZE : source.size()) + (source.from() == -1
            ? SearchService.DEFAULT_FROM
            : source.from());
    }

    public record ReducedQueryPhase(
        // the sum of all hits across all reduces shards
        TotalHits totalHits,
        // the number of returned hits (doc IDs) across all reduces shards
        long fetchHits,
        // the max score across all reduces hits or {@link Float#NaN} if no hits returned
        float maxScore,
        // <code>true</code> if at least one reduced result timed out
        boolean timedOut,
        // non null and true if at least one reduced result was terminated early
        Boolean terminatedEarly,
        // the reduced suggest results
        Suggest suggest,
        // the reduced internal aggregations
        InternalAggregations aggregations,
        // the reduced profile results
        SearchProfileResultsBuilder profileBuilder,
        // encloses info about the merged top docs, the sort fields used to sort the score docs etc.
        SortedTopDocs sortedTopDocs,
        // sort value formats used to sort / format the result
        DocValueFormat[] sortValueFormats,
        // the rank context if ranking is used
        RankCoordinatorContext rankCoordinatorContext,
        // the number of reduces phases
        int numReducePhases,
        // the size of the top hits to return
        int size,
        // the offset into the merged top hits
        int from,
        // <code>true</code> iff the query phase had no results. Otherwise <code>false</code>
        boolean isEmptyResult
    ) {

        public ReducedQueryPhase {
            if (numReducePhases <= 0) {
                throw new IllegalArgumentException("at least one reduce phase must have been applied but was: " + numReducePhases);
            }
        }

        /**
         * Creates a new search response from the given merged hits.
         * @see #merge(boolean, ReducedQueryPhase, Collection, IntFunction)
         */
        public InternalSearchResponse buildResponse(SearchHits hits, Collection<? extends SearchPhaseResult> fetchResults) {
            return new InternalSearchResponse(
                hits,
                aggregations,
                suggest,
                buildSearchProfileResults(fetchResults),
                timedOut,
                terminatedEarly,
                numReducePhases
            );
        }

        private SearchProfileResults buildSearchProfileResults(Collection<? extends SearchPhaseResult> fetchResults) {
            if (profileBuilder == null) {
                assert fetchResults.stream()
                    .map(SearchPhaseResult::fetchResult)
                    .filter(r -> r != null)
                    .allMatch(r -> r.profileResult() == null) : "found fetch profile without search profile";
                return null;

            }
            return profileBuilder.build(fetchResults);
        }
    }

    AggregationReduceContext.Builder getReduceContext(Supplier<Boolean> isCanceled, AggregatorFactories.Builder aggs) {
        return requestToAggReduceContextBuilder.apply(isCanceled, aggs);
    }

    /**
     * Returns a new {@link QueryPhaseResultConsumer} instance that reduces search responses incrementally.
     */
    QueryPhaseResultConsumer newSearchPhaseResults(
        Executor executor,
        CircuitBreaker circuitBreaker,
        Supplier<Boolean> isCanceled,
        SearchProgressListener listener,
        SearchRequest request,
        int numShards,
        Consumer<Exception> onPartialMergeFailure
    ) {
        return new QueryPhaseResultConsumer(
            request,
            executor,
            circuitBreaker,
            this,
            isCanceled,
            listener,
            numShards,
            onPartialMergeFailure
        );
    }

    public static final class TopDocsStats {
        final int trackTotalHitsUpTo;
        long totalHits;
        private TotalHits.Relation totalHitsRelation;
        public long fetchHits;
        private float maxScore = Float.NEGATIVE_INFINITY;
        public boolean timedOut;
        public Boolean terminatedEarly;

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

        void add(TopDocsAndMaxScore topDocs, boolean timedOut, Boolean terminatedEarly) {
            if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                totalHits += topDocs.topDocs.totalHits.value;
                if (topDocs.topDocs.totalHits.relation == Relation.GREATER_THAN_OR_EQUAL_TO) {
                    totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                }
            }
            fetchHits += topDocs.topDocs.scoreDocs.length;
            if (Float.isNaN(topDocs.maxScore) == false) {
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

    public record SortedTopDocs(
        // the searches merged top docs
        ScoreDoc[] scoreDocs,
        // <code>true</code> iff the result score docs is sorted by a field (not score), this implies that <code>sortField</code> is set.
        boolean isSortedByField,
        // the top docs sort fields used to sort the score docs, <code>null</code> if the results are not sorted
        SortField[] sortFields,
        String collapseField,
        Object[] collapseValues,
        int numberOfCompletionsSuggestions
    ) {
        public static final SortedTopDocs EMPTY = new SortedTopDocs(EMPTY_DOCS, false, null, null, null, 0);
    }
}
