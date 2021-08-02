/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import com.carrotsearch.hppc.IntArrayList;
import com.carrotsearch.hppc.ObjectObjectHashMap;

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
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

public final class SearchPhaseController {
    private static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final NamedWriteableRegistry namedWriteableRegistry;
    private final Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder;

    public SearchPhaseController(NamedWriteableRegistry namedWriteableRegistry,
            Function<SearchRequest, InternalAggregation.ReduceContextBuilder> requestToAggReduceContextBuilder) {
        this.namedWriteableRegistry = namedWriteableRegistry;
        this.requestToAggReduceContextBuilder = requestToAggReduceContextBuilder;
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

            assert lEntry.fieldStatistics().containsKey(null) == false;
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
     * @param topDocs the buffered top docs
     * @param from the offset into the search results top docs
     * @param size the number of hits to return from the merged top docs
     */
    static SortedTopDocs sortDocs(boolean ignoreFrom, final Collection<TopDocs> topDocs, int from, int size,
                                  List<CompletionSuggestion> reducedCompletionSuggestions) {
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
        return new SortedTopDocs(scoreDocs, isSortedByField, sortFields, collapseField, collapseValues, numSuggestDocs);
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
        } else if (topDocs instanceof CollapseTopFieldDocs) {
            CollapseTopFieldDocs firstTopDocs = (CollapseTopFieldDocs) topDocs;
            final Sort sort = new Sort(firstTopDocs.fields);
            final CollapseTopFieldDocs[] shardTopDocs = results.toArray(new CollapseTopFieldDocs[numShards]);
            for (int i = 0; i < shardTopDocs.length; i++) {
                for (ScoreDoc sd : shardTopDocs[i].scoreDocs) {
                    sd.shardIndex = i;
                }
            }
            mergedTopDocs = CollapseTopFieldDocs.merge(sort, from, topN, shardTopDocs, false);
        } else if (topDocs instanceof TopFieldDocs) {
            TopFieldDocs firstTopDocs = (TopFieldDocs) topDocs;
            final Sort sort = new Sort(firstTopDocs.fields);
            final TopFieldDocs[] shardTopDocs = results.toArray(new TopFieldDocs[numShards]);
            for (int i = 0; i < shardTopDocs.length; i++) {
                for (ScoreDoc sd : shardTopDocs[i].scoreDocs) {
                    sd.shardIndex = i;
                }
            }
            mergedTopDocs = TopDocs.merge(sort, from, topN, shardTopDocs);
        } else {
            final TopDocs[] shardTopDocs = results.toArray(new TopDocs[numShards]);
            for (int i = 0; i < shardTopDocs.length; i++) {
                for (ScoreDoc sd : shardTopDocs[i].scoreDocs) {
                    sd.shardIndex = i;
                }
            }
            mergedTopDocs = TopDocs.merge(from, topN, shardTopDocs);
        }
        return mergedTopDocs;
    }

    static void setShardIndex(TopDocs topDocs, int shardIndex) {
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
        return reducedQueryPhase(queryResults, Collections.emptyList(), topDocs, topDocsStats,
            0, true, aggReduceContextBuilder, true);
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
    ReducedQueryPhase reducedQueryPhase(Collection<? extends SearchPhaseResult> queryResults,
                                        List<InternalAggregations> bufferedAggs,
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
        validateMergeSortValueFormats(queryResults);
        final boolean hasSuggest = queryResults.stream().anyMatch(res -> res.queryResult().suggest() != null);
        final boolean hasProfileResults =  queryResults.stream().anyMatch(res -> res.queryResult().hasProfileResults());

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        final Map<String, List<Suggestion<?>>> groupedSuggestions = hasSuggest ? new HashMap<>() : Collections.emptyMap();
        final Map<String, ProfileShardResult> profileResults = hasProfileResults ? new HashMap<>(queryResults.size())
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
                    if (suggestion instanceof CompletionSuggestion) {
                        CompletionSuggestion completionSuggestion = (CompletionSuggestion) suggestion;
                        completionSuggestion.setShardIndex(result.getShardIndex());
                    }
                }
            }
            if (bufferedTopDocs.isEmpty() == false) {
                assert result.hasConsumedTopDocs() : "firstResult has no aggs but we got non null buffered aggs?";
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
        final InternalAggregations aggregations = reduceAggs(aggReduceContextBuilder, performFinalReduce, bufferedAggs);
        final SearchProfileShardResults shardResults = profileResults.isEmpty() ? null : new SearchProfileShardResults(profileResults);
        final SortedTopDocs sortedTopDocs = sortDocs(isScrollRequest, bufferedTopDocs, from, size, reducedCompletionSuggestions);
        final TotalHits totalHits = topDocsStats.getTotalHits();
        return new ReducedQueryPhase(totalHits, topDocsStats.fetchHits, topDocsStats.getMaxScore(),
            topDocsStats.timedOut, topDocsStats.terminatedEarly, reducedSuggest, aggregations, shardResults, sortedTopDocs,
            sortValueFormats, numReducePhases, size, from, false);
    }

    private static InternalAggregations reduceAggs(InternalAggregation.ReduceContextBuilder aggReduceContextBuilder,
                                                   boolean performFinalReduce,
                                                   List<InternalAggregations> toReduce) {
        return toReduce.isEmpty() ? null : InternalAggregations.topLevelReduce(toReduce,
            performFinalReduce ? aggReduceContextBuilder.forFinalReduction() : aggReduceContextBuilder.forPartialReduction());
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
                        throw new IllegalArgumentException("Can't do sort across indices, as a field has [unsigned_long] type " +
                            "in one index, and different type in another index!");
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

    InternalAggregation.ReduceContextBuilder getReduceContext(SearchRequest request) {
        return requestToAggReduceContextBuilder.apply(request);
    }

    /**
     * Returns a new {@link QueryPhaseResultConsumer} instance that reduces search responses incrementally.
     */
    QueryPhaseResultConsumer newSearchPhaseResults(Executor executor,
                                                   CircuitBreaker circuitBreaker,
                                                   SearchProgressListener listener,
                                                   SearchRequest request,
                                                   int numShards,
                                                   Consumer<Exception> onPartialMergeFailure) {
        return new QueryPhaseResultConsumer(request, executor, circuitBreaker,
            this,  listener, namedWriteableRegistry, numShards, onPartialMergeFailure);
    }

    static final class TopDocsStats {
        final int trackTotalHitsUpTo;
        long totalHits;
        private TotalHits.Relation totalHitsRelation;
        long fetchHits;
        private float maxScore = Float.NEGATIVE_INFINITY;
        boolean timedOut;
        Boolean terminatedEarly;

        TopDocsStats(int trackTotalHitsUpTo) {
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

    static final class SortedTopDocs {
        static final SortedTopDocs EMPTY = new SortedTopDocs(EMPTY_DOCS, false, null, null, null, 0);
        // the searches merged top docs
        final ScoreDoc[] scoreDocs;
        // <code>true</code> iff the result score docs is sorted by a field (not score), this implies that <code>sortField</code> is set.
        final boolean isSortedByField;
        // the top docs sort fields used to sort the score docs, <code>null</code> if the results are not sorted
        final SortField[] sortFields;
        final String collapseField;
        final Object[] collapseValues;
        final int numberOfCompletionsSuggestions;

        SortedTopDocs(ScoreDoc[] scoreDocs, boolean isSortedByField, SortField[] sortFields,
                      String collapseField, Object[] collapseValues, int numberOfCompletionsSuggestions) {
            this.scoreDocs = scoreDocs;
            this.isSortedByField = isSortedByField;
            this.sortFields = sortFields;
            this.collapseField = collapseField;
            this.collapseValues = collapseValues;
            this.numberOfCompletionsSuggestions = numberOfCompletionsSuggestions;
        }
    }
}
