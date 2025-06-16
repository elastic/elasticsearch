/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.fielddata.IndexFieldData;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchService;
import org.elasticsearch.search.SearchSortValues;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileResultsBuilder;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
import org.elasticsearch.search.sort.ShardDocSortField;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static org.elasticsearch.search.SearchService.DEFAULT_SIZE;

public final class SearchPhaseController {

    private final BiFunction<
        Supplier<Boolean>,
        AggregatorFactories.Builder,
        AggregationReduceContext.Builder> requestToAggReduceContextBuilder;

    public SearchPhaseController(
        BiFunction<Supplier<Boolean>, AggregatorFactories.Builder, AggregationReduceContext.Builder> requestToAggReduceContextBuilder
    ) {
        this.requestToAggReduceContextBuilder = requestToAggReduceContextBuilder;
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
        final ScoreDoc[] mergedScoreDocs = mergedTopDocs == null ? Lucene.EMPTY_SCORE_DOCS : mergedTopDocs.scoreDocs;
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
        List<TopDocs> topDocsList = results.stream().filter(Objects::nonNull).toList();
        if (topDocsList.isEmpty()) {
            return null;
        }
        final TopDocs topDocs = topDocsList.stream().findFirst().get();
        final int numShards = results.size();
        if (numShards == 1 && from == 0) { // only one shard and no pagination we can just return the topDocs as we got them.
            return topDocs;
        }
        final TopDocs mergedTopDocs;
        if (topDocs instanceof TopFieldGroups firstTopDocs) {
            final Sort sort = validateSameSortTypesAndMaybeRewrite(results, firstTopDocs.fields);
            TopFieldGroups[] shardTopDocs = topDocsList.toArray(new TopFieldGroups[0]);
            mergedTopDocs = TopFieldGroups.merge(sort, from, topN, shardTopDocs, false);
        } else if (topDocs instanceof TopFieldDocs firstTopDocs) {
            TopFieldDocs[] shardTopDocs = topDocsList.toArray(new TopFieldDocs[0]);
            final Sort sort = validateSameSortTypesAndMaybeRewrite(results, firstTopDocs.fields);
            mergedTopDocs = TopDocs.merge(sort, from, topN, shardTopDocs);
        } else {
            final TopDocs[] shardTopDocs = topDocsList.toArray(new TopDocs[0]);
            mergedTopDocs = TopDocs.merge(from, topN, shardTopDocs);
        }
        return mergedTopDocs;
    }

    private static Sort validateSameSortTypesAndMaybeRewrite(Collection<TopDocs> results, SortField[] firstSortFields) {
        Sort sort = new Sort(firstSortFields);
        if (results.size() < 2) return sort;

        SortField.Type[] firstTypes = null;
        boolean isFirstResult = true;
        Set<Integer> fieldIdsWithMixedIntAndLongSorts = new HashSet<>();
        for (TopDocs topDocs : results) {
            // We don't actually merge in empty score docs, so ignore potentially mismatched types if there are no docs
            if (topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0) {
                continue;
            }
            SortField[] curSortFields = ((TopFieldDocs) topDocs).fields;
            if (isFirstResult) {
                sort = new Sort(curSortFields);
                firstTypes = new SortField.Type[curSortFields.length];
                for (int i = 0; i < curSortFields.length; i++) {
                    firstTypes[i] = getType(curSortFields[i]);
                    if (firstTypes[i] == SortField.Type.CUSTOM) {
                        // for custom types that we can't resolve, we can't do the check
                        return sort;
                    }
                }
                isFirstResult = false;
            } else {
                for (int i = 0; i < curSortFields.length; i++) {
                    SortField.Type curType = getType(curSortFields[i]);
                    if (curType != firstTypes[i]) {
                        if (curType == SortField.Type.CUSTOM) {
                            // for custom types that we can't resolve, we can't do the check
                            return sort;
                        }
                        // Check if we are mixing INT and LONG sort types, which is allowed
                        if ((firstTypes[i] == SortField.Type.INT && curType == SortField.Type.LONG)
                            || (firstTypes[i] == SortField.Type.LONG && curType == SortField.Type.INT)) fieldIdsWithMixedIntAndLongSorts
                                .add(i);
                        else {
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
        if (fieldIdsWithMixedIntAndLongSorts.size() > 0) {
            sort = rewriteSortAndResultsToLong(sort, results, fieldIdsWithMixedIntAndLongSorts);
        }
        return sort;
    }

    /**
     * Rewrite Sort objects and shards results for long sort for mixed fields:
     * convert Sort to Long sort and convert fields' values to Long values.
     * This is necessary to enable comparison of fields' values across shards for merging.
     */
    private static Sort rewriteSortAndResultsToLong(Sort sort, Collection<TopDocs> results, Set<Integer> fieldIdsWithMixedIntAndLongSorts) {
        SortField[] newSortFields = sort.getSort();
        for (int fieldIdx : fieldIdsWithMixedIntAndLongSorts) {
            for (TopDocs topDocs : results) {
                if (topDocs.scoreDocs == null || topDocs.scoreDocs.length == 0) continue;
                SortField[] sortFields = ((TopFieldDocs) topDocs).fields;
                if (getType(sortFields[fieldIdx]) == SortField.Type.INT) {
                    for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                        FieldDoc fieldDoc = (FieldDoc) scoreDoc;
                        fieldDoc.fields[fieldIdx] = ((Number) fieldDoc.fields[fieldIdx]).longValue();
                    }
                } else { // SortField.Type.LONG
                    newSortFields[fieldIdx] = sortFields[fieldIdx];
                }
            }
        }
        return new Sort(newSortFields);
    }

    private static SortField.Type getType(SortField sortField) {
        if (sortField instanceof SortedNumericSortField sf) {
            return sf.getNumericType();
        } else if (sortField instanceof SortedSetSortField) {
            return SortField.Type.STRING;
        } else if (sortField.getComparatorSource() instanceof IndexFieldData.XFieldComparatorSource cmp) {
            // This can occur if the sort field wasn't rewritten by Lucene#rewriteMergeSortField because all search shards are local.
            return cmp.reducedType();
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
    public static SearchResponseSections merge(
        boolean ignoreFrom,
        ReducedQueryPhase reducedQueryPhase,
        AtomicArray<? extends SearchPhaseResult> fetchResultsArray
    ) {
        if (reducedQueryPhase.isEmptyResult) {
            return SearchResponseSections.EMPTY_WITH_TOTAL_HITS;
        }
        var fetchResults = fetchResultsArray.asList();
        SearchHits hits = getHits(reducedQueryPhase, ignoreFrom, fetchResultsArray);
        try {
            if (reducedQueryPhase.suggest != null && fetchResults.isEmpty() == false) {
                mergeSuggest(reducedQueryPhase, fetchResultsArray, hits.getHits().length, reducedQueryPhase.sortedTopDocs.scoreDocs);
            }
            var res = reducedQueryPhase.buildResponse(hits, fetchResults);
            hits = null;
            return res;
        } finally {
            if (hits != null) {
                hits.decRef();
            }
        }
    }

    private static void mergeSuggest(
        ReducedQueryPhase reducedQueryPhase,
        AtomicArray<? extends SearchPhaseResult> fetchResultsArray,
        int currentOffset,
        ScoreDoc[] sortedDocs
    ) {
        for (CompletionSuggestion suggestion : reducedQueryPhase.suggest.filter(CompletionSuggestion.class)) {
            final List<CompletionSuggestion.Entry.Option> suggestionOptions = suggestion.getOptions();
            for (int scoreDocIndex = currentOffset; scoreDocIndex < currentOffset + suggestionOptions.size(); scoreDocIndex++) {
                ScoreDoc shardDoc = sortedDocs[scoreDocIndex];
                SearchPhaseResult searchResultProvider = fetchResultsArray.get(shardDoc.shardIndex);
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

    private static SearchHits getHits(
        ReducedQueryPhase reducedQueryPhase,
        boolean ignoreFrom,
        AtomicArray<? extends SearchPhaseResult> fetchResultsArray
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
        var fetchResults = fetchResultsArray.asList();
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
                SearchPhaseResult fetchResultProvider = fetchResultsArray.get(shardDoc.shardIndex);
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
                if (shardDoc instanceof RankDoc) {
                    searchHit.setRank(((RankDoc) shardDoc).rank);
                    searchHit.score(shardDoc.score);
                    long shardAndDoc = ShardDocSortField.encodeShardAndDoc(shardDoc.shardIndex, shardDoc.doc);
                    searchHit.sortValues(
                        new SearchSortValues(
                            new Object[] { shardDoc.score, shardAndDoc },
                            new DocValueFormat[] { DocValueFormat.RAW, DocValueFormat.RAW }
                        )
                    );
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
                searchHit.incRef();
            }
        }
        return new SearchHits(
            hits.toArray(SearchHits.EMPTY),
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
     * @param reducedAggs already reduced aggregations
     * @param bufferedTopDocs a list of pre-collected top docs.
     * @param numReducePhases the number of non-final reduce phases applied to the query results.
     * @see QuerySearchResult#getAggs()
     * @see QuerySearchResult#consumeProfileResult()
     */
    static ReducedQueryPhase reducedQueryPhase(
        Collection<? extends SearchPhaseResult> queryResults,
        @Nullable InternalAggregations reducedAggs,
        List<TopDocs> bufferedTopDocs,
        TopDocsStats topDocsStats,
        int numReducePhases,
        boolean isScrollRequest,
        QueryPhaseRankCoordinatorContext queryPhaseRankCoordinatorContext
    ) {
        assert numReducePhases >= 0 : "num reduce phases must be >= 0 but was: " + numReducePhases;
        numReducePhases++; // increment for this phase
        if (queryResults.isEmpty()) { // early terminate we have nothing to reduce
            return new ReducedQueryPhase(
                topDocsStats.getTotalHits(),
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
        final List<QuerySearchResult> nonNullResults = new ArrayList<>();
        boolean hasSuggest = false;
        boolean hasProfileResults = false;
        for (SearchPhaseResult queryResult : queryResults) {
            var res = queryResult.queryResult();
            if (res.isNull()) {
                continue;
            }
            hasSuggest |= res.suggest() != null;
            hasProfileResults |= res.hasProfileResults();
            nonNullResults.add(res);
        }
        validateMergeSortValueFormats(nonNullResults);
        if (nonNullResults.isEmpty()) {
            var ex = new IllegalStateException("must have at least one non-empty search result, got 0 out of " + queryResults.size());
            assert false : ex;
            throw ex;
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        final Map<String, List<Suggestion<?>>> groupedSuggestions = hasSuggest ? new HashMap<>() : Collections.emptyMap();
        final Map<String, SearchProfileQueryPhaseResult> profileShardResults = hasProfileResults
            ? Maps.newMapWithExpectedSize(nonNullResults.size())
            : Collections.emptyMap();
        int from = 0;
        int size = 0;
        DocValueFormat[] sortValueFormats = null;
        for (QuerySearchResult result : nonNullResults) {
            from = result.from();
            // sorted queries can set the size to 0 if they have enough competitive hits.
            size = Math.max(result.size(), size);
            if (result.sortValueFormats() != null) {
                sortValueFormats = result.sortValueFormats();
            }

            if (hasSuggest) {
                assert result.suggest() != null;
                for (Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> suggestion : result.suggest()) {
                    groupedSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>()).add(suggestion);
                    if (suggestion instanceof CompletionSuggestion completionSuggestion) {
                        completionSuggestion.setShardIndex(result.getShardIndex());
                    }
                }
            }
            assert bufferedTopDocs.isEmpty() || result.hasConsumedTopDocs() : "firstResult has no aggs but we got non null buffered aggs?";
            if (hasProfileResults) {
                profileShardResults.put(result.getSearchShardTarget().toString(), result.consumeProfileResult());
            }
        }
        final Suggest reducedSuggest = groupedSuggestions.isEmpty() ? null : new Suggest(Suggest.reduce(groupedSuggestions));
        final SortedTopDocs sortedTopDocs;
        if (queryPhaseRankCoordinatorContext == null) {
            sortedTopDocs = sortDocs(
                isScrollRequest,
                bufferedTopDocs,
                from,
                size,
                reducedSuggest == null ? Collections.emptyList() : reducedSuggest.filter(CompletionSuggestion.class)
            );
        } else {
            sortedTopDocs = new SortedTopDocs(
                queryPhaseRankCoordinatorContext.rankQueryPhaseResults(nonNullResults, topDocsStats),
                false,
                null,
                null,
                null,
                0
            );
            size = sortedTopDocs.scoreDocs.length;
            // we need to reset from here as pagination and result trimming has already taken place
            // within the `QueryPhaseRankCoordinatorContext#rankQueryPhaseResults` and we don't want
            // to apply it again in the `getHits` method.
            from = 0;
        }
        return new ReducedQueryPhase(
            topDocsStats.getTotalHits(),
            topDocsStats.fetchHits,
            topDocsStats.getMaxScore(),
            topDocsStats.timedOut,
            topDocsStats.terminatedEarly,
            reducedSuggest,
            reducedAggs,
            profileShardResults.isEmpty() ? null : new SearchProfileResultsBuilder(profileShardResults),
            sortedTopDocs,
            sortValueFormats,
            queryPhaseRankCoordinatorContext,
            numReducePhases,
            size,
            from,
            false
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
                    ulFormats[i] = formats[i] == DocValueFormat.UNSIGNED_LONG_SHIFTED;
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
            return DEFAULT_SIZE;
        }
        SearchSourceBuilder source = request.source();
        if (source.rankBuilder() != null) {
            // if we have a RankBuilder defined, it needs to have access to all the documents in order to rerank them
            // so we override size here and keep all `rank_window_size` docs.
            // Pagination is taking place later through RankFeaturePhaseRankCoordinatorContext#rankAndPaginate
            return source.rankBuilder().rankWindowSize();
        }
        return (source.size() == -1 ? DEFAULT_SIZE : source.size()) + (source.from() == -1 ? SearchService.DEFAULT_FROM : source.from());
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
        QueryPhaseRankCoordinatorContext queryPhaseRankCoordinatorContext,
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
         * @see #merge(boolean, ReducedQueryPhase, AtomicArray)
         */
        public SearchResponseSections buildResponse(SearchHits hits, Collection<? extends SearchPhaseResult> fetchResults) {
            return new SearchResponseSections(
                hits,
                aggregations,
                suggest,
                timedOut,
                terminatedEarly,
                buildSearchProfileResults(fetchResults),
                numReducePhases
            );
        }

        private SearchProfileResults buildSearchProfileResults(Collection<? extends SearchPhaseResult> fetchResults) {
            if (profileBuilder == null) {
                assert fetchResults.stream().map(SearchPhaseResult::fetchResult).allMatch(r -> r == null || r.profileResult() == null)
                    : "found fetch profile without search profile";
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
    SearchPhaseResults<SearchPhaseResult> newSearchPhaseResults(
        Executor executor,
        CircuitBreaker circuitBreaker,
        Supplier<Boolean> isCanceled,
        SearchProgressListener listener,
        SearchRequest request,
        int numShards,
        Consumer<Exception> onPartialMergeFailure
    ) {
        final int size = request.source() == null || request.source().size() == -1 ? SearchService.DEFAULT_SIZE : request.source().size();
        // Use CountOnlyQueryPhaseResultConsumer for requests without aggs, suggest, etc. things only wanting a total count and
        // returning no hits
        if (size == 0
            && (request.source() == null
                || (request.source().aggregations() == null
                    && request.source().suggest() == null
                    && request.source().rankBuilder() == null
                    && request.source().knnSearch().isEmpty()
                    && request.source().profile() == false))
            && request.resolveTrackTotalHitsUpTo() == SearchContext.TRACK_TOTAL_HITS_ACCURATE) {
            return new CountOnlyQueryPhaseResultConsumer(listener, numShards);
        }
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

    public static final class TopDocsStats implements Writeable {
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

        void add(TopDocsStats other) {
            if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                totalHits += other.totalHits;
                if (other.totalHitsRelation == Relation.GREATER_THAN_OR_EQUAL_TO) {
                    totalHitsRelation = TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO;
                }
            }
            fetchHits += other.fetchHits;
            if (Float.isNaN(other.maxScore) == false) {
                maxScore = Math.max(maxScore, other.maxScore);
            }
            if (other.timedOut) {
                this.timedOut = true;
            }
            if (other.terminatedEarly != null) {
                if (this.terminatedEarly == null) {
                    this.terminatedEarly = other.terminatedEarly;
                } else if (terminatedEarly) {
                    this.terminatedEarly = true;
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

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(trackTotalHitsUpTo);
            out.writeFloat(maxScore);
            Lucene.writeTotalHits(out, new TotalHits(totalHits, totalHitsRelation));
            out.writeVLong(fetchHits);
            out.writeFloat(maxScore);
            out.writeBoolean(timedOut);
            out.writeOptionalBoolean(terminatedEarly);
        }

        public static TopDocsStats readFrom(StreamInput in) throws IOException {
            TopDocsStats res = new TopDocsStats(in.readVInt());
            res.maxScore = in.readFloat();
            TotalHits totalHits = Lucene.readTotalHits(in);
            res.totalHits = totalHits.value;
            res.totalHitsRelation = totalHits.relation;
            res.fetchHits = in.readVLong();
            res.maxScore = in.readFloat();
            res.timedOut = in.readBoolean();
            res.terminatedEarly = in.readOptionalBoolean();
            return res;
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
        public static final SortedTopDocs EMPTY = new SortedTopDocs(Lucene.EMPTY_SCORE_DOCS, false, null, null, null, 0);
    }
}
