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
import org.apache.lucene.index.Term;
import org.apache.lucene.search.CollectionStatistics;
import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TermStatistics;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.profile.ProfileShardResult;
import org.elasticsearch.search.profile.SearchProfileShardResults;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.Suggest.Suggestion;
import org.elasticsearch.search.suggest.Suggest.Suggestion.Entry;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class SearchPhaseController extends AbstractComponent {

    private static final Comparator<AtomicArray.Entry<? extends QuerySearchResultProvider>> QUERY_RESULT_ORDERING = (o1, o2) -> {
        int i = o1.value.shardTarget().getIndex().compareTo(o2.value.shardTarget().getIndex());
        if (i == 0) {
            i = o1.value.shardTarget().getShardId().id() - o2.value.shardTarget().getShardId().id();
        }
        return i;
    };

    private static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final BigArrays bigArrays;
    private final ScriptService scriptService;

    public SearchPhaseController(Settings settings, BigArrays bigArrays, ScriptService scriptService) {
        super(settings);
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
    }

    public AggregatedDfs aggregateDfs(AtomicArray<DfsSearchResult> results) {
        ObjectObjectHashMap<Term, TermStatistics> termStatistics = HppcMaps.newNoNullKeysMap();
        ObjectObjectHashMap<String, CollectionStatistics> fieldStatistics = HppcMaps.newNoNullKeysMap();
        long aggMaxDoc = 0;
        for (AtomicArray.Entry<DfsSearchResult> lEntry : results.asList()) {
            final Term[] terms = lEntry.value.terms();
            final TermStatistics[] stats = lEntry.value.termStatistics();
            assert terms.length == stats.length;
            for (int i = 0; i < terms.length; i++) {
                assert terms[i] != null;
                TermStatistics existing = termStatistics.get(terms[i]);
                if (existing != null) {
                    assert terms[i].bytes().equals(existing.term());
                    // totalTermFrequency is an optional statistic we need to check if either one or both
                    // are set to -1 which means not present and then set it globally to -1
                    termStatistics.put(terms[i], new TermStatistics(existing.term(),
                            existing.docFreq() + stats[i].docFreq(),
                            optionalSum(existing.totalTermFreq(), stats[i].totalTermFreq())));
                } else {
                    termStatistics.put(terms[i], stats[i]);
                }

            }

            assert !lEntry.value.fieldStatistics().containsKey(null);
            final Object[] keys = lEntry.value.fieldStatistics().keys;
            final Object[] values = lEntry.value.fieldStatistics().values;
            for (int i = 0; i < keys.length; i++) {
                if (keys[i] != null) {
                    String key = (String) keys[i];
                    CollectionStatistics value = (CollectionStatistics) values[i];
                    assert key != null;
                    CollectionStatistics existing = fieldStatistics.get(key);
                    if (existing != null) {
                        CollectionStatistics merged = new CollectionStatistics(
                                key, existing.maxDoc() + value.maxDoc(),
                                optionalSum(existing.docCount(), value.docCount()),
                                optionalSum(existing.sumTotalTermFreq(), value.sumTotalTermFreq()),
                                optionalSum(existing.sumDocFreq(), value.sumDocFreq())
                        );
                        fieldStatistics.put(key, merged);
                    } else {
                        fieldStatistics.put(key, value);
                    }
                }
            }
            aggMaxDoc += lEntry.value.maxDoc();
        }
        return new AggregatedDfs(termStatistics, fieldStatistics, aggMaxDoc);
    }

    private static long optionalSum(long left, long right) {
        return Math.min(left, right) == -1 ? -1 : left + right;
    }

    /**
     * Returns a score doc array of top N search docs across all shards, followed by top suggest docs for each
     * named completion suggestion across all shards. If more than one named completion suggestion is specified in the
     * request, the suggest docs for a named suggestion are ordered by the suggestion name.
     *
     * @param ignoreFrom Whether to ignore the from and sort all hits in each shard result.
     *                   Enabled only for scroll search, because that only retrieves hits of length 'size' in the query phase.
     * @param resultsArr Shard result holder
     */
    public ScoreDoc[] sortDocs(boolean ignoreFrom, AtomicArray<? extends QuerySearchResultProvider> resultsArr) throws IOException {
        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> results = resultsArr.asList();
        if (results.isEmpty()) {
            return EMPTY_DOCS;
        }

        boolean canOptimize = false;
        QuerySearchResult result = null;
        int shardIndex = -1;
        if (results.size() == 1) {
            canOptimize = true;
            result = results.get(0).value.queryResult();
            shardIndex = results.get(0).index;
        } else {
            // lets see if we only got hits from a single shard, if so, we can optimize...
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : results) {
                if (entry.value.queryResult().hasHits()) {
                    if (result != null) { // we already have one, can't really optimize
                        canOptimize = false;
                        break;
                    }
                    canOptimize = true;
                    result = entry.value.queryResult();
                    shardIndex = entry.index;
                }
            }
        }
        if (canOptimize) {
            int offset = result.from();
            if (ignoreFrom) {
                offset = 0;
            }
            ScoreDoc[] scoreDocs = result.topDocs().scoreDocs;
            ScoreDoc[] docs;
            int numSuggestDocs = 0;
            final Suggest suggest = result.queryResult().suggest();
            final List<CompletionSuggestion> completionSuggestions;
            if (suggest != null) {
                completionSuggestions = suggest.filter(CompletionSuggestion.class);
                for (CompletionSuggestion suggestion : completionSuggestions) {
                    numSuggestDocs += suggestion.getOptions().size();
                }
            } else {
                completionSuggestions = Collections.emptyList();
            }
            int docsOffset = 0;
            if (scoreDocs.length == 0 || scoreDocs.length < offset) {
                docs = new ScoreDoc[numSuggestDocs];
            } else {
                int resultDocsSize = result.size();
                if ((scoreDocs.length - offset) < resultDocsSize) {
                    resultDocsSize = scoreDocs.length - offset;
                }
                docs = new ScoreDoc[resultDocsSize + numSuggestDocs];
                for (int i = 0; i < resultDocsSize; i++) {
                    ScoreDoc scoreDoc = scoreDocs[offset + i];
                    scoreDoc.shardIndex = shardIndex;
                    docs[i] = scoreDoc;
                    docsOffset++;
                }
            }
            for (CompletionSuggestion suggestion: completionSuggestions) {
                for (CompletionSuggestion.Entry.Option option : suggestion.getOptions()) {
                    ScoreDoc doc = option.getDoc();
                    doc.shardIndex = shardIndex;
                    docs[docsOffset++] = doc;
                }
            }
            return docs;
        }

        @SuppressWarnings("unchecked")
        AtomicArray.Entry<? extends QuerySearchResultProvider>[] sortedResults = results.toArray(new AtomicArray.Entry[results.size()]);
        Arrays.sort(sortedResults, QUERY_RESULT_ORDERING);
        QuerySearchResultProvider firstResult = sortedResults[0].value;

        int topN = firstResult.queryResult().size();
        int from = firstResult.queryResult().from();
        if (ignoreFrom) {
            from = 0;
        }

        final TopDocs mergedTopDocs;
        int numShards = resultsArr.length();
        if (firstResult.queryResult().topDocs() instanceof CollapseTopFieldDocs) {
            CollapseTopFieldDocs firstTopDocs = (CollapseTopFieldDocs) firstResult.queryResult().topDocs();
            final Sort sort = new Sort(firstTopDocs.fields);

            final CollapseTopFieldDocs[] shardTopDocs = new CollapseTopFieldDocs[numShards];
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> sortedResult : sortedResults) {
                TopDocs topDocs = sortedResult.value.queryResult().topDocs();
                // the 'index' field is the position in the resultsArr atomic array
                shardTopDocs[sortedResult.index] = (CollapseTopFieldDocs) topDocs;
            }
            // TopDocs#merge can't deal with null shard TopDocs
            for (int i = 0; i < shardTopDocs.length; ++i) {
                if (shardTopDocs[i] == null) {
                    shardTopDocs[i] = new CollapseTopFieldDocs(firstTopDocs.field, 0, new FieldDoc[0],
                        sort.getSort(), new Object[0], Float.NaN);
                }
            }
            mergedTopDocs = CollapseTopFieldDocs.merge(sort, from, topN, shardTopDocs);
        } else if (firstResult.queryResult().topDocs() instanceof TopFieldDocs) {
            TopFieldDocs firstTopDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            final Sort sort = new Sort(firstTopDocs.fields);

            final TopFieldDocs[] shardTopDocs = new TopFieldDocs[resultsArr.length()];
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> sortedResult : sortedResults) {
                TopDocs topDocs = sortedResult.value.queryResult().topDocs();
                // the 'index' field is the position in the resultsArr atomic array
                shardTopDocs[sortedResult.index] = (TopFieldDocs) topDocs;
            }
            // TopDocs#merge can't deal with null shard TopDocs
            for (int i = 0; i < shardTopDocs.length; ++i) {
                if (shardTopDocs[i] == null) {
                    shardTopDocs[i] = new TopFieldDocs(0, new FieldDoc[0], sort.getSort(), Float.NaN);
                }
            }
            mergedTopDocs = TopDocs.merge(sort, from, topN, shardTopDocs);
        } else {
            final TopDocs[] shardTopDocs = new TopDocs[resultsArr.length()];
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> sortedResult : sortedResults) {
                TopDocs topDocs = sortedResult.value.queryResult().topDocs();
                // the 'index' field is the position in the resultsArr atomic array
                shardTopDocs[sortedResult.index] = topDocs;
            }
            // TopDocs#merge can't deal with null shard TopDocs
            for (int i = 0; i < shardTopDocs.length; ++i) {
                if (shardTopDocs[i] == null) {
                    shardTopDocs[i] = Lucene.EMPTY_TOP_DOCS;
                }
            }
            mergedTopDocs = TopDocs.merge(from, topN, shardTopDocs);
        }

        ScoreDoc[] scoreDocs = mergedTopDocs.scoreDocs;
        final Map<String, List<Suggestion<CompletionSuggestion.Entry>>> groupedCompletionSuggestions = new HashMap<>();
        // group suggestions and assign shard index
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> sortedResult : sortedResults) {
            Suggest shardSuggest = sortedResult.value.queryResult().suggest();
            if (shardSuggest != null) {
                for (CompletionSuggestion suggestion : shardSuggest.filter(CompletionSuggestion.class)) {
                    suggestion.setShardIndex(sortedResult.index);
                    List<Suggestion<CompletionSuggestion.Entry>> suggestions =
                        groupedCompletionSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                    suggestions.add(suggestion);
                }
            }
        }
        if (groupedCompletionSuggestions.isEmpty() == false) {
            int numSuggestDocs = 0;
            List<Suggestion<? extends Entry<? extends Entry.Option>>> completionSuggestions =
                new ArrayList<>(groupedCompletionSuggestions.size());
            for (List<Suggestion<CompletionSuggestion.Entry>> groupedSuggestions : groupedCompletionSuggestions.values()) {
                final CompletionSuggestion completionSuggestion = CompletionSuggestion.reduceTo(groupedSuggestions);
                assert completionSuggestion != null;
                numSuggestDocs += completionSuggestion.getOptions().size();
                completionSuggestions.add(completionSuggestion);
            }
            scoreDocs = new ScoreDoc[mergedTopDocs.scoreDocs.length + numSuggestDocs];
            System.arraycopy(mergedTopDocs.scoreDocs, 0, scoreDocs, 0, mergedTopDocs.scoreDocs.length);
            int offset = mergedTopDocs.scoreDocs.length;
            Suggest suggestions = new Suggest(completionSuggestions);
            for (CompletionSuggestion completionSuggestion : suggestions.filter(CompletionSuggestion.class)) {
                for (CompletionSuggestion.Entry.Option option : completionSuggestion.getOptions()) {
                    scoreDocs[offset++] = option.getDoc();
                }
            }
        }
        return scoreDocs;
    }

    public ScoreDoc[] getLastEmittedDocPerShard(ReducedQueryPhase reducedQueryPhase,
                                                ScoreDoc[] sortedScoreDocs, int numShards) {
        ScoreDoc[] lastEmittedDocPerShard = new ScoreDoc[numShards];
        if (reducedQueryPhase.isEmpty() == false) {
            // from is always zero as when we use scroll, we ignore from
            long size = Math.min(reducedQueryPhase.fetchHits, reducedQueryPhase.oneResult.size());
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
    public InternalSearchResponse merge(boolean ignoreFrom, ScoreDoc[] sortedDocs,
                                        ReducedQueryPhase reducedQueryPhase,
                                        AtomicArray<? extends QuerySearchResultProvider> fetchResultsArr) {
        if (reducedQueryPhase.isEmpty()) {
            return InternalSearchResponse.empty();
        }
        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> fetchResults = fetchResultsArr.asList();
        SearchHits hits = getHits(reducedQueryPhase, ignoreFrom, sortedDocs, fetchResultsArr);
        if (reducedQueryPhase.suggest != null) {
            if (!fetchResults.isEmpty()) {
                int currentOffset = hits.getHits().length;
                for (CompletionSuggestion suggestion : reducedQueryPhase.suggest.filter(CompletionSuggestion.class)) {
                    final List<CompletionSuggestion.Entry.Option> suggestionOptions = suggestion.getOptions();
                    for (int scoreDocIndex = currentOffset; scoreDocIndex < currentOffset + suggestionOptions.size(); scoreDocIndex++) {
                        ScoreDoc shardDoc = sortedDocs[scoreDocIndex];
                        QuerySearchResultProvider searchResultProvider = fetchResultsArr.get(shardDoc.shardIndex);
                        if (searchResultProvider == null) {
                            continue;
                        }
                        FetchSearchResult fetchResult = searchResultProvider.fetchResult();
                        int fetchResultIndex = fetchResult.counterGetAndIncrement();
                        if (fetchResultIndex < fetchResult.hits().internalHits().length) {
                            SearchHit hit = fetchResult.hits().internalHits()[fetchResultIndex];
                            CompletionSuggestion.Entry.Option suggestOption =
                                suggestionOptions.get(scoreDocIndex - currentOffset);
                            hit.score(shardDoc.score);
                            hit.shard(fetchResult.shardTarget());
                            suggestOption.setHit(hit);
                        }
                    }
                    currentOffset += suggestionOptions.size();
                }
                assert currentOffset == sortedDocs.length : "expected no more score doc slices";
            }
        }
        return reducedQueryPhase.buildResponse(hits);
    }

    private SearchHits getHits(ReducedQueryPhase reducedQueryPhase, boolean ignoreFrom, ScoreDoc[] sortedDocs,
                               AtomicArray<? extends QuerySearchResultProvider> fetchResultsArr) {
        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> fetchResults = fetchResultsArr.asList();
        boolean sorted = false;
        int sortScoreIndex = -1;
        if (reducedQueryPhase.oneResult.topDocs() instanceof TopFieldDocs) {
            TopFieldDocs fieldDocs = (TopFieldDocs) reducedQueryPhase.oneResult.queryResult().topDocs();
            if (fieldDocs instanceof CollapseTopFieldDocs &&
                fieldDocs.fields.length == 1 && fieldDocs.fields[0].getType() == SortField.Type.SCORE) {
                sorted = false;
            } else {
                sorted = true;
                for (int i = 0; i < fieldDocs.fields.length; i++) {
                    if (fieldDocs.fields[i].getType() == SortField.Type.SCORE) {
                        sortScoreIndex = i;
                    }
                }
            }
        }
        // clean the fetch counter
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : fetchResults) {
            entry.value.fetchResult().initCounter();
        }
        int from = ignoreFrom ? 0 : reducedQueryPhase.oneResult.queryResult().from();
        int numSearchHits = (int) Math.min(reducedQueryPhase.fetchHits - from, reducedQueryPhase.oneResult.size());
        // with collapsing we can have more fetch hits than sorted docs
        numSearchHits = Math.min(sortedDocs.length, numSearchHits);
        // merge hits
        List<SearchHit> hits = new ArrayList<>();
        if (!fetchResults.isEmpty()) {
            for (int i = 0; i < numSearchHits; i++) {
                ScoreDoc shardDoc = sortedDocs[i];
                QuerySearchResultProvider fetchResultProvider = fetchResultsArr.get(shardDoc.shardIndex);
                if (fetchResultProvider == null) {
                    continue;
                }
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                int index = fetchResult.counterGetAndIncrement();
                if (index < fetchResult.hits().internalHits().length) {
                    SearchHit searchHit = fetchResult.hits().internalHits()[index];
                    searchHit.score(shardDoc.score);
                    searchHit.shard(fetchResult.shardTarget());
                    if (sorted) {
                        FieldDoc fieldDoc = (FieldDoc) shardDoc;
                        searchHit.sortValues(fieldDoc.fields, reducedQueryPhase.oneResult.sortValueFormats());
                        if (sortScoreIndex != -1) {
                            searchHit.score(((Number) fieldDoc.fields[sortScoreIndex]).floatValue());
                        }
                    }
                    hits.add(searchHit);
                }
            }
        }
        return new SearchHits(hits.toArray(new SearchHit[hits.size()]), reducedQueryPhase.totalHits,
            reducedQueryPhase.maxScore);
    }

    /**
     * Reduces the given query results and consumes all aggregations and profile results.
     * @see QuerySearchResult#consumeAggs()
     * @see QuerySearchResult#consumeProfileResult()
     */
    public final ReducedQueryPhase reducedQueryPhase(List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> queryResults) {
        long totalHits = 0;
        long fetchHits = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
        if (queryResults.isEmpty()) {
            return new ReducedQueryPhase(totalHits, fetchHits, maxScore, timedOut, terminatedEarly, null, null, null, null);
        }
        QuerySearchResult firstResult = queryResults.get(0).value.queryResult();
        final boolean hasSuggest = firstResult.suggest() != null;
        final boolean hasAggs = firstResult.hasAggs();
        final boolean hasProfileResults = firstResult.hasProfileResults();
        final List<InternalAggregations> aggregationsList = hasAggs ? new ArrayList<>(queryResults.size()) : Collections.emptyList();
        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        final Map<String, List<Suggestion>> groupedSuggestions = hasSuggest ? new HashMap<>() : Collections.emptyMap();
        final Map<String, ProfileShardResult> profileResults = hasProfileResults ? new HashMap<>(queryResults.size())
            : Collections.emptyMap();
        for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
            QuerySearchResult result = entry.value.queryResult();
            if (result.searchTimedOut()) {
                timedOut = true;
            }
            if (result.terminatedEarly() != null) {
                if (terminatedEarly == null) {
                    terminatedEarly = result.terminatedEarly();
                } else if (result.terminatedEarly()) {
                    terminatedEarly = true;
                }
            }
            totalHits += result.topDocs().totalHits;
            fetchHits += result.topDocs().scoreDocs.length;
            if (!Float.isNaN(result.topDocs().getMaxScore())) {
                maxScore = Math.max(maxScore, result.topDocs().getMaxScore());
            }
            if (hasSuggest) {
                assert result.suggest() != null;
                for (Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> suggestion : result.suggest()) {
                    List<Suggestion> suggestionList = groupedSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                    suggestionList.add(suggestion);
                }
            }
            if (hasAggs) {
                aggregationsList.add((InternalAggregations) result.consumeAggs());
            }
            if (hasProfileResults) {
                String key = result.shardTarget().toString();
                profileResults.put(key, result.consumeProfileResult());
            }
        }
        final Suggest suggest = groupedSuggestions.isEmpty() ? null : new Suggest(Suggest.reduce(groupedSuggestions));
        final InternalAggregations aggregations = aggregationsList.isEmpty() ? null : reduceAggs(aggregationsList,
            firstResult.pipelineAggregators());
        final SearchProfileShardResults shardResults = profileResults.isEmpty() ? null : new SearchProfileShardResults(profileResults);
        return new ReducedQueryPhase(totalHits, fetchHits, maxScore, timedOut, terminatedEarly, firstResult, suggest, aggregations,
            shardResults);
    }

    private InternalAggregations reduceAggs(List<InternalAggregations> aggregationsList,
                                            List<SiblingPipelineAggregator> pipelineAggregators) {
        ReduceContext reduceContext = new ReduceContext(bigArrays, scriptService);
        InternalAggregations aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
        if (pipelineAggregators != null) {
            List<InternalAggregation> newAggs = StreamSupport.stream(aggregations.spliterator(), false)
                .map((p) -> (InternalAggregation) p)
                .collect(Collectors.toList());
            for (SiblingPipelineAggregator pipelineAggregator : pipelineAggregators) {
                InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(newAggs), reduceContext);
                newAggs.add(newAgg);
            }
            return new InternalAggregations(newAggs);
        }
        return aggregations;
    }

    public static final class ReducedQueryPhase {
        // the sum of all hits across all reduces shards
        final long totalHits;
        // the number of returned hits (doc IDs) across all reduces shards
        final long fetchHits;
        // the max score across all reduces hits or {@link Float#NaN} if no hits returned
        final float maxScore;
        // <code>true</code> if at least one reduced result timed out
        final boolean timedOut;
        // non null and true if at least one reduced result was terminated early
        final Boolean terminatedEarly;
        // an non-null arbitrary query result if was at least one reduced result
        final QuerySearchResult oneResult;
        // the reduced suggest results
        final Suggest suggest;
        // the reduced internal aggregations
        final InternalAggregations aggregations;
        // the reduced profile results
        final SearchProfileShardResults shardResults;

        ReducedQueryPhase(long totalHits, long fetchHits, float maxScore, boolean timedOut, Boolean terminatedEarly,
                                 QuerySearchResult oneResult, Suggest suggest, InternalAggregations aggregations,
                                 SearchProfileShardResults shardResults) {
            this.totalHits = totalHits;
            this.fetchHits = fetchHits;
            if (Float.isInfinite(maxScore)) {
                this.maxScore = Float.NaN;
            } else {
                this.maxScore = maxScore;
            }
            this.timedOut = timedOut;
            this.terminatedEarly = terminatedEarly;
            this.oneResult = oneResult;
            this.suggest = suggest;
            this.aggregations = aggregations;
            this.shardResults = shardResults;
        }

        /**
         * Creates a new search response from the given merged hits.
         * @see #merge(boolean, ScoreDoc[], ReducedQueryPhase, AtomicArray)
         */
        public InternalSearchResponse buildResponse(SearchHits hits) {
            return new InternalSearchResponse(hits, aggregations, suggest, shardResults, timedOut, terminatedEarly);
        }

        /**
         * Returns <code>true</code> iff the query phase had no results. Otherwise <code>false</code>
         */
        public boolean isEmpty() {
            return oneResult == null;
        }
    }

}
