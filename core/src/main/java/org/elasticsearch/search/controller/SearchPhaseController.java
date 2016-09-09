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

package org.elasticsearch.search.controller;

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
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.collect.HppcMaps;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.dfs.AggregatedDfs;
import org.elasticsearch.search.dfs.DfsSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
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

/**
 *
 */
public class SearchPhaseController extends AbstractComponent {

    public static final Comparator<AtomicArray.Entry<? extends QuerySearchResultProvider>> QUERY_RESULT_ORDERING = (o1, o2) -> {
        int i = o1.value.shardTarget().index().compareTo(o2.value.shardTarget().index());
        if (i == 0) {
            i = o1.value.shardTarget().shardId().id() - o2.value.shardTarget().shardId().id();
        }
        return i;
    };

    public static final ScoreDoc[] EMPTY_DOCS = new ScoreDoc[0];

    private final BigArrays bigArrays;
    private final ScriptService scriptService;
    private final ClusterService clusterService;

    @Inject
    public SearchPhaseController(Settings settings, BigArrays bigArrays, ScriptService scriptService, ClusterService clusterService) {
        super(settings);
        this.bigArrays = bigArrays;
        this.scriptService = scriptService;
        this.clusterService = clusterService;
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

        int topN = topN(results);
        int from = firstResult.queryResult().from();
        if (ignoreFrom) {
            from = 0;
        }

        final TopDocs mergedTopDocs;
        if (firstResult.queryResult().topDocs() instanceof TopFieldDocs) {
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

    public ScoreDoc[] getLastEmittedDocPerShard(List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> queryResults,
                                                ScoreDoc[] sortedScoreDocs, int numShards) {
        ScoreDoc[] lastEmittedDocPerShard = new ScoreDoc[numShards];
        if (queryResults.isEmpty() == false) {
            long fetchHits = 0;
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> queryResult : queryResults) {
                fetchHits += queryResult.value.queryResult().topDocs().scoreDocs.length;
            }
            // from is always zero as when we use scroll, we ignore from
            long size = Math.min(fetchHits, topN(queryResults));
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
    public void fillDocIdsToLoad(AtomicArray<IntArrayList> docIdsToLoad, ScoreDoc[] shardDocs) {
        for (ScoreDoc shardDoc : shardDocs) {
            IntArrayList shardDocIdsToLoad = docIdsToLoad.get(shardDoc.shardIndex);
            if (shardDocIdsToLoad == null) {
                shardDocIdsToLoad = new IntArrayList(); // can't be shared!, uses unsafe on it later on
                docIdsToLoad.set(shardDoc.shardIndex, shardDocIdsToLoad);
            }
            shardDocIdsToLoad.add(shardDoc.doc);
        }
    }

    /**
     * Enriches search hits and completion suggestion hits from <code>sortedDocs</code> using <code>fetchResultsArr</code>,
     * merges suggestions, aggregations and profile results
     *
     * Expects sortedDocs to have top search docs across all shards, optionally followed by top suggest docs for each named
     * completion suggestion ordered by suggestion name
     */
    public InternalSearchResponse merge(boolean ignoreFrom, ScoreDoc[] sortedDocs,
                                        AtomicArray<? extends QuerySearchResultProvider> queryResultsArr,
                                        AtomicArray<? extends FetchSearchResultProvider> fetchResultsArr) {

        List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> queryResults = queryResultsArr.asList();
        List<? extends AtomicArray.Entry<? extends FetchSearchResultProvider>> fetchResults = fetchResultsArr.asList();

        if (queryResults.isEmpty()) {
            return InternalSearchResponse.empty();
        }

        QuerySearchResult firstResult = queryResults.get(0).value.queryResult();

        boolean sorted = false;
        int sortScoreIndex = -1;
        if (firstResult.topDocs() instanceof TopFieldDocs) {
            sorted = true;
            TopFieldDocs fieldDocs = (TopFieldDocs) firstResult.queryResult().topDocs();
            for (int i = 0; i < fieldDocs.fields.length; i++) {
                if (fieldDocs.fields[i].getType() == SortField.Type.SCORE) {
                    sortScoreIndex = i;
                }
            }
        }

        // count the total (we use the query result provider here, since we might not get any hits (we scrolled past them))
        long totalHits = 0;
        long fetchHits = 0;
        float maxScore = Float.NEGATIVE_INFINITY;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
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
        }
        if (Float.isInfinite(maxScore)) {
            maxScore = Float.NaN;
        }

        // clean the fetch counter
        for (AtomicArray.Entry<? extends FetchSearchResultProvider> entry : fetchResults) {
            entry.value.fetchResult().initCounter();
        }
        int from = ignoreFrom ? 0 : firstResult.queryResult().from();
        int numSearchHits = (int) Math.min(fetchHits - from, topN(queryResults));
        // merge hits
        List<InternalSearchHit> hits = new ArrayList<>();
        if (!fetchResults.isEmpty()) {
            for (int i = 0; i < numSearchHits; i++) {
                ScoreDoc shardDoc = sortedDocs[i];
                FetchSearchResultProvider fetchResultProvider = fetchResultsArr.get(shardDoc.shardIndex);
                if (fetchResultProvider == null) {
                    continue;
                }
                FetchSearchResult fetchResult = fetchResultProvider.fetchResult();
                int index = fetchResult.counterGetAndIncrement();
                if (index < fetchResult.hits().internalHits().length) {
                    InternalSearchHit searchHit = fetchResult.hits().internalHits()[index];
                    searchHit.score(shardDoc.score);
                    searchHit.shard(fetchResult.shardTarget());
                    if (sorted) {
                        FieldDoc fieldDoc = (FieldDoc) shardDoc;
                        searchHit.sortValues(fieldDoc.fields, firstResult.sortValueFormats());
                        if (sortScoreIndex != -1) {
                            searchHit.score(((Number) fieldDoc.fields[sortScoreIndex]).floatValue());
                        }
                    }
                    hits.add(searchHit);
                }
            }
        }

        // merge suggest results
        Suggest suggest = null;
        if (firstResult.suggest() != null) {
            final Map<String, List<Suggestion>> groupedSuggestions = new HashMap<>();
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> queryResult : queryResults) {
                Suggest shardSuggest = queryResult.value.queryResult().suggest();
                if (shardSuggest != null) {
                    for (Suggestion<? extends Suggestion.Entry<? extends Suggestion.Entry.Option>> suggestion : shardSuggest) {
                        List<Suggestion> suggestionList = groupedSuggestions.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                        suggestionList.add(suggestion);
                    }
                }
            }
            if (groupedSuggestions.isEmpty() == false) {
                suggest = new Suggest(Suggest.reduce(groupedSuggestions));
                if (!fetchResults.isEmpty()) {
                    int currentOffset = numSearchHits;
                    for (CompletionSuggestion suggestion : suggest.filter(CompletionSuggestion.class)) {
                        final List<CompletionSuggestion.Entry.Option> suggestionOptions = suggestion.getOptions();
                        for (int scoreDocIndex = currentOffset; scoreDocIndex < currentOffset + suggestionOptions.size(); scoreDocIndex++) {
                            ScoreDoc shardDoc = sortedDocs[scoreDocIndex];
                            FetchSearchResultProvider fetchSearchResultProvider = fetchResultsArr.get(shardDoc.shardIndex);
                            if (fetchSearchResultProvider == null) {
                                continue;
                            }
                            FetchSearchResult fetchResult = fetchSearchResultProvider.fetchResult();
                            int fetchResultIndex = fetchResult.counterGetAndIncrement();
                            if (fetchResultIndex < fetchResult.hits().internalHits().length) {
                                InternalSearchHit hit = fetchResult.hits().internalHits()[fetchResultIndex];
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
        }

        // merge Aggregation
        InternalAggregations aggregations = null;
        if (firstResult.aggregations() != null && firstResult.aggregations().asList() != null) {
            List<InternalAggregations> aggregationsList = new ArrayList<>(queryResults.size());
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                aggregationsList.add((InternalAggregations) entry.value.queryResult().aggregations());
            }
            ReduceContext reduceContext = new ReduceContext(bigArrays, scriptService, clusterService.state());
            aggregations = InternalAggregations.reduce(aggregationsList, reduceContext);
            List<SiblingPipelineAggregator> pipelineAggregators = firstResult.pipelineAggregators();
            if (pipelineAggregators != null) {
                List<InternalAggregation> newAggs = StreamSupport.stream(aggregations.spliterator(), false)
                    .map((p) -> (InternalAggregation) p)
                    .collect(Collectors.toList());
                for (SiblingPipelineAggregator pipelineAggregator : pipelineAggregators) {
                    InternalAggregation newAgg = pipelineAggregator.doReduce(new InternalAggregations(newAggs), reduceContext);
                    newAggs.add(newAgg);
                }
                aggregations = new InternalAggregations(newAggs);
            }
        }

        //Collect profile results
        SearchProfileShardResults shardResults = null;
        if (firstResult.profileResults() != null) {
            Map<String, ProfileShardResult> profileResults = new HashMap<>(queryResults.size());
            for (AtomicArray.Entry<? extends QuerySearchResultProvider> entry : queryResults) {
                String key = entry.value.queryResult().shardTarget().toString();
                profileResults.put(key, entry.value.queryResult().profileResults());
            }
            shardResults = new SearchProfileShardResults(profileResults);
        }

        InternalSearchHits searchHits = new InternalSearchHits(hits.toArray(new InternalSearchHit[hits.size()]), totalHits, maxScore);

        return new InternalSearchResponse(searchHits, aggregations, suggest, shardResults, timedOut, terminatedEarly);
    }

    /**
     * returns the number of top results to be considered across all shards
     */
    private static int topN(List<? extends AtomicArray.Entry<? extends QuerySearchResultProvider>> queryResults) {
        QuerySearchResultProvider firstResult = queryResults.get(0).value;
        int topN = firstResult.queryResult().size();
        if (firstResult.includeFetch()) {
            // if we did both query and fetch on the same go, we have fetched all the docs from each shards already, use them...
            // this is also important since we shortcut and fetch only docs from "from" and up to "size"
            topN *= queryResults.size();
        }
        return topN;
    }
}
