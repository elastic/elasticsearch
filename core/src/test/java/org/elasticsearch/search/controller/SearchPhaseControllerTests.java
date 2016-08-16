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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class SearchPhaseControllerTests extends ESTestCase {
    private SearchPhaseController searchPhaseController;

    @Before
    public void setup() {
        searchPhaseController = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null, null);
    }

    public void testSort() throws Exception {
        List<CompletionSuggestion> suggestions = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            suggestions.add(new CompletionSuggestion(randomAsciiOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20)));
        }
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<QuerySearchResultProvider> results = generateQueryResults(nShards, suggestions, queryResultSize);
        ScoreDoc[] sortedDocs = searchPhaseController.sortDocs(true, results);
        int accumulatedLength = Math.min(queryResultSize, getTotalQueryHits(results));
        for (Suggest.Suggestion<?> suggestion : reducedSuggest(results)) {
            int suggestionSize = suggestion.getEntries().get(0).getOptions().size();
            accumulatedLength += suggestionSize;
        }
        assertThat(sortedDocs.length, equalTo(accumulatedLength));
    }

    public void testMerge() throws IOException {
        List<CompletionSuggestion> suggestions = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            suggestions.add(new CompletionSuggestion(randomAsciiOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20)));
        }
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<QuerySearchResultProvider> queryResults = generateQueryResults(nShards, suggestions, queryResultSize);

        // calculate offsets and score doc array
        List<ScoreDoc> mergedScoreDocs = new ArrayList<>();
        ScoreDoc[] mergedSearchDocs = getTopShardDocs(queryResults);
        mergedScoreDocs.addAll(Arrays.asList(mergedSearchDocs));
        Suggest mergedSuggest = reducedSuggest(queryResults);
        for (Suggest.Suggestion<?> suggestion : mergedSuggest) {
            if (suggestion instanceof CompletionSuggestion) {
                CompletionSuggestion completionSuggestion = ((CompletionSuggestion) suggestion);
                mergedScoreDocs.addAll(completionSuggestion.getOptions().stream()
                    .map(CompletionSuggestion.Entry.Option::getDoc)
                    .collect(Collectors.toList()));
            }
        }
        ScoreDoc[] sortedDocs = mergedScoreDocs.toArray(new ScoreDoc[mergedScoreDocs.size()]);

        InternalSearchResponse mergedResponse = searchPhaseController.merge(true, sortedDocs, queryResults,
            generateFetchResults(nShards, mergedSearchDocs, mergedSuggest));
        assertThat(mergedResponse.hits().getHits().length, equalTo(mergedSearchDocs.length));
        Suggest suggestResult = mergedResponse.suggest();
        for (Suggest.Suggestion<?> suggestion : mergedSuggest) {
            assertThat(suggestion, instanceOf(CompletionSuggestion.class));
            if (suggestion.getEntries().get(0).getOptions().size() > 0) {
                CompletionSuggestion suggestionResult = suggestResult.getSuggestion(suggestion.getName());
                assertNotNull(suggestionResult);
                List<CompletionSuggestion.Entry.Option> options = suggestionResult.getEntries().get(0).getOptions();
                assertThat(options.size(), equalTo(suggestion.getEntries().get(0).getOptions().size()));
                for (CompletionSuggestion.Entry.Option option : options) {
                    assertNotNull(option.getHit());
                }
            }
        }
    }

    private AtomicArray<QuerySearchResultProvider> generateQueryResults(int nShards,
                                                                        List<CompletionSuggestion> suggestions,
                                                                        int searchHitsSize) {
        AtomicArray<QuerySearchResultProvider> queryResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            QuerySearchResult querySearchResult = new QuerySearchResult(shardIndex,
                new SearchShardTarget("", new Index("", ""), shardIndex));
            TopDocs topDocs = new TopDocs(0, new ScoreDoc[0], 0);
            if (searchHitsSize > 0) {
                int nDocs = randomIntBetween(0, searchHitsSize);
                ScoreDoc[] scoreDocs = new ScoreDoc[nDocs];
                float maxScore = 0F;
                for (int i = 0; i < nDocs; i++) {
                    float score = Math.abs(randomFloat());
                    scoreDocs[i] = new ScoreDoc(i, score);
                    if (score > maxScore) {
                        maxScore = score;
                    }
                }
                topDocs = new TopDocs(scoreDocs.length, scoreDocs, maxScore);
            }
            List<CompletionSuggestion> shardSuggestion = new ArrayList<>();
            for (CompletionSuggestion completionSuggestion : suggestions) {
                CompletionSuggestion suggestion = new CompletionSuggestion(
                    completionSuggestion.getName(), completionSuggestion.getSize());
                final CompletionSuggestion.Entry completionEntry = new CompletionSuggestion.Entry(new Text(""), 0, 5);
                suggestion.addTerm(completionEntry);
                int optionSize = randomIntBetween(1, suggestion.getSize());
                float maxScore = randomIntBetween(suggestion.getSize(), (int) Float.MAX_VALUE);
                for (int i = 0; i < optionSize; i++) {
                    completionEntry.addOption(new CompletionSuggestion.Entry.Option(i, new Text(""), maxScore,
                        Collections.emptyMap()));
                    float dec = randomIntBetween(0, optionSize);
                    if (dec <= maxScore) {
                        maxScore -= dec;
                    }
                }
                suggestion.setShardIndex(shardIndex);
                shardSuggestion.add(suggestion);
            }
            querySearchResult.topDocs(topDocs, null);
            querySearchResult.size(searchHitsSize);
            querySearchResult.suggest(new Suggest(new ArrayList<>(shardSuggestion)));
            queryResults.set(shardIndex, querySearchResult);
        }
        return queryResults;
    }

    private int getTotalQueryHits(AtomicArray<QuerySearchResultProvider> results) {
        int resultCount = 0;
        for (AtomicArray.Entry<QuerySearchResultProvider> shardResult : results.asList()) {
            resultCount += shardResult.value.queryResult().topDocs().totalHits;
        }
        return resultCount;
    }

    private Suggest reducedSuggest(AtomicArray<QuerySearchResultProvider> results) {
        Map<String, List<Suggest.Suggestion<CompletionSuggestion.Entry>>> groupedSuggestion = new HashMap<>();
        for (AtomicArray.Entry<QuerySearchResultProvider> entry : results.asList()) {
            for (Suggest.Suggestion<?> suggestion : entry.value.queryResult().suggest()) {
                List<Suggest.Suggestion<CompletionSuggestion.Entry>> suggests =
                    groupedSuggestion.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                suggests.add((Suggest.Suggestion<CompletionSuggestion.Entry>) suggestion);
            }
        }
        return new Suggest(groupedSuggestion.values().stream().map(CompletionSuggestion::reduceTo)
            .collect(Collectors.toList()));
    }

    private ScoreDoc[] getTopShardDocs(AtomicArray<QuerySearchResultProvider> results) throws IOException {
        List<AtomicArray.Entry<QuerySearchResultProvider>> resultList = results.asList();
        TopDocs[] shardTopDocs = new TopDocs[resultList.size()];
        for (int i = 0; i < resultList.size(); i++) {
            shardTopDocs[i] = resultList.get(i).value.queryResult().topDocs();
        }
        int topN = Math.min(results.get(0).queryResult().size(), getTotalQueryHits(results));
        return TopDocs.merge(topN, shardTopDocs).scoreDocs;
    }

    private AtomicArray<FetchSearchResultProvider> generateFetchResults(int nShards, ScoreDoc[] mergedSearchDocs, Suggest mergedSuggest) {
        AtomicArray<FetchSearchResultProvider> fetchResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            float maxScore = -1F;
            SearchShardTarget shardTarget = new SearchShardTarget("", new Index("", ""), shardIndex);
            FetchSearchResult fetchSearchResult = new FetchSearchResult(shardIndex, shardTarget);
            List<InternalSearchHit> internalSearchHits = new ArrayList<>();
            for (ScoreDoc scoreDoc : mergedSearchDocs) {
                if (scoreDoc.shardIndex == shardIndex) {
                    internalSearchHits.add(new InternalSearchHit(scoreDoc.doc, "", new Text(""), Collections.emptyMap()));
                    if (scoreDoc.score > maxScore) {
                        maxScore = scoreDoc.score;
                    }
                }
            }
            for (Suggest.Suggestion<?> suggestion : mergedSuggest) {
                if (suggestion instanceof CompletionSuggestion) {
                    for (CompletionSuggestion.Entry.Option option : ((CompletionSuggestion) suggestion).getOptions()) {
                        ScoreDoc doc = option.getDoc();
                        if (doc.shardIndex == shardIndex) {
                            internalSearchHits.add(new InternalSearchHit(doc.doc, "", new Text(""), Collections.emptyMap()));
                            if (doc.score > maxScore) {
                                maxScore = doc.score;
                            }
                        }
                    }
                }
            }
            InternalSearchHit[] hits = internalSearchHits.toArray(new InternalSearchHit[internalSearchHits.size()]);
            fetchSearchResult.hits(new InternalSearchHits(hits, hits.length, maxScore));
            fetchResults.set(shardIndex, fetchSearchResult);
        }
        return fetchResults;
    }
}
