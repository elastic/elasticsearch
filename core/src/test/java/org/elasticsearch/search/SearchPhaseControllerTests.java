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

package org.elasticsearch.search;

import com.carrotsearch.hppc.IntArrayList;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.suggest.xdocument.TopSuggestDocs;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.search.controller.SearchPhaseController;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.fetch.FetchSearchResultProvider;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.query.QuerySearchResultProvider;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.test.ESSingleNodeTestCase;

import java.io.IOException;
import java.util.*;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchPhaseControllerTests extends ESSingleNodeTestCase {

    public void testSortSuggestDocs() throws IOException {
        SearchPhaseController searchPhaseController = getInstanceFromNode(SearchPhaseController.class);
        Map<String, Integer> suggestionMap = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            suggestionMap.put(randomAsciiOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20));
        }
        List<Suggest> suggests = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            suggests.add(suggest(suggestionMap));
        }
        AtomicArray<QuerySearchResultProvider> results = fromSuggestions(suggests);
        Map<String, ScoreDoc[]> namedScoreDocs = searchPhaseController.sortSuggestDocs(results);
        for (Map.Entry<String, Integer> entry : suggestionMap.entrySet()) {
            String name = entry.getKey();
            int size = entry.getValue();
            assertThat(namedScoreDocs.containsKey(name), equalTo(true));
            ScoreDoc[] scoreDocs = namedScoreDocs.get(name);
            assertThat(scoreDocs.length, lessThanOrEqualTo(size));
            float maxScore = scoreDocs[0].score;
            for (int i = 1; i < scoreDocs.length; i++) {
                assertThat(scoreDocs[i].score, lessThanOrEqualTo(maxScore));
                maxScore = scoreDocs[i].score;
            }
            int actualTotalOptionsSize = 0;
            for (Suggest suggest : suggests) {
                Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestion = suggest.getSuggestion(entry.getKey());
                actualTotalOptionsSize += suggestion.options().size();
            }
            assertThat(actualTotalOptionsSize, equalTo(scoreDocs.length));
        }
    }

    public void testMergeSuggestions() throws IOException {
        SearchPhaseController searchPhaseController = getInstanceFromNode(SearchPhaseController.class);
        Map<String, Integer> suggestionMap = new HashMap<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            suggestionMap.put(randomAsciiOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20));
        }
        List<Suggest> suggests = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 20); i++) {
            suggests.add(suggest(suggestionMap));
        }
        AtomicArray<QuerySearchResultProvider> results = fromSuggestions(suggests);
        Map<String, ScoreDoc[]> expectedScoreDocs = searchPhaseController.sortSuggestDocs(results);
        AtomicArray<Map<String, IntArrayList>> docIdSets = new AtomicArray<>(results.length());
        searchPhaseController.fillNamedDocIdSets(docIdSets, expectedScoreDocs);
        AtomicArray<FetchSearchResultProvider> fetchResults = fetchResultsFromSuggestions(docIdSets);
        InternalSearchResponse searchResponse = searchPhaseController.mergeSuggestions(expectedScoreDocs, results, fetchResults);
        Suggest suggest = searchResponse.suggest();
        Map<String, ScoreDoc[]> actualScoreDocs = suggest.toNamedScoreDocs(0);
        assertThat(actualScoreDocs.size(), equalTo(expectedScoreDocs.size()));
        for (Map.Entry<String, Integer> entry : suggestionMap.entrySet()) {
            CompletionSuggestion suggestion = suggest.getSuggestion(entry.getKey());
            assertThat(suggestion.options().size(), equalTo(expectedScoreDocs.get(entry.getKey()).length));
            for (CompletionSuggestion.Entry.Option option : suggestion.options()) {
               assertNotNull(option.hit());
            }
        }
    }

    private AtomicArray<FetchSearchResultProvider> fetchResultsFromSuggestions(AtomicArray<Map<String, IntArrayList>> docIdSets) {
        List<AtomicArray.Entry<Map<String, IntArrayList>>> entries = docIdSets.asList();
        AtomicArray<FetchSearchResultProvider> results = new AtomicArray<>(docIdSets.length());
        for (AtomicArray.Entry<Map<String, IntArrayList>> entry : entries) {
            FetchSearchResult fetchSearchResult = new FetchSearchResult();
            for (Map.Entry<String, IntArrayList> namedDocIdSet : entry.value.entrySet()) {
                IntArrayList value = namedDocIdSet.getValue();
                if (value == null) {
                    continue;
                }
                InternalSearchHit[] internalSearchHits = new InternalSearchHit[value.size()];
                for (int i = 0; i < value.size(); i++) {
                    internalSearchHits[i] = new InternalSearchHit(value.get(i), "" + value.get(i), new StringText("type"), null);
                }
                InternalSearchHits hits = new InternalSearchHits(internalSearchHits, internalSearchHits.length, 0);
                fetchSearchResult.namedHits(namedDocIdSet.getKey(), hits);
            }
            results.set(entry.index, fetchSearchResult);
        }
        return results;
    }

    private Suggest suggest(Map<String, Integer> suggestionMap) {
        List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : suggestionMap.entrySet()) {
            String name = entry.getKey();
            int size = entry.getValue();
            CompletionSuggestion completionSuggestion = new CompletionSuggestion(name, size);
            TopSuggestDocs.SuggestScoreDoc[] suggestScoreDocs = new TopSuggestDocs.SuggestScoreDoc[randomIntBetween(1, size)];
            float maxScore = randomIntBetween(size, (int) Float.MAX_VALUE);
            for (int i = 0; i < suggestScoreDocs.length; i++) {
                suggestScoreDocs[i] = new TopSuggestDocs.SuggestScoreDoc(i, "", null, maxScore);
                float dec = randomIntBetween(0, suggestScoreDocs.length);
                if (dec <= maxScore) {
                    maxScore -= dec;
                }
            }
            TopSuggestDocs topSuggestDocs = new TopSuggestDocs(suggestScoreDocs.length, suggestScoreDocs, suggestScoreDocs[0].score);
            completionSuggestion.populateEntry("", topSuggestDocs, size, null);
            suggestions.add(completionSuggestion);
        }
        return new Suggest(suggestions);
    }

    private AtomicArray<QuerySearchResultProvider> fromSuggestions(List<Suggest> suggestions) {
        AtomicArray<QuerySearchResultProvider> results = new AtomicArray<>(suggestions.size() + randomIntBetween(0, 2));
        for (int i = 0; i < suggestions.size(); i++) {
            QuerySearchResult querySearchResult = new QuerySearchResult(1, new SearchShardTarget("", "", 1));
            querySearchResult.suggest(suggestions.get(i));
            results.set(i, querySearchResult);
        }
        return results;
    }
}
