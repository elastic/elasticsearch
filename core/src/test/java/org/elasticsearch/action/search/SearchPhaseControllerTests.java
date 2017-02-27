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

import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.max.InternalMax;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;

public class SearchPhaseControllerTests extends ESTestCase {
    private SearchPhaseController searchPhaseController;

    @Before
    public void setup() {
        searchPhaseController = new SearchPhaseController(Settings.EMPTY, BigArrays.NON_RECYCLING_INSTANCE, null);
    }

    public void testSort() throws Exception {
        List<CompletionSuggestion> suggestions = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            suggestions.add(new CompletionSuggestion(randomAsciiOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20)));
        }
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<QuerySearchResultProvider> results = generateQueryResults(nShards, suggestions, queryResultSize, false);
        ScoreDoc[] sortedDocs = searchPhaseController.sortDocs(true, results);
        int accumulatedLength = Math.min(queryResultSize, getTotalQueryHits(results));
        for (Suggest.Suggestion<?> suggestion : reducedSuggest(results)) {
            int suggestionSize = suggestion.getEntries().get(0).getOptions().size();
            accumulatedLength += suggestionSize;
        }
        assertThat(sortedDocs.length, equalTo(accumulatedLength));
    }

    public void testSortIsIdempotent() throws IOException {
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<QuerySearchResultProvider> results = generateQueryResults(nShards, Collections.emptyList(), queryResultSize,
            randomBoolean() || true);
        boolean ignoreFrom = randomBoolean();
        ScoreDoc[] sortedDocs = searchPhaseController.sortDocs(ignoreFrom, results);

        ScoreDoc[] sortedDocs2 = searchPhaseController.sortDocs(ignoreFrom, results);
        assertArrayEquals(sortedDocs, sortedDocs2);
    }

    public void testMerge() throws IOException {
        List<CompletionSuggestion> suggestions = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            suggestions.add(new CompletionSuggestion(randomAsciiOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20)));
        }
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<QuerySearchResultProvider> queryResults = generateQueryResults(nShards, suggestions, queryResultSize, false);

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
        InternalSearchResponse mergedResponse = searchPhaseController.merge(true, sortedDocs,
            searchPhaseController.reducedQueryPhase(queryResults.asList()),
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
                                                                        int searchHitsSize, boolean useConstantScore) {
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
                    float score = useConstantScore ? 1.0F : Math.abs(randomFloat());
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

    private AtomicArray<QuerySearchResultProvider> generateFetchResults(int nShards, ScoreDoc[] mergedSearchDocs, Suggest mergedSuggest) {
        AtomicArray<QuerySearchResultProvider> fetchResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            float maxScore = -1F;
            SearchShardTarget shardTarget = new SearchShardTarget("", new Index("", ""), shardIndex);
            FetchSearchResult fetchSearchResult = new FetchSearchResult(shardIndex, shardTarget);
            List<SearchHit> searchHits = new ArrayList<>();
            for (ScoreDoc scoreDoc : mergedSearchDocs) {
                if (scoreDoc.shardIndex == shardIndex) {
                    searchHits.add(new SearchHit(scoreDoc.doc, "", new Text(""), Collections.emptyMap()));
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
                            searchHits.add(new SearchHit(doc.doc, "", new Text(""), Collections.emptyMap()));
                            if (doc.score > maxScore) {
                                maxScore = doc.score;
                            }
                        }
                    }
                }
            }
            SearchHit[] hits = searchHits.toArray(new SearchHit[searchHits.size()]);
            fetchSearchResult.hits(new SearchHits(hits, hits.length, maxScore));
            fetchResults.set(shardIndex, fetchSearchResult);
        }
        return fetchResults;
    }

    public void testConsumer() {
        int bufferSize = randomIntBetween(2, 3);
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")));
        request.setBatchedReduceSize(bufferSize);
        InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> consumer = searchPhaseController.newSearchPhaseResults(request, 3);
        QuerySearchResult result = new QuerySearchResult(0, new SearchShardTarget("node", new Index("a", "b"), 0));
        result.topDocs(new TopDocs(0, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
        InternalAggregations aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", 1.0D, DocValueFormat.RAW,
            Collections.emptyList(), Collections.emptyMap())));
        result.aggregations(aggs);
        consumer.consumeResult(0, result);

        result = new QuerySearchResult(1, new SearchShardTarget("node", new Index("a", "b"), 0));
        result.topDocs(new TopDocs(0, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
        aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", 3.0D, DocValueFormat.RAW,
            Collections.emptyList(), Collections.emptyMap())));
        result.aggregations(aggs);
        consumer.consumeResult(2, result);

        result = new QuerySearchResult(1, new SearchShardTarget("node", new Index("a", "b"), 0));
        result.topDocs(new TopDocs(0, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
        aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", 2.0D, DocValueFormat.RAW,
            Collections.emptyList(), Collections.emptyMap())));
        result.aggregations(aggs);
        consumer.consumeResult(1, result);
        int numTotalReducePhases = 1;
        if (bufferSize == 2) {
            assertThat(consumer, instanceOf(SearchPhaseController.QueryPhaseResultConsumer.class));
            assertEquals(1, ((SearchPhaseController.QueryPhaseResultConsumer)consumer).getNumReducePhases());
            assertEquals(2, ((SearchPhaseController.QueryPhaseResultConsumer)consumer).getNumBuffered());
            numTotalReducePhases++;
        } else {
            assertThat(consumer, not(instanceOf(SearchPhaseController.QueryPhaseResultConsumer.class)));
        }

        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertEquals(numTotalReducePhases, reduce.numReducePhases);
        InternalMax max = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(3.0D, max.getValue(), 0.0D);
    }

    public void testConsumerConcurrently() throws InterruptedException {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);

        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")));
        request.setBatchedReduceSize(bufferSize);
        InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> consumer =
            searchPhaseController.newSearchPhaseResults(request, expectedNumResults);
        AtomicInteger max = new AtomicInteger();
        CountDownLatch latch = new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            int id = i;
            Thread t = new Thread(() -> {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                QuerySearchResult result = new QuerySearchResult(id, new SearchShardTarget("node", new Index("a", "b"), id));
                result.topDocs(new TopDocs(id, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
                InternalAggregations aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", (double) number,
                    DocValueFormat.RAW, Collections.emptyList(), Collections.emptyMap())));
                result.aggregations(aggs);
                consumer.consumeResult(id, result);
                latch.countDown();

            });
            t.start();
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        InternalMax internalMax = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(max.get(), internalMax.getValue(), 0.0D);
    }

    public void testNewSearchPhaseResults() {
        for (int i = 0; i < 10; i++) {
            int expectedNumResults = randomIntBetween(1, 10);
            int bufferSize = randomIntBetween(2, 10);
            SearchRequest request = new SearchRequest();
            final boolean hasAggs;
            if ((hasAggs = randomBoolean())) {
                request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")));
            }
            request.setBatchedReduceSize(bufferSize);
            InitialSearchPhase.SearchPhaseResults<QuerySearchResultProvider> consumer
                = searchPhaseController.newSearchPhaseResults(request, expectedNumResults);
            if (hasAggs && expectedNumResults > bufferSize) {
                assertThat("expectedNumResults: " + expectedNumResults + " bufferSize: " + bufferSize,
                    consumer, instanceOf(SearchPhaseController.QueryPhaseResultConsumer.class));
            } else {
                assertThat("expectedNumResults: " + expectedNumResults + " bufferSize: " + bufferSize,
                    consumer, not(instanceOf(SearchPhaseController.QueryPhaseResultConsumer.class)));
            }
        }
    }
}
