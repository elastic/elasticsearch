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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.index.Index;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchPhaseResult;
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
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
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
            suggestions.add(new CompletionSuggestion(randomAlphaOfLength(randomIntBetween(1, 5)), randomIntBetween(1, 20), false));
        }
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<SearchPhaseResult> results = generateQueryResults(nShards, suggestions, queryResultSize, false);
        Optional<SearchPhaseResult> first = results.asList().stream().findFirst();
        int from = 0, size = 0;
        if (first.isPresent()) {
            from = first.get().queryResult().from();
            size = first.get().queryResult().size();
        }
        int accumulatedLength = Math.min(queryResultSize, getTotalQueryHits(results));
        ScoreDoc[] sortedDocs = searchPhaseController.sortDocs(true, results.asList(), null, new SearchPhaseController.TopDocsStats(),
            from, size)
            .scoreDocs;
        for (Suggest.Suggestion<?> suggestion : reducedSuggest(results)) {
            int suggestionSize = suggestion.getEntries().get(0).getOptions().size();
            accumulatedLength += suggestionSize;
        }
        assertThat(sortedDocs.length, equalTo(accumulatedLength));
    }

    public void testSortIsIdempotent() throws Exception {
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        long randomSeed = randomLong();
        boolean useConstantScore = randomBoolean();
        AtomicArray<SearchPhaseResult> results = generateSeededQueryResults(randomSeed, nShards, Collections.emptyList(), queryResultSize,
            useConstantScore);
        boolean ignoreFrom = randomBoolean();
        Optional<SearchPhaseResult> first = results.asList().stream().findFirst();
        int from = 0, size = 0;
        if (first.isPresent()) {
            from = first.get().queryResult().from();
            size = first.get().queryResult().size();
        }
        SearchPhaseController.TopDocsStats topDocsStats = new SearchPhaseController.TopDocsStats();
        ScoreDoc[] sortedDocs = searchPhaseController.sortDocs(ignoreFrom, results.asList(), null, topDocsStats, from, size).scoreDocs;

        results = generateSeededQueryResults(randomSeed, nShards, Collections.emptyList(), queryResultSize,
            useConstantScore);
        SearchPhaseController.TopDocsStats topDocsStats2 = new SearchPhaseController.TopDocsStats();
        ScoreDoc[] sortedDocs2 = searchPhaseController.sortDocs(ignoreFrom, results.asList(), null, topDocsStats2, from, size).scoreDocs;
        assertEquals(sortedDocs.length, sortedDocs2.length);
        for (int i = 0; i < sortedDocs.length; i++) {
            assertEquals(sortedDocs[i].doc, sortedDocs2[i].doc);
            assertEquals(sortedDocs[i].shardIndex, sortedDocs2[i].shardIndex);
            assertEquals(sortedDocs[i].score, sortedDocs2[i].score, 0.0f);
        }
        assertEquals(topDocsStats.maxScore, topDocsStats2.maxScore, 0.0f);
        assertEquals(topDocsStats.totalHits, topDocsStats2.totalHits);
        assertEquals(topDocsStats.fetchHits, topDocsStats2.fetchHits);
    }

    private AtomicArray<SearchPhaseResult> generateSeededQueryResults(long seed, int nShards,
                                                                      List<CompletionSuggestion> suggestions,
                                                                      int searchHitsSize, boolean useConstantScore) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed,
            () -> generateQueryResults(nShards, suggestions, searchHitsSize, useConstantScore));
    }

    public void testMerge() throws IOException {
        List<CompletionSuggestion> suggestions = new ArrayList<>();
        int maxSuggestSize = 0;
        for (int i = 0; i < randomIntBetween(1, 5); i++) {
            int size = randomIntBetween(1, 20);
            maxSuggestSize += size;
            suggestions.add(new CompletionSuggestion(randomAlphaOfLength(randomIntBetween(1, 5)), size, false));
        }
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        AtomicArray<SearchPhaseResult> queryResults = generateQueryResults(nShards, suggestions, queryResultSize, false);
        for (boolean trackTotalHits : new boolean[] {true, false}) {
            SearchPhaseController.ReducedQueryPhase reducedQueryPhase =
                searchPhaseController.reducedQueryPhase(queryResults.asList(), false, trackTotalHits);
            AtomicArray<SearchPhaseResult> searchPhaseResultAtomicArray = generateFetchResults(nShards, reducedQueryPhase.scoreDocs,
                reducedQueryPhase.suggest);
            InternalSearchResponse mergedResponse = searchPhaseController.merge(false,
                reducedQueryPhase,
                searchPhaseResultAtomicArray.asList(), searchPhaseResultAtomicArray::get);
            if (trackTotalHits == false) {
                assertThat(mergedResponse.hits.totalHits, equalTo(-1L));
            }
            int suggestSize = 0;
            for (Suggest.Suggestion s : reducedQueryPhase.suggest) {
                Stream<CompletionSuggestion.Entry> stream = s.getEntries().stream();
                suggestSize += stream.collect(Collectors.summingInt(e -> e.getOptions().size()));
            }
            assertThat(suggestSize, lessThanOrEqualTo(maxSuggestSize));
            assertThat(mergedResponse.hits().getHits().length, equalTo(reducedQueryPhase.scoreDocs.length - suggestSize));
            Suggest suggestResult = mergedResponse.suggest();
            for (Suggest.Suggestion<?> suggestion : reducedQueryPhase.suggest) {
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
    }

    private AtomicArray<SearchPhaseResult> generateQueryResults(int nShards,
                                                                List<CompletionSuggestion> suggestions,
                                                                int searchHitsSize, boolean useConstantScore) {
        AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            QuerySearchResult querySearchResult = new QuerySearchResult(shardIndex,
                new SearchShardTarget("", new Index("", ""), shardIndex, null));
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
                    completionSuggestion.getName(), completionSuggestion.getSize(), false);
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
            querySearchResult.setShardIndex(shardIndex);
            queryResults.set(shardIndex, querySearchResult);
        }
        return queryResults;
    }

    private int getTotalQueryHits(AtomicArray<SearchPhaseResult> results) {
        int resultCount = 0;
        for (SearchPhaseResult shardResult : results.asList()) {
            resultCount += shardResult.queryResult().topDocs().totalHits;
        }
        return resultCount;
    }

    private Suggest reducedSuggest(AtomicArray<SearchPhaseResult> results) {
        Map<String, List<Suggest.Suggestion<CompletionSuggestion.Entry>>> groupedSuggestion = new HashMap<>();
        for (SearchPhaseResult entry : results.asList()) {
            for (Suggest.Suggestion<?> suggestion : entry.queryResult().suggest()) {
                List<Suggest.Suggestion<CompletionSuggestion.Entry>> suggests =
                    groupedSuggestion.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                suggests.add((Suggest.Suggestion<CompletionSuggestion.Entry>) suggestion);
            }
        }
        return new Suggest(groupedSuggestion.values().stream().map(CompletionSuggestion::reduceTo)
            .collect(Collectors.toList()));
    }

    private AtomicArray<SearchPhaseResult> generateFetchResults(int nShards, ScoreDoc[] mergedSearchDocs, Suggest mergedSuggest) {
        AtomicArray<SearchPhaseResult> fetchResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            float maxScore = -1F;
            SearchShardTarget shardTarget = new SearchShardTarget("", new Index("", ""), shardIndex, null);
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
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(request, 3);
        QuerySearchResult result = new QuerySearchResult(0, new SearchShardTarget("node", new Index("a", "b"), 0, null));
        result.topDocs(new TopDocs(0, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
        InternalAggregations aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", 1.0D, DocValueFormat.RAW,
            Collections.emptyList(), Collections.emptyMap())));
        result.aggregations(aggs);
        result.setShardIndex(0);
        consumer.consumeResult(result);

        result = new QuerySearchResult(1, new SearchShardTarget("node", new Index("a", "b"), 0, null));
        result.topDocs(new TopDocs(0, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
        aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", 3.0D, DocValueFormat.RAW,
            Collections.emptyList(), Collections.emptyMap())));
        result.aggregations(aggs);
        result.setShardIndex(2);
        consumer.consumeResult(result);

        result = new QuerySearchResult(1, new SearchShardTarget("node", new Index("a", "b"), 0, null));
        result.topDocs(new TopDocs(0, new ScoreDoc[0], 0.0F), new DocValueFormat[0]);
        aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", 2.0D, DocValueFormat.RAW,
            Collections.emptyList(), Collections.emptyMap())));
        result.aggregations(aggs);
        result.setShardIndex(1);
        consumer.consumeResult(result);
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
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> consumer =
            searchPhaseController.newSearchPhaseResults(request, expectedNumResults);
        AtomicInteger max = new AtomicInteger();
        Thread[] threads = new Thread[expectedNumResults];
        for (int i = 0; i < expectedNumResults; i++) {
            int id = i;
            threads[i] = new Thread(() -> {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                QuerySearchResult result = new QuerySearchResult(id, new SearchShardTarget("node", new Index("a", "b"), id, null));
                result.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(0, number)}, number), new DocValueFormat[0]);
                InternalAggregations aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", (double) number,
                    DocValueFormat.RAW, Collections.emptyList(), Collections.emptyMap())));
                result.aggregations(aggs);
                result.setShardIndex(id);
                result.size(1);
                consumer.consumeResult(result);

            });
            threads[i].start();
        }
        for (int i = 0; i < expectedNumResults; i++) {
            threads[i].join();
        }
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        InternalMax internalMax = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(max.get(), internalMax.getValue(), 0.0D);
        assertEquals(1, reduce.scoreDocs.length);
        assertEquals(max.get(), reduce.maxScore, 0.0f);
        assertEquals(expectedNumResults, reduce.totalHits);
        assertEquals(max.get(), reduce.scoreDocs[0].score, 0.0f);
    }

    public void testConsumerOnlyAggs() throws InterruptedException {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")).size(0));
        request.setBatchedReduceSize(bufferSize);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> consumer =
            searchPhaseController.newSearchPhaseResults(request, expectedNumResults);
        AtomicInteger max = new AtomicInteger();
        for (int i = 0; i < expectedNumResults; i++) {
            int id = i;
            int number = randomIntBetween(1, 1000);
            max.updateAndGet(prev -> Math.max(prev, number));
            QuerySearchResult result = new QuerySearchResult(id, new SearchShardTarget("node", new Index("a", "b"), id, null));
            result.topDocs(new TopDocs(1, new ScoreDoc[0], number), new DocValueFormat[0]);
            InternalAggregations aggs = new InternalAggregations(Arrays.asList(new InternalMax("test", (double) number,
                DocValueFormat.RAW, Collections.emptyList(), Collections.emptyMap())));
            result.aggregations(aggs);
            result.setShardIndex(id);
            result.size(1);
            consumer.consumeResult(result);
        }
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        InternalMax internalMax = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(max.get(), internalMax.getValue(), 0.0D);
        assertEquals(0, reduce.scoreDocs.length);
        assertEquals(max.get(), reduce.maxScore, 0.0f);
        assertEquals(expectedNumResults, reduce.totalHits);
    }


    public void testConsumerOnlyHits() throws InterruptedException {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = new SearchRequest();
        if (randomBoolean()) {
            request.source(new SearchSourceBuilder().size(randomIntBetween(1, 10)));
        }
        request.setBatchedReduceSize(bufferSize);
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> consumer =
            searchPhaseController.newSearchPhaseResults(request, expectedNumResults);
        AtomicInteger max = new AtomicInteger();
        for (int i = 0; i < expectedNumResults; i++) {
            int id = i;
            int number = randomIntBetween(1, 1000);
            max.updateAndGet(prev -> Math.max(prev, number));
            QuerySearchResult result = new QuerySearchResult(id, new SearchShardTarget("node", new Index("a", "b"), id, null));
            result.topDocs(new TopDocs(1, new ScoreDoc[] {new ScoreDoc(0, number)}, number), new DocValueFormat[0]);
            result.setShardIndex(id);
            result.size(1);
            consumer.consumeResult(result);
        }
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertEquals(1, reduce.scoreDocs.length);
        assertEquals(max.get(), reduce.maxScore, 0.0f);
        assertEquals(expectedNumResults, reduce.totalHits);
        assertEquals(max.get(), reduce.scoreDocs[0].score, 0.0f);
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
            final boolean hasTopDocs;
            if ((hasTopDocs = randomBoolean())) {
                if (request.source() != null) {
                    request.source().size(randomIntBetween(1, 100));
                } // no source means size = 10
            } else {
                if (request.source() == null) {
                    request.source(new SearchSourceBuilder().size(0));
                } else {
                    request.source().size(0);
                }
            }
            request.setBatchedReduceSize(bufferSize);
            InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> consumer
                = searchPhaseController.newSearchPhaseResults(request, expectedNumResults);
            if ((hasAggs || hasTopDocs) && expectedNumResults > bufferSize) {
                assertThat("expectedNumResults: " + expectedNumResults + " bufferSize: " + bufferSize,
                    consumer, instanceOf(SearchPhaseController.QueryPhaseResultConsumer.class));
            } else {
                assertThat("expectedNumResults: " + expectedNumResults + " bufferSize: " + bufferSize,
                    consumer, not(instanceOf(SearchPhaseController.QueryPhaseResultConsumer.class)));
            }
        }
    }

    public void testReduceTopNWithFromOffset() {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().size(5).from(5));
        request.setBatchedReduceSize(randomIntBetween(2, 4));
        InitialSearchPhase.ArraySearchPhaseResults<SearchPhaseResult> consumer =
            searchPhaseController.newSearchPhaseResults(request, 4);
        int score = 100;
        for (int i = 0; i < 4; i++) {
            QuerySearchResult result = new QuerySearchResult(i, new SearchShardTarget("node", new Index("a", "b"), i, null));
            ScoreDoc[] docs = new ScoreDoc[3];
            for (int j = 0; j < docs.length; j++) {
                docs[j] = new ScoreDoc(0, score--);
            }
            result.topDocs(new TopDocs(3, docs, docs[0].score), new DocValueFormat[0]);
            result.setShardIndex(i);
            result.size(5);
            result.from(5);
            consumer.consumeResult(result);
        }
        // 4*3 results = 12 we get result 5 to 10 here with from=5 and size=5

        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertEquals(5, reduce.scoreDocs.length);
        assertEquals(100.f, reduce.maxScore, 0.0f);
        assertEquals(12, reduce.totalHits);
        assertEquals(95.0f, reduce.scoreDocs[0].score, 0.0f);
        assertEquals(94.0f, reduce.scoreDocs[1].score, 0.0f);
        assertEquals(93.0f, reduce.scoreDocs[2].score, 0.0f);
        assertEquals(92.0f, reduce.scoreDocs[3].score, 0.0f);
        assertEquals(91.0f, reduce.scoreDocs[4].score, 0.0f);
    }
}
