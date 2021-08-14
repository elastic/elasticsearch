/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.search;

import com.carrotsearch.randomizedtesting.RandomizedContext;

import org.apache.lucene.search.FieldDoc;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.TopFieldDocs;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.search.TotalHits.Relation;
import org.apache.lucene.search.grouping.CollapseTopFieldDocs;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.OriginalIndices;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.breaker.CircuitBreaker;
import org.elasticsearch.common.breaker.CircuitBreakingException;
import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.lucene.Lucene;
import org.elasticsearch.common.lucene.search.TopDocsAndMaxScore;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregation.ReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.InternalMax;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.PipelineTree;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.suggest.SortBy;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.InternalAggregationTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchPhaseControllerTests extends ESTestCase {
    private ThreadPool threadPool;
    private EsThreadPoolExecutor fixedExecutor;
    private SearchPhaseController searchPhaseController;
    private List<Boolean> reductions;

    @Override
    protected NamedWriteableRegistry writableRegistry() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(new SearchModule(Settings.EMPTY, emptyList()).getNamedWriteables());
        return new NamedWriteableRegistry(entries);
    }

    @Before
    public void setup() {
        reductions = new CopyOnWriteArrayList<>();
        searchPhaseController = new SearchPhaseController(writableRegistry(), s -> new InternalAggregation.ReduceContextBuilder() {
            @Override
            public ReduceContext forPartialReduction() {
                reductions.add(false);
                return InternalAggregation.ReduceContext.forPartialReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, () -> PipelineTree.EMPTY);
            }

            public ReduceContext forFinalReduction() {
                reductions.add(true);
                return InternalAggregation.ReduceContext.forFinalReduction(
                        BigArrays.NON_RECYCLING_INSTANCE, null, b -> {}, PipelineTree.EMPTY);
            };
        });
        threadPool = new TestThreadPool(SearchPhaseControllerTests.class.getName());
        fixedExecutor = EsExecutors.newFixed("test", 1, 10,
            EsExecutors.daemonThreadFactory("test"), threadPool.getThreadContext(), randomBoolean());
    }

    @After
    public void cleanup() {
        fixedExecutor.shutdownNow();
        terminate(threadPool);
    }

    public void testSortDocs() {
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
        List<CompletionSuggestion> reducedCompletionSuggestions = reducedSuggest(results);
        for (Suggest.Suggestion<?> suggestion : reducedCompletionSuggestions) {
            int suggestionSize = suggestion.getEntries().get(0).getOptions().size();
            accumulatedLength += suggestionSize;
        }
        List<TopDocs> topDocsList = new ArrayList<>();
        for (SearchPhaseResult result : results.asList()) {
            QuerySearchResult queryResult = result.queryResult();
            TopDocs topDocs = queryResult.consumeTopDocs().topDocs;
            SearchPhaseController.setShardIndex(topDocs, result.getShardIndex());
            topDocsList.add(topDocs);
        }
        ScoreDoc[] sortedDocs = SearchPhaseController.sortDocs(true, topDocsList,
            from, size, reducedCompletionSuggestions).scoreDocs;
        assertThat(sortedDocs.length, equalTo(accumulatedLength));
    }

    public void testSortDocsIsIdempotent() throws Exception {
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
        List<TopDocs> topDocsList = new ArrayList<>();
        for (SearchPhaseResult result : results.asList()) {
            QuerySearchResult queryResult = result.queryResult();
            TopDocs topDocs = queryResult.consumeTopDocs().topDocs;
            topDocsList.add(topDocs);
            SearchPhaseController.setShardIndex(topDocs, result.getShardIndex());
        }
        ScoreDoc[] sortedDocs = SearchPhaseController.sortDocs(ignoreFrom, topDocsList, from, size,
            Collections.emptyList()).scoreDocs;

        results = generateSeededQueryResults(randomSeed, nShards, Collections.emptyList(), queryResultSize,
            useConstantScore);
        topDocsList = new ArrayList<>();
        for (SearchPhaseResult result : results.asList()) {
            QuerySearchResult queryResult = result.queryResult();
            TopDocs topDocs = queryResult.consumeTopDocs().topDocs;
            topDocsList.add(topDocs);
            SearchPhaseController.setShardIndex(topDocs, result.getShardIndex());
        }
        ScoreDoc[] sortedDocs2 = SearchPhaseController.sortDocs(ignoreFrom, topDocsList, from, size,
            Collections.emptyList()).scoreDocs;
        assertEquals(sortedDocs.length, sortedDocs2.length);
        for (int i = 0; i < sortedDocs.length; i++) {
            assertEquals(sortedDocs[i].doc, sortedDocs2[i].doc);
            assertEquals(sortedDocs[i].shardIndex, sortedDocs2[i].shardIndex);
            assertEquals(sortedDocs[i].score, sortedDocs2[i].score, 0.0f);
        }
    }

    private AtomicArray<SearchPhaseResult> generateSeededQueryResults(long seed, int nShards,
                                                                      List<CompletionSuggestion> suggestions,
                                                                      int searchHitsSize, boolean useConstantScore) throws Exception {
        return RandomizedContext.current().runWithPrivateRandomness(seed,
            () -> generateQueryResults(nShards, suggestions, searchHitsSize, useConstantScore));
    }

    public void testMerge() {
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
        for (int trackTotalHits : new int[] { SearchContext.TRACK_TOTAL_HITS_DISABLED, SearchContext.TRACK_TOTAL_HITS_ACCURATE }) {
            SearchPhaseController.ReducedQueryPhase reducedQueryPhase = searchPhaseController.reducedQueryPhase(queryResults.asList(),
                new ArrayList<>(), new ArrayList<>(), new SearchPhaseController.TopDocsStats(trackTotalHits),
                0, true, InternalAggregationTestCase.emptyReduceContextBuilder(), true);
            AtomicArray<SearchPhaseResult> fetchResults = generateFetchResults(nShards,
                reducedQueryPhase.sortedTopDocs.scoreDocs, reducedQueryPhase.suggest);
            InternalSearchResponse mergedResponse = searchPhaseController.merge(false,
                reducedQueryPhase, fetchResults.asList(), fetchResults::get);
            if (trackTotalHits == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                assertNull(mergedResponse.hits.getTotalHits());
            } else {
                assertThat(mergedResponse.hits.getTotalHits().value, equalTo(0L));
                assertEquals(mergedResponse.hits.getTotalHits().relation, Relation.EQUAL_TO);
            }
            for (SearchHit hit : mergedResponse.hits().getHits()) {
                SearchPhaseResult searchPhaseResult = fetchResults.get(hit.getShard().getShardId().id());
                assertSame(searchPhaseResult.getSearchShardTarget(), hit.getShard());
            }
            int suggestSize = 0;
            for (Suggest.Suggestion<?> s : reducedQueryPhase.suggest) {
                suggestSize += s.getEntries().stream().mapToInt(e -> e.getOptions().size()).sum();
            }
            assertThat(suggestSize, lessThanOrEqualTo(maxSuggestSize));
            assertThat(mergedResponse.hits().getHits().length, equalTo(reducedQueryPhase.sortedTopDocs.scoreDocs.length - suggestSize));
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
                        SearchPhaseResult searchPhaseResult = fetchResults.get(option.getHit().getShard().getShardId().id());
                        assertSame(searchPhaseResult.getSearchShardTarget(), option.getHit().getShard());
                    }
                }
            }
        }
    }

    /**
     * Generate random query results received from the provided number of shards, including the provided
     * number of search hits and randomly generated completion suggestions based on the name and size of the provided ones.
     * Note that <code>shardIndex</code> is already set to the generated completion suggestions to simulate what
     * {@link SearchPhaseController#reducedQueryPhase} does,
     * meaning that the returned query results can be fed directly to {@link SearchPhaseController#sortDocs}
     */
    private static AtomicArray<SearchPhaseResult> generateQueryResults(int nShards, List<CompletionSuggestion> suggestions,
                                                                       int searchHitsSize, boolean useConstantScore) {
        AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            String clusterAlias = randomBoolean() ? null : "remote";
            SearchShardTarget searchShardTarget = new SearchShardTarget("", new ShardId("", "", shardIndex),
                clusterAlias, OriginalIndices.NONE);
            QuerySearchResult querySearchResult = new QuerySearchResult(new ShardSearchContextId("", shardIndex), searchShardTarget, null);
            final TopDocs topDocs;
            float maxScore = 0;
            if (searchHitsSize == 0) {
                topDocs = Lucene.EMPTY_TOP_DOCS;
            } else {
                int nDocs = randomIntBetween(0, searchHitsSize);
                ScoreDoc[] scoreDocs = new ScoreDoc[nDocs];
                for (int i = 0; i < nDocs; i++) {
                    float score = useConstantScore ? 1.0F : Math.abs(randomFloat());
                    scoreDocs[i] = new ScoreDoc(i, score);
                    maxScore = Math.max(score, maxScore);
                }
                topDocs = new TopDocs(new TotalHits(scoreDocs.length, TotalHits.Relation.EQUAL_TO), scoreDocs);
            }
            List<CompletionSuggestion> shardSuggestion = new ArrayList<>();
            for (CompletionSuggestion completionSuggestion : suggestions) {
                CompletionSuggestion suggestion = new CompletionSuggestion(
                    completionSuggestion.getName(), completionSuggestion.getSize(), false);
                final CompletionSuggestion.Entry completionEntry = new CompletionSuggestion.Entry(new Text(""), 0, 5);
                suggestion.addTerm(completionEntry);
                int optionSize = randomIntBetween(1, suggestion.getSize());
                float maxScoreValue = randomIntBetween(suggestion.getSize(), (int) Float.MAX_VALUE);
                for (int i = 0; i < optionSize; i++) {
                    completionEntry.addOption(new CompletionSuggestion.Entry.Option(i, new Text(""), maxScoreValue,
                        Collections.emptyMap()));
                    float dec = randomIntBetween(0, optionSize);
                    if (dec <= maxScoreValue) {
                        maxScoreValue -= dec;
                    }
                }
                suggestion.setShardIndex(shardIndex);
                shardSuggestion.add(suggestion);
            }
            querySearchResult.topDocs(new TopDocsAndMaxScore(topDocs, maxScore), null);
            querySearchResult.size(searchHitsSize);
            querySearchResult.suggest(new Suggest(new ArrayList<>(shardSuggestion)));
            querySearchResult.setShardIndex(shardIndex);
            queryResults.set(shardIndex, querySearchResult);
        }
        return queryResults;
    }

    private static int getTotalQueryHits(AtomicArray<SearchPhaseResult> results) {
        int resultCount = 0;
        for (SearchPhaseResult shardResult : results.asList()) {
            TopDocs topDocs = shardResult.queryResult().topDocs().topDocs;
            assert topDocs.totalHits.relation == Relation.EQUAL_TO;
            resultCount += topDocs.totalHits.value;
        }
        return resultCount;
    }

    private static List<CompletionSuggestion> reducedSuggest(AtomicArray<SearchPhaseResult> results) {
        Map<String, List<Suggest.Suggestion<CompletionSuggestion.Entry>>> groupedSuggestion = new HashMap<>();
        for (SearchPhaseResult entry : results.asList()) {
            for (Suggest.Suggestion<?> suggestion : entry.queryResult().suggest()) {
                List<Suggest.Suggestion<CompletionSuggestion.Entry>> suggests =
                    groupedSuggestion.computeIfAbsent(suggestion.getName(), s -> new ArrayList<>());
                suggests.add((CompletionSuggestion) suggestion);
            }
        }
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(null, -1, randomBoolean());
        return groupedSuggestion.values().stream().map(completionSuggestion::reduce).collect(Collectors.toList());
    }

    private static AtomicArray<SearchPhaseResult> generateFetchResults(int nShards, ScoreDoc[] mergedSearchDocs, Suggest mergedSuggest) {
        AtomicArray<SearchPhaseResult> fetchResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            float maxScore = -1F;
            String clusterAlias = randomBoolean() ? null : "remote";
            SearchShardTarget shardTarget = new SearchShardTarget("", new ShardId("", "", shardIndex), clusterAlias, OriginalIndices.NONE);
            FetchSearchResult fetchSearchResult = new FetchSearchResult(new ShardSearchContextId("", shardIndex), shardTarget);
            List<SearchHit> searchHits = new ArrayList<>();
            for (ScoreDoc scoreDoc : mergedSearchDocs) {
                if (scoreDoc.shardIndex == shardIndex) {
                    searchHits.add(new SearchHit(scoreDoc.doc, "", Collections.emptyMap(), Collections.emptyMap()));
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
                            searchHits.add(new SearchHit(doc.doc, "", Collections.emptyMap(), Collections.emptyMap()));
                            if (doc.score > maxScore) {
                                maxScore = doc.score;
                            }
                        }
                    }
                }
            }
            SearchHit[] hits = searchHits.toArray(new SearchHit[0]);
            fetchSearchResult.hits(new SearchHits(hits, new TotalHits(hits.length, Relation.EQUAL_TO), maxScore));
            fetchResults.set(shardIndex, fetchSearchResult);
        }
        return fetchResults;
    }

    private static SearchRequest randomSearchRequest() {
        return randomBoolean() ? new SearchRequest() : SearchRequest.subSearchRequest(new TaskId("n", 1), new SearchRequest(),
            Strings.EMPTY_ARRAY, "remote", 0, randomBoolean());
    }

    public void testConsumer() throws Exception {
        consumerTestCase(0);
    }

    public void testConsumerWithEmptyResponse() throws Exception {
        consumerTestCase(randomIntBetween(1, 5));
    }

    private void consumerTestCase(int numEmptyResponses) throws Exception {
        long beforeCompletedTasks = fixedExecutor.getCompletedTaskCount();
        int numShards = 3 + numEmptyResponses;
        int bufferSize = randomIntBetween(2, 3);
        CountDownLatch latch = new CountDownLatch(numShards);
        SearchRequest request = randomSearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")));
        request.setBatchedReduceSize(bufferSize);
        ArraySearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, 3+numEmptyResponses, exc  -> {});
        if (numEmptyResponses == 0) {
            assertEquals(0, reductions.size());
        }
        if (numEmptyResponses > 0) {
            QuerySearchResult empty = QuerySearchResult.nullInstance();
            int shardId = 2 + numEmptyResponses;
            empty.setShardIndex(2+numEmptyResponses);
            empty.setSearchShardTarget(new SearchShardTarget("node", new ShardId("a", "b", shardId), null, OriginalIndices.NONE));
            consumer.consumeResult(empty, latch::countDown);
            numEmptyResponses --;
        }

        QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", 0),
            new SearchShardTarget("node", new ShardId("a", "b", 0), null, OriginalIndices.NONE), null);
        result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
            new DocValueFormat[0]);
        InternalAggregations aggs = InternalAggregations.from(singletonList(new InternalMax("test", 1.0D, DocValueFormat.RAW, emptyMap())));
        result.aggregations(aggs);
        result.setShardIndex(0);
        consumer.consumeResult(result, latch::countDown);

        result = new QuerySearchResult(new ShardSearchContextId("", 1),
            new SearchShardTarget("node", new ShardId("a", "b", 0), null, OriginalIndices.NONE), null);
        result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
            new DocValueFormat[0]);
        aggs = InternalAggregations.from(singletonList(new InternalMax("test", 3.0D, DocValueFormat.RAW, emptyMap())));
        result.aggregations(aggs);
        result.setShardIndex(2);
        consumer.consumeResult(result, latch::countDown);

        result = new QuerySearchResult(new ShardSearchContextId("", 1),
            new SearchShardTarget("node", new ShardId("a", "b", 0), null, OriginalIndices.NONE), null);
        result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
            new DocValueFormat[0]);
        aggs = InternalAggregations.from(singletonList(new InternalMax("test", 2.0D, DocValueFormat.RAW, emptyMap())));
        result.aggregations(aggs);
        result.setShardIndex(1);
        consumer.consumeResult(result, latch::countDown);

        while (numEmptyResponses > 0) {
            result = QuerySearchResult.nullInstance();
            int shardId = 2 + numEmptyResponses;
            result.setShardIndex(shardId);
            result.setSearchShardTarget(new SearchShardTarget("node", new ShardId("a", "b", shardId), null, OriginalIndices.NONE));
            consumer.consumeResult(result, latch::countDown);
            numEmptyResponses--;

        }
        latch.await();
        final int numTotalReducePhases;
        if (numShards > bufferSize) {
            if (bufferSize == 2) {
                assertEquals(1, ((QueryPhaseResultConsumer) consumer).getNumReducePhases());
                assertEquals(1, reductions.size());
                assertEquals(false, reductions.get(0));
                numTotalReducePhases = 2;
            } else {
                assertEquals(0, ((QueryPhaseResultConsumer) consumer).getNumReducePhases());
                assertEquals(0, reductions.size());
                numTotalReducePhases = 1;
            }
        } else {
            assertEquals(0, reductions.size());
            numTotalReducePhases = 1;
        }

        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertEquals(numTotalReducePhases, reduce.numReducePhases);
        assertEquals(numTotalReducePhases, reductions.size());
        assertAggReduction(request);
        InternalMax max = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(3.0D, max.getValue(), 0.0D);
        assertFalse(reduce.sortedTopDocs.isSortedByField);
        assertNull(reduce.sortedTopDocs.sortFields);
        assertNull(reduce.sortedTopDocs.collapseField);
        assertNull(reduce.sortedTopDocs.collapseValues);
    }

    public void testConsumerConcurrently() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);

        SearchRequest request = randomSearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")));
        request.setBatchedReduceSize(bufferSize);
        ArraySearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc  -> {});
        AtomicInteger max = new AtomicInteger();
        Thread[] threads = new Thread[expectedNumResults];
        CountDownLatch latch = new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            int id = i;
            threads[i] = new Thread(() -> {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", id),
                    new SearchShardTarget("node", new ShardId("a", "b", id), null, OriginalIndices.NONE), null);
                result.topDocs(new TopDocsAndMaxScore(
                    new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] {new ScoreDoc(0, number)}), number),
                    new DocValueFormat[0]);
                InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(new InternalMax("test", (double) number,
                    DocValueFormat.RAW, Collections.emptyMap())));
                result.aggregations(aggs);
                result.setShardIndex(id);
                result.size(1);
                consumer.consumeResult(result, latch::countDown);

            });
            threads[i].start();
        }
        for (int i = 0; i < expectedNumResults; i++) {
            threads[i].join();
        }
        latch.await();

        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertAggReduction(request);
        InternalMax internalMax = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(max.get(), internalMax.getValue(), 0.0D);
        assertEquals(1, reduce.sortedTopDocs.scoreDocs.length);
        assertEquals(max.get(), reduce.maxScore, 0.0f);
        assertEquals(expectedNumResults, reduce.totalHits.value);
        assertEquals(max.get(), reduce.sortedTopDocs.scoreDocs[0].score, 0.0f);
        assertFalse(reduce.sortedTopDocs.isSortedByField);
        assertNull(reduce.sortedTopDocs.sortFields);
        assertNull(reduce.sortedTopDocs.collapseField);
        assertNull(reduce.sortedTopDocs.collapseValues);
    }

    public void testConsumerOnlyAggs() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")).size(0));
        request.setBatchedReduceSize(bufferSize);
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc  -> {});
        AtomicInteger max = new AtomicInteger();
        CountDownLatch latch =  new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            int number = randomIntBetween(1, 1000);
            max.updateAndGet(prev -> Math.max(prev, number));
            QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", i),
                new SearchShardTarget("node", new ShardId("a", "b", i), null, OriginalIndices.NONE), null);
            result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), number),
                    new DocValueFormat[0]);
            InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(new InternalMax("test", (double) number,
                DocValueFormat.RAW, Collections.emptyMap())));
            result.aggregations(aggs);
            result.setShardIndex(i);
            result.size(1);
            consumer.consumeResult(result, latch::countDown);
        }
        latch.await();

        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertAggReduction(request);
        InternalMax internalMax = (InternalMax) reduce.aggregations.asList().get(0);
        assertEquals(max.get(), internalMax.getValue(), 0.0D);
        assertEquals(0, reduce.sortedTopDocs.scoreDocs.length);
        assertEquals(max.get(), reduce.maxScore, 0.0f);
        assertEquals(expectedNumResults, reduce.totalHits.value);
        assertFalse(reduce.sortedTopDocs.isSortedByField);
        assertNull(reduce.sortedTopDocs.sortFields);
        assertNull(reduce.sortedTopDocs.collapseField);
        assertNull(reduce.sortedTopDocs.collapseValues);
    }

    public void testConsumerOnlyHits() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        if (randomBoolean()) {
            request.source(new SearchSourceBuilder().size(randomIntBetween(1, 10)));
        }
        request.setBatchedReduceSize(bufferSize);
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc  -> {});
        AtomicInteger max = new AtomicInteger();
        CountDownLatch latch =  new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            int number = randomIntBetween(1, 1000);
            max.updateAndGet(prev -> Math.max(prev, number));
            QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", i),
                new SearchShardTarget("node", new ShardId("a", "b", i), null, OriginalIndices.NONE), null);
            result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO),
                    new ScoreDoc[] {new ScoreDoc(0, number)}), number), new DocValueFormat[0]);
            result.setShardIndex(i);
            result.size(1);
            consumer.consumeResult(result, latch::countDown);
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertAggReduction(request);
        assertEquals(1, reduce.sortedTopDocs.scoreDocs.length);
        assertEquals(max.get(), reduce.maxScore, 0.0f);
        assertEquals(expectedNumResults, reduce.totalHits.value);
        assertEquals(max.get(), reduce.sortedTopDocs.scoreDocs[0].score, 0.0f);
        assertFalse(reduce.sortedTopDocs.isSortedByField);
        assertNull(reduce.sortedTopDocs.sortFields);
        assertNull(reduce.sortedTopDocs.collapseField);
        assertNull(reduce.sortedTopDocs.collapseValues);
    }

    private void assertAggReduction(SearchRequest searchRequest) {
        if (searchRequest.source() == null || searchRequest.source().aggregations() == null ||
                searchRequest.source().aggregations().getAggregatorFactories().isEmpty()) {
            // When there aren't any aggregations we don't perform any aggregation reductions.
            assertThat(reductions.size(), equalTo(0));
        } else {
            assertThat(reductions.size(), greaterThanOrEqualTo(1));
            assertEquals(searchRequest.isFinalReduce(), reductions.get(reductions.size() - 1));
        }
    }

    public void testReduceTopNWithFromOffset() throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().size(5).from(5));
        request.setBatchedReduceSize(randomIntBetween(2, 4));
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP
            , request, 4, exc  -> {});
        int score = 100;
        CountDownLatch latch =  new CountDownLatch(4);
        for (int i = 0; i < 4; i++) {
            QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", i),
                new SearchShardTarget("node", new ShardId("a", "b", i), null, OriginalIndices.NONE), null);
            ScoreDoc[] docs = new ScoreDoc[3];
            for (int j = 0; j < docs.length; j++) {
                docs[j] = new ScoreDoc(0, score--);
            }
            result.topDocs(new TopDocsAndMaxScore(new TopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), docs), docs[0].score),
                    new DocValueFormat[0]);
            result.setShardIndex(i);
            result.size(5);
            result.from(5);
            consumer.consumeResult(result, latch::countDown);
        }
        latch.await();
        // 4*3 results = 12 we get result 5 to 10 here with from=5 and size=5
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        ScoreDoc[] scoreDocs = reduce.sortedTopDocs.scoreDocs;
        assertEquals(5, scoreDocs.length);
        assertEquals(100.f, reduce.maxScore, 0.0f);
        assertEquals(12, reduce.totalHits.value);
        assertEquals(95.0f, scoreDocs[0].score, 0.0f);
        assertEquals(94.0f, scoreDocs[1].score, 0.0f);
        assertEquals(93.0f, scoreDocs[2].score, 0.0f);
        assertEquals(92.0f, scoreDocs[3].score, 0.0f);
        assertEquals(91.0f, scoreDocs[4].score, 0.0f);
    }

    public void testConsumerSortByField() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        int size = randomIntBetween(1, 10);
        request.setBatchedReduceSize(bufferSize);
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc  -> {});
        AtomicInteger max = new AtomicInteger();
        SortField[] sortFields = {new SortField("field", SortField.Type.INT, true)};
        DocValueFormat[] docValueFormats = {DocValueFormat.RAW};
        CountDownLatch latch = new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            int number = randomIntBetween(1, 1000);
            max.updateAndGet(prev -> Math.max(prev, number));
            FieldDoc[] fieldDocs = {new FieldDoc(0, Float.NaN, new Object[]{number})};
            TopDocs topDocs = new TopFieldDocs(new TotalHits(1, Relation.EQUAL_TO), fieldDocs, sortFields);
            QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", i),
                new SearchShardTarget("node", new ShardId("a", "b", i), null, OriginalIndices.NONE), null);
            result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), docValueFormats);
            result.setShardIndex(i);
            result.size(size);
            consumer.consumeResult(result, latch::countDown);
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertAggReduction(request);
        assertEquals(Math.min(expectedNumResults, size), reduce.sortedTopDocs.scoreDocs.length);
        assertEquals(expectedNumResults, reduce.totalHits.value);
        assertEquals(max.get(), ((FieldDoc)reduce.sortedTopDocs.scoreDocs[0]).fields[0]);
        assertTrue(reduce.sortedTopDocs.isSortedByField);
        assertEquals(1, reduce.sortedTopDocs.sortFields.length);
        assertEquals("field", reduce.sortedTopDocs.sortFields[0].getField());
        assertEquals(SortField.Type.INT, reduce.sortedTopDocs.sortFields[0].getType());
        assertNull(reduce.sortedTopDocs.collapseField);
        assertNull(reduce.sortedTopDocs.collapseValues);
    }

    public void testConsumerFieldCollapsing() throws Exception {
        int expectedNumResults = randomIntBetween(30, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        int size = randomIntBetween(5, 10);
        request.setBatchedReduceSize(bufferSize);
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc  -> {});
        SortField[] sortFields = {new SortField("field", SortField.Type.STRING)};
        BytesRef a = new BytesRef("a");
        BytesRef b = new BytesRef("b");
        BytesRef c = new BytesRef("c");
        Object[] collapseValues = new Object[]{a, b, c};
        DocValueFormat[] docValueFormats = {DocValueFormat.RAW};
        CountDownLatch latch = new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            Object[] values = {randomFrom(collapseValues)};
            FieldDoc[] fieldDocs = {new FieldDoc(0, Float.NaN, values)};
            TopDocs topDocs = new CollapseTopFieldDocs("field", new TotalHits(1, Relation.EQUAL_TO), fieldDocs, sortFields, values);
            QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", i),
                new SearchShardTarget("node", new ShardId("a", "b", i), null, OriginalIndices.NONE), null);
            result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), docValueFormats);
            result.setShardIndex(i);
            result.size(size);
            consumer.consumeResult(result, latch::countDown);
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertAggReduction(request);
        assertEquals(3, reduce.sortedTopDocs.scoreDocs.length);
        assertEquals(expectedNumResults, reduce.totalHits.value);
        assertEquals(a, ((FieldDoc)reduce.sortedTopDocs.scoreDocs[0]).fields[0]);
        assertEquals(b, ((FieldDoc)reduce.sortedTopDocs.scoreDocs[1]).fields[0]);
        assertEquals(c, ((FieldDoc)reduce.sortedTopDocs.scoreDocs[2]).fields[0]);
        assertTrue(reduce.sortedTopDocs.isSortedByField);
        assertEquals(1, reduce.sortedTopDocs.sortFields.length);
        assertEquals("field", reduce.sortedTopDocs.sortFields[0].getField());
        assertEquals(SortField.Type.STRING, reduce.sortedTopDocs.sortFields[0].getType());
        assertEquals("field", reduce.sortedTopDocs.collapseField);
        assertArrayEquals(collapseValues, reduce.sortedTopDocs.collapseValues);
    }

    public void testConsumerSuggestions() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        request.setBatchedReduceSize(bufferSize);
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc  -> {});
        int maxScoreTerm = -1;
        int maxScorePhrase = -1;
        int maxScoreCompletion = -1;
        CountDownLatch latch = new CountDownLatch(expectedNumResults);
        for (int i = 0; i < expectedNumResults; i++) {
            QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", i),
                new SearchShardTarget("node", new ShardId("a", "b", i), null, OriginalIndices.NONE), null);
            List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions =
                new ArrayList<>();
            {
                TermSuggestion termSuggestion = new TermSuggestion("term", 1, SortBy.SCORE);
                TermSuggestion.Entry entry = new TermSuggestion.Entry(new Text("entry"), 0, 10);
                int numOptions = randomIntBetween(1, 10);
                for (int j = 0; j < numOptions; j++) {
                    int score = numOptions - j;
                    maxScoreTerm = Math.max(maxScoreTerm, score);
                    entry.addOption(new TermSuggestion.Entry.Option(new Text("option"), randomInt(), score));
                }
                termSuggestion.addTerm(entry);
                suggestions.add(termSuggestion);
            }
            {
                PhraseSuggestion phraseSuggestion = new PhraseSuggestion("phrase", 1);
                PhraseSuggestion.Entry entry = new PhraseSuggestion.Entry(new Text("entry"), 0, 10);
                int numOptions = randomIntBetween(1, 10);
                for (int j = 0; j < numOptions; j++) {
                    int score = numOptions - j;
                    maxScorePhrase = Math.max(maxScorePhrase, score);
                    entry.addOption(new PhraseSuggestion.Entry.Option(new Text("option"), new Text("option"), score));
                }
                phraseSuggestion.addTerm(entry);
                suggestions.add(phraseSuggestion);
            }
            {
                CompletionSuggestion completionSuggestion = new CompletionSuggestion("completion", 1, false);
                CompletionSuggestion.Entry entry = new CompletionSuggestion.Entry(new Text("entry"), 0, 10);
                int numOptions = randomIntBetween(1, 10);
                for (int j = 0; j < numOptions; j++) {
                    int score = numOptions - j;
                    maxScoreCompletion = Math.max(maxScoreCompletion, score);
                    CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(j,
                        new Text("option"), score, Collections.emptyMap());
                    entry.addOption(option);
                }
                completionSuggestion.addTerm(entry);
                suggestions.add(completionSuggestion);
            }
            result.suggest(new Suggest(suggestions));
            result.topDocs(new TopDocsAndMaxScore(Lucene.EMPTY_TOP_DOCS, Float.NaN), new DocValueFormat[0]);
            result.setShardIndex(i);
            result.size(0);
            consumer.consumeResult(result, latch::countDown);
        }
        latch.await();
        SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
        assertEquals(3, reduce.suggest.size());
        {
            TermSuggestion term = reduce.suggest.getSuggestion("term");
            assertEquals(1, term.getEntries().size());
            assertEquals(1, term.getEntries().get(0).getOptions().size());
            assertEquals(maxScoreTerm, term.getEntries().get(0).getOptions().get(0).getScore(), 0f);
        }
        {
            PhraseSuggestion phrase = reduce.suggest.getSuggestion("phrase");
            assertEquals(1, phrase.getEntries().size());
            assertEquals(1, phrase.getEntries().get(0).getOptions().size());
            assertEquals(maxScorePhrase, phrase.getEntries().get(0).getOptions().get(0).getScore(), 0f);
        }
        {
            CompletionSuggestion completion = reduce.suggest.getSuggestion("completion");
            assertEquals(1, completion.getSize());
            assertEquals(1, completion.getOptions().size());
            CompletionSuggestion.Entry.Option option = completion.getOptions().get(0);
            assertEquals(maxScoreCompletion, option.getScore(), 0f);
        }
        assertAggReduction(request);
        assertEquals(1, reduce.sortedTopDocs.scoreDocs.length);
        assertEquals(maxScoreCompletion, reduce.sortedTopDocs.scoreDocs[0].score, 0f);
        assertEquals(0, reduce.sortedTopDocs.scoreDocs[0].doc);
        assertNotEquals(-1, reduce.sortedTopDocs.scoreDocs[0].shardIndex);
        assertEquals(0, reduce.totalHits.value);
        assertFalse(reduce.sortedTopDocs.isSortedByField);
        assertNull(reduce.sortedTopDocs.sortFields);
        assertNull(reduce.sortedTopDocs.collapseField);
        assertNull(reduce.sortedTopDocs.collapseValues);
    }

    public void testProgressListener() throws Exception {
        int expectedNumResults = randomIntBetween(10, 100);
        for (int bufferSize : new int[] {expectedNumResults, expectedNumResults/2, expectedNumResults/4, 2}) {
            SearchRequest request = randomSearchRequest();
            request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")));
            request.setBatchedReduceSize(bufferSize);
            AtomicInteger numQueryResultListener = new AtomicInteger();
            AtomicInteger numQueryFailureListener = new AtomicInteger();
            AtomicInteger numReduceListener = new AtomicInteger();
            AtomicReference<InternalAggregations> finalAggsListener = new AtomicReference<>();
            AtomicReference<TotalHits> totalHitsListener = new AtomicReference<>();
            SearchProgressListener progressListener = new SearchProgressListener() {
                @Override
                public void onQueryResult(int shardIndex) {
                    assertThat(shardIndex, lessThan(expectedNumResults));
                    numQueryResultListener.incrementAndGet();
                }

                @Override
                public void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
                    assertThat(shardIndex, lessThan(expectedNumResults));
                    numQueryFailureListener.incrementAndGet();
                }

                @Override
                public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits,
                                            InternalAggregations aggs, int reducePhase) {
                    assertEquals(numReduceListener.incrementAndGet(), reducePhase);
                }

                @Override
                public void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                    totalHitsListener.set(totalHits);
                    finalAggsListener.set(aggs);
                    assertEquals(numReduceListener.incrementAndGet(), reducePhase);
                }
            };
            QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST), progressListener, request, expectedNumResults, exc  -> {});
            AtomicInteger max = new AtomicInteger();
            Thread[] threads = new Thread[expectedNumResults];
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            for (int i = 0; i < expectedNumResults; i++) {
                int id = i;
                threads[i] = new Thread(() -> {
                    int number = randomIntBetween(1, 1000);
                    max.updateAndGet(prev -> Math.max(prev, number));
                    QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId("", id),
                        new SearchShardTarget("node", new ShardId("a", "b", id), null, OriginalIndices.NONE), null);
                    result.topDocs(new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[]{new ScoreDoc(0, number)}), number),
                        new DocValueFormat[0]);
                    InternalAggregations aggs = InternalAggregations.from(Collections.singletonList(
                        new InternalMax("test", (double) number, DocValueFormat.RAW, Collections.emptyMap())));
                    result.aggregations(aggs);
                    result.setShardIndex(id);
                    result.size(1);
                    consumer.consumeResult(result, latch::countDown);
                });
                threads[i].start();
            }
            for (int i = 0; i < expectedNumResults; i++) {
                threads[i].join();
            }
            latch.await();
            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertAggReduction(request);
            InternalMax internalMax = (InternalMax) reduce.aggregations.asList().get(0);
            assertEquals(max.get(), internalMax.getValue(), 0.0D);
            assertEquals(1, reduce.sortedTopDocs.scoreDocs.length);
            assertEquals(max.get(), reduce.maxScore, 0.0f);
            assertEquals(expectedNumResults, reduce.totalHits.value);
            assertEquals(max.get(), reduce.sortedTopDocs.scoreDocs[0].score, 0.0f);
            assertFalse(reduce.sortedTopDocs.isSortedByField);
            assertNull(reduce.sortedTopDocs.sortFields);
            assertNull(reduce.sortedTopDocs.collapseField);
            assertNull(reduce.sortedTopDocs.collapseValues);

            assertEquals(reduce.aggregations, finalAggsListener.get());
            assertEquals(reduce.totalHits, totalHitsListener.get());

            assertEquals(expectedNumResults, numQueryResultListener.get());
            assertEquals(0, numQueryFailureListener.get());
            assertEquals(numReduceListener.get(), reduce.numReducePhases);
        }
    }

    public void testCoordCircuitBreaker() throws Exception {
        int numShards = randomIntBetween(20, 200);
        testReduceCase(numShards, numShards, true);
        testReduceCase(numShards, numShards, false);
        testReduceCase(numShards, randomIntBetween(2, numShards-1), true);
        testReduceCase(numShards, randomIntBetween(2, numShards-1), false);
    }

    private void testReduceCase(int numShards, int bufferSize, boolean shouldFail) throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")).size(0));
        request.setBatchedReduceSize(bufferSize);
        AtomicBoolean hasConsumedFailure = new AtomicBoolean();
        AssertingCircuitBreaker circuitBreaker = new AssertingCircuitBreaker(CircuitBreaker.REQUEST);
        boolean shouldFailPartial = shouldFail && randomBoolean();
        if (shouldFailPartial) {
            circuitBreaker.shouldBreak.set(true);
        }
        QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            circuitBreaker, SearchProgressListener.NOOP,
            request, numShards, exc -> hasConsumedFailure.set(true));
        CountDownLatch latch = new CountDownLatch(numShards);
        Thread[] threads = new Thread[numShards];
        for (int i =  0; i < numShards; i++) {
            final int index = i;
            threads[index] = new Thread(() -> {
                QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId(UUIDs.randomBase64UUID(), index),
                    new SearchShardTarget("node", new ShardId("a", "b", index), null, OriginalIndices.NONE),
                    null);
                result.topDocs(new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS), Float.NaN),
                    new DocValueFormat[0]);
                InternalAggregations aggs = InternalAggregations.from(
                    Collections.singletonList(new InternalMax("test", 0d, DocValueFormat.RAW, Collections.emptyMap()))
                );
                result.aggregations(aggs);
                result.setShardIndex(index);
                result.size(1);
                consumer.consumeResult(result, latch::countDown);
            });
            threads[index].start();
        }
        for (int i = 0; i < numShards; i++) {
            threads[i].join();
        }
        latch.await();
        if (shouldFail) {
            if (shouldFailPartial == false) {
                circuitBreaker.shouldBreak.set(true);
            } else {
                circuitBreaker.shouldBreak.set(false);
            }
            CircuitBreakingException exc = expectThrows(CircuitBreakingException.class, () -> consumer.reduce());
            assertEquals(shouldFailPartial, hasConsumedFailure.get());
            assertThat(exc.getMessage(), containsString("<reduce_aggs>"));
            circuitBreaker.shouldBreak.set(false);
        } else {
            SearchPhaseController.ReducedQueryPhase phase = consumer.reduce();
        }
        consumer.close();
        assertThat(circuitBreaker.allocated, equalTo(0L));
    }

    public void testFailConsumeAggs() throws Exception {
        int expectedNumResults = randomIntBetween(20, 200);
        int bufferSize = randomIntBetween(2, expectedNumResults - 1);
        SearchRequest request = new SearchRequest();

        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")).size(0));
        request.setBatchedReduceSize(bufferSize);
        AtomicBoolean hasConsumedFailure = new AtomicBoolean();
        try (QueryPhaseResultConsumer consumer = searchPhaseController.newSearchPhaseResults(fixedExecutor,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST), SearchProgressListener.NOOP,
            request, expectedNumResults, exc -> hasConsumedFailure.set(true))) {
            for (int i = 0; i < expectedNumResults; i++) {
                final int index = i;
                QuerySearchResult result = new QuerySearchResult(new ShardSearchContextId(UUIDs.randomBase64UUID(), index),
                    new SearchShardTarget("node", new ShardId("a", "b", index), null, OriginalIndices.NONE),
                    null);
                result.topDocs(new TopDocsAndMaxScore(
                        new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS), Float.NaN),
                    new DocValueFormat[0]);
                result.aggregations(null);
                result.setShardIndex(index);
                result.size(1);
                expectThrows(Exception.class, () -> consumer.consumeResult(result, () -> {
                }));
            }
            assertNull(consumer.reduce().aggregations);
        }
    }

    private static class AssertingCircuitBreaker extends NoopCircuitBreaker {
        private final AtomicBoolean shouldBreak = new AtomicBoolean(false);

        private volatile long allocated;

        AssertingCircuitBreaker(String name) {
            super(name);
        }

        @Override
        public void addEstimateBytesAndMaybeBreak(long bytes, String label) throws CircuitBreakingException {
            assert bytes >= 0;
            if (shouldBreak.get()) {
                throw new CircuitBreakingException(label, getDurability());
            }
            allocated += bytes;
        }

        @Override
        public void addWithoutBreaking(long bytes) {
            allocated += bytes;
        }
    }
}
