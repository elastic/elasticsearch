/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.action.search.SearchPhaseController.TopDocsStats;
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
import org.elasticsearch.common.util.concurrent.EsExecutors.TaskTrackingConfig;
import org.elasticsearch.common.util.concurrent.EsThreadPoolExecutor;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.lucene.grouping.TopFieldGroups;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.SearchPhaseResult;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.AggregationReduceContext;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.FetchSearchResult;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.internal.ShardSearchContextId;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileQueryPhaseResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.query.QuerySearchResult;
import org.elasticsearch.search.rank.RankDoc;
import org.elasticsearch.search.rank.RankShardResult;
import org.elasticsearch.search.rank.TestRankShardResult;
import org.elasticsearch.search.rank.context.QueryPhaseRankCoordinatorContext;
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
import org.elasticsearch.transport.TransportMessage;
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

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.anEmptyMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
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
        searchPhaseController = new SearchPhaseController((t, agg) -> new AggregationReduceContext.Builder() {
            @Override
            public AggregationReduceContext forPartialReduction() {
                reductions.add(false);
                return new AggregationReduceContext.ForPartial(BigArrays.NON_RECYCLING_INSTANCE, null, t, agg, b -> {});
            }

            public AggregationReduceContext forFinalReduction() {
                reductions.add(true);
                return new AggregationReduceContext.ForFinal(BigArrays.NON_RECYCLING_INSTANCE, null, t, agg, b -> {});
            };
        });
        threadPool = new TestThreadPool(SearchPhaseControllerTests.class.getName());
        fixedExecutor = EsExecutors.newFixed(
            "test",
            1,
            10,
            EsExecutors.daemonThreadFactory("test"),
            threadPool.getThreadContext(),
            randomFrom(TaskTrackingConfig.DEFAULT, TaskTrackingConfig.DO_NOT_TRACK)
        );
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
        AtomicArray<SearchPhaseResult> results = generateQueryResults(nShards, suggestions, queryResultSize, false, false, false);
        try {
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
            ScoreDoc[] sortedDocs = SearchPhaseController.sortDocs(true, topDocsList, from, size, reducedCompletionSuggestions).scoreDocs();
            assertThat(sortedDocs.length, equalTo(accumulatedLength));
        } finally {
            results.asList().forEach(TransportMessage::decRef);
        }
    }

    public void testSortDocsIsIdempotent() throws Exception {
        int nShards = randomIntBetween(1, 20);
        int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        long randomSeed = randomLong();
        boolean useConstantScore = randomBoolean();
        AtomicArray<SearchPhaseResult> results = generateSeededQueryResults(
            randomSeed,
            nShards,
            Collections.emptyList(),
            queryResultSize,
            useConstantScore
        );
        List<TopDocs> topDocsList = new ArrayList<>();
        boolean ignoreFrom = randomBoolean();
        int from = 0, size = 0;
        ScoreDoc[] sortedDocs;
        try {
            Optional<SearchPhaseResult> first = results.asList().stream().findFirst();
            if (first.isPresent()) {
                from = first.get().queryResult().from();
                size = first.get().queryResult().size();
            }
            for (SearchPhaseResult result : results.asList()) {
                QuerySearchResult queryResult = result.queryResult();
                TopDocs topDocs = queryResult.consumeTopDocs().topDocs;
                topDocsList.add(topDocs);
                SearchPhaseController.setShardIndex(topDocs, result.getShardIndex());
            }
            sortedDocs = SearchPhaseController.sortDocs(ignoreFrom, topDocsList, from, size, Collections.emptyList()).scoreDocs();
        } finally {
            results.asList().forEach(TransportMessage::decRef);
        }
        results = generateSeededQueryResults(randomSeed, nShards, Collections.emptyList(), queryResultSize, useConstantScore);
        try {
            topDocsList = new ArrayList<>();
            for (SearchPhaseResult result : results.asList()) {
                QuerySearchResult queryResult = result.queryResult();
                TopDocs topDocs = queryResult.consumeTopDocs().topDocs;
                topDocsList.add(topDocs);
                SearchPhaseController.setShardIndex(topDocs, result.getShardIndex());
            }
            ScoreDoc[] sortedDocs2 = SearchPhaseController.sortDocs(ignoreFrom, topDocsList, from, size, Collections.emptyList())
                .scoreDocs();
            assertEquals(sortedDocs.length, sortedDocs2.length);
            for (int i = 0; i < sortedDocs.length; i++) {
                assertEquals(sortedDocs[i].doc, sortedDocs2[i].doc);
                assertEquals(sortedDocs[i].shardIndex, sortedDocs2[i].shardIndex);
                assertEquals(sortedDocs[i].score, sortedDocs2[i].score, 0.0f);
            }
        } finally {
            results.asList().forEach(TransportMessage::decRef);
        }
    }

    private AtomicArray<SearchPhaseResult> generateSeededQueryResults(
        long seed,
        int nShards,
        List<CompletionSuggestion> suggestions,
        int searchHitsSize,
        boolean useConstantScore
    ) throws Exception {
        return RandomizedContext.current()
            .runWithPrivateRandomness(
                seed,
                () -> generateQueryResults(nShards, suggestions, searchHitsSize, useConstantScore, false, false)
            );
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
        boolean profile = randomBoolean();
        for (int trackTotalHits : new int[] { SearchContext.TRACK_TOTAL_HITS_DISABLED, SearchContext.TRACK_TOTAL_HITS_ACCURATE }) {
            AtomicArray<SearchPhaseResult> queryResults = generateQueryResults(
                nShards,
                suggestions,
                queryResultSize,
                false,
                profile,
                false
            );
            try {
                SearchPhaseController.ReducedQueryPhase reducedQueryPhase = SearchPhaseController.reducedQueryPhase(
                    queryResults.asList(),
                    new ArrayList<>(),
                    new ArrayList<>(),
                    new TopDocsStats(trackTotalHits),
                    0,
                    true,
                    InternalAggregationTestCase.emptyReduceContextBuilder(),
                    null,
                    true
                );
                List<SearchShardTarget> shards = queryResults.asList()
                    .stream()
                    .map(SearchPhaseResult::getSearchShardTarget)
                    .collect(toList());
                AtomicArray<SearchPhaseResult> fetchResults = generateFetchResults(
                    shards,
                    reducedQueryPhase.sortedTopDocs().scoreDocs(),
                    reducedQueryPhase.suggest(),
                    profile
                );
                final SearchResponseSections mergedResponse = SearchPhaseController.merge(false, reducedQueryPhase, fetchResults);
                try {
                    if (trackTotalHits == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                        assertNull(mergedResponse.hits.getTotalHits());
                    } else {
                        assertThat(mergedResponse.hits.getTotalHits().value(), equalTo(0L));
                        assertEquals(mergedResponse.hits.getTotalHits().relation(), Relation.EQUAL_TO);
                    }
                    for (SearchHit hit : mergedResponse.hits().getHits()) {
                        SearchPhaseResult searchPhaseResult = fetchResults.get(hit.getShard().getShardId().id());
                        assertSame(searchPhaseResult.getSearchShardTarget(), hit.getShard());
                    }
                    int suggestSize = 0;
                    for (Suggest.Suggestion<?> s : reducedQueryPhase.suggest()) {
                        suggestSize += s.getEntries().stream().mapToInt(e -> e.getOptions().size()).sum();
                    }
                    assertThat(suggestSize, lessThanOrEqualTo(maxSuggestSize));
                    assertThat(
                        mergedResponse.hits().getHits().length,
                        equalTo(reducedQueryPhase.sortedTopDocs().scoreDocs().length - suggestSize)
                    );
                    Suggest suggestResult = mergedResponse.suggest();
                    for (Suggest.Suggestion<?> suggestion : reducedQueryPhase.suggest()) {
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
                    if (profile) {
                        assertThat(mergedResponse.profile().entrySet(), hasSize(nShards));
                        assertThat(
                            // All shards should have a query profile
                            mergedResponse.profile().toString(),
                            mergedResponse.profile().values().stream().filter(r -> r.getQueryProfileResults() != null).count(),
                            equalTo((long) nShards)
                        );
                        assertThat(
                            // Some or all shards should have a fetch profile
                            mergedResponse.profile().toString(),
                            mergedResponse.profile().values().stream().filter(r -> r.getFetchPhase() != null).count(),
                            both(greaterThan(0L)).and(lessThanOrEqualTo((long) nShards))
                        );
                    } else {
                        assertThat(mergedResponse.profile(), is(anEmptyMap()));
                    }
                } finally {
                    mergedResponse.decRef();
                    fetchResults.asList().forEach(TransportMessage::decRef);
                }
            } finally {
                queryResults.asList().forEach(TransportMessage::decRef);
            }
        }
    }

    public void testMergeWithRank() {
        final int nShards = randomIntBetween(1, 20);
        final int fsize = randomIntBetween(1, 10);
        final int windowSize = randomIntBetween(11, 100);
        final int queryResultSize = randomBoolean() ? 0 : randomIntBetween(1, nShards * 2);
        for (int trackTotalHits : new int[] { SearchContext.TRACK_TOTAL_HITS_DISABLED, SearchContext.TRACK_TOTAL_HITS_ACCURATE }) {
            AtomicArray<SearchPhaseResult> queryResults = generateQueryResults(nShards, List.of(), queryResultSize, false, false, true);
            try {
                SearchPhaseController.ReducedQueryPhase reducedQueryPhase = SearchPhaseController.reducedQueryPhase(
                    queryResults.asList(),
                    new ArrayList<>(),
                    new ArrayList<>(),
                    new TopDocsStats(trackTotalHits),
                    0,
                    true,
                    InternalAggregationTestCase.emptyReduceContextBuilder(),
                    new QueryPhaseRankCoordinatorContext(windowSize) {
                        @Override
                        public ScoreDoc[] rankQueryPhaseResults(List<QuerySearchResult> querySearchResults, TopDocsStats topDocStats) {
                            PriorityQueue<RankDoc> queue = new PriorityQueue<>(windowSize) {
                                @Override
                                protected boolean lessThan(RankDoc a, RankDoc b) {
                                    return a.score < b.score;
                                }
                            };
                            for (QuerySearchResult qsr : querySearchResults) {
                                RankShardResult rsr = qsr.getRankShardResult();
                                if (rsr != null) {
                                    for (RankDoc rd : ((TestRankShardResult) rsr).testRankDocs) {
                                        queue.insertWithOverflow(rd);
                                    }
                                }
                            }
                            int size = Math.min(fsize, queue.size());
                            RankDoc[] topResults = new RankDoc[size];
                            for (int rdi = 0; rdi < size; ++rdi) {
                                topResults[rdi] = queue.pop();
                                topResults[rdi].rank = rdi + 1;
                            }
                            topDocStats.fetchHits = topResults.length;
                            return topResults;
                        }
                    },
                    true
                );
                List<SearchShardTarget> shards = queryResults.asList()
                    .stream()
                    .map(SearchPhaseResult::getSearchShardTarget)
                    .collect(toList());
                AtomicArray<SearchPhaseResult> fetchResults = generateFetchResults(
                    shards,
                    reducedQueryPhase.sortedTopDocs().scoreDocs(),
                    reducedQueryPhase.suggest(),
                    false
                );
                SearchResponseSections mergedResponse = SearchPhaseController.merge(false, reducedQueryPhase, fetchResults);
                try {
                    if (trackTotalHits == SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                        assertNull(mergedResponse.hits.getTotalHits());
                    } else {
                        assertThat(mergedResponse.hits.getTotalHits().value(), equalTo(0L));
                        assertEquals(mergedResponse.hits.getTotalHits().relation(), Relation.EQUAL_TO);
                    }
                    int rank = 1;
                    for (SearchHit hit : mergedResponse.hits().getHits()) {
                        SearchPhaseResult searchPhaseResult = fetchResults.get(hit.getShard().getShardId().id());
                        assertSame(searchPhaseResult.getSearchShardTarget(), hit.getShard());
                        assertEquals(rank++, hit.getRank());
                    }
                    assertThat(mergedResponse.hits().getHits().length, equalTo(reducedQueryPhase.sortedTopDocs().scoreDocs().length));
                    assertThat(mergedResponse.profile(), is(anEmptyMap()));
                } finally {
                    mergedResponse.decRef();
                    fetchResults.asList().forEach(TransportMessage::decRef);
                }
            } finally {

                queryResults.asList().forEach(TransportMessage::decRef);
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
    private static AtomicArray<SearchPhaseResult> generateQueryResults(
        int nShards,
        List<CompletionSuggestion> suggestions,
        int searchHitsSize,
        boolean useConstantScore,
        boolean profile,
        boolean rank
    ) {
        AtomicArray<SearchPhaseResult> queryResults = new AtomicArray<>(nShards);
        for (int shardIndex = 0; shardIndex < nShards; shardIndex++) {
            String clusterAlias = randomBoolean() ? null : "remote";
            SearchShardTarget searchShardTarget = new SearchShardTarget("", new ShardId("", "", shardIndex), clusterAlias);
            QuerySearchResult querySearchResult = new QuerySearchResult(new ShardSearchContextId("", shardIndex), searchShardTarget, null);
            final TopDocs topDocs;
            float maxScore = 0;
            if (searchHitsSize == 0) {
                topDocs = Lucene.EMPTY_TOP_DOCS;
            } else if (rank) {
                int nDocs = randomIntBetween(0, searchHitsSize);
                RankDoc[] rankDocs = new RankDoc[nDocs];
                for (int i = 0; i < nDocs; i++) {
                    float score = useConstantScore ? 1.0F : Math.abs(randomFloat());
                    rankDocs[i] = new RankDoc(i, score, shardIndex);
                    maxScore = Math.max(score, maxScore);
                }
                querySearchResult.setRankShardResult(new TestRankShardResult(rankDocs));
                topDocs = new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS);
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
                    completionSuggestion.getName(),
                    completionSuggestion.getSize(),
                    false
                );
                final CompletionSuggestion.Entry completionEntry = new CompletionSuggestion.Entry(new Text(""), 0, 5);
                suggestion.addTerm(completionEntry);
                int optionSize = randomIntBetween(1, suggestion.getSize());
                float maxScoreValue = randomIntBetween(suggestion.getSize(), (int) Float.MAX_VALUE);
                for (int i = 0; i < optionSize; i++) {
                    completionEntry.addOption(
                        new CompletionSuggestion.Entry.Option(i, new Text(""), maxScoreValue, Collections.emptyMap())
                    );
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
            if (profile) {
                querySearchResult.profileResults(
                    new SearchProfileQueryPhaseResult(List.of(), new AggregationProfileShardResult(List.of()))
                );
            }
            queryResults.set(shardIndex, querySearchResult);
        }
        return queryResults;
    }

    private static int getTotalQueryHits(AtomicArray<SearchPhaseResult> results) {
        int resultCount = 0;
        for (SearchPhaseResult shardResult : results.asList()) {
            TopDocs topDocs = shardResult.queryResult().topDocs().topDocs;
            assert topDocs.totalHits.relation() == Relation.EQUAL_TO;
            resultCount += (int) topDocs.totalHits.value();
        }
        return resultCount;
    }

    private static List<CompletionSuggestion> reducedSuggest(AtomicArray<SearchPhaseResult> results) {
        Map<String, List<Suggest.Suggestion<CompletionSuggestion.Entry>>> groupedSuggestion = new HashMap<>();
        for (SearchPhaseResult entry : results.asList()) {
            for (Suggest.Suggestion<?> suggestion : entry.queryResult().suggest()) {
                List<Suggest.Suggestion<CompletionSuggestion.Entry>> suggests = groupedSuggestion.computeIfAbsent(
                    suggestion.getName(),
                    s -> new ArrayList<>()
                );
                suggests.add((CompletionSuggestion) suggestion);
            }
        }
        CompletionSuggestion completionSuggestion = new CompletionSuggestion(null, -1, randomBoolean());
        return groupedSuggestion.values().stream().map(completionSuggestion::reduce).toList();
    }

    private static AtomicArray<SearchPhaseResult> generateFetchResults(
        List<SearchShardTarget> shards,
        ScoreDoc[] mergedSearchDocs,
        Suggest mergedSuggest,
        boolean profile
    ) {
        AtomicArray<SearchPhaseResult> fetchResults = new AtomicArray<>(shards.size());
        for (int shardIndex = 0; shardIndex < shards.size(); shardIndex++) {
            float maxScore = -1F;
            SearchShardTarget shardTarget = shards.get(shardIndex);
            FetchSearchResult fetchSearchResult = new FetchSearchResult(new ShardSearchContextId("", shardIndex), shardTarget);
            List<SearchHit> searchHits = new ArrayList<>();
            for (ScoreDoc scoreDoc : mergedSearchDocs) {
                if (scoreDoc.shardIndex == shardIndex) {
                    searchHits.add(SearchHit.unpooled(scoreDoc.doc, ""));
                    if (scoreDoc.score > maxScore) {
                        maxScore = scoreDoc.score;
                    }
                }
            }
            if (mergedSuggest != null) {
                for (Suggest.Suggestion<?> suggestion : mergedSuggest) {
                    if (suggestion instanceof CompletionSuggestion) {
                        for (CompletionSuggestion.Entry.Option option : ((CompletionSuggestion) suggestion).getOptions()) {
                            ScoreDoc doc = option.getDoc();
                            if (doc.shardIndex == shardIndex) {
                                searchHits.add(SearchHit.unpooled(doc.doc, ""));
                                if (doc.score > maxScore) {
                                    maxScore = doc.score;
                                }
                            }
                        }
                    }
                }
            }
            SearchHit[] hits = searchHits.toArray(SearchHits.EMPTY);
            ProfileResult profileResult = profile && searchHits.size() > 0
                ? new ProfileResult("fetch", "fetch", Map.of(), Map.of(), randomNonNegativeLong(), List.of())
                : null;
            fetchSearchResult.shardResult(
                SearchHits.unpooled(hits, new TotalHits(hits.length, Relation.EQUAL_TO), maxScore),
                profileResult
            );
            fetchResults.set(shardIndex, fetchSearchResult);
        }
        return fetchResults;
    }

    private static SearchRequest randomSearchRequest() {
        return randomBoolean()
            ? new SearchRequest()
            : SearchRequest.subSearchRequest(new TaskId("n", 1), new SearchRequest(), Strings.EMPTY_ARRAY, "remote", 0, randomBoolean());
    }

    public void testConsumer() throws Exception {
        consumerTestCase(0);
    }

    public void testConsumerWithEmptyResponse() throws Exception {
        consumerTestCase(randomIntBetween(1, 5));
    }

    private void consumerTestCase(int numEmptyResponses) throws Exception {
        int numShards = 3 + numEmptyResponses;
        int bufferSize = randomIntBetween(2, 3);
        CountDownLatch latch = new CountDownLatch(numShards);
        SearchRequest request = randomSearchRequest();
        request.source(new SearchSourceBuilder().aggregation(new MaxAggregationBuilder("test")));
        request.setBatchedReduceSize(bufferSize);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                3 + numEmptyResponses,
                exc -> {}
            )
        ) {
            if (numEmptyResponses == 0) {
                assertEquals(0, reductions.size());
            }
            if (numEmptyResponses > 0) {
                QuerySearchResult empty = QuerySearchResult.nullInstance();
                int shardId = 2 + numEmptyResponses;
                empty.setShardIndex(2 + numEmptyResponses);
                empty.setSearchShardTarget(new SearchShardTarget("node", new ShardId("a", "b", shardId), null));
                consumer.consumeResult(empty, latch::countDown);
                numEmptyResponses--;
            }

            QuerySearchResult result = new QuerySearchResult(
                new ShardSearchContextId("", 0),
                new SearchShardTarget("node", new ShardId("a", "b", 0), null),
                null
            );
            try {
                result.topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                    new DocValueFormat[0]
                );
                InternalAggregations aggs = InternalAggregations.from(singletonList(new Max("test", 1.0D, DocValueFormat.RAW, emptyMap())));
                result.aggregations(aggs);
                result.setShardIndex(0);
                consumer.consumeResult(result, latch::countDown);
            } finally {
                result.decRef();
            }
            result = new QuerySearchResult(
                new ShardSearchContextId("", 1),
                new SearchShardTarget("node", new ShardId("a", "b", 0), null),
                null
            );
            try {
                result.topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                    new DocValueFormat[0]
                );
                InternalAggregations aggs = InternalAggregations.from(singletonList(new Max("test", 3.0D, DocValueFormat.RAW, emptyMap())));
                result.aggregations(aggs);
                result.setShardIndex(2);
                consumer.consumeResult(result, latch::countDown);
            } finally {
                result.decRef();
            }
            result = new QuerySearchResult(
                new ShardSearchContextId("", 1),
                new SearchShardTarget("node", new ShardId("a", "b", 0), null),
                null
            );
            try {
                result.topDocs(
                    new TopDocsAndMaxScore(new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), Float.NaN),
                    new DocValueFormat[0]
                );
                InternalAggregations aggs = InternalAggregations.from(singletonList(new Max("test", 2.0D, DocValueFormat.RAW, emptyMap())));
                result.aggregations(aggs);
                result.setShardIndex(1);
                consumer.consumeResult(result, latch::countDown);
            } finally {
                result.decRef();
            }
            while (numEmptyResponses > 0) {
                result = QuerySearchResult.nullInstance();
                try {
                    int shardId = 2 + numEmptyResponses;
                    result.setShardIndex(shardId);
                    result.setSearchShardTarget(new SearchShardTarget("node", new ShardId("a", "b", shardId), null));
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
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
            assertEquals(numTotalReducePhases, reduce.numReducePhases());
            assertEquals(numTotalReducePhases, reductions.size());
            assertAggReduction(request);
            Max max = (Max) reduce.aggregations().asList().get(0);
            assertEquals(3.0D, max.value(), 0.0D);
            assertFalse(reduce.sortedTopDocs().isSortedByField());
            assertNull(reduce.sortedTopDocs().sortFields());
            assertNull(reduce.sortedTopDocs().collapseField());
            assertNull(reduce.sortedTopDocs().collapseValues());
        }
    }

    public void testConsumerConcurrently() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);

        SearchRequest request = randomSearchRequest();
        request.source(new SearchSourceBuilder().aggregation(new MaxAggregationBuilder("test")));
        request.setBatchedReduceSize(bufferSize);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> {}
            )
        ) {
            AtomicInteger max = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            runInParallel(expectedNumResults, id -> {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", id),
                    new SearchShardTarget("node", new ShardId("a", "b", id), null),
                    null
                );
                try {
                    result.topDocs(
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, number) }),
                            number
                        ),
                        new DocValueFormat[0]
                    );
                    InternalAggregations aggs = InternalAggregations.from(
                        Collections.singletonList(new Max("test", (double) number, DocValueFormat.RAW, Collections.emptyMap()))
                    );
                    result.aggregations(aggs);
                    result.setShardIndex(id);
                    result.size(1);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            });
            latch.await();

            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertAggReduction(request);
            Max internalMax = (Max) reduce.aggregations().asList().get(0);
            assertEquals(max.get(), internalMax.value(), 0.0D);
            assertEquals(1, reduce.sortedTopDocs().scoreDocs().length);
            assertEquals(max.get(), reduce.maxScore(), 0.0f);
            assertEquals(expectedNumResults, reduce.totalHits().value());
            assertEquals(max.get(), reduce.sortedTopDocs().scoreDocs()[0].score, 0.0f);
            assertFalse(reduce.sortedTopDocs().isSortedByField());
            assertNull(reduce.sortedTopDocs().sortFields());
            assertNull(reduce.sortedTopDocs().collapseField());
            assertNull(reduce.sortedTopDocs().collapseValues());
        }
    }

    public void testConsumerOnlyAggs() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        request.source(new SearchSourceBuilder().aggregation(new MaxAggregationBuilder("test")).size(0));
        request.setBatchedReduceSize(bufferSize);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> {}
            )
        ) {
            AtomicInteger max = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            for (int i = 0; i < expectedNumResults; i++) {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", i),
                    new SearchShardTarget("node", new ShardId("a", "b", i), null),
                    null
                );
                try {
                    result.topDocs(
                        new TopDocsAndMaxScore(new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[0]), number),
                        new DocValueFormat[0]
                    );
                    InternalAggregations aggs = InternalAggregations.from(
                        Collections.singletonList(new Max("test", (double) number, DocValueFormat.RAW, Collections.emptyMap()))
                    );
                    result.aggregations(aggs);
                    result.setShardIndex(i);
                    result.size(1);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            }
            latch.await();

            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertAggReduction(request);
            Max internalMax = (Max) reduce.aggregations().asList().get(0);
            assertEquals(max.get(), internalMax.value(), 0.0D);
            assertEquals(0, reduce.sortedTopDocs().scoreDocs().length);
            assertEquals(max.get(), reduce.maxScore(), 0.0f);
            assertEquals(expectedNumResults, reduce.totalHits().value());
            assertFalse(reduce.sortedTopDocs().isSortedByField());
            assertNull(reduce.sortedTopDocs().sortFields());
            assertNull(reduce.sortedTopDocs().collapseField());
            assertNull(reduce.sortedTopDocs().collapseValues());
        }
    }

    public void testConsumerOnlyHits() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        if (randomBoolean()) {
            request.source(new SearchSourceBuilder().size(randomIntBetween(1, 10)));
        }
        request.setBatchedReduceSize(bufferSize);

        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> {}
            )
        ) {
            AtomicInteger max = new AtomicInteger();
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            for (int i = 0; i < expectedNumResults; i++) {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", i),
                    new SearchShardTarget("node", new ShardId("a", "b", i), null),
                    null
                );
                try {
                    result.topDocs(
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, number) }),
                            number
                        ),
                        new DocValueFormat[0]
                    );
                    result.setShardIndex(i);
                    result.size(1);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            }
            latch.await();
            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertAggReduction(request);
            assertEquals(1, reduce.sortedTopDocs().scoreDocs().length);
            assertEquals(max.get(), reduce.maxScore(), 0.0f);
            assertEquals(expectedNumResults, reduce.totalHits().value());
            assertEquals(max.get(), reduce.sortedTopDocs().scoreDocs()[0].score, 0.0f);
            assertFalse(reduce.sortedTopDocs().isSortedByField());
            assertNull(reduce.sortedTopDocs().sortFields());
            assertNull(reduce.sortedTopDocs().collapseField());
            assertNull(reduce.sortedTopDocs().collapseValues());
        }
    }

    private void assertAggReduction(SearchRequest searchRequest) {
        if (searchRequest.source() == null
            || searchRequest.source().aggregations() == null
            || searchRequest.source().aggregations().getAggregatorFactories().isEmpty()) {
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
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                4,
                exc -> {}
            )
        ) {
            int score = 100;
            CountDownLatch latch = new CountDownLatch(4);
            for (int i = 0; i < 4; i++) {
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", i),
                    new SearchShardTarget("node", new ShardId("a", "b", i), null),
                    null
                );
                try {
                    ScoreDoc[] docs = new ScoreDoc[3];
                    for (int j = 0; j < docs.length; j++) {
                        docs[j] = new ScoreDoc(0, score--);
                    }
                    result.topDocs(
                        new TopDocsAndMaxScore(new TopDocs(new TotalHits(3, TotalHits.Relation.EQUAL_TO), docs), docs[0].score),
                        new DocValueFormat[0]
                    );
                    result.setShardIndex(i);
                    result.size(5);
                    result.from(5);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            }
            latch.await();
            // 4*3 results = 12 we get result 5 to 10 here with from=5 and size=5
            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            ScoreDoc[] scoreDocs = reduce.sortedTopDocs().scoreDocs();
            assertEquals(5, scoreDocs.length);
            assertEquals(100.f, reduce.maxScore(), 0.0f);
            assertEquals(12, reduce.totalHits().value());
            assertEquals(95.0f, scoreDocs[0].score, 0.0f);
            assertEquals(94.0f, scoreDocs[1].score, 0.0f);
            assertEquals(93.0f, scoreDocs[2].score, 0.0f);
            assertEquals(92.0f, scoreDocs[3].score, 0.0f);
            assertEquals(91.0f, scoreDocs[4].score, 0.0f);
        }
    }

    public void testConsumerSortByField() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        int size = randomIntBetween(1, 10);
        request.setBatchedReduceSize(bufferSize);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> {}
            )
        ) {
            AtomicInteger max = new AtomicInteger();
            SortField[] sortFields = { new SortField("field", SortField.Type.INT, true) };
            DocValueFormat[] docValueFormats = { DocValueFormat.RAW };
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            for (int i = 0; i < expectedNumResults; i++) {
                int number = randomIntBetween(1, 1000);
                max.updateAndGet(prev -> Math.max(prev, number));
                FieldDoc[] fieldDocs = { new FieldDoc(0, Float.NaN, new Object[] { number }) };
                TopDocs topDocs = new TopFieldDocs(new TotalHits(1, Relation.EQUAL_TO), fieldDocs, sortFields);
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", i),
                    new SearchShardTarget("node", new ShardId("a", "b", i), null),
                    null
                );
                try {
                    result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), docValueFormats);
                    result.setShardIndex(i);
                    result.size(size);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            }
            latch.await();
            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertAggReduction(request);
            assertEquals(Math.min(expectedNumResults, size), reduce.sortedTopDocs().scoreDocs().length);
            assertEquals(expectedNumResults, reduce.totalHits().value());
            assertEquals(max.get(), ((FieldDoc) reduce.sortedTopDocs().scoreDocs()[0]).fields[0]);
            assertTrue(reduce.sortedTopDocs().isSortedByField());
            assertEquals(1, reduce.sortedTopDocs().sortFields().length);
            assertEquals("field", reduce.sortedTopDocs().sortFields()[0].getField());
            assertEquals(SortField.Type.INT, reduce.sortedTopDocs().sortFields()[0].getType());
            assertNull(reduce.sortedTopDocs().collapseField());
            assertNull(reduce.sortedTopDocs().collapseValues());
        }
    }

    public void testConsumerFieldCollapsing() throws Exception {
        int expectedNumResults = randomIntBetween(30, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        int size = randomIntBetween(5, 10);
        request.setBatchedReduceSize(bufferSize);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> {}
            )
        ) {
            SortField[] sortFields = { new SortField("field", SortField.Type.STRING) };
            BytesRef a = new BytesRef("a");
            BytesRef b = new BytesRef("b");
            BytesRef c = new BytesRef("c");
            Object[] collapseValues = new Object[] { a, b, c };
            DocValueFormat[] docValueFormats = { DocValueFormat.RAW };
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            for (int i = 0; i < expectedNumResults; i++) {
                Object[] values = { randomFrom(collapseValues) };
                FieldDoc[] fieldDocs = { new FieldDoc(0, Float.NaN, values) };
                TopDocs topDocs = new TopFieldGroups("field", new TotalHits(1, Relation.EQUAL_TO), fieldDocs, sortFields, values);
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", i),
                    new SearchShardTarget("node", new ShardId("a", "b", i), null),
                    null
                );
                try {
                    result.topDocs(new TopDocsAndMaxScore(topDocs, Float.NaN), docValueFormats);
                    result.setShardIndex(i);
                    result.size(size);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            }
            latch.await();
            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertAggReduction(request);
            assertEquals(3, reduce.sortedTopDocs().scoreDocs().length);
            assertEquals(expectedNumResults, reduce.totalHits().value());
            assertEquals(a, ((FieldDoc) reduce.sortedTopDocs().scoreDocs()[0]).fields[0]);
            assertEquals(b, ((FieldDoc) reduce.sortedTopDocs().scoreDocs()[1]).fields[0]);
            assertEquals(c, ((FieldDoc) reduce.sortedTopDocs().scoreDocs()[2]).fields[0]);
            assertTrue(reduce.sortedTopDocs().isSortedByField());
            assertEquals(1, reduce.sortedTopDocs().sortFields().length);
            assertEquals("field", reduce.sortedTopDocs().sortFields()[0].getField());
            assertEquals(SortField.Type.STRING, reduce.sortedTopDocs().sortFields()[0].getType());
            assertEquals("field", reduce.sortedTopDocs().collapseField());
            assertArrayEquals(collapseValues, reduce.sortedTopDocs().collapseValues());
        }
    }

    public void testConsumerSuggestions() throws Exception {
        int expectedNumResults = randomIntBetween(1, 100);
        int bufferSize = randomIntBetween(2, 200);
        SearchRequest request = randomSearchRequest();
        request.setBatchedReduceSize(bufferSize);
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> {}
            )
        ) {
            int maxScoreTerm = -1;
            int maxScorePhrase = -1;
            int maxScoreCompletion = -1;
            CountDownLatch latch = new CountDownLatch(expectedNumResults);
            for (int i = 0; i < expectedNumResults; i++) {
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId("", i),
                    new SearchShardTarget("node", new ShardId("a", "b", i), null),
                    null
                );
                try {
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
                            CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(
                                j,
                                new Text("option"),
                                score,
                                Collections.emptyMap()
                            );
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
                } finally {
                    result.decRef();
                }
            }
            latch.await();
            SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
            assertEquals(3, reduce.suggest().size());
            {
                TermSuggestion term = reduce.suggest().getSuggestion("term");
                assertEquals(1, term.getEntries().size());
                assertEquals(1, term.getEntries().get(0).getOptions().size());
                assertEquals(maxScoreTerm, term.getEntries().get(0).getOptions().get(0).getScore(), 0f);
            }
            {
                PhraseSuggestion phrase = reduce.suggest().getSuggestion("phrase");
                assertEquals(1, phrase.getEntries().size());
                assertEquals(1, phrase.getEntries().get(0).getOptions().size());
                assertEquals(maxScorePhrase, phrase.getEntries().get(0).getOptions().get(0).getScore(), 0f);
            }
            {
                CompletionSuggestion completion = reduce.suggest().getSuggestion("completion");
                assertEquals(1, completion.getSize());
                assertEquals(1, completion.getOptions().size());
                CompletionSuggestion.Entry.Option option = completion.getOptions().get(0);
                assertEquals(maxScoreCompletion, option.getScore(), 0f);
            }
            assertAggReduction(request);
            assertEquals(1, reduce.sortedTopDocs().scoreDocs().length);
            assertEquals(maxScoreCompletion, reduce.sortedTopDocs().scoreDocs()[0].score, 0f);
            assertEquals(0, reduce.sortedTopDocs().scoreDocs()[0].doc);
            assertNotEquals(-1, reduce.sortedTopDocs().scoreDocs()[0].shardIndex);
            assertEquals(0, reduce.totalHits().value());
            assertFalse(reduce.sortedTopDocs().isSortedByField());
            assertNull(reduce.sortedTopDocs().sortFields());
            assertNull(reduce.sortedTopDocs().collapseField());
            assertNull(reduce.sortedTopDocs().collapseValues());
        }
    }

    public void testProgressListener() throws Exception {
        int expectedNumResults = randomIntBetween(10, 100);
        for (int bufferSize : new int[] { expectedNumResults, expectedNumResults / 2, expectedNumResults / 4, 2 }) {
            SearchRequest request = randomSearchRequest();
            request.source(new SearchSourceBuilder().aggregation(new MaxAggregationBuilder("test")));
            request.setBatchedReduceSize(bufferSize);
            AtomicInteger numQueryResultListener = new AtomicInteger();
            AtomicInteger numQueryFailureListener = new AtomicInteger();
            AtomicInteger numReduceListener = new AtomicInteger();
            AtomicReference<InternalAggregations> finalAggsListener = new AtomicReference<>();
            AtomicReference<TotalHits> totalHitsListener = new AtomicReference<>();
            SearchProgressListener progressListener = new SearchProgressListener() {
                @Override
                public void onQueryResult(int shardIndex, QuerySearchResult queryResult) {
                    assertThat(shardIndex, lessThan(expectedNumResults));
                    numQueryResultListener.incrementAndGet();
                }

                @Override
                public void onQueryFailure(int shardIndex, SearchShardTarget shardTarget, Exception exc) {
                    assertThat(shardIndex, lessThan(expectedNumResults));
                    numQueryFailureListener.incrementAndGet();
                }

                @Override
                public void onPartialReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                    assertEquals(numReduceListener.incrementAndGet(), reducePhase);
                }

                @Override
                public void onFinalReduce(List<SearchShard> shards, TotalHits totalHits, InternalAggregations aggs, int reducePhase) {
                    totalHitsListener.set(totalHits);
                    finalAggsListener.set(aggs);
                    assertEquals(numReduceListener.incrementAndGet(), reducePhase);
                }
            };
            try (
                SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                    fixedExecutor,
                    new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                    () -> false,
                    progressListener,
                    request,
                    expectedNumResults,
                    exc -> {}
                )
            ) {
                AtomicInteger max = new AtomicInteger();
                CountDownLatch latch = new CountDownLatch(expectedNumResults);
                runInParallel(expectedNumResults, id -> {
                    int number = randomIntBetween(1, 1000);
                    max.updateAndGet(prev -> Math.max(prev, number));
                    QuerySearchResult result = new QuerySearchResult(
                        new ShardSearchContextId("", id),
                        new SearchShardTarget("node", new ShardId("a", "b", id), null),
                        null
                    );
                    try {
                        result.topDocs(
                            new TopDocsAndMaxScore(
                                new TopDocs(new TotalHits(1, TotalHits.Relation.EQUAL_TO), new ScoreDoc[] { new ScoreDoc(0, number) }),
                                number
                            ),
                            new DocValueFormat[0]
                        );
                        InternalAggregations aggs = InternalAggregations.from(
                            Collections.singletonList(new Max("test", (double) number, DocValueFormat.RAW, Collections.emptyMap()))
                        );
                        result.aggregations(aggs);
                        result.setShardIndex(id);
                        result.size(1);
                        consumer.consumeResult(result, latch::countDown);
                    } finally {
                        result.decRef();
                    }
                });
                latch.await();
                SearchPhaseController.ReducedQueryPhase reduce = consumer.reduce();
                assertAggReduction(request);
                Max internalMax = (Max) reduce.aggregations().asList().get(0);
                assertEquals(max.get(), internalMax.value(), 0.0D);
                assertEquals(1, reduce.sortedTopDocs().scoreDocs().length);
                assertEquals(max.get(), reduce.maxScore(), 0.0f);
                assertEquals(expectedNumResults, reduce.totalHits().value());
                assertEquals(max.get(), reduce.sortedTopDocs().scoreDocs()[0].score, 0.0f);
                assertFalse(reduce.sortedTopDocs().isSortedByField());
                assertNull(reduce.sortedTopDocs().sortFields());
                assertNull(reduce.sortedTopDocs().collapseField());
                assertNull(reduce.sortedTopDocs().collapseValues());

                assertEquals(reduce.aggregations(), finalAggsListener.get());
                assertEquals(reduce.totalHits(), totalHitsListener.get());

                assertEquals(expectedNumResults, numQueryResultListener.get());
                assertEquals(0, numQueryFailureListener.get());
                assertEquals(numReduceListener.get(), reduce.numReducePhases());
            }
        }
    }

    public void testCoordCircuitBreaker() throws Exception {
        int numShards = randomIntBetween(20, 200);
        testReduceCase(numShards, numShards, true);
        testReduceCase(numShards, numShards, false);
        testReduceCase(numShards, randomIntBetween(2, numShards - 1), true);
        testReduceCase(numShards, randomIntBetween(2, numShards - 1), false);
    }

    private void testReduceCase(int numShards, int bufferSize, boolean shouldFail) throws Exception {
        SearchRequest request = new SearchRequest();
        request.source(new SearchSourceBuilder().aggregation(new MaxAggregationBuilder("test")).size(0));
        request.setBatchedReduceSize(bufferSize);
        AtomicBoolean hasConsumedFailure = new AtomicBoolean();
        AssertingCircuitBreaker circuitBreaker = new AssertingCircuitBreaker(CircuitBreaker.REQUEST);
        boolean shouldFailPartial = shouldFail && randomBoolean();
        if (shouldFailPartial) {
            circuitBreaker.shouldBreak.set(true);
        }
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                circuitBreaker,
                () -> false,
                SearchProgressListener.NOOP,
                request,
                numShards,
                exc -> hasConsumedFailure.set(true)
            )
        ) {
            CountDownLatch latch = new CountDownLatch(numShards);
            runInParallel(numShards, index -> {
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), index),
                    new SearchShardTarget("node", new ShardId("a", "b", index), null),
                    null
                );
                try {
                    result.topDocs(
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                            Float.NaN
                        ),
                        new DocValueFormat[0]
                    );
                    InternalAggregations aggs = InternalAggregations.from(
                        Collections.singletonList(new Max("test", 0d, DocValueFormat.RAW, Collections.emptyMap()))
                    );
                    result.aggregations(aggs);
                    result.setShardIndex(index);
                    result.size(1);
                    consumer.consumeResult(result, latch::countDown);
                } finally {
                    result.decRef();
                }
            });
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
                consumer.reduce();
            }
        }
        assertThat(circuitBreaker.allocated, equalTo(0L));
    }

    public void testFailConsumeAggs() throws Exception {
        int expectedNumResults = randomIntBetween(20, 200);
        int bufferSize = randomIntBetween(2, expectedNumResults - 1);
        SearchRequest request = new SearchRequest();

        request.source(new SearchSourceBuilder().aggregation(AggregationBuilders.avg("foo")).size(0));
        request.setBatchedReduceSize(bufferSize);
        AtomicBoolean hasConsumedFailure = new AtomicBoolean();
        try (
            SearchPhaseResults<SearchPhaseResult> consumer = searchPhaseController.newSearchPhaseResults(
                fixedExecutor,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST),
                () -> false,
                SearchProgressListener.NOOP,
                request,
                expectedNumResults,
                exc -> hasConsumedFailure.set(true)
            )
        ) {
            for (int i = 0; i < expectedNumResults; i++) {
                final int index = i;
                QuerySearchResult result = new QuerySearchResult(
                    new ShardSearchContextId(UUIDs.randomBase64UUID(), index),
                    new SearchShardTarget("node", new ShardId("a", "b", index), null),
                    null
                );
                try {
                    result.topDocs(
                        new TopDocsAndMaxScore(
                            new TopDocs(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Lucene.EMPTY_SCORE_DOCS),
                            Float.NaN
                        ),
                        new DocValueFormat[0]
                    );
                    result.aggregations(null);
                    result.setShardIndex(index);
                    result.size(1);
                    expectThrows(Exception.class, () -> consumer.consumeResult(result, () -> {}));
                } finally {
                    result.decRef();
                }
            }
            assertNull(consumer.reduce().aggregations());
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
