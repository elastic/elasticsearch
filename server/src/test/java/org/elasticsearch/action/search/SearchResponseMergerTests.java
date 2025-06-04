/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.apache.lucene.search.SortField;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.search.TransportSearchAction.SearchTimeProvider;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.SearchShardTarget;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.InternalDateRange;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.metrics.Max;
import org.elasticsearch.search.aggregations.metrics.MaxAggregationBuilder;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.profile.SearchProfileResults;
import org.elasticsearch.search.profile.SearchProfileResultsTests;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.completion.CompletionSuggestion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.RemoteClusterAware;
import org.elasticsearch.transport.RemoteClusterService;
import org.junit.Before;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.TreeMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.elasticsearch.test.InternalAggregationTestCase.emptyReduceContextBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class SearchResponseMergerTests extends ESTestCase {

    private int numResponses;
    private ExecutorService executorService;

    @Before
    public void init() {
        numResponses = randomIntBetween(1, 10);
        executorService = Executors.newFixedThreadPool(numResponses);
    }

    private void addResponse(SearchResponseMerger searchResponseMerger, SearchResponse searchResponse) {
        if (randomBoolean()) {
            searchResponse.incRef();
            executorService.submit(() -> {
                try {
                    searchResponseMerger.add(searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            });
        } else {
            searchResponseMerger.add(searchResponse);
        }
    }

    private void awaitResponsesAdded() throws InterruptedException {
        executorService.shutdown();
        assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
    }

    public void testMergeTookInMillis() throws InterruptedException {
        long currentRelativeTime = randomNonNegativeLong();
        SearchTimeProvider timeProvider = new SearchTimeProvider(randomLong(), 0, () -> currentRelativeTime);
        try (
            SearchResponseMerger merger = new SearchResponseMerger(
                randomIntBetween(0, 1000),
                randomIntBetween(0, 10000),
                SearchContext.TRACK_TOTAL_HITS_ACCURATE,
                timeProvider,
                emptyReduceContextBuilder()
            )
        ) {
            for (int i = 0; i < numResponses; i++) {
                SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
                    null,
                    1,
                    1,
                    0,
                    randomNonNegativeLong(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponseTests.randomClusters()
                );
                try {
                    addResponse(merger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            SearchResponse searchResponse = merger.getMergedResponse(SearchResponse.Clusters.EMPTY);
            try {
                assertEquals(TimeUnit.NANOSECONDS.toMillis(currentRelativeTime), searchResponse.getTook().millis());
            } finally {
                searchResponse.decRef();
            }
        }
    }

    public void testMergeShardFailures() throws InterruptedException {
        SearchTimeProvider searchTimeProvider = new SearchTimeProvider(0, 0, () -> 0);
        try (
            SearchResponseMerger merger = new SearchResponseMerger(
                0,
                0,
                SearchContext.TRACK_TOTAL_HITS_ACCURATE,
                searchTimeProvider,
                emptyReduceContextBuilder()
            )
        ) {
            PriorityQueue<Tuple<SearchShardTarget, ShardSearchFailure>> priorityQueue = new PriorityQueue<>(
                Comparator.comparing(Tuple::v1, (o1, o2) -> {
                    int compareTo = o1.getShardId().compareTo(o2.getShardId());
                    if (compareTo != 0) {
                        return compareTo;
                    }
                    return o1.getClusterAlias().compareTo(o2.getClusterAlias());
                })
            );
            int numIndices = numResponses * randomIntBetween(1, 3);
            Iterator<Map.Entry<String, Index[]>> indicesPerCluster = randomRealisticIndices(numIndices, numResponses).entrySet().iterator();
            for (int i = 0; i < numResponses; i++) {
                Map.Entry<String, Index[]> entry = indicesPerCluster.next();
                String clusterAlias = entry.getKey();
                Index[] indices = entry.getValue();
                int numFailures = randomIntBetween(1, 10);
                ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[numFailures];
                for (int j = 0; j < numFailures; j++) {
                    ShardId shardId = new ShardId(randomFrom(indices), j);
                    SearchShardTarget searchShardTarget = new SearchShardTarget(randomAlphaOfLength(6), shardId, clusterAlias);
                    ShardSearchFailure failure = new ShardSearchFailure(new IllegalArgumentException(), searchShardTarget);
                    shardSearchFailures[j] = failure;
                    priorityQueue.add(Tuple.tuple(searchShardTarget, failure));
                }
                SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
                    null,
                    1,
                    1,
                    0,
                    100L,
                    shardSearchFailures,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    addResponse(merger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, merger.numResponses());
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse mergedResponse = merger.getMergedResponse(clusters);
            try {
                assertSame(clusters, mergedResponse.getClusters());
                assertEquals(numResponses, mergedResponse.getTotalShards());
                assertEquals(numResponses, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertEquals(priorityQueue.size(), mergedResponse.getFailedShards());
                ShardSearchFailure[] shardFailures = mergedResponse.getShardFailures();
                assertEquals(priorityQueue.size(), shardFailures.length);
                for (ShardSearchFailure shardFailure : shardFailures) {
                    ShardSearchFailure expected = priorityQueue.poll().v2();
                    assertSame(expected, shardFailure);
                }
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeShardFailuresNullShardTarget() throws InterruptedException {
        SearchTimeProvider searchTimeProvider = new SearchTimeProvider(0, 0, () -> 0);
        try (
            SearchResponseMerger merger = new SearchResponseMerger(
                0,
                0,
                SearchContext.TRACK_TOTAL_HITS_ACCURATE,
                searchTimeProvider,
                emptyReduceContextBuilder()
            )
        ) {
            PriorityQueue<Tuple<ShardId, ShardSearchFailure>> priorityQueue = new PriorityQueue<>(Comparator.comparing(Tuple::v1));
            for (int i = 0; i < numResponses; i++) {
                int numFailures = randomIntBetween(1, 10);
                ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[numFailures];
                for (int j = 0; j < numFailures; j++) {
                    String index = "index-" + i;
                    ShardId shardId = new ShardId(index, index + "-uuid", j);
                    ElasticsearchException elasticsearchException = new ElasticsearchException(new IllegalArgumentException());
                    elasticsearchException.setShard(shardId);
                    ShardSearchFailure failure = new ShardSearchFailure(elasticsearchException);
                    shardSearchFailures[j] = failure;
                    priorityQueue.add(Tuple.tuple(shardId, failure));
                }
                SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
                    null,
                    1,
                    1,
                    0,
                    100L,
                    shardSearchFailures,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    addResponse(merger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, merger.numResponses());
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse mergedResponse = merger.getMergedResponse(clusters);
            try {
                assertSame(clusters, mergedResponse.getClusters());
                assertEquals(numResponses, mergedResponse.getTotalShards());
                assertEquals(numResponses, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertEquals(priorityQueue.size(), mergedResponse.getFailedShards());
                ShardSearchFailure[] shardFailures = mergedResponse.getShardFailures();
                assertEquals(priorityQueue.size(), shardFailures.length);
                for (ShardSearchFailure shardFailure : shardFailures) {
                    ShardSearchFailure expected = priorityQueue.poll().v2();
                    assertSame(expected, shardFailure);
                }
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeShardFailuresNullShardId() throws InterruptedException {
        SearchTimeProvider searchTimeProvider = new SearchTimeProvider(0, 0, () -> 0);
        try (
            SearchResponseMerger merger = new SearchResponseMerger(
                0,
                0,
                SearchContext.TRACK_TOTAL_HITS_ACCURATE,
                searchTimeProvider,
                emptyReduceContextBuilder()
            )
        ) {
            List<ShardSearchFailure> expectedFailures = new ArrayList<>();
            for (int i = 0; i < numResponses; i++) {
                int numFailures = randomIntBetween(1, 50);
                ShardSearchFailure[] shardSearchFailures = new ShardSearchFailure[numFailures];
                for (int j = 0; j < numFailures; j++) {
                    ShardSearchFailure shardSearchFailure = new ShardSearchFailure(
                        new ElasticsearchException(new IllegalArgumentException())
                    );
                    shardSearchFailures[j] = shardSearchFailure;
                    expectedFailures.add(shardSearchFailure);
                }
                SearchResponse searchResponse = SearchResponseUtils.emptyWithTotalHits(
                    null,
                    1,
                    1,
                    0,
                    100L,
                    shardSearchFailures,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    addResponse(merger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, merger.numResponses());
            var mergedResponse = merger.getMergedResponse(SearchResponse.Clusters.EMPTY);
            try {
                ShardSearchFailure[] shardFailures = mergedResponse.getShardFailures();
                assertThat(Arrays.asList(shardFailures), containsInAnyOrder(expectedFailures.toArray(ShardSearchFailure.EMPTY_ARRAY)));
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeProfileResults() throws InterruptedException {
        SearchTimeProvider searchTimeProvider = new SearchTimeProvider(0, 0, () -> 0);
        try (
            SearchResponseMerger merger = new SearchResponseMerger(
                0,
                0,
                SearchContext.TRACK_TOTAL_HITS_ACCURATE,
                searchTimeProvider,
                emptyReduceContextBuilder()
            )
        ) {
            Map<String, SearchProfileShardResult> expectedProfile = new HashMap<>();
            for (int i = 0; i < numResponses; i++) {
                SearchProfileResults profile = SearchProfileResultsTests.createTestItem();
                expectedProfile.putAll(profile.getShardResults());
                SearchResponse searchResponse = new SearchResponse(
                    SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
                    null,
                    null,
                    false,
                    null,
                    profile,
                    1,
                    null,
                    1,
                    1,
                    0,
                    100L,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    addResponse(merger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, merger.numResponses());
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse mergedResponse = merger.getMergedResponse(clusters);
            try {
                assertSame(clusters, mergedResponse.getClusters());
                assertEquals(numResponses, mergedResponse.getTotalShards());
                assertEquals(numResponses, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertEquals(0, mergedResponse.getFailedShards());
                assertEquals(0, mergedResponse.getShardFailures().length);
                assertEquals(expectedProfile, mergedResponse.getProfileResults());
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeCompletionSuggestions() throws InterruptedException {
        String suggestionName = randomAlphaOfLengthBetween(4, 8);
        int size = randomIntBetween(1, 100);
        try (
            SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                0,
                0,
                0,
                new SearchTimeProvider(0, 0, () -> 0),
                emptyReduceContextBuilder()
            )
        ) {
            for (int i = 0; i < numResponses; i++) {
                List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions =
                    new ArrayList<>();
                CompletionSuggestion completionSuggestion = new CompletionSuggestion(suggestionName, size, false);
                CompletionSuggestion.Entry options = new CompletionSuggestion.Entry(new Text("suggest"), 0, 10);
                int docId = randomIntBetween(0, Integer.MAX_VALUE);
                CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(
                    docId,
                    new Text(randomAlphaOfLengthBetween(5, 10)),
                    i,
                    Collections.emptyMap()
                );
                SearchHit hit = SearchHit.unpooled(docId);
                ShardId shardId = new ShardId(
                    randomAlphaOfLengthBetween(5, 10),
                    randomAlphaOfLength(10),
                    randomIntBetween(0, Integer.MAX_VALUE)
                );
                String clusterAlias = randomBoolean() ? "" : randomAlphaOfLengthBetween(5, 10);
                hit.shard(new SearchShardTarget("node", shardId, clusterAlias));
                option.setHit(hit);
                options.addOption(option);
                completionSuggestion.addTerm(options);
                suggestions.add(completionSuggestion);
                Suggest suggest = new Suggest(suggestions);
                SearchHits searchHits = SearchHits.empty(null, Float.NaN);
                SearchResponse searchResponse = new SearchResponse(
                    searchHits,
                    null,
                    suggest,
                    false,
                    null,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    randomLong(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    addResponse(searchResponseMerger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, searchResponseMerger.numResponses());
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(clusters);
            try {
                assertSame(clusters, mergedResponse.getClusters());
                assertEquals(numResponses, mergedResponse.getTotalShards());
                assertEquals(numResponses, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertEquals(0, mergedResponse.getFailedShards());
                assertEquals(0, mergedResponse.getShardFailures().length);
                Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> suggestion =
                    mergedResponse.getSuggest().getSuggestion(suggestionName);
                assertEquals(1, suggestion.getEntries().size());
                Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> options = suggestion.getEntries().get(0);
                assertEquals(Math.min(numResponses, size), options.getOptions().size());
                int i = numResponses;
                for (Suggest.Suggestion.Entry.Option option : options) {
                    assertEquals(--i, option.getScore(), 0f);
                }
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeCompletionSuggestionsTieBreak() throws InterruptedException {
        String suggestionName = randomAlphaOfLengthBetween(4, 8);
        int size = randomIntBetween(1, 100);
        try (
            SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                0,
                0,
                0,
                new SearchTimeProvider(0, 0, () -> 0),
                emptyReduceContextBuilder()
            )
        ) {
            for (int i = 0; i < numResponses; i++) {
                List<Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>>> suggestions =
                    new ArrayList<>();
                CompletionSuggestion completionSuggestion = new CompletionSuggestion(suggestionName, size, false);
                CompletionSuggestion.Entry options = new CompletionSuggestion.Entry(new Text("suggest"), 0, 10);
                int docId = randomIntBetween(0, Integer.MAX_VALUE);
                CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(
                    docId,
                    new Text("suggestion"),
                    1F,
                    Collections.emptyMap()
                );
                SearchHit searchHit = SearchHit.unpooled(docId);
                searchHit.shard(
                    new SearchShardTarget(
                        "node",
                        new ShardId("index", "uuid", randomIntBetween(0, Integer.MAX_VALUE)),
                        randomBoolean() ? RemoteClusterService.LOCAL_CLUSTER_GROUP_KEY : randomAlphaOfLengthBetween(5, 10)
                    )
                );
                option.setHit(searchHit);
                options.addOption(option);
                completionSuggestion.addTerm(options);
                suggestions.add(completionSuggestion);
                Suggest suggest = new Suggest(suggestions);
                SearchResponse searchResponse = new SearchResponse(
                    SearchHits.empty(null, Float.NaN),
                    null,
                    suggest,
                    false,
                    null,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    randomLong(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    addResponse(searchResponseMerger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, searchResponseMerger.numResponses());
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(clusters);
            try {
                assertSame(clusters, mergedResponse.getClusters());
                assertEquals(numResponses, mergedResponse.getTotalShards());
                assertEquals(numResponses, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertEquals(0, mergedResponse.getFailedShards());
                assertEquals(0, mergedResponse.getShardFailures().length);
                CompletionSuggestion suggestion = mergedResponse.getSuggest().getSuggestion(suggestionName);
                assertEquals(1, suggestion.getEntries().size());
                CompletionSuggestion.Entry options = suggestion.getEntries().get(0);
                assertEquals(Math.min(numResponses, size), options.getOptions().size());
                int lastShardId = 0;
                String lastClusterAlias = null;
                for (CompletionSuggestion.Entry.Option option : options) {
                    assertEquals("suggestion", option.getText().string());
                    SearchShardTarget shard = option.getHit().getShard();
                    int currentShardId = shard.getShardId().id();
                    assertThat(currentShardId, greaterThanOrEqualTo(lastShardId));
                    if (currentShardId == lastShardId) {
                        assertThat(shard.getClusterAlias(), greaterThan(lastClusterAlias));
                    } else {
                        lastShardId = currentShardId;
                    }
                    lastClusterAlias = shard.getClusterAlias();
                }
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    /** Test merging results where one result has a raw format, for instance if
     * searching over multiple indexes where the field isn't mapped in all indexes.
     */
    public void testMergeEmptyFormat() throws InterruptedException {
        DateFormatter dateFormatter = DateFieldMapper.DEFAULT_DATE_TIME_FORMATTER;
        Max max1 = Max.createEmptyMax("field1", DocValueFormat.RAW, Collections.emptyMap());
        Max max2 = new Max(
            "field1",
            dateFormatter.parseMillis("2021-05-01T00:00:00.000Z"),
            new DocValueFormat.DateTime(dateFormatter, ZoneId.of("UTC"), DateFieldMapper.Resolution.MILLISECONDS),
            Collections.emptyMap()
        );

        try (
            SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                0,
                0,
                0,
                new SearchTimeProvider(0, 0, () -> 0),
                emptyReduceContextBuilder(new AggregatorFactories.Builder().addAggregator(new MaxAggregationBuilder("field1")))
            )
        ) {
            for (Max max : Arrays.asList(max1, max2)) {
                InternalAggregations aggs = InternalAggregations.from(Arrays.asList(max));
                SearchResponse searchResponse = new SearchResponse(
                    SearchHits.empty(null, Float.NaN),
                    aggs,
                    null,
                    false,
                    null,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    randomLong(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    searchResponseMerger.add(searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse searchResponse = searchResponseMerger.getMergedResponse(clusters);
            try {
                Max mergedMax = searchResponse.getAggregations().get("field1");
                assertEquals(mergedMax.getValueAsString(), "2021-05-01T00:00:00.000Z");
            } finally {
                searchResponse.decRef();
            }
        }
    }

    public void testMergeAggs() throws InterruptedException {
        String maxAggName = randomAlphaOfLengthBetween(5, 8);
        String rangeAggName = randomAlphaOfLengthBetween(5, 8);
        try (
            SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                0,
                0,
                0,
                new SearchTimeProvider(0, 0, () -> 0),
                emptyReduceContextBuilder(
                    new AggregatorFactories.Builder().addAggregator(new MaxAggregationBuilder(maxAggName))
                        .addAggregator(new DateRangeAggregationBuilder(rangeAggName))
                )
            )
        ) {
            int totalCount = 0;
            double maxValue = Double.MIN_VALUE;
            for (int i = 0; i < numResponses; i++) {
                double value = randomDouble();
                maxValue = Math.max(value, maxValue);
                Max max = new Max(maxAggName, value, DocValueFormat.RAW, Collections.emptyMap());
                InternalDateRange.Factory factory = new InternalDateRange.Factory();
                int count = randomIntBetween(1, 1000);
                totalCount += count;
                InternalDateRange.Bucket bucket = factory.createBucket(
                    "bucket",
                    0D,
                    10000D,
                    count,
                    InternalAggregations.EMPTY,
                    DocValueFormat.RAW
                );
                InternalDateRange range = factory.create(rangeAggName, singletonList(bucket), DocValueFormat.RAW, false, emptyMap());
                InternalAggregations aggs = InternalAggregations.from(Arrays.asList(range, max));
                SearchResponse searchResponse = new SearchResponse(
                    SearchHits.empty(null, Float.NaN),
                    aggs,
                    null,
                    false,
                    null,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    randomLong(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );

                try {
                    addResponse(searchResponseMerger, searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            awaitResponsesAdded();
            assertEquals(numResponses, searchResponseMerger.numResponses());
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(clusters);
            try {
                assertSame(clusters, mergedResponse.getClusters());
                assertEquals(numResponses, mergedResponse.getTotalShards());
                assertEquals(numResponses, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertEquals(0, mergedResponse.getFailedShards());
                assertEquals(0, mergedResponse.getShardFailures().length);
                assertEquals(0, mergedResponse.getHits().getHits().length);
                assertEquals(2, mergedResponse.getAggregations().asList().size());
                Max max = mergedResponse.getAggregations().get(maxAggName);
                assertEquals(maxValue, max.value(), 0d);
                Range range = mergedResponse.getAggregations().get(rangeAggName);
                assertEquals(1, range.getBuckets().size());
                Range.Bucket bucket = range.getBuckets().get(0);
                assertEquals("0.0", bucket.getFromAsString());
                assertEquals("10000.0", bucket.getToAsString());
                assertEquals(totalCount, bucket.getDocCount());
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeSearchHits() throws InterruptedException {
        final long currentRelativeTime = randomNonNegativeLong();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(randomLong(), 0, () -> currentRelativeTime);
        final int size = randomIntBetween(0, 100);
        final int from = size > 0 ? randomIntBetween(0, 100) : 0;
        final int requestedSize = from + size;
        final SortField[] sortFields;
        final String collapseField;
        boolean scoreSort = false;
        if (randomBoolean()) {
            int numFields = randomIntBetween(1, 3);
            sortFields = new SortField[numFields];
            for (int i = 0; i < numFields; i++) {
                final SortField sortField;
                if (randomBoolean()) {
                    sortField = new SortField("field-" + i, SortField.Type.INT, randomBoolean());
                } else {
                    scoreSort = true;
                    sortField = SortField.FIELD_SCORE;
                }
                sortFields[i] = sortField;
            }
            collapseField = randomBoolean() ? "collapse" : null;
        } else {
            collapseField = null;
            sortFields = null;
            scoreSort = true;
        }
        Tuple<Integer, TotalHits.Relation> randomTrackTotalHits = randomTrackTotalHits();
        int trackTotalHitsUpTo = randomTrackTotalHits.v1();
        TotalHits.Relation totalHitsRelation = randomTrackTotalHits.v2();

        PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(new SearchHitComparator(sortFields));
        try (
            SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                from,
                size,
                trackTotalHitsUpTo,
                timeProvider,
                emptyReduceContextBuilder()
            )
        ) {

            TotalHits expectedTotalHits = null;
            int expectedTotal = 0;
            int expectedSuccessful = 0;
            int expectedSkipped = 0;
            int expectedReducePhases = 1;
            boolean expectedTimedOut = false;
            Boolean expectedTerminatedEarly = null;
            float expectedMaxScore = Float.NEGATIVE_INFINITY;
            int numIndices = requestedSize == 0 ? 0 : randomIntBetween(1, requestedSize);
            Iterator<Map.Entry<String, Index[]>> indicesIterator = randomRealisticIndices(numIndices, numResponses).entrySet().iterator();
            boolean hasHits = false;
            for (int i = 0; i < numResponses; i++) {
                Map.Entry<String, Index[]> entry = indicesIterator.next();
                String clusterAlias = entry.getKey();
                Index[] indices = entry.getValue();
                int total = randomIntBetween(1, 1000);
                expectedTotal += total;
                int successful = randomIntBetween(1, total);
                expectedSuccessful += successful;
                int skipped = randomIntBetween(1, total);
                expectedSkipped += skipped;

                TotalHits totalHits = null;
                if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                    totalHits = new TotalHits(randomLongBetween(0, 1000), totalHitsRelation);
                    long previousValue = expectedTotalHits == null ? 0 : expectedTotalHits.value();
                    expectedTotalHits = new TotalHits(Math.min(previousValue + totalHits.value(), trackTotalHitsUpTo), totalHitsRelation);
                }

                final int numDocs = totalHits == null || totalHits.value() >= requestedSize ? requestedSize : (int) totalHits.value();
                int scoreFactor = randomIntBetween(1, numResponses);
                float maxScore = scoreSort ? numDocs * scoreFactor : Float.NaN;
                SearchHit[] hits = randomSearchHitArray(
                    numDocs,
                    numResponses,
                    clusterAlias,
                    indices,
                    maxScore,
                    scoreFactor,
                    sortFields,
                    priorityQueue
                );
                hasHits |= hits.length > 0;
                expectedMaxScore = Math.max(expectedMaxScore, maxScore);

                Object[] collapseValues = null;
                if (collapseField != null) {
                    collapseValues = new Object[numDocs];
                    for (int j = 0; j < numDocs; j++) {
                        // set different collapse values for each cluster for simplicity
                        collapseValues[j] = j + 1000 * i;
                    }
                }

                SearchHits searchHits = new SearchHits(
                    hits,
                    totalHits,
                    maxScore == Float.NEGATIVE_INFINITY ? Float.NaN : maxScore,
                    sortFields,
                    collapseField,
                    collapseValues
                );

                int numReducePhases = randomIntBetween(1, 5);
                expectedReducePhases += numReducePhases;
                boolean timedOut = rarely();
                expectedTimedOut = expectedTimedOut || timedOut;
                Boolean terminatedEarly = frequently() ? null : true;
                expectedTerminatedEarly = expectedTerminatedEarly == null ? terminatedEarly : expectedTerminatedEarly;

                SearchResponse searchResponse = new SearchResponse(
                    searchHits,
                    null,
                    null,
                    timedOut,
                    terminatedEarly,
                    null,
                    numReducePhases,
                    null,
                    total,
                    successful,
                    skipped,
                    randomLong(),
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponseTests.randomClusters()
                );

                try {
                    addResponse(searchResponseMerger, searchResponse);
                } finally {
                    searchHits.decRef();
                    searchResponse.decRef();
                }
            }

            awaitResponsesAdded();
            assertEquals(numResponses, searchResponseMerger.numResponses());
            final SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            SearchResponse searchResponse = searchResponseMerger.getMergedResponse(clusters);
            try {
                assertEquals(TimeUnit.NANOSECONDS.toMillis(currentRelativeTime), searchResponse.getTook().millis());
                assertEquals(expectedTotal, searchResponse.getTotalShards());
                assertEquals(expectedSuccessful, searchResponse.getSuccessfulShards());
                assertEquals(expectedSkipped, searchResponse.getSkippedShards());
                assertEquals(0, searchResponse.getShardFailures().length);
                assertEquals(expectedReducePhases, searchResponse.getNumReducePhases());
                assertEquals(expectedTimedOut, searchResponse.isTimedOut());
                assertEquals(expectedTerminatedEarly, searchResponse.isTerminatedEarly());

                assertSame(clusters, searchResponse.getClusters());
                assertNull(searchResponse.getScrollId());

                SearchHits searchHits = searchResponse.getHits();
                // the sort fields and the collapse field are not returned when hits are empty
                if (hasHits) {
                    assertArrayEquals(sortFields, searchHits.getSortFields());
                    assertEquals(collapseField, searchHits.getCollapseField());
                } else {
                    assertNull(searchHits.getSortFields());
                    assertNull(searchHits.getCollapseField());
                }
                if (expectedTotalHits == null) {
                    assertNull(searchHits.getTotalHits());
                } else {
                    assertNotNull(searchHits.getTotalHits());
                    assertEquals(expectedTotalHits.value(), searchHits.getTotalHits().value());
                    assertSame(expectedTotalHits.relation(), searchHits.getTotalHits().relation());
                }
                if (expectedMaxScore == Float.NEGATIVE_INFINITY) {
                    assertTrue(Float.isNaN(searchHits.getMaxScore()));
                } else {
                    assertEquals(expectedMaxScore, searchHits.getMaxScore(), 0f);
                }

                for (int i = 0; i < from; i++) {
                    priorityQueue.poll();
                }
                SearchHit[] hits = searchHits.getHits();
                if (collapseField != null
                    // the collapse field is not returned when hits are empty
                    && hasHits) {
                    assertEquals(hits.length, searchHits.getCollapseValues().length);
                } else {
                    assertNull(searchHits.getCollapseValues());
                }
                assertThat(hits.length, lessThanOrEqualTo(size));
                for (SearchHit hit : hits) {
                    SearchHit expected = priorityQueue.poll();
                    assertSame(expected, hit);
                }
            } finally {
                searchResponse.decRef();
            }
        }
    }

    public void testMergeNoResponsesAdded() {
        long currentRelativeTime = randomNonNegativeLong();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(randomLong(), 0, () -> currentRelativeTime);
        try (SearchResponseMerger merger = new SearchResponseMerger(0, 10, Integer.MAX_VALUE, timeProvider, emptyReduceContextBuilder())) {
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            assertEquals(0, merger.numResponses());
            SearchResponse response = merger.getMergedResponse(clusters);
            try {
                assertSame(clusters, response.getClusters());
                assertEquals(TimeUnit.NANOSECONDS.toMillis(currentRelativeTime), response.getTook().millis());
                assertEquals(0, response.getTotalShards());
                assertEquals(0, response.getSuccessfulShards());
                assertEquals(0, response.getSkippedShards());
                assertEquals(0, response.getFailedShards());
                assertEquals(0, response.getNumReducePhases());
                assertFalse(response.isTimedOut());
                assertNotNull(response.getHits().getTotalHits());
                assertEquals(0, response.getHits().getTotalHits().value());
                assertEquals(0, response.getHits().getHits().length);
                assertEquals(TotalHits.Relation.EQUAL_TO, response.getHits().getTotalHits().relation());
                assertNull(response.getScrollId());
                assertSame(InternalAggregations.EMPTY, response.getAggregations());
                assertNull(response.getSuggest());
                assertEquals(0, response.getProfileResults().size());
                assertNull(response.isTerminatedEarly());
                assertEquals(0, response.getShardFailures().length);
            } finally {
                response.decRef();
            }
        }
    }

    public void testMergeEmptySearchHitsWithNonEmpty() {
        long currentRelativeTime = randomLong();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(randomLong(), 0, () -> currentRelativeTime);
        try (SearchResponseMerger merger = new SearchResponseMerger(0, 10, Integer.MAX_VALUE, timeProvider, emptyReduceContextBuilder())) {
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            int numFields = randomIntBetween(1, 3);
            SortField[] sortFields = new SortField[numFields];
            for (int i = 0; i < numFields; i++) {
                sortFields[i] = new SortField("field-" + i, SortField.Type.INT, randomBoolean());
            }
            PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(new SearchHitComparator(sortFields));
            SearchHit[] hits = randomSearchHitArray(
                10,
                1,
                "remote",
                new Index[] { new Index("index", "uuid") },
                Float.NaN,
                1,
                sortFields,
                priorityQueue
            );
            {
                SearchHits searchHits = new SearchHits(
                    hits,
                    new TotalHits(10, TotalHits.Relation.EQUAL_TO),
                    Float.NaN,
                    sortFields,
                    null,
                    null
                );
                SearchResponse searchResponse = new SearchResponse(
                    searchHits,
                    null,
                    null,
                    false,
                    false,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    1L,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    merger.add(searchResponse);
                } finally {
                    searchHits.decRef();
                    searchResponse.decRef();
                }
            }
            {
                SearchResponse searchResponse = new SearchResponse(
                    SearchHits.empty(new TotalHits(0, TotalHits.Relation.EQUAL_TO), Float.NaN),
                    null,
                    null,
                    false,
                    false,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    1L,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    merger.add(searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            assertEquals(2, merger.numResponses());
            SearchResponse mergedResponse = merger.getMergedResponse(clusters);
            try {
                assertEquals(10, mergedResponse.getHits().getTotalHits().value());
                assertEquals(10, mergedResponse.getHits().getHits().length);
                assertEquals(2, mergedResponse.getTotalShards());
                assertEquals(2, mergedResponse.getSuccessfulShards());
                assertEquals(0, mergedResponse.getSkippedShards());
                assertArrayEquals(sortFields, mergedResponse.getHits().getSortFields());
                assertArrayEquals(hits, mergedResponse.getHits().getHits());
                assertEquals(clusters, mergedResponse.getClusters());
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    public void testMergeOnlyEmptyHits() {
        long currentRelativeTime = randomLong();
        final SearchTimeProvider timeProvider = new SearchTimeProvider(randomLong(), 0, () -> currentRelativeTime);
        SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
        Tuple<Integer, TotalHits.Relation> randomTrackTotalHits = randomTrackTotalHits();
        int trackTotalHitsUpTo = randomTrackTotalHits.v1();
        TotalHits.Relation totalHitsRelation = randomTrackTotalHits.v2();
        try (SearchResponseMerger merger = new SearchResponseMerger(0, 10, trackTotalHitsUpTo, timeProvider, emptyReduceContextBuilder())) {
            int numResponses = randomIntBetween(1, 5);
            TotalHits expectedTotalHits = null;
            for (int i = 0; i < numResponses; i++) {
                TotalHits totalHits = null;
                if (trackTotalHitsUpTo != SearchContext.TRACK_TOTAL_HITS_DISABLED) {
                    totalHits = new TotalHits(randomLongBetween(0, 1000), totalHitsRelation);
                    long previousValue = expectedTotalHits == null ? 0 : expectedTotalHits.value();
                    expectedTotalHits = new TotalHits(Math.min(previousValue + totalHits.value(), trackTotalHitsUpTo), totalHitsRelation);
                }
                SearchResponse searchResponse = new SearchResponse(
                    SearchHits.empty(totalHits, Float.NaN),
                    null,
                    null,
                    false,
                    false,
                    null,
                    1,
                    null,
                    1,
                    1,
                    0,
                    1L,
                    ShardSearchFailure.EMPTY_ARRAY,
                    SearchResponse.Clusters.EMPTY
                );
                try {
                    merger.add(searchResponse);
                } finally {
                    searchResponse.decRef();
                }
            }
            SearchResponse mergedResponse = merger.getMergedResponse(clusters);
            try {
                assertEquals(expectedTotalHits, mergedResponse.getHits().getTotalHits());
            } finally {
                mergedResponse.decRef();
            }
        }
    }

    private static Tuple<Integer, TotalHits.Relation> randomTrackTotalHits() {
        return switch (randomIntBetween(0, 2)) {
            case 0 -> Tuple.tuple(SearchContext.TRACK_TOTAL_HITS_DISABLED, null);
            case 1 -> Tuple.tuple(randomIntBetween(10, 1000), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO);
            case 2 -> Tuple.tuple(SearchContext.TRACK_TOTAL_HITS_ACCURATE, TotalHits.Relation.EQUAL_TO);
            default -> throw new UnsupportedOperationException();
        };
    }

    private static SearchHit[] randomSearchHitArray(
        int numDocs,
        int numResponses,
        String clusterAlias,
        Index[] indices,
        float maxScore,
        int scoreFactor,
        SortField[] sortFields,
        PriorityQueue<SearchHit> priorityQueue
    ) {
        SearchHit[] hits = new SearchHit[numDocs];

        int[] sortFieldFactors = new int[sortFields == null ? 0 : sortFields.length];
        for (int j = 0; j < sortFieldFactors.length; j++) {
            sortFieldFactors[j] = randomIntBetween(1, numResponses);
        }

        for (int j = 0; j < numDocs; j++) {
            ShardId shardId = new ShardId(randomFrom(indices), randomIntBetween(0, 10));
            SearchShardTarget shardTarget = new SearchShardTarget(randomAlphaOfLengthBetween(3, 8), shardId, clusterAlias);
            SearchHit hit = new SearchHit(randomIntBetween(0, Integer.MAX_VALUE));

            float score = Float.NaN;
            if (Float.isNaN(maxScore) == false) {
                score = (maxScore - j) * scoreFactor;
                hit.score(score);
            }

            hit.shard(shardTarget);
            if (sortFields != null) {
                Object[] rawSortValues = new Object[sortFields.length];
                DocValueFormat[] docValueFormats = new DocValueFormat[sortFields.length];
                for (int k = 0; k < sortFields.length; k++) {
                    SortField sortField = sortFields[k];
                    if (sortField == SortField.FIELD_SCORE) {
                        hit.score(score);
                        rawSortValues[k] = score;
                    } else {
                        rawSortValues[k] = sortField.getReverse() ? numDocs * sortFieldFactors[k] - j : j;
                    }
                    docValueFormats[k] = DocValueFormat.RAW;
                }
                hit.sortValues(rawSortValues, docValueFormats);
            }
            hits[j] = hit;
            priorityQueue.add(hit);
        }
        return hits;
    }

    /**
     * Tests the partial results scenario used by MutableSearchResponse when
     * doing cross-cluster search with minimize_roundtrips=true
     */
    public void testPartialAggsMixedWithFullResponses() {
        String maxAggName = "max123";
        String rangeAggName = "range123";

        // partial aggs from local cluster (no search hits)
        double value = 33.33;
        int count = 33;
        SearchResponse searchResponsePartialAggs = new SearchResponse(
            SearchHits.empty(new TotalHits(0L, TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO), Float.NaN),
            createDeterminsticAggregation(maxAggName, rangeAggName, value, count),
            null,
            false,
            null,
            null,
            1,
            null,
            2,
            2,
            0,
            33,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // full response from remote1 remote cluster
        value = 44.44;
        count = 44;
        String clusterAlias = "remote1";
        int total = 3;
        int successful = 2;
        int skipped = 1;
        Index[] indices = new Index[] { new Index("foo_idx", "1bba9f5b-c5a1-4664-be1b-26be590c1aff") };
        final SearchResponse searchResponseRemote1 = new SearchResponse(
            createSimpleDeterministicSearchHits(clusterAlias, indices),
            createDeterminsticAggregation(maxAggName, rangeAggName, value, count),
            null,
            false,
            null,
            null,
            1,
            null,
            total,
            successful,
            skipped,
            44,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );

        // full response from remote2 remote cluster
        value = 55.55;
        count = 55;
        clusterAlias = "remote2";
        total = 3;
        successful = 2;
        skipped = 1;
        indices = new Index[] { new Index("foo_idx", "ae024679-097a-4a27-abf8-403f1e9189de") };
        SearchResponse searchResponseRemote2 = new SearchResponse(
            createSimpleDeterministicSearchHits(clusterAlias, indices),
            createDeterminsticAggregation(maxAggName, rangeAggName, value, count),
            null,
            false,
            null,
            null,
            1,
            null,
            total,
            successful,
            skipped,
            55,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        try {
            SearchResponse.Clusters clusters = SearchResponseTests.createCCSClusterObject(
                3,
                2,
                true,
                2,
                1,
                0,
                0,
                new ShardSearchFailure[0]
            );

            // merge partial aggs with remote1, check, then merge in remote2, check
            try (
                SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                    0,
                    10,
                    10,
                    new SearchTimeProvider(0, 0, () -> 0),
                    emptyReduceContextBuilder(
                        new AggregatorFactories.Builder().addAggregator(new MaxAggregationBuilder(maxAggName))
                            .addAggregator(new DateRangeAggregationBuilder(rangeAggName))
                    )
                )
            ) {
                searchResponseMerger.add(searchResponsePartialAggs);
                searchResponseMerger.add(searchResponseRemote1);
                SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(clusters);
                try {
                    SearchHits hits = mergedResponse.getHits();
                    assertThat(hits.getTotalHits().value(), equalTo(2L)); // should be 2 hits from remote1
                    SearchHit hit1 = hits.getHits()[0];
                    String expectedHit1 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit1.toString(), expectedHit1);

                    SearchHit hit2 = hits.getHits()[1];
                    String expectedHit2 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit2.toString(), expectedHit2);

                    double expectedMaxValue = 44.44;  // value from remote1
                    long expectedBucketsDocCount = 33 + 44;
                    Max max = mergedResponse.getAggregations().get(maxAggName);
                    assertEquals(expectedMaxValue, max.value(), 0d);
                    Range range = mergedResponse.getAggregations().get(rangeAggName);
                    assertEquals(1, range.getBuckets().size());
                    Range.Bucket bucket = range.getBuckets().get(0);
                    assertEquals("0.0", bucket.getFromAsString());
                    assertEquals("10000.0", bucket.getToAsString());
                    assertEquals(expectedBucketsDocCount, bucket.getDocCount());
                } finally {
                    mergedResponse.decRef();
                }

                searchResponseMerger.add(searchResponseRemote2);
                mergedResponse = searchResponseMerger.getMergedResponse(clusters);
                try {
                    SearchHits hits = mergedResponse.getHits();
                    assertThat(hits.getTotalHits().value(), equalTo(4L)); // should be 2 hits from remote1, 2 from remote2

                    SearchHit hit1 = hits.getHits()[0];
                    String expectedHit1 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit1.toString(), expectedHit1);

                    SearchHit hit2 = hits.getHits()[1];
                    String expectedHit2 = """
                        {
                          "_index" : "remote2:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit2.toString(), expectedHit2);

                    SearchHit hit3 = hits.getHits()[2];
                    String expectedHit3 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit3.toString(), expectedHit3);

                    SearchHit hit4 = hits.getHits()[3];
                    String expectedHit4 = """
                        {
                          "_index" : "remote2:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit4.toString(), expectedHit4);

                    double expectedMaxValue = 55.55;  // value from remote2
                    long expectedBucketsDocCount = 33 + 44 + 55;
                    Max max = mergedResponse.getAggregations().get(maxAggName);
                    assertEquals(expectedMaxValue, max.value(), 0d);
                    Range range = mergedResponse.getAggregations().get(rangeAggName);
                    assertEquals(1, range.getBuckets().size());
                    Range.Bucket bucket = range.getBuckets().get(0);
                    assertEquals("0.0", bucket.getFromAsString());
                    assertEquals("10000.0", bucket.getToAsString());
                    assertEquals(expectedBucketsDocCount, bucket.getDocCount());
                } finally {
                    mergedResponse.decRef();
                }
            }

            // merge remote1 and remote2, no partial aggs, check, then merge in partial aggs from local, check
            try (
                SearchResponseMerger searchResponseMerger = new SearchResponseMerger(
                    0,
                    10,
                    10,
                    new SearchTimeProvider(0, 0, () -> 0),
                    emptyReduceContextBuilder(
                        new AggregatorFactories.Builder().addAggregator(new MaxAggregationBuilder(maxAggName))
                            .addAggregator(new DateRangeAggregationBuilder(rangeAggName))
                    )
                )
            ) {
                searchResponseMerger.add(searchResponseRemote2);
                searchResponseMerger.add(searchResponseRemote1);
                SearchResponse mergedResponse = searchResponseMerger.getMergedResponse(clusters);
                try {
                    SearchHits hits = mergedResponse.getHits();
                    SearchHit hit1 = hits.getHits()[0];
                    String expectedHit1 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit1.toString(), expectedHit1);

                    SearchHit hit2 = hits.getHits()[1];
                    String expectedHit2 = """
                        {
                          "_index" : "remote2:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit2.toString(), expectedHit2);

                    SearchHit hit3 = hits.getHits()[2];
                    String expectedHit3 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit3.toString(), expectedHit3);

                    SearchHit hit4 = hits.getHits()[3];
                    String expectedHit4 = """
                        {
                          "_index" : "remote2:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit4.toString(), expectedHit4);

                    double expectedMaxValue = 55.55;  // value from remote2
                    long expectedBucketsDocCount = 44 + 55; // missing 33 from local partial aggs
                    Max max = mergedResponse.getAggregations().get(maxAggName);
                    assertEquals(expectedMaxValue, max.value(), 0d);
                    Range range = mergedResponse.getAggregations().get(rangeAggName);
                    assertEquals(1, range.getBuckets().size());
                    Range.Bucket bucket = range.getBuckets().get(0);
                    assertEquals("0.0", bucket.getFromAsString());
                    assertEquals("10000.0", bucket.getToAsString());
                    assertEquals(expectedBucketsDocCount, bucket.getDocCount());
                } finally {
                    mergedResponse.decRef();
                }

                searchResponseMerger.add(searchResponsePartialAggs);
                mergedResponse = searchResponseMerger.getMergedResponse(clusters);
                try {
                    SearchHits hits = mergedResponse.getHits();
                    assertThat(hits.getTotalHits().value(), equalTo(4L)); // should be 2 hits from remote1, 2 from remote2

                    SearchHit hit1 = hits.getHits()[0];
                    String expectedHit1 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit1.toString(), expectedHit1);

                    SearchHit hit2 = hits.getHits()[1];
                    String expectedHit2 = """
                        {
                          "_index" : "remote2:foo_idx",
                          "_score" : 2.0,
                          "sort" : [
                            2.0
                          ]
                        }""";
                    assertEquals(hit2.toString(), expectedHit2);

                    SearchHit hit3 = hits.getHits()[2];
                    String expectedHit3 = """
                        {
                          "_index" : "remote1:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit3.toString(), expectedHit3);

                    SearchHit hit4 = hits.getHits()[3];
                    String expectedHit4 = """
                        {
                          "_index" : "remote2:foo_idx",
                          "_score" : 1.0,
                          "sort" : [
                            1.0
                          ]
                        }""";
                    assertEquals(hit4.toString(), expectedHit4);

                    double expectedMaxValue = 55.55;  // value from remote2
                    long expectedBucketsDocCount = 33 + 44 + 55;  // contributions from all 3 search responses
                    Max max = mergedResponse.getAggregations().get(maxAggName);
                    assertEquals(expectedMaxValue, max.value(), 0d);
                    Range range = mergedResponse.getAggregations().get(rangeAggName);
                    assertEquals(1, range.getBuckets().size());
                    Range.Bucket bucket = range.getBuckets().get(0);
                    assertEquals("0.0", bucket.getFromAsString());
                    assertEquals("10000.0", bucket.getToAsString());
                    assertEquals(expectedBucketsDocCount, bucket.getDocCount());
                } finally {
                    mergedResponse.decRef();
                }
            }
        } finally {
            searchResponseRemote1.decRef();
            searchResponseRemote2.decRef();
            searchResponsePartialAggs.decRef();
        }
    }

    private SearchHits createSimpleDeterministicSearchHits(String clusterAlias, Index[] indices) {
        TotalHits totalHits = new TotalHits(2, TotalHits.Relation.EQUAL_TO);
        final int numDocs = (int) totalHits.value();
        int scoreFactor = 1;
        float maxScore = numDocs;
        int numFields = 1;
        SortField[] sortFields = new SortField[numFields];
        sortFields[0] = SortField.FIELD_SCORE;
        PriorityQueue<SearchHit> priorityQueue = new PriorityQueue<>(new SearchHitComparator(sortFields));
        SearchHit[] hits = deterministicSearchHitArray(numDocs, clusterAlias, indices, maxScore, scoreFactor, sortFields, priorityQueue);

        return SearchHits.unpooled(hits, totalHits, maxScore == Float.NEGATIVE_INFINITY ? Float.NaN : maxScore, sortFields, null, null);
    }

    private static InternalAggregations createDeterminsticAggregation(String maxAggName, String rangeAggName, double value, int count) {
        Max max = new Max(maxAggName, value, DocValueFormat.RAW, Collections.emptyMap());
        InternalDateRange.Factory factory = new InternalDateRange.Factory();
        InternalDateRange.Bucket bucket = factory.createBucket("bucket", 0D, 10000D, count, InternalAggregations.EMPTY, DocValueFormat.RAW);

        InternalDateRange range = factory.create(rangeAggName, singletonList(bucket), DocValueFormat.RAW, false, emptyMap());
        InternalAggregations aggs = InternalAggregations.from(Arrays.asList(range, max));
        return aggs;
    }

    private static SearchHit[] deterministicSearchHitArray(
        int numDocs,
        String clusterAlias,
        Index[] indices,
        float maxScore,
        int scoreFactor,
        SortField[] sortFields,
        PriorityQueue<SearchHit> priorityQueue
    ) {
        SearchHit[] hits = new SearchHit[numDocs];

        int[] sortFieldFactors = new int[sortFields == null ? 0 : sortFields.length];
        for (int j = 0; j < sortFieldFactors.length; j++) {
            sortFieldFactors[j] = 1;
        }

        for (int j = 0; j < numDocs; j++) {
            ShardId shardId = new ShardId(randomFrom(indices), j);
            SearchShardTarget shardTarget = new SearchShardTarget("abc123", shardId, clusterAlias);
            SearchHit hit = SearchHit.unpooled(j);

            float score = Float.NaN;
            if (Float.isNaN(maxScore) == false) {
                score = (maxScore - j) * scoreFactor;
                hit.score(score);
            }

            hit.shard(shardTarget);
            if (sortFields != null) {
                Object[] rawSortValues = new Object[sortFields.length];
                DocValueFormat[] docValueFormats = new DocValueFormat[sortFields.length];
                for (int k = 0; k < sortFields.length; k++) {
                    SortField sortField = sortFields[k];
                    if (sortField == SortField.FIELD_SCORE) {
                        hit.score(score);
                        rawSortValues[k] = score;
                    } else {
                        rawSortValues[k] = sortField.getReverse() ? numDocs * sortFieldFactors[k] - j : j;
                    }
                    docValueFormats[k] = DocValueFormat.RAW;
                }
                hit.sortValues(rawSortValues, docValueFormats);
            }
            hits[j] = hit;
            priorityQueue.add(hit);
        }
        return hits;
    }

    private static Map<String, Index[]> randomRealisticIndices(int numIndices, int numClusters) {
        String[] indicesNames = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indicesNames[i] = randomAlphaOfLengthBetween(5, 10);
        }
        Map<String, Index[]> indicesPerCluster = new TreeMap<>();
        for (int i = 0; i < numClusters; i++) {
            Index[] indices = new Index[indicesNames.length];
            for (int j = 0; j < indices.length; j++) {
                String indexName = indicesNames[j];
                // Realistically clusters have the same indices with same names, but different uuid. Yet it can happen that the same cluster
                // is registered twice with different aliases and searched multiple times as part of the same search request.
                String indexUuid = frequently() ? randomAlphaOfLength(10) : indexName;
                indices[j] = new Index(indexName, indexUuid);
            }
            String clusterAlias;
            if (frequently() || indicesPerCluster.containsKey(RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY)) {
                clusterAlias = randomAlphaOfLengthBetween(5, 10);
            } else {
                clusterAlias = RemoteClusterAware.LOCAL_CLUSTER_GROUP_KEY;
            }
            indicesPerCluster.put(clusterAlias, indices);
        }
        return indicesPerCluster;
    }

    private static final class SearchHitComparator implements Comparator<SearchHit> {

        private final SortField[] sortFields;

        SearchHitComparator(SortField[] sortFields) {
            this.sortFields = sortFields;
        }

        @Override
        public int compare(SearchHit a, SearchHit b) {
            if (sortFields == null) {
                int scoreCompare = Float.compare(b.getScore(), a.getScore());
                if (scoreCompare != 0) {
                    return scoreCompare;
                }
            } else {
                for (int i = 0; i < sortFields.length; i++) {
                    SortField sortField = sortFields[i];
                    if (sortField == SortField.FIELD_SCORE) {
                        int scoreCompare = Float.compare(b.getScore(), a.getScore());
                        if (scoreCompare != 0) {
                            return scoreCompare;
                        }
                    } else {
                        Integer aSortValue = (Integer) a.getRawSortValues()[i];
                        Integer bSortValue = (Integer) b.getRawSortValues()[i];
                        final int compare;
                        if (sortField.getReverse()) {
                            compare = Integer.compare(bSortValue, aSortValue);
                        } else {
                            compare = Integer.compare(aSortValue, bSortValue);
                        }
                        if (compare != 0) {
                            return compare;
                        }
                    }
                }
            }
            SearchShardTarget aShard = a.getShard();
            SearchShardTarget bShard = b.getShard();
            int shardIdCompareTo = aShard.getShardId().compareTo(bShard.getShardId());
            if (shardIdCompareTo != 0) {
                return shardIdCompareTo;
            }
            int clusterAliasCompareTo = aShard.getClusterAlias().compareTo(bShard.getClusterAlias());
            if (clusterAliasCompareTo != 0) {
                return clusterAliasCompareTo;
            }
            return Integer.compare(a.docId(), b.docId());
        }
    }
}
