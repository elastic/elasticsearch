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

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.util.TimeUnits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.internal.io.IOUtils;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.CardinalityAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.SumAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.TopHitsAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.MaxBucketPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.collapse.CollapseBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.rescore.QueryRescoreMode;
import org.elasticsearch.search.rescore.QueryRescorerBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.completion.CompletionSuggestionBuilder;
import org.elasticsearch.search.suggest.phrase.DirectCandidateGeneratorBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestion;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;

/**
 * This test class executes twice, first against the remote cluster, and then against another cluster that has the remote cluster
 * registered. Given that each test gets executed against both clusters, {@link #assumeMultiClusterSetup()} needs to be used to run a test
 * against the multi cluster setup only, which is required for testing cross-cluster search.
 * The goal of this test is not to test correctness of CCS responses, but rather to verify that CCS returns the same responses when
 * <code>minimizeRoundTrips</code> is set to either <code>true</code> or <code>false</code>. In fact the execution differs depending on
 * such parameter, hence we want to verify that results are the same in both scenarios.
 */
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
public class CCSDuelIT extends ESRestTestCase {

    private static final String INDEX_NAME = "ccs_duel_index";
    private static final String REMOTE_INDEX_NAME = "my_remote_cluster:" + INDEX_NAME;
    private static final String[] TAGS = new String[] {"java", "xml", "sql", "html", "php", "ruby", "python", "perl"};

    private static RestHighLevelClient restHighLevelClient;

    @Before
    public void init() throws Exception {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
            String destinationCluster = System.getProperty("tests.rest.suite");
            //we index docs with private randomness otherwise the two clusters end up with exactly the same documents
            //given that this test class is run twice with same seed.
            RandomizedContext.current().runWithPrivateRandomness(random().nextLong() + destinationCluster.hashCode(),
                (Callable<Void>) () -> {
                    indexDocuments(destinationCluster + "-");
                    return null;
                });
        }
    }

    private static class HighLevelClient extends RestHighLevelClient {
        private HighLevelClient(RestClient restClient) {
            super(restClient, (client) -> {}, Collections.emptyList());
        }
    }

    @AfterClass
    public static void cleanupClient() throws IOException {
        IOUtils.close(restHighLevelClient);
        restHighLevelClient = null;
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    private static void indexDocuments(String idPrefix) throws IOException, InterruptedException {
        //this index with a single document is used to test partial failures
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME + "_err");
        indexRequest.id("id");
        indexRequest.source("id", "id", "creationDate", "err");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        assertEquals(201, indexResponse.status().getStatus());

        CreateIndexRequest createEmptyIndexRequest = new CreateIndexRequest(INDEX_NAME + "_empty");
        CreateIndexResponse response = restHighLevelClient.indices().create(createEmptyIndexRequest, RequestOptions.DEFAULT);
        assertTrue(response.isAcknowledged());

        int numShards = randomIntBetween(1, 5);
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(INDEX_NAME);
        createIndexRequest.settings(Settings.builder().put("index.number_of_shards", numShards).put("index.number_of_replicas", 0));
        createIndexRequest.mapping("{\"properties\":{" +
                "\"id\":{\"type\":\"keyword\"}," +
                "\"suggest\":{\"type\":\"completion\"}," +
                "\"join\":{\"type\":\"join\", \"relations\": {\"question\":\"answer\"}}}}", XContentType.JSON);
        CreateIndexResponse createIndexResponse = restHighLevelClient.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        assertTrue(createIndexResponse.isAcknowledged());

        BulkProcessor bulkProcessor = BulkProcessor.builder((r, l) -> restHighLevelClient.bulkAsync(r, RequestOptions.DEFAULT, l),
            new BulkProcessor.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    assertFalse(response.hasFailures());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
                    throw new AssertionError("Failed to execute bulk", failure);
                }
            }).build();

        int numQuestions = randomIntBetween(50, 100);
        for (int i = 0; i < numQuestions; i++) {
            bulkProcessor.add(buildIndexRequest(idPrefix + i, "question", null));
        }
        int numAnswers = randomIntBetween(100, 150);
        for (int i = 0; i < numAnswers; i++) {
            bulkProcessor.add(buildIndexRequest(idPrefix + (i + 1000), "answer", idPrefix + randomIntBetween(0, numQuestions - 1)));
        }
        assertTrue(bulkProcessor.awaitClose(30, TimeUnit.SECONDS));

        RefreshResponse refreshResponse = restHighLevelClient.indices().refresh(new RefreshRequest(INDEX_NAME), RequestOptions.DEFAULT);
        assertEquals(0, refreshResponse.getFailedShards());
        assertEquals(numShards, refreshResponse.getSuccessfulShards());
    }

    private static IndexRequest buildIndexRequest(String id, String type, String questionId) {
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME);
        indexRequest.id(id);
        if (questionId != null) {
            indexRequest.routing(questionId);
        }
        indexRequest.create(true);
        int numTags = randomIntBetween(1, 3);
        Set<String> tags = new HashSet<>();
        if (questionId == null) {
            for (int i = 0; i < numTags; i++) {
                tags.add(randomFrom(TAGS));
            }
        }
        String[] tagsArray = tags.toArray(new String[0]);
        String date = LocalDate.of(2019, 1, randomIntBetween(1, 31)).format(DateTimeFormatter.ofPattern("yyyy/MM/dd", Locale.ROOT));
        Map<String, String> joinField = new HashMap<>();
        joinField.put("name", type);
        if (questionId != null) {
            joinField.put("parent", questionId);
        }
        indexRequest.source(XContentType.JSON,
            "id", id,
            "type", type,
            "votes", randomIntBetween(0, 30),
            "questionId", questionId,
            "tags", tagsArray,
            "user", "user" + randomIntBetween(1, 10),
            "suggest", Collections.singletonMap("input", tagsArray),
            "creationDate", date,
            "join", joinField);
        return indexRequest;
    }

    public void testMatchAll() throws Exception {
        assumeMultiClusterSetup();
        //verify that the order in which documents are returned when they all have the same score is the same
        SearchRequest searchRequest = initSearchRequest();
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testMatchQuery() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "php"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testTrackTotalHitsUpTo() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.trackTotalHitsUpTo(5);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "sql"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testTerminateAfter() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.terminateAfter(10);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "perl"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testPagination() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(10);
        sourceBuilder.size(20);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "python"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> assertHits(response, 10));
    }

    public void testHighlighting() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.highlighter(new HighlightBuilder().field("tags"));
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "xml"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertFalse(response.getHits().getHits()[0].getHighlightFields().isEmpty());
        });
    }

    public void testFetchSource() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(new String[]{"tags"}, Strings.EMPTY_ARRAY);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertEquals(1, response.getHits().getHits()[0].getSourceAsMap().size());
        });
    }

    public void testDocValueFields() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.docValueField("user.keyword");
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "xml"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertEquals(1, response.getHits().getHits()[0].getFields().size());
            assertNotNull(response.getHits().getHits()[0].getFields().get("user.keyword"));
        });
    }

    public void testScriptFields() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.scriptField("parent", new Script(ScriptType.INLINE, "painless", "doc['join#question']", Collections.emptyMap()));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertEquals(1, response.getHits().getHits()[0].getFields().size());
            assertNotNull(response.getHits().getHits()[0].getFields().get("parent"));
        });
    }

    public void testExplain() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.explain(true);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "sql"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertNotNull(response.getHits().getHits()[0].getExplanation());
        });
    }

    public void testRescore() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "xml"));
        QueryRescorerBuilder rescorerBuilder = new QueryRescorerBuilder(new MatchQueryBuilder("tags", "java"));
        rescorerBuilder.setScoreMode(QueryRescoreMode.Multiply);
        rescorerBuilder.setRescoreQueryWeight(5);
        sourceBuilder.addRescorer(rescorerBuilder);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testHasParentWithInnerHit() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder("question", QueryBuilders.matchQuery("tags", "xml"), true);
        hasParentQueryBuilder.innerHit(new InnerHitBuilder("inner"));
        sourceBuilder.query(hasParentQueryBuilder);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testHasChildWithInnerHit() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("creationDate").gte("2019/01/01").lte("2019/01/31");
        HasChildQueryBuilder query = new HasChildQueryBuilder("answer", rangeQueryBuilder, ScoreMode.Total);
        query.innerHit(new InnerHitBuilder("inner"));
        sourceBuilder.query(query);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testProfile() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.profile(true);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "html"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertFalse(response.getProfileResults().isEmpty());
        });
    }

    public void testSortByField() throws Exception  {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(30);
        sourceBuilder.size(25);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "php"));
        sourceBuilder.sort("type.keyword", SortOrder.ASC);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort("user.keyword", SortOrder.ASC);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response, 30);
            if (response.getHits().getTotalHits().value > 30) {
                assertEquals(3, response.getHits().getHits()[0].getSortValues().length);
            }
        });
    }

    public void testSortByFieldOneClusterHasNoResults() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        // set to a value greater than the number of shards to avoid differences due to the skipping of shards
        searchRequest.setPreFilterShardSize(128);
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        boolean onlyRemote = randomBoolean();
        sourceBuilder.query(new TermQueryBuilder("_index", onlyRemote ? REMOTE_INDEX_NAME : INDEX_NAME));
        sourceBuilder.sort("type.keyword", SortOrder.ASC);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort("user.keyword", SortOrder.ASC);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            SearchHit[] hits = response.getHits().getHits();
            for (SearchHit hit : hits) {
                assertEquals(3, hit.getSortValues().length);
                assertEquals(INDEX_NAME, hit.getIndex());
                if (onlyRemote) {
                    assertEquals("my_remote_cluster", hit.getClusterAlias());
                } else {
                    assertNull(hit.getClusterAlias());
                }
            }
        });
    }

    public void testFieldCollapsingOneClusterHasNoResults() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        boolean onlyRemote = randomBoolean();
        sourceBuilder.query(new TermQueryBuilder("_index", onlyRemote ? REMOTE_INDEX_NAME : INDEX_NAME));
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertHits(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals(INDEX_NAME, hit.getIndex());
                if (onlyRemote) {
                    assertEquals("my_remote_cluster", hit.getClusterAlias());
                } else {
                    assertNull(hit.getClusterAlias());
                }
            }
        });
    }

    public void testFieldCollapsingSortByScore() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testFieldCollapsingSortByField() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort(new ScoreSortBuilder());
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        duelSearch(searchRequest, response -> {
            assertHits(response);
            assertEquals(2, response.getHits().getHits()[0].getSortValues().length);
        });
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40005")
    public void testTermsAggs() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        searchRequest.source(buildTermsAggsSource());
        duelSearch(searchRequest, CCSDuelIT::assertAggs);
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40005")
    public void testTermsAggsWithProfile() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        searchRequest.source(buildTermsAggsSource().profile(true));
        duelSearch(searchRequest, CCSDuelIT::assertAggs);
    }

    private static SearchSourceBuilder buildTermsAggsSource() {
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        TermsAggregationBuilder cluster = new TermsAggregationBuilder("cluster123").userValueTypeHint(ValueType.STRING);
        cluster.field("_index");
        TermsAggregationBuilder type = new TermsAggregationBuilder("type").userValueTypeHint(ValueType.STRING);
        type.field("type.keyword");
        type.showTermDocCountError(true);
        type.order(BucketOrder.key(true));
        cluster.subAggregation(type);
        sourceBuilder.aggregation(cluster);

        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags.field("tags.keyword");
        tags.showTermDocCountError(true);
        tags.size(100);
        sourceBuilder.aggregation(tags);

        TermsAggregationBuilder tags2 = new TermsAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags2.field("tags.keyword");
        tags.subAggregation(tags2);

        FilterAggregationBuilder answers = new FilterAggregationBuilder("answers", new TermQueryBuilder("type", "answer"));
        TermsAggregationBuilder answerPerQuestion = new TermsAggregationBuilder("answer_per_question")
            .userValueTypeHint(ValueType.STRING);
        answerPerQuestion.showTermDocCountError(true);
        answerPerQuestion.field("questionId.keyword");
        answers.subAggregation(answerPerQuestion);
        TermsAggregationBuilder answerPerUser = new TermsAggregationBuilder("answer_per_user").userValueTypeHint(ValueType.STRING);
        answerPerUser.field("user.keyword");
        answerPerUser.size(30);
        answerPerUser.showTermDocCountError(true);
        answers.subAggregation(answerPerUser);
        sourceBuilder.aggregation(answers);
        return sourceBuilder;
    }

    public void testDateHistogram() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags.field("tags.keyword");
        tags.showTermDocCountError(true);
        DateHistogramAggregationBuilder creation = new DateHistogramAggregationBuilder("creation");
        creation.field("creationDate");
        creation.calendarInterval(DateHistogramInterval.QUARTER);
        creation.subAggregation(tags);
        sourceBuilder.aggregation(creation);
        duelSearch(searchRequest, CCSDuelIT::assertAggs);
    }

    public void testCardinalityAgg() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        searchRequest.source(sourceBuilder);
        CardinalityAggregationBuilder tags = new CardinalityAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags.field("tags.keyword");
        sourceBuilder.aggregation(tags);
        duelSearch(searchRequest, CCSDuelIT::assertAggs);
    }

    public void testPipelineAggs() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new TermQueryBuilder("type", "answer"));
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(0);
        DateHistogramAggregationBuilder daily = new DateHistogramAggregationBuilder("daily");
        daily.field("creationDate");
        daily.calendarInterval(DateHistogramInterval.DAY);
        sourceBuilder.aggregation(daily);
        daily.subAggregation(new DerivativePipelineAggregationBuilder("derivative", "_count"));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("biggest_day", "daily._count"));
        daily.subAggregation(new SumAggregationBuilder("votes").field("votes"));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("most_voted", "daily>votes"));
        duelSearch(searchRequest, response -> {
            assertAggs(response);
            assertNotNull(response.getAggregations().get("most_voted"));
        });
        duelSearch(searchRequest, CCSDuelIT::assertAggs);
    }

    public void testTopHits() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        sourceBuilder.size(0);
        TopHitsAggregationBuilder topHits = new TopHitsAggregationBuilder("top");
        topHits.from(10);
        topHits.size(10);
        topHits.sort("creationDate", SortOrder.DESC);
        topHits.sort("id", SortOrder.ASC);
        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags.field("tags.keyword");
        tags.size(10);
        tags.subAggregation(topHits);
        sourceBuilder.aggregation(tags);
        duelSearch(searchRequest, CCSDuelIT::assertAggs);
    }

    public void testTermsLookup() throws Exception {
        assumeMultiClusterSetup();
        IndexRequest indexRequest = new IndexRequest("lookup_index");
        indexRequest.id("id");
        indexRequest.source("tags", new String[]{"java", "sql", "html", "jax-ws"});
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        assertEquals(201, indexResponse.status().getStatus());
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("tags", new TermsLookup("lookup_index", "id", "tags"));
        sourceBuilder.query(termsQueryBuilder);
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, CCSDuelIT::assertHits);
    }

    public void testShardFailures() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME + "*", REMOTE_INDEX_NAME + "*");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("creationDate", "err"));
        searchRequest.source(sourceBuilder);
        duelSearch(searchRequest, response -> {
            assertMultiClusterSearchResponse(response);
            assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
            assertNull(response.getAggregations());
            assertNull(response.getSuggest());
            assertThat(response.getHits().getHits().length, greaterThan(0));
            assertThat(response.getFailedShards(), greaterThanOrEqualTo(2));
        });
    }

    public void testTermSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("jva hml");
        suggestBuilder.addSuggestion("tags", new TermSuggestionBuilder("tags")
            .suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest, response -> {
            assertMultiClusterSearchResponse(response);
            assertEquals(1, response.getSuggest().size());
            TermSuggestion tags = response.getSuggest().getSuggestion("tags");
            assertThat(tags.getEntries().size(), greaterThan(0));
        });
    }

    public void testPhraseSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("jva and hml");
        suggestBuilder.addSuggestion("tags", new PhraseSuggestionBuilder("tags").addCandidateGenerator(
            new DirectCandidateGeneratorBuilder("tags").suggestMode("always")).highlight("<em>", "</em>"));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest, response -> {
            assertMultiClusterSearchResponse(response);
            assertEquals(1, response.getSuggest().size());
            PhraseSuggestion tags = response.getSuggest().getSuggestion("tags");
            assertThat(tags.getEntries().size(), greaterThan(0));
        });
    }

    public void testCompletionSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = initSearchRequest();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        searchRequest.source(sourceBuilder);
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("python", new CompletionSuggestionBuilder("suggest").size(10).text("pyth"));
        suggestBuilder.addSuggestion("java", new CompletionSuggestionBuilder("suggest").size(20).text("jav"));
        suggestBuilder.addSuggestion("ruby", new CompletionSuggestionBuilder("suggest").size(30).text("rub"));
        sourceBuilder.suggest(suggestBuilder);
        duelSearch(searchRequest, response -> {
            assertMultiClusterSearchResponse(response);
            assertEquals(Strings.toString(response, true, true), 3, response.getSuggest().size());
            assertThat(response.getSuggest().getSuggestion("python").getEntries().size(), greaterThan(0));
            assertThat(response.getSuggest().getSuggestion("java").getEntries().size(), greaterThan(0));
            assertThat(response.getSuggest().getSuggestion("ruby").getEntries().size(), greaterThan(0));
        });
    }

    private static void assumeMultiClusterSetup() {
        assumeTrue("must run only against the multi_cluster setup", "multi_cluster".equals(System.getProperty("tests.rest.suite")));
    }

    private static SearchRequest initSearchRequest() {
        List<String> indices = Arrays.asList(INDEX_NAME, "my_remote_cluster:" + INDEX_NAME);
        Collections.shuffle(indices, random());
        return new SearchRequest(indices.toArray(new String[0]));
    }

    private static void duelSearch(SearchRequest searchRequest, Consumer<SearchResponse> responseChecker) throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<Exception> exception1 = new AtomicReference<>();
        AtomicReference<SearchResponse> minimizeRoundtripsResponse = new AtomicReference<>();
        searchRequest.setCcsMinimizeRoundtrips(true);
        restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT,
            new LatchedActionListener<>(ActionListener.wrap(minimizeRoundtripsResponse::set, exception1::set), latch));

        AtomicReference<Exception> exception2 = new AtomicReference<>();
        AtomicReference<SearchResponse> fanOutResponse = new AtomicReference<>();
        searchRequest.setCcsMinimizeRoundtrips(false);
        restHighLevelClient.searchAsync(searchRequest, RequestOptions.DEFAULT,
            new LatchedActionListener<>(ActionListener.wrap(fanOutResponse::set, exception2::set), latch));

        latch.await();

        if (exception1.get() != null && exception2.get() != null) {
            exception1.get().addSuppressed(exception2.get());
            throw new AssertionError("both requests returned an exception", exception1.get());
        } else {
            if (exception1.get() != null) {
                throw new AssertionError("one of the two requests returned an exception", exception1.get());
            }
            if (exception2.get() != null) {
                throw new AssertionError("one of the two requests returned an exception", exception2.get());
            }
            SearchResponse minimizeRoundtripsSearchResponse = minimizeRoundtripsResponse.get();
            responseChecker.accept(minimizeRoundtripsSearchResponse);
            assertEquals(3, minimizeRoundtripsSearchResponse.getNumReducePhases());
            SearchResponse fanOutSearchResponse = fanOutResponse.get();
            responseChecker.accept(fanOutSearchResponse);
            assertEquals(1, fanOutSearchResponse.getNumReducePhases());
            Map<String, Object> minimizeRoundtripsResponseMap = responseToMap(minimizeRoundtripsSearchResponse);
            Map<String, Object> fanOutResponseMap = responseToMap(fanOutSearchResponse);
            if (minimizeRoundtripsResponseMap.equals(fanOutResponseMap) == false) {
                NotEqualMessageBuilder message = new NotEqualMessageBuilder();
                message.compareMaps(minimizeRoundtripsResponseMap, fanOutResponseMap);
                throw new AssertionError("Didn't match expected value:\n" + message);
            }
        }
    }

    private static void assertMultiClusterSearchResponse(SearchResponse searchResponse) {
        assertEquals(2, searchResponse.getClusters().getTotal());
        assertEquals(2, searchResponse.getClusters().getSuccessful());
        assertThat(searchResponse.getTotalShards(), greaterThan(1));
        assertThat(searchResponse.getSuccessfulShards(), greaterThan(1));
    }

    private static void assertHits(SearchResponse response) {
        assertHits(response, 0);
    }

    private static void assertHits(SearchResponse response, int from) {
        assertMultiClusterSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
        assertEquals(0, response.getFailedShards());
        assertNull(response.getAggregations());
        assertNull(response.getSuggest());
        if (response.getHits().getTotalHits().value > from) {
            assertThat(response.getHits().getHits().length, greaterThan(0));
        } else {
            assertThat(response.getHits().getHits().length, equalTo(0));
        }
    }

    private static void assertAggs(SearchResponse response) {
        assertMultiClusterSearchResponse(response);
        assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
        assertEquals(0, response.getHits().getHits().length);
        assertNull(response.getSuggest());
        assertNotNull(response.getAggregations());
        List<Aggregation> aggregations = response.getAggregations().asList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation instanceof MultiBucketsAggregation) {
                MultiBucketsAggregation multiBucketsAggregation = (MultiBucketsAggregation) aggregation;
                assertThat("agg " + multiBucketsAggregation.getName() + " has 0 buckets",
                    multiBucketsAggregation.getBuckets().size(), greaterThan(0));
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> responseToMap(SearchResponse response) throws IOException {
        BytesReference bytesReference = XContentHelper.toXContent(response, XContentType.JSON, false);
        Map<String, Object> responseMap = XContentHelper.convertToMap(bytesReference, false, XContentType.JSON).v2();
        assertNotNull(responseMap.put("took", -1));
        responseMap.remove("num_reduce_phases");
        Map<String, Object> profile = (Map<String, Object>)responseMap.get("profile");
        if (profile != null) {
            List<Map<String, Object>> shards = (List <Map<String, Object>>)profile.get("shards");
            for (Map<String, Object> shard : shards) {
                replaceProfileTime(shard);
            }
        }
        return responseMap;
    }

    @SuppressWarnings("unchecked")
    private static void replaceProfileTime(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().contains("time")) {
                assertThat(entry.getValue(), instanceOf(Number.class));
                assertNotNull(entry.setValue(-1));
            }
            if (entry.getKey().equals("breakdown")) {
                Map<String, Long> breakdown = (Map<String, Long>) entry.getValue();
                for (String key : breakdown.keySet()) {
                    assertNotNull(breakdown.put(key, -1L));
                }
            }
            if (entry.getValue() instanceof Map) {
                replaceProfileTime((Map<String, Object>) entry.getValue());
            }
            if (entry.getValue() instanceof List) {
                List<Object> list = (List<Object>) entry.getValue();
                for (Object obj : list) {
                    if (obj instanceof Map) {
                        replaceProfileTime((Map<String, Object>) obj);
                    }
                }
            }
        }
    }
}
