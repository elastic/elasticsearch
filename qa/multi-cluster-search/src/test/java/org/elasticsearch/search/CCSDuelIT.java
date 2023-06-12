/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search;

import com.carrotsearch.randomizedtesting.RandomizedContext;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.nio.entity.NByteArrayEntity;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.tests.util.TimeUnits;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.refresh.RefreshResponse;
import org.elasticsearch.action.bulk.BulkProcessor2;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.asyncsearch.AsyncSearchResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.DeterministicTaskQueue;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.core.IOUtils;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.InnerHitBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.indices.TermsLookup;
import org.elasticsearch.join.query.HasChildQueryBuilder;
import org.elasticsearch.join.query.HasParentQueryBuilder;
import org.elasticsearch.rest.action.search.RestSearchAction;
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
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.junit.AfterClass;
import org.junit.Before;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
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

import static java.util.stream.Collectors.toList;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;

/**
 * This test class executes twice, first against the remote cluster, and then against another cluster that has the remote cluster
 * registered. Given that each test gets executed against both clusters, {@link #assumeMultiClusterSetup()} needs to be used to run a test
 * against the multi cluster setup only, which is required for testing cross-cluster search.
 * The goal of this test is not to test correctness of CCS responses, but rather to verify that CCS returns the same responses when
 * <code>minimizeRoundTrips</code> is set to either <code>true</code> or <code>false</code>. In fact the execution differs depending on
 * such parameter, hence we want to verify that results are the same in both scenarios.
 */
@TimeoutSuite(millis = 5 * TimeUnits.MINUTE) // to account for slow as hell VMs
@SuppressWarnings("removal")
public class CCSDuelIT extends ESRestTestCase {

    private static final String INDEX_NAME = "ccs_duel_index";
    private static final String REMOTE_INDEX_NAME = "my_remote_cluster:" + INDEX_NAME;
    private static final String[] TAGS = new String[] { "java", "xml", "sql", "html", "php", "ruby", "python", "perl" };

    private static RestHighLevelClient restHighLevelClient;

    @Before
    public void init() throws Exception {
        super.initClient();
        if (restHighLevelClient == null) {
            restHighLevelClient = new HighLevelClient(client());
            String destinationCluster = System.getProperty("tests.rest.suite");
            // we index docs with private randomness otherwise the two clusters end up with exactly the same documents
            // given that this test class is run twice with same seed.
            RandomizedContext.current()
                .runWithPrivateRandomness(random().nextLong() + destinationCluster.hashCode(), (Callable<Void>) () -> {
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
        // this index with a single document is used to test partial failures
        IndexRequest indexRequest = new IndexRequest(INDEX_NAME + "_err");
        indexRequest.id("id");
        indexRequest.source("id", "id", "creationDate", "err");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        assertEquals(201, indexResponse.status().getStatus());

        CreateIndexResponse response = createIndex(INDEX_NAME + "_empty");
        assertTrue(response.isAcknowledged());

        int numShards = randomIntBetween(1, 5);
        Settings settings = indexSettings(numShards, 0).build();
        String mapping = """
            {
              "properties": {
                "id": {
                  "type": "keyword"
                },
                "suggest": {
                  "type": "completion"
                },
                "join": {
                  "type": "join",
                  "relations": {
                    "question": "answer"
                  }
                }
              }
            }""";
        response = createIndex(INDEX_NAME, settings, mapping);
        assertTrue(response.isAcknowledged());

        BulkProcessor2 bulkProcessor = BulkProcessor2.builder(
            (r, l) -> restHighLevelClient.bulkAsync(r, RequestOptions.DEFAULT, l),
            new BulkProcessor2.Listener() {
                @Override
                public void beforeBulk(long executionId, BulkRequest request) {}

                @Override
                public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
                    assertFalse(response.hasFailures());
                }

                @Override
                public void afterBulk(long executionId, BulkRequest request, Exception failure) {
                    throw new AssertionError("Failed to execute bulk", failure);
                }
            },
            new DeterministicTaskQueue(random()).getThreadPool()
        ).build();

        int numQuestions = randomIntBetween(50, 100);
        for (int i = 0; i < numQuestions; i++) {
            bulkProcessor.add(buildIndexRequest(idPrefix + i, "question", null));
        }
        int numAnswers = randomIntBetween(100, 150);
        for (int i = 0; i < numAnswers; i++) {
            bulkProcessor.add(buildIndexRequest(idPrefix + (i + 1000), "answer", idPrefix + randomIntBetween(0, numQuestions - 1)));
        }
        assertTrue(bulkProcessor.awaitClose(30, TimeUnit.SECONDS));

        RefreshResponse refreshResponse = refresh(INDEX_NAME);
        ElasticsearchAssertions.assertNoFailures(refreshResponse);
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
        indexRequest.source(
            XContentType.JSON,
            "id",
            id,
            "type",
            type,
            "votes",
            randomIntBetween(0, 30),
            "questionId",
            questionId,
            "tags",
            tagsArray,
            "user",
            "user" + randomIntBetween(1, 10),
            "suggest",
            Collections.singletonMap("input", tagsArray),
            "creationDate",
            date,
            "join",
            joinField
        );
        return indexRequest;
    }

    public void testMatchAll() throws Exception {
        assumeMultiClusterSetup();
        // verify that the order in which documents are returned when they all have the same score is the same
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testMatchQuery() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(50);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "php"));

        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testTrackTotalHitsUpTo() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.trackTotalHitsUpTo(5);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "sql"));
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testTerminateAfter() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.terminateAfter(10);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "perl"));
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testPagination() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(10);
        sourceBuilder.size(20);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "python"));

        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, response -> assertHits(response, 10));
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, response -> assertHits(response, 10));
        }
    }

    public void testHighlighting() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.highlighter(new HighlightBuilder().field("tags"));
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "xml"));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertFalse(response.getHits().getHits()[0].getHighlightFields().isEmpty());
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testFetchSource() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.fetchSource(new String[] { "tags" }, Strings.EMPTY_ARRAY);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));

        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertEquals(1, response.getHits().getHits()[0].getSourceAsMap().size());
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testDocValueFields() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.docValueField("user.keyword");
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "xml"));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertEquals(1, response.getHits().getHits()[0].getFields().size());
            assertNotNull(response.getHits().getHits()[0].getFields().get("user.keyword"));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testScriptFields() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.scriptField("parent", new Script(ScriptType.INLINE, "painless", "doc['join#question']", Collections.emptyMap()));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertEquals(1, response.getHits().getHits()[0].getFields().size());
            assertNotNull(response.getHits().getHits()[0].getFields().get("parent"));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testExplain() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.explain(true);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "sql"));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertNotNull(response.getHits().getHits()[0].getExplanation());
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testRescore() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "xml"));
        QueryRescorerBuilder rescorerBuilder = new QueryRescorerBuilder(new MatchQueryBuilder("tags", "java"));
        rescorerBuilder.setScoreMode(QueryRescoreMode.Multiply);
        rescorerBuilder.setRescoreQueryWeight(5);
        sourceBuilder.addRescorer(rescorerBuilder);

        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testHasParentWithInnerHit() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        HasParentQueryBuilder hasParentQueryBuilder = new HasParentQueryBuilder("question", QueryBuilders.matchQuery("tags", "xml"), true);
        hasParentQueryBuilder.innerHit(new InnerHitBuilder("inner"));
        sourceBuilder.query(hasParentQueryBuilder);
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testHasChildWithInnerHit() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        RangeQueryBuilder rangeQueryBuilder = new RangeQueryBuilder("creationDate").gte("2019/01/01").lte("2019/01/31");
        HasChildQueryBuilder query = new HasChildQueryBuilder("answer", rangeQueryBuilder, ScoreMode.Total);
        query.innerHit(new InnerHitBuilder("inner"));
        sourceBuilder.query(query);
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testProfile() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.profile(true);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "html"));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertFalse(response.getProfileResults().isEmpty());
            assertThat(
                response.getProfileResults().values().stream().filter(sr -> sr.getFetchPhase() != null).collect(toList()),
                not(empty())
            );
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testSortByField() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.from(30);
        sourceBuilder.size(25);
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "php"));
        sourceBuilder.sort("type.keyword", SortOrder.ASC);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort("user.keyword", SortOrder.ASC);
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response, 30);
            if (response.getHits().getTotalHits().value > 30) {
                assertEquals(3, response.getHits().getHits()[0].getSortValues().length);
            }
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    public void testSortByFieldOneClusterHasNoResults() throws Exception {
        assumeMultiClusterSetup();
        // set to a value greater than the number of shards to avoid differences due to the skipping of shards
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        boolean onlyRemote = randomBoolean();
        sourceBuilder.query(new TermQueryBuilder("_index", onlyRemote ? REMOTE_INDEX_NAME : INDEX_NAME));
        sourceBuilder.sort("type.keyword", SortOrder.ASC);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort("user.keyword", SortOrder.ASC);
        Consumer<SearchResponse> responseChecker = response -> {
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
        };
        SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
        searchRequest.source(sourceBuilder);
        duelRequest(searchRequest, responseChecker);
    }

    public void testFieldCollapsingOneClusterHasNoResults() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        boolean onlyRemote = randomBoolean();
        sourceBuilder.query(new TermQueryBuilder("_index", onlyRemote ? REMOTE_INDEX_NAME : INDEX_NAME));
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            for (SearchHit hit : response.getHits().getHits()) {
                assertEquals(INDEX_NAME, hit.getIndex());
                if (onlyRemote) {
                    assertEquals("my_remote_cluster", hit.getClusterAlias());
                } else {
                    assertNull(hit.getClusterAlias());
                }
            }
        };
        SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
        searchRequest.source(sourceBuilder);
        duelRequest(searchRequest, responseChecker);
    }

    public void testFieldCollapsingSortByScore() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testFieldCollapsingSortByField() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("tags", "ruby"));
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort(new ScoreSortBuilder());
        sourceBuilder.collapse(new CollapseBuilder("user.keyword"));
        Consumer<SearchResponse> responseChecker = response -> {
            assertHits(response);
            assertEquals(2, response.getHits().getHits()[0].getSortValues().length);
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, responseChecker);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40005")
    public void testTermsAggs() throws Exception {
        assumeMultiClusterSetup();
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(buildTermsAggsSource());
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(buildTermsAggsSource());
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/40005")
    public void testTermsAggsWithProfile() throws Exception {
        assumeMultiClusterSetup();
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(buildTermsAggsSource().profile(true));
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(buildTermsAggsSource().profile(true));
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
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
        TermsAggregationBuilder answerPerQuestion = new TermsAggregationBuilder("answer_per_question").userValueTypeHint(ValueType.STRING);
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
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        TermsAggregationBuilder tags = new TermsAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags.field("tags.keyword");
        tags.showTermDocCountError(true);
        DateHistogramAggregationBuilder creation = new DateHistogramAggregationBuilder("creation");
        creation.field("creationDate");
        creation.calendarInterval(DateHistogramInterval.QUARTER);
        creation.subAggregation(tags);
        sourceBuilder.aggregation(creation);
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
    }

    public void testCardinalityAgg() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.size(0);
        CardinalityAggregationBuilder tags = new CardinalityAggregationBuilder("tags").userValueTypeHint(ValueType.STRING);
        tags.field("tags.keyword");
        sourceBuilder.aggregation(tags);
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
    }

    public void testPipelineAggs() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(new TermQueryBuilder("type", "answer"));
        sourceBuilder.size(0);
        DateHistogramAggregationBuilder daily = new DateHistogramAggregationBuilder("daily");
        daily.field("creationDate");
        daily.calendarInterval(DateHistogramInterval.DAY);
        sourceBuilder.aggregation(daily);
        daily.subAggregation(new DerivativePipelineAggregationBuilder("derivative", "_count"));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("biggest_day", "daily._count"));
        daily.subAggregation(new SumAggregationBuilder("votes").field("votes"));
        sourceBuilder.aggregation(new MaxBucketPipelineAggregationBuilder("most_voted", "daily>votes"));
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, response -> {
                assertAggs(response);
                assertNotNull(response.getAggregations().get("most_voted"));
            });
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, response -> {
                assertAggs(response);
                assertNotNull(response.getAggregations().get("most_voted"));
            });
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
    }

    public void testTopHits() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
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
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
    }

    public void testTermsLookup() throws Exception {
        assumeMultiClusterSetup();
        IndexRequest indexRequest = new IndexRequest("lookup_index");
        indexRequest.id("id");
        indexRequest.source("tags", new String[] { "java", "sql", "html", "jax-ws" });
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
        IndexResponse indexResponse = restHighLevelClient.index(indexRequest, RequestOptions.DEFAULT);
        assertEquals(201, indexResponse.status().getStatus());
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        TermsQueryBuilder termsQueryBuilder = new TermsQueryBuilder("tags", new TermsLookup("lookup_index", "id", "tags"));
        sourceBuilder.query(termsQueryBuilder);
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, CCSDuelIT::assertHits);
        }
    }

    public void testShardFailures() throws Exception {
        assumeMultiClusterSetup();
        SearchRequest searchRequest = new SearchRequest(INDEX_NAME + "*", REMOTE_INDEX_NAME + "*");
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        sourceBuilder.query(QueryBuilders.matchQuery("creationDate", "err"));
        searchRequest.source(sourceBuilder);
        // TODO: for this test there is a slight discrepancy between the shard failure reason exception format
        // TODO: between sync and async searches, so skip that check for now and revisit as to why that is happening
        boolean compareAsyncAndSyncResponses = false;
        duelRequest(searchRequest, response -> {
            assertMultiClusterSearchResponse(response);
            assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
            assertNull(response.getAggregations());
            assertNull(response.getSuggest());
            assertThat(response.getHits().getHits().length, greaterThan(0));
            assertThat(response.getFailedShards(), greaterThanOrEqualTo(2));
        }, compareAsyncAndSyncResponses);
    }

    public void testTermSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("jva hml");
        suggestBuilder.addSuggestion("tags", new TermSuggestionBuilder("tags").suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR));
        sourceBuilder.suggest(suggestBuilder);
        Consumer<SearchResponse> responseChecker = response -> {
            assertEquals(1, response.getSuggest().size());
            TermSuggestion tags = response.getSuggest().getSuggestion("tags");
            assertThat(tags.getEntries().size(), greaterThan(0));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            responseChecker.andThen(CCSDuelIT::assertMultiClusterSearchResponse);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            responseChecker.andThen(CCSDuelIT::assertSingleRemoteClusterSearchResponse);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker);
        }
    }

    public void testPhraseSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("jva and hml");
        suggestBuilder.addSuggestion(
            "tags",
            new PhraseSuggestionBuilder("tags").addCandidateGenerator(new DirectCandidateGeneratorBuilder("tags").suggestMode("always"))
                .highlight("<em>", "</em>")
        );
        sourceBuilder.suggest(suggestBuilder);
        Consumer<SearchResponse> responseChecker = response -> {
            assertEquals(1, response.getSuggest().size());
            PhraseSuggestion tags = response.getSuggest().getSuggestion("tags");
            assertThat(tags.getEntries().size(), greaterThan(0));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            responseChecker.andThen(CCSDuelIT::assertMultiClusterSearchResponse);
            duelSearchSync(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            responseChecker.andThen(CCSDuelIT::assertSingleRemoteClusterSearchResponse);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker);
        }
    }

    public void testCompletionSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.addSuggestion("python", new CompletionSuggestionBuilder("suggest").size(10).text("pyth"));
        suggestBuilder.addSuggestion("java", new CompletionSuggestionBuilder("suggest").size(20).text("jav"));
        suggestBuilder.addSuggestion("ruby", new CompletionSuggestionBuilder("suggest").size(30).text("rub"));
        sourceBuilder.suggest(suggestBuilder);
        Consumer<SearchResponse> responseChecker = response -> {
            assertEquals(Strings.toString(response, true, true), 3, response.getSuggest().size());
            assertThat(response.getSuggest().getSuggestion("python").getEntries().size(), greaterThan(0));
            assertThat(response.getSuggest().getSuggestion("java").getEntries().size(), greaterThan(0));
            assertThat(response.getSuggest().getSuggestion("ruby").getEntries().size(), greaterThan(0));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            responseChecker.andThen(CCSDuelIT::assertMultiClusterSearchResponse);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            responseChecker.andThen(CCSDuelIT::assertSingleRemoteClusterSearchResponse);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker);
        }
    }

    private static void assumeMultiClusterSetup() {
        assumeTrue("must run only against the multi_cluster setup", "multi_cluster".equals(System.getProperty("tests.rest.suite")));
    }

    private static SearchRequest initLocalAndRemoteSearchRequest() {
        List<String> indices = Arrays.asList(INDEX_NAME, "my_remote_cluster:" + INDEX_NAME);
        Collections.shuffle(indices, random());
        final SearchRequest request = new SearchRequest(indices.toArray(new String[0]));
        if (randomBoolean()) {
            request.setPreFilterShardSize(between(1, 20));
        }
        return request;
    }

    private static SearchRequest initRemoteOnlySearchRequest() {
        List<String> indices = Arrays.asList("my_remote_cluster:" + INDEX_NAME);
        final SearchRequest request = new SearchRequest(indices.toArray(new String[0]));
        if (randomBoolean()) {
            request.setPreFilterShardSize(between(1, 20));
        }
        return request;
    }

    private void duelRequest(SearchRequest searchRequest, Consumer<SearchResponse> responseChecker) throws Exception {
        duelRequest(searchRequest, responseChecker, true);
    }

    private void duelRequest(SearchRequest searchRequest, Consumer<SearchResponse> responseChecker, boolean compareAsyncToSyncResponses)
        throws Exception {
        Map<String, Object> syncResponseMap = duelSearchSync(searchRequest, responseChecker);
        Map<String, Object> asyncResponseMap = duelSearchAsync(searchRequest, responseChecker);
        if (compareAsyncToSyncResponses) {
            compareResponseMaps(syncResponseMap, asyncResponseMap, "Comparing sync_search CCS vs. async_search CCS");
        }
    }

    /**
     * @return responseMap from one of the Synchronous Search Requests
     */
    private static Map<String, Object> duelSearchSync(SearchRequest searchRequest, Consumer<SearchResponse> responseChecker)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<Exception> exception1 = new AtomicReference<>();
        AtomicReference<SearchResponse> minimizeRoundtripsResponse = new AtomicReference<>();
        searchRequest.setCcsMinimizeRoundtrips(true);
        restHighLevelClient.searchAsync(
            searchRequest,
            RequestOptions.DEFAULT,
            new LatchedActionListener<>(ActionListener.wrap(minimizeRoundtripsResponse::set, exception1::set), latch)
        );

        AtomicReference<Exception> exception2 = new AtomicReference<>();
        AtomicReference<SearchResponse> fanOutResponse = new AtomicReference<>();
        searchRequest.setCcsMinimizeRoundtrips(false);
        restHighLevelClient.searchAsync(
            searchRequest,
            RequestOptions.DEFAULT,
            new LatchedActionListener<>(ActionListener.wrap(fanOutResponse::set, exception2::set), latch)
        );

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
            SearchResponse.Clusters clusters = minimizeRoundtripsSearchResponse.getClusters();

            // if only the remote cluster was searched, then only one reduce phase is expected
            int expectedReducePhasesMinRoundTrip = 1;
            if (searchRequest.indices().length > 1) {
                expectedReducePhasesMinRoundTrip = searchRequest.indices().length + 1;
            }

            assertEquals(expectedReducePhasesMinRoundTrip, minimizeRoundtripsSearchResponse.getNumReducePhases());
            SearchResponse fanOutSearchResponse = fanOutResponse.get();
            responseChecker.accept(fanOutSearchResponse);
            assertEquals(1, fanOutSearchResponse.getNumReducePhases());
            Map<String, Object> minimizeRoundtripsResponseMap = responseToMap(minimizeRoundtripsSearchResponse);
            Map<String, Object> fanOutResponseMap = responseToMap(fanOutSearchResponse);
            compareResponseMaps(minimizeRoundtripsResponseMap, fanOutResponseMap, "Comparing sync_search minimizeRoundTrip vs. fanOut");
            assertThat(minimizeRoundtripsSearchResponse.getSkippedShards(), lessThanOrEqualTo(fanOutSearchResponse.getSkippedShards()));
            return minimizeRoundtripsResponseMap;
        }
    }

    /**
     * @return responseMap from one of the async searches
     */
    private static Map<String, Object> duelSearchAsync(SearchRequest searchRequest, Consumer<SearchResponse> responseChecker)
        throws Exception {
        searchRequest.setCcsMinimizeRoundtrips(true);
        AsyncSearchResponse minimizeRoundtripsResponse = submitAsyncSearch(
            searchRequest,
            TimeValue.timeValueSeconds(1),
            restHighLevelClient.getParserConfig()
        );

        try {
            final String responseId = minimizeRoundtripsResponse.getId();
            assertBusy(() -> {
                AsyncSearchResponse resp = getAsyncSearch(responseId, restHighLevelClient.getParserConfig());
                assertThat(resp.isRunning(), equalTo(false));
            });
            minimizeRoundtripsResponse = getAsyncSearch(responseId, restHighLevelClient.getParserConfig());
        } finally {
            deleteAsyncSearch(minimizeRoundtripsResponse.getId());
        }

        searchRequest.setCcsMinimizeRoundtrips(false);
        AsyncSearchResponse fanOutResponse = submitAsyncSearch(
            searchRequest,
            TimeValue.timeValueSeconds(1),
            restHighLevelClient.getParserConfig()
        );
        try {
            final String responseId = fanOutResponse.getId();
            assertBusy(() -> {
                AsyncSearchResponse resp = getAsyncSearch(responseId, restHighLevelClient.getParserConfig());
                assertThat(resp.isRunning(), equalTo(false));
            });
            fanOutResponse = getAsyncSearch(responseId, restHighLevelClient.getParserConfig());
        } finally {
            deleteAsyncSearch(fanOutResponse.getId());
        }
        SearchResponse minimizeRoundtripsSearchResponse = minimizeRoundtripsResponse.getSearchResponse();
        SearchResponse fanOutSearchResponse = fanOutResponse.getSearchResponse();

        responseChecker.accept(minimizeRoundtripsSearchResponse);

        // if only the remote cluster was searched, then only one reduce phase is expected
        int expectedReducePhasesMinRoundTrip = 1;
        if (searchRequest.indices().length > 1) {
            expectedReducePhasesMinRoundTrip = searchRequest.indices().length + 1;
        }
        assertEquals(expectedReducePhasesMinRoundTrip, minimizeRoundtripsSearchResponse.getNumReducePhases());

        responseChecker.accept(fanOutSearchResponse);
        assertEquals(1, fanOutSearchResponse.getNumReducePhases());
        Map<String, Object> minimizeRoundtripsResponseMap = responseToMap(minimizeRoundtripsSearchResponse);
        Map<String, Object> fanOutResponseMap = responseToMap(fanOutSearchResponse);
        compareResponseMaps(minimizeRoundtripsResponseMap, fanOutResponseMap, "Comparing async_search minimizeRoundTrip vs. fanOut");
        assertThat(minimizeRoundtripsSearchResponse.getSkippedShards(), lessThanOrEqualTo(fanOutSearchResponse.getSkippedShards()));
        return minimizeRoundtripsResponseMap;
    }

    private static void compareResponseMaps(Map<String, Object> responseMap1, Map<String, Object> responseMap2, String info) {
        String diff = XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder(responseMap1, responseMap2);
        if (diff != null) {
            NotEqualMessageBuilder builder = new NotEqualMessageBuilder();
            builder.compareMaps(responseMap1, responseMap2);
            String message = Strings.format("%s. Didn't match expected value. %s.\nFull diff %s", info, diff, builder);
            throw new AssertionError(message);
        }
    }

    private static AsyncSearchResponse submitAsyncSearch(
        SearchRequest searchRequest,
        TimeValue waitForCompletion,
        XContentParserConfiguration parserConfig
    ) throws IOException {
        String indices = Strings.collectionToDelimitedString(List.of(searchRequest.indices()), ",");
        final Request request = new Request("POST", URLEncoder.encode(indices, StandardCharsets.UTF_8) + "/_async_search");

        request.addParameter("wait_for_completion_timeout", waitForCompletion.toString());
        request.addParameter("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        request.addParameter("batched_reduce_size", Integer.toString(searchRequest.getBatchedReduceSize()));
        request.addParameter("keep_on_completion", "true");
        request.addParameter(RestSearchAction.TYPED_KEYS_PARAM, "true");
        request.setEntity(createEntity(searchRequest.source(), XContentType.JSON, ToXContent.EMPTY_PARAMS));
        Response resp = restHighLevelClient.getLowLevelClient().performRequest(request);
        return parseEntity(resp.getEntity(), AsyncSearchResponse::fromXContent, parserConfig);
    }

    private static AsyncSearchResponse getAsyncSearch(String id, XContentParserConfiguration parserConfig) throws IOException {
        final Request request = new Request("GET", "/_async_search/" + id);
        request.addParameter("wait_for_completion_timeout", "0ms");
        request.addParameter(RestSearchAction.TYPED_KEYS_PARAM, "true");
        Response resp = restHighLevelClient.getLowLevelClient().performRequest(request);
        return parseEntity(resp.getEntity(), AsyncSearchResponse::fromXContent, parserConfig);
    }

    private static Response deleteAsyncSearch(String id) throws IOException {
        final Request request = new Request("DELETE", "/_async_search/" + id);
        return restHighLevelClient.getLowLevelClient().performRequest(request);
    }

    private static HttpEntity createEntity(ToXContent toXContent, XContentType xContentType, ToXContent.Params toXContentParams)
        throws IOException {
        BytesRef source = XContentHelper.toXContent(toXContent, xContentType, toXContentParams, false).toBytesRef();
        return new NByteArrayEntity(source.bytes, source.offset, source.length, createContentType(xContentType));
    }

    private static <Resp> Resp parseEntity(
        final HttpEntity entity,
        final CheckedFunction<XContentParser, Resp, IOException> entityParser,
        final XContentParserConfiguration parserConfig
    ) throws IOException {
        if (entity == null) {
            throw new IllegalStateException("Response body expected but not returned");
        }
        if (entity.getContentType() == null) {
            throw new IllegalStateException("Elasticsearch didn't return the [Content-Type] header, unable to parse response body");
        }
        XContentType xContentType = XContentType.fromMediaType(entity.getContentType().getValue());
        if (xContentType == null) {
            throw new IllegalStateException("Unsupported Content-Type: " + entity.getContentType().getValue());
        }
        try (XContentParser parser = xContentType.xContent().createParser(parserConfig, entity.getContent())) {
            return entityParser.apply(parser);
        }
    }

    static ContentType createContentType(final XContentType xContentType) {
        return ContentType.create(xContentType.mediaTypeWithoutParameters(), (Charset) null);
    }

    private static void assertMultiClusterSearchResponse(SearchResponse searchResponse) {
        assertEquals(2, searchResponse.getClusters().getTotal());
        assertEquals(2, searchResponse.getClusters().getSuccessful());
        assertThat(searchResponse.getTotalShards(), greaterThan(1));
        assertThat(searchResponse.getSuccessfulShards(), greaterThan(1));
    }

    private static void assertSingleRemoteClusterSearchResponse(SearchResponse searchResponse) {
        assertEquals(1, searchResponse.getClusters().getTotal());
        assertEquals(1, searchResponse.getClusters().getSuccessful());
        assertThat(searchResponse.getTotalShards(), greaterThanOrEqualTo(1));
        assertThat(searchResponse.getSuccessfulShards(), greaterThanOrEqualTo(1));
    }

    private static void assertHits(SearchResponse response) {
        assertHits(response, 0);
    }

    private static void assertHits(SearchResponse response, int from) {
        if (response.getClusters().getTotal() == 1) {
            assertSingleRemoteClusterSearchResponse(response);
        } else {
            assertMultiClusterSearchResponse(response);
        }
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
        if (response.getClusters().getTotal() == 1) {
            assertSingleRemoteClusterSearchResponse(response);
        } else {
            assertMultiClusterSearchResponse(response);
        }
        assertThat(response.getHits().getTotalHits().value, greaterThan(0L));
        assertEquals(0, response.getHits().getHits().length);
        assertNull(response.getSuggest());
        assertNotNull(response.getAggregations());
        List<Aggregation> aggregations = response.getAggregations().asList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation instanceof MultiBucketsAggregation multiBucketsAggregation) {
                assertThat(
                    "agg " + multiBucketsAggregation.getName() + " has 0 buckets",
                    multiBucketsAggregation.getBuckets().size(),
                    greaterThan(0)
                );
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> responseToMap(SearchResponse response) throws IOException {
        BytesReference bytesReference = XContentHelper.toXContent(response, XContentType.JSON, false);
        Map<String, Object> responseMap = XContentHelper.convertToMap(bytesReference, false, XContentType.JSON).v2();
        assertNotNull(responseMap.put("took", -1));
        responseMap.remove("num_reduce_phases");
        Map<String, Object> profile = (Map<String, Object>) responseMap.get("profile");
        if (profile != null) {
            List<Map<String, Object>> shards = (List<Map<String, Object>>) profile.get("shards");
            for (Map<String, Object> shard : shards) {
                replaceProfileTime(shard);
                /*
                 * The way we try to reduce round trips is by fetching all
                 * of the results we could possibly need from the remote
                 * cluster and then merging *those* together locally. This
                 * will end up fetching more documents total. So we can't
                 * really compare the fetch profiles here.
                 */
                shard.remove("fetch");
            }
        }
        Map<String, Object> shards = (Map<String, Object>) responseMap.get("_shards");
        if (shards != null) {
            shards.remove("skipped");
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
