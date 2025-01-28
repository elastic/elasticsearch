/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
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
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.support.broadcast.BroadcastResponse;
import org.elasticsearch.aggregations.pipeline.DerivativePipelineAggregationBuilder;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.CheckedConsumer;
import org.elasticsearch.core.CheckedFunction;
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
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
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
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestionBuilder;
import org.elasticsearch.test.NotEqualMessageBuilder;
import org.elasticsearch.test.XContentTestUtils;
import org.elasticsearch.test.hamcrest.ElasticsearchAssertions;
import org.elasticsearch.test.rest.ESRestTestCase;
import org.elasticsearch.test.rest.ObjectPath;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URLEncoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.CoreMatchers.anyOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

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
    private static final String[] TAGS = new String[] { "java", "xml", "sql", "html", "php", "ruby", "python", "perl" };

    private static boolean init = false;

    @Before
    public void init() throws Exception {
        super.initClient();
        if (init == false) {
            init = true;
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

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true;
    }

    @Override
    protected boolean preserveDataStreamsUponCompletion() {
        return true;
    }

    private void indexDocuments(String idPrefix) throws IOException, InterruptedException {
        // this index with a single document is used to test partial failures
        Request request = new Request("POST", "/" + INDEX_NAME + "_err/_doc");
        request.addParameter("refresh", "wait_for");
        request.setJsonEntity("{ \"id\" : \"id\",  \"creationDate\" : \"err\" }");
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());

        ElasticsearchAssertions.assertAcked(createIndex(INDEX_NAME + "_empty"));

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
        ElasticsearchAssertions.assertAcked(createIndex(INDEX_NAME, settings, mapping));

        CountDownLatch latch = new CountDownLatch(2);

        int numQuestions = randomIntBetween(50, 100);
        {
            StringBuilder builder = new StringBuilder();
            for (int i = 0; i < numQuestions; i++) {
                buildIndexRequest(builder, idPrefix + i, "question", null);
            }
            executeBulkAsync(builder.toString(), latch);
        }
        {
            StringBuilder builder = new StringBuilder();
            int numAnswers = randomIntBetween(100, 150);
            for (int i = 0; i < numAnswers; i++) {
                buildIndexRequest(builder, idPrefix + (i + 1000), "answer", idPrefix + randomIntBetween(0, numQuestions - 1));
            }
            executeBulkAsync(builder.toString(), latch);
        }

        assertTrue(latch.await(30, TimeUnit.SECONDS));

        BroadcastResponse refreshResponse = refresh(INDEX_NAME);
        ElasticsearchAssertions.assertNoFailures(refreshResponse);
    }

    private void executeBulkAsync(String body, CountDownLatch latch) {
        Request bulk = new Request("POST", "/_bulk");
        bulk.setJsonEntity(body);
        client().performRequestAsync(bulk, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    ObjectPath objectPath = ObjectPath.createFromResponse(response);
                    assertThat(objectPath.evaluate("errors"), Matchers.equalTo(false));
                } catch (IOException ioException) {
                    throw new UncheckedIOException(ioException);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception exception) {
                try {
                    fail(exception.getMessage());
                } finally {
                    latch.countDown();
                }
            }
        });
    }

    private static void buildIndexRequest(StringBuilder buffer, String id, String type, String questionId) {
        // { "index" : { "_index" : "test", "_id" : "1" } }/n
        buffer.append("{ \"index\" : { \"_index\" : \"").append(INDEX_NAME).append("\", \"_id\" : \"").append(id).append("\"");
        if (questionId != null) {
            buffer.append(", \"routing\" : \"").append(questionId).append("\"");
        }
        buffer.append(" } }\n");
        int numTags = randomIntBetween(1, 3);
        Set<String> tags = new HashSet<>();
        if (questionId == null) {
            for (int i = 0; i < numTags; i++) {
                tags.add("\"" + randomFrom(TAGS) + "\"");
            }
        }
        String[] tagsArray = tags.toArray(new String[0]);
        String date = LocalDate.of(2019, 1, randomIntBetween(1, 31)).format(DateTimeFormatter.ofPattern("yyyy/MM/dd", Locale.ROOT));

        buffer.append("{ ");
        buffer.append("\"id\" : \"").append(id).append("\",");
        buffer.append("\"type\" : \"").append(type).append("\",");
        buffer.append("\"votes\" : ").append(randomIntBetween(0, 30)).append(",");
        if (questionId != null) {
            buffer.append("\"questionId\" : \"").append(questionId).append("\",");
        } else {
            buffer.append("\"questionId\" : ").append(questionId).append(",");
        }
        buffer.append("\"tags\" : [").append(String.join(",", Arrays.asList(tagsArray))).append("],");
        buffer.append("\"user\" : \"").append("user").append(randomIntBetween(1, 10)).append("\",");
        buffer.append("\"suggest\" : ")
            .append("{")
            .append("\"input\" : [")
            .append(String.join(",", Arrays.asList(tagsArray)))
            .append("]},");
        buffer.append("\"creationDate\" : \"").append(date).append("\",");
        buffer.append("\"join\" : {");
        buffer.append("\"name\" : \"").append(type).append("\"");
        if (questionId != null) {
            buffer.append(", \"parent\" : \"").append(questionId).append("\"");
        }
        buffer.append("}}\n");
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertFalse(response.evaluateMapKeys("hits.hits.0.highlight").isEmpty());
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

        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertThat(response.evaluateMapKeys("hits.hits.0._source").size(), equalTo(1));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertThat(response.evaluateMapKeys("hits.hits.0.fields").size(), equalTo(1));
            assertTrue(response.evaluateMapKeys("hits.hits.0.fields").contains("user.keyword"));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertThat(response.evaluateMapKeys("hits.hits.0.fields").size(), equalTo(1));
            assertTrue(response.evaluateMapKeys("hits.hits.0.fields").contains("parent"));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertNotNull(response.evaluate("hits.hits.0._explanation"));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertFalse(response.evaluateMapKeys("profile").isEmpty());
            int size = response.evaluateArraySize("profile.shards");
            boolean fail = true;
            for (int i = 0; i < size; i++) {
                if (response.evaluate("profile.shards." + i + ".fetch") != null) {
                    fail = false;
                    break;
                }
            }
            assertFalse("profile might be incomplete", fail);
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response, 30);
            int total = response.evaluate("hits.total.value");
            if (total > 30) {
                assertThat(response.evaluateArraySize("hits.hits.0.sort"), equalTo(3));
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
        // setting aggs to avoid differences due to the skipping of shards when matching none
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        boolean onlyRemote = randomBoolean();
        sourceBuilder.query(new TermQueryBuilder("_index", onlyRemote ? REMOTE_INDEX_NAME : INDEX_NAME));
        sourceBuilder.sort("type.keyword", SortOrder.ASC);
        sourceBuilder.sort("creationDate", SortOrder.DESC);
        sourceBuilder.sort("user.keyword", SortOrder.ASC);
        sourceBuilder.aggregation(AggregationBuilders.max("max").field("creationDate"));
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            int size = response.evaluateArraySize("hits.hits");
            for (int i = 0; i < size; i++) {
                String hit = "hits.hits." + i;
                assertThat(response.evaluateArraySize(hit + ".sort"), equalTo(3));
                if (onlyRemote) {
                    assertThat(response.evaluate(hit + "._index"), equalTo(REMOTE_INDEX_NAME));
                } else {
                    assertThat(response.evaluate(hit + "._index"), equalTo(INDEX_NAME));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            int size = response.evaluateArraySize("hits.hits");
            for (int i = 0; i < size; i++) {
                String hit = "hits.hits." + i;
                if (onlyRemote) {
                    assertThat(response.evaluate(hit + "._index"), equalTo(REMOTE_INDEX_NAME));
                } else {
                    assertThat(response.evaluate(hit + "._index"), equalTo(INDEX_NAME));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertHits(response);
            assertThat(response.evaluateArraySize("hits.hits.0.sort"), equalTo(2));
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
                assertTrue(response.evaluateMapKeys("aggregations").contains("bucket_metric_value#most_voted"));
            });
            duelRequest(searchRequest, CCSDuelIT::assertAggs);
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            duelRequest(searchRequest, response -> {
                assertAggs(response);
                assertTrue(response.evaluateMapKeys("aggregations").contains("bucket_metric_value#most_voted"));
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
        Request request = new Request("POST", "/lookup_index/_doc/id");
        request.addParameter("refresh", "wait_for");
        request.setJsonEntity("{ \"tags\" : [ \"java\", \"sql\", \"html\", \"jax-ws\" ] }");
        Response response = client().performRequest(request);
        assertEquals(201, response.getStatusLine().getStatusCode());

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
            assertThat(response.evaluate("hits.total.value"), greaterThan(0));
            assertNull(response.evaluate("aggregations"));
            assertNull(response.evaluate("suggest"));
            assertThat(response.evaluateArraySize("hits.hits"), greaterThan(0));
            assertThat(response.evaluate("_shards.failed"), greaterThanOrEqualTo(2));
        }, compareAsyncAndSyncResponses);
    }

    public void testTermSuggester() throws Exception {
        assumeMultiClusterSetup();
        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder();
        SuggestBuilder suggestBuilder = new SuggestBuilder();
        suggestBuilder.setGlobalText("jva hml");
        suggestBuilder.addSuggestion("tags", new TermSuggestionBuilder("tags").suggestMode(TermSuggestionBuilder.SuggestMode.POPULAR));
        sourceBuilder.suggest(suggestBuilder);
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertThat(response.evaluateMapKeys("suggest").size(), equalTo(1));
            assertThat(response.evaluateArraySize("suggest.term#tags"), greaterThan(0));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker.andThen(CCSDuelIT::assertMultiClusterSearchResponse));
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker.andThen(CCSDuelIT::assertSingleRemoteClusterSearchResponse));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertEquals(1, response.evaluateMapKeys("suggest").size());
            assertThat(response.evaluateArraySize("suggest.phrase#tags"), greaterThan(0));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker.andThen(CCSDuelIT::assertMultiClusterSearchResponse));
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker.andThen(CCSDuelIT::assertSingleRemoteClusterSearchResponse));
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
        CheckedConsumer<ObjectPath, IOException> responseChecker = response -> {
            assertThat(response.evaluateMapKeys("suggest").size(), equalTo(3));
            assertThat(response.evaluateArraySize("suggest.completion#python"), greaterThan(0));
            assertThat(response.evaluateArraySize("suggest.completion#java"), greaterThan(0));
            assertThat(response.evaluateArraySize("suggest.completion#ruby"), greaterThan(0));
        };
        {
            SearchRequest searchRequest = initLocalAndRemoteSearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker.andThen(CCSDuelIT::assertMultiClusterSearchResponse));
        }
        {
            SearchRequest searchRequest = initRemoteOnlySearchRequest();
            searchRequest.source(sourceBuilder);
            // suggest-only queries are not supported by _async_search, so only test against sync search API
            duelSearchSync(searchRequest, responseChecker.andThen(CCSDuelIT::assertSingleRemoteClusterSearchResponse));
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
        List<String> indices = List.of("my_remote_cluster:" + INDEX_NAME);
        final SearchRequest request = new SearchRequest(indices.toArray(new String[0]));
        if (randomBoolean()) {
            request.setPreFilterShardSize(between(1, 20));
        }
        return request;
    }

    private void duelRequest(SearchRequest searchRequest, CheckedConsumer<ObjectPath, IOException> responseChecker) throws Exception {
        duelRequest(searchRequest, responseChecker, true);
    }

    private void duelRequest(
        SearchRequest searchRequest,
        CheckedConsumer<ObjectPath, IOException> responseChecker,
        boolean compareAsyncToSyncResponses
    ) throws Exception {
        Map<String, Object> syncResponseMap = duelSearchSync(searchRequest, responseChecker);
        Map<String, Object> asyncResponseMap = duelSearchAsync(searchRequest, responseChecker);
        if (compareAsyncToSyncResponses) {
            compareResponseMaps(syncResponseMap, asyncResponseMap, "Comparing sync_search CCS vs. async_search CCS");
        }
    }

    /**
     * @return responseMap from one of the Synchronous Search Requests
     */
    private static Map<String, Object> duelSearchSync(SearchRequest searchRequest, CheckedConsumer<ObjectPath, IOException> responseChecker)
        throws Exception {
        CountDownLatch latch = new CountDownLatch(2);
        AtomicReference<Exception> exception1 = new AtomicReference<>();
        AtomicReference<Response> minimizeRoundtripsResponse = new AtomicReference<>();
        searchRequest.setCcsMinimizeRoundtrips(true);
        submitSyncSearch(searchRequest, minimizeRoundtripsResponse, exception1, latch);
        AtomicReference<Exception> exception2 = new AtomicReference<>();
        AtomicReference<Response> fanOutResponse = new AtomicReference<>();
        searchRequest.setCcsMinimizeRoundtrips(false);
        submitSyncSearch(searchRequest, fanOutResponse, exception2, latch);

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
            ObjectPath minimizeRoundtripsSearchResponse = ObjectPath.createFromResponse(minimizeRoundtripsResponse.get());
            responseChecker.accept(minimizeRoundtripsSearchResponse);

            // if only the remote cluster was searched, then only one reduce phase is expected
            int expectedReducePhasesMinRoundTrip = 1;
            if (searchRequest.indices().length > 1) {
                expectedReducePhasesMinRoundTrip = searchRequest.indices().length + 1;
            }
            if (expectedReducePhasesMinRoundTrip == 1) {
                assertThat(
                    minimizeRoundtripsSearchResponse.evaluate("num_reduce_phases"),
                    anyOf(equalTo(expectedReducePhasesMinRoundTrip), nullValue())
                );
            } else {
                assertThat(minimizeRoundtripsSearchResponse.evaluate("num_reduce_phases"), equalTo(expectedReducePhasesMinRoundTrip));
            }
            ObjectPath fanOutSearchResponse = ObjectPath.createFromResponse(fanOutResponse.get());
            responseChecker.accept(fanOutSearchResponse);
            assertThat(fanOutSearchResponse.evaluate("num_reduce_phases"), anyOf(equalTo(1), nullValue())); // default value is 1?

            // compare Clusters objects
            assertThat(
                minimizeRoundtripsSearchResponse.evaluate("_cluster.total"),
                equalTo(fanOutSearchResponse.evaluate("_cluster.total"))
            );
            assertThat(
                minimizeRoundtripsSearchResponse.evaluate("_cluster.successful"),
                equalTo(fanOutSearchResponse.evaluate("_cluster.successful"))
            );
            assertThat(
                minimizeRoundtripsSearchResponse.evaluate("_cluster.skipped"),
                equalTo(fanOutSearchResponse.evaluate("_cluster.skipped"))
            );
            assertThat(
                minimizeRoundtripsSearchResponse.evaluate("_cluster.running"),
                equalTo(fanOutSearchResponse.evaluate("_cluster.running"))
            );
            assertThat(
                minimizeRoundtripsSearchResponse.evaluate("_cluster.partial"),
                equalTo(fanOutSearchResponse.evaluate("_cluster.partial"))
            );
            assertThat(
                minimizeRoundtripsSearchResponse.evaluate("_cluster.failed"),
                equalTo(fanOutSearchResponse.evaluate("_cluster.failed"))
            );

            Map<String, Object> minimizeRoundtripsResponseMap = responseToMap(minimizeRoundtripsSearchResponse);
            if (minimizeRoundtripsSearchResponse.evaluate("_clusters") != null && fanOutSearchResponse.evaluate("_clusters") != null) {
                Map<String, Object> fanOutResponseMap = responseToMap(fanOutSearchResponse);
                compareResponseMaps(minimizeRoundtripsResponseMap, fanOutResponseMap, "Comparing sync_search minimizeRoundTrip vs. fanOut");
                assertThat(
                    minimizeRoundtripsSearchResponse.evaluate("_shards.skipped"),
                    lessThanOrEqualTo((Integer) fanOutSearchResponse.evaluate("_shards.skipped"))
                );
            }
            return minimizeRoundtripsResponseMap;
        }
    }

    private static void submitSyncSearch(
        SearchRequest searchRequest,
        AtomicReference<Response> responseRef,
        AtomicReference<Exception> exceptionRef,
        CountDownLatch latch
    ) throws IOException {
        String indices = Strings.collectionToDelimitedString(List.of(searchRequest.indices()), ",");
        final Request request = new Request("POST", URLEncoder.encode(indices, StandardCharsets.UTF_8) + "/_search");
        request.addParameter("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        request.addParameter(RestSearchAction.TYPED_KEYS_PARAM, "true");
        request.setEntity(createEntity(searchRequest.source(), XContentType.JSON, ToXContent.EMPTY_PARAMS));
        client().performRequestAsync(request, new ResponseListener() {
            @Override
            public void onSuccess(Response response) {
                try {
                    responseRef.set(response);
                } finally {
                    latch.countDown();
                }
            }

            @Override
            public void onFailure(Exception exception) {
                try {
                    exceptionRef.set(exception);
                } finally {
                    latch.countDown();
                }
            }
        });
    }

    /**
     * @return responseMap from one of the async searches
     */
    private static Map<String, Object> duelSearchAsync(
        SearchRequest searchRequest,
        CheckedConsumer<ObjectPath, IOException> responseChecker
    ) throws Exception {
        searchRequest.setCcsMinimizeRoundtrips(true);
        ObjectPath minimizeRoundtripsResponse = submitAsyncSearch(searchRequest, TimeValue.timeValueSeconds(1));

        try {
            final String responseId = minimizeRoundtripsResponse.evaluate("id");// minimizeRoundtripsResponse.getId();
            assertBusy(() -> {
                ObjectPath resp = getAsyncSearch(responseId);
                assertThat(resp.evaluate("is_running"), equalTo(false));
            });
            minimizeRoundtripsResponse = getAsyncSearch(responseId);
        } finally {
            deleteAsyncSearch(minimizeRoundtripsResponse.evaluate("id"));
        }

        searchRequest.setCcsMinimizeRoundtrips(false);
        ObjectPath fanOutResponse = submitAsyncSearch(searchRequest, TimeValue.timeValueSeconds(1));
        try {
            final String responseId = fanOutResponse.evaluate("id");
            assertBusy(() -> {
                ObjectPath resp = getAsyncSearch(responseId);
                assertThat(resp.evaluate("is_running"), equalTo(false));
            });
            fanOutResponse = getAsyncSearch(responseId);
        } finally {
            deleteAsyncSearch(fanOutResponse.evaluate("id"));
        }

        // extract the response
        minimizeRoundtripsResponse = new ObjectPath(minimizeRoundtripsResponse.evaluate("response"));
        fanOutResponse = new ObjectPath(fanOutResponse.evaluate("response"));

        responseChecker.accept(minimizeRoundtripsResponse);

        // if only the remote cluster was searched, then only one reduce phase is expected
        int expectedReducePhasesMinRoundTrip = 1;
        if (searchRequest.indices().length > 1) {
            expectedReducePhasesMinRoundTrip = searchRequest.indices().length + 1;
        }
        if (expectedReducePhasesMinRoundTrip == 1) {
            assertThat(
                minimizeRoundtripsResponse.evaluate("num_reduce_phases"),
                anyOf(equalTo(expectedReducePhasesMinRoundTrip), nullValue())
            );
        } else {
            assertThat(minimizeRoundtripsResponse.evaluate("num_reduce_phases"), equalTo(expectedReducePhasesMinRoundTrip));
        }

        responseChecker.accept(fanOutResponse);
        assertThat(fanOutResponse.evaluate("num_reduce_phases"), anyOf(equalTo(1), nullValue())); // default value is 1?

        assertThat(minimizeRoundtripsResponse.evaluate("_cluster.total"), equalTo(fanOutResponse.evaluate("_cluster.total")));
        assertThat(minimizeRoundtripsResponse.evaluate("_cluster.successful"), equalTo(fanOutResponse.evaluate("_cluster.successful")));
        assertThat(minimizeRoundtripsResponse.evaluate("_cluster.skipped"), equalTo(fanOutResponse.evaluate("_cluster.skipped")));
        assertThat(minimizeRoundtripsResponse.evaluate("_cluster.running"), equalTo(fanOutResponse.evaluate("_cluster.running")));
        assertThat(minimizeRoundtripsResponse.evaluate("_cluster.partial"), equalTo(fanOutResponse.evaluate("_cluster.partial")));
        assertThat(minimizeRoundtripsResponse.evaluate("_cluster.failed"), equalTo(fanOutResponse.evaluate("_cluster.failed")));
        Map<String, Object> minimizeRoundtripsResponseMap = responseToMap(minimizeRoundtripsResponse);
        if (minimizeRoundtripsResponse.evaluate("_clusters") != null && fanOutResponse.evaluate("_clusters") != null) {
            Map<String, Object> fanOutResponseMap = responseToMap(fanOutResponse);
            compareResponseMaps(minimizeRoundtripsResponseMap, fanOutResponseMap, "Comparing async_search minimizeRoundTrip vs. fanOut");
            assertThat(
                minimizeRoundtripsResponse.evaluate("_shards.skipped"),
                lessThanOrEqualTo((Integer) fanOutResponse.evaluate("_shards.skipped"))
            );
        }
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

    private static ObjectPath submitAsyncSearch(SearchRequest searchRequest, TimeValue waitForCompletion) throws IOException {
        String indices = Strings.collectionToDelimitedString(List.of(searchRequest.indices()), ",");
        final Request request = new Request("POST", URLEncoder.encode(indices, StandardCharsets.UTF_8) + "/_async_search");

        request.addParameter("wait_for_completion_timeout", waitForCompletion.toString());
        request.addParameter("ccs_minimize_roundtrips", Boolean.toString(searchRequest.isCcsMinimizeRoundtrips()));
        request.addParameter("batched_reduce_size", Integer.toString(searchRequest.getBatchedReduceSize()));
        request.addParameter("keep_on_completion", "true");
        request.addParameter(RestSearchAction.TYPED_KEYS_PARAM, "true");
        request.setEntity(createEntity(searchRequest.source(), XContentType.JSON, ToXContent.EMPTY_PARAMS));
        Response resp = client().performRequest(request);
        return ObjectPath.createFromResponse(resp);
    }

    private static ObjectPath getAsyncSearch(String id) throws IOException {
        final Request request = new Request("GET", "/_async_search/" + id);
        request.addParameter("wait_for_completion_timeout", "0ms");
        request.addParameter(RestSearchAction.TYPED_KEYS_PARAM, "true");
        Response resp = client().performRequest(request);
        return ObjectPath.createFromResponse(resp);
    }

    private static Response deleteAsyncSearch(String id) throws IOException {
        final Request request = new Request("DELETE", "/_async_search/" + id);
        return client().performRequest(request);
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

    private static void assertMultiClusterSearchResponse(ObjectPath searchResponse) throws IOException {
        assertThat(searchResponse.evaluate("_clusters.total"), equalTo(2));
        // for bwc checks we expect SUCCESSFUL + PARTIAL to be equal to 2
        int bwcSuccessful = searchResponse.evaluate("_clusters.successful");
        bwcSuccessful += (Integer) searchResponse.evaluate("_clusters.partial");
        assertEquals(2, bwcSuccessful);
        assertThat(searchResponse.evaluate("_clusters.skipped"), equalTo(0));
        assertThat(searchResponse.evaluate("_clusters.running"), equalTo(0));
        assertThat(searchResponse.evaluate("_clusters.failed"), equalTo(0));
        assertThat(searchResponse.evaluate("_shards.total"), greaterThan(1));
        assertThat(searchResponse.evaluate("_shards.successful"), greaterThan(1));
    }

    private static void assertSingleRemoteClusterSearchResponse(ObjectPath searchResponse) throws IOException {
        assertThat(searchResponse.evaluate("_clusters.total"), equalTo(1));
        assertThat(searchResponse.evaluate("_clusters.successful"), equalTo(1));
        assertThat(searchResponse.evaluate("_shards.total"), greaterThanOrEqualTo(1));
        assertThat(searchResponse.evaluate("_shards.successful"), greaterThanOrEqualTo(1));
    }

    private static void assertHits(ObjectPath response) throws IOException {
        assertHits(response, 0);
    }

    private static void assertHits(ObjectPath response, int from) throws IOException {
        int totalClusters = response.evaluate("_clusters.total");
        if (totalClusters == 1) {
            assertSingleRemoteClusterSearchResponse(response);
        } else {
            assertMultiClusterSearchResponse(response);
        }
        int totalHits = response.evaluate("hits.total.value");
        assertThat(totalHits, greaterThan(0));
        assertThat(response.evaluate("_shards.failed"), Matchers.equalTo(0));
        assertNull(response.evaluate("hits.aggregations"));
        assertNull(response.evaluate("hits.suggest"));
        if (totalHits > from) {
            assertThat(response.evaluateArraySize("hits.hits"), greaterThan(0));
        } else {
            assertThat(response.evaluateArraySize("hits.hits"), equalTo(0));
        }
    }

    private static void assertAggs(ObjectPath response) throws IOException {
        int totalHits = response.evaluate("_clusters.total");
        if (totalHits == 1) {
            assertSingleRemoteClusterSearchResponse(response);
        } else {
            assertMultiClusterSearchResponse(response);
        }
        assertThat(response.evaluate("hits.total.value"), greaterThan(0));
        assertThat(response.evaluateArraySize("hits.hits"), equalTo(0));
        assertNull(response.evaluate("suggest"));
        assertNotNull(response.evaluate("aggregations"));
        Set<String> aggregations = response.evaluateMapKeys("aggregations");
        for (String aggregation : aggregations) {
            if (aggregation.startsWith("date_histogram") || aggregation.startsWith("sterms")) {
                assertThat(
                    aggregation + " has 0 buckets",
                    response.evaluateArraySize("aggregations." + aggregation + ".buckets"),
                    greaterThan(0)
                );
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> responseToMap(ObjectPath response) throws IOException {
        BytesReference bytesReference = BytesReference.bytes(response.toXContentBuilder(XContentType.JSON.xContent()));
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
                 * the results we could possibly need from the remote
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
        Map<String, Object> clusters = (Map<String, Object>) responseMap.get("_clusters");
        homogenizeClustersEntries(clusters);
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

    private static void homogenizeClustersEntries(Map<String, Object> map) {
        replaceTookTime(map);
        replaceSkippedEntries(map);
    }

    @SuppressWarnings("unchecked")
    private static void replaceSkippedEntries(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().contains("skipped")) {
                assertThat(entry.getValue(), instanceOf(Number.class));
                assertNotNull(entry.setValue(0));
            }
            if (entry.getValue() instanceof Map) {
                replaceSkippedEntries((Map<String, Object>) entry.getValue());
            }
            if (entry.getValue() instanceof List) {
                List<Object> list = (List<Object>) entry.getValue();
                for (Object obj : list) {
                    if (obj instanceof Map) {
                        replaceSkippedEntries((Map<String, Object>) obj);
                    }
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private static void replaceTookTime(Map<String, Object> map) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getKey().contains("took")) {
                assertThat(entry.getValue(), instanceOf(Number.class));
                assertNotNull(entry.setValue(-1));
            }
            if (entry.getValue() instanceof Map) {
                replaceTookTime((Map<String, Object>) entry.getValue());
            }
            if (entry.getValue() instanceof List) {
                List<Object> list = (List<Object>) entry.getValue();
                for (Object obj : list) {
                    if (obj instanceof Map) {
                        replaceTookTime((Map<String, Object>) obj);
                    }
                }
            }
        }
    }

}
