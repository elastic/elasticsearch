/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.documentation;

import org.apache.lucene.search.Explanation;
import org.apache.lucene.search.TotalHits;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.OpenPointInTimeRequest;
import org.elasticsearch.action.search.OpenPointInTimeResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.CreateIndexResponse;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.document.DocumentField;
import org.elasticsearch.common.text.Text;
import org.elasticsearch.common.unit.Fuzziness;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.get.GetResult;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.rankeval.EvalQueryQuality;
import org.elasticsearch.index.rankeval.EvaluationMetric;
import org.elasticsearch.index.rankeval.MetricDetail;
import org.elasticsearch.index.rankeval.PrecisionAtK;
import org.elasticsearch.index.rankeval.RankEvalRequest;
import org.elasticsearch.index.rankeval.RankEvalResponse;
import org.elasticsearch.index.rankeval.RankEvalSpec;
import org.elasticsearch.index.rankeval.RatedDocument;
import org.elasticsearch.index.rankeval.RatedRequest;
import org.elasticsearch.index.rankeval.RatedSearchHit;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse.Item;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.Terms.Bucket;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.Avg;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightField;
import org.elasticsearch.search.profile.ProfileResult;
import org.elasticsearch.search.profile.SearchProfileShardResult;
import org.elasticsearch.search.profile.aggregation.AggregationProfileShardResult;
import org.elasticsearch.search.profile.query.CollectorResult;
import org.elasticsearch.search.profile.query.QueryProfileShardResult;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.SuggestBuilders;
import org.elasticsearch.search.suggest.SuggestionBuilder;
import org.elasticsearch.search.suggest.term.TermSuggestion;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.index.query.QueryBuilders.matchQuery;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;

/**
 * Documentation for search APIs in the high level java client.
 * Code wrapped in {@code tag} and {@code end} tags is included in the docs.
 */
public class SearchDocumentationIT extends ESRestHighLevelClientTestCase {

    @SuppressWarnings({"unused", "unchecked"})
    public void testSearch() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();
        {
            // tag::search-request-basic
            SearchRequest searchRequest = new SearchRequest(); // <1>
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); // <2>
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()); // <3>
            searchRequest.source(searchSourceBuilder); // <4>
            // end::search-request-basic
        }
        {
            // tag::search-request-indices
            SearchRequest searchRequest = new SearchRequest("posts"); // <1>
            // end::search-request-indices
            // tag::search-request-routing
            searchRequest.routing("routing"); // <1>
            // end::search-request-routing
            // tag::search-request-indicesOptions
            searchRequest.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
            // end::search-request-indicesOptions
            // tag::search-request-preference
            searchRequest.preference("_local"); // <1>
            // end::search-request-preference
            assertNotNull(client.search(searchRequest, RequestOptions.DEFAULT));
        }
        {
            // tag::search-source-basics
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); // <1>
            sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy")); // <2>
            sourceBuilder.from(0); // <3>
            sourceBuilder.size(5); // <4>
            sourceBuilder.timeout(new TimeValue(60, TimeUnit.SECONDS)); // <5>
            // end::search-source-basics

            // tag::search-source-sorting
            sourceBuilder.sort(new ScoreSortBuilder().order(SortOrder.DESC)); // <1>
            sourceBuilder.sort(new FieldSortBuilder("id").order(SortOrder.ASC));  // <2>
            // end::search-source-sorting

            // tag::search-source-filtering-off
            sourceBuilder.fetchSource(false);
            // end::search-source-filtering-off
            // tag::search-source-filtering-includes
            String[] includeFields = new String[] {"title", "innerObject.*"};
            String[] excludeFields = new String[] {"user"};
            sourceBuilder.fetchSource(includeFields, excludeFields);
            // end::search-source-filtering-includes
            sourceBuilder.fetchSource(true);

            // tag::search-source-setter
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.indices("posts");
            searchRequest.source(sourceBuilder);
            // end::search-source-setter

            // tag::search-execute
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // end::search-execute

            // tag::search-execute-listener
            ActionListener<SearchResponse> listener = new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::search-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::search-execute-async
            client.searchAsync(searchRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::search-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

            // tag::search-response-1
            RestStatus status = searchResponse.status();
            TimeValue took = searchResponse.getTook();
            Boolean terminatedEarly = searchResponse.isTerminatedEarly();
            boolean timedOut = searchResponse.isTimedOut();
            // end::search-response-1

            // tag::search-response-2
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();
            int failedShards = searchResponse.getFailedShards();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                // failures should be handled here
            }
            // end::search-response-2
            assertNotNull(searchResponse);

            // tag::search-hits-get
            SearchHits hits = searchResponse.getHits();
            // end::search-hits-get
            // tag::search-hits-info
            TotalHits totalHits = hits.getTotalHits();
            // the total number of hits, must be interpreted in the context of totalHits.relation
            long numHits = totalHits.value;
            // whether the number of hits is accurate (EQUAL_TO) or a lower bound of the total (GREATER_THAN_OR_EQUAL_TO)
            TotalHits.Relation relation = totalHits.relation;
            float maxScore = hits.getMaxScore();
            // end::search-hits-info
            // tag::search-hits-singleHit
            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                // do something with the SearchHit
            }
            // end::search-hits-singleHit
            for (SearchHit hit : searchHits) {
                // tag::search-hits-singleHit-properties
                String index = hit.getIndex();
                String id = hit.getId();
                float score = hit.getScore();
                // end::search-hits-singleHit-properties
                // tag::search-hits-singleHit-source
                String sourceAsString = hit.getSourceAsString();
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                String documentTitle = (String) sourceAsMap.get("title");
                List<Object> users = (List<Object>) sourceAsMap.get("user");
                Map<String, Object> innerObject =
                        (Map<String, Object>) sourceAsMap.get("innerObject");
                // end::search-hits-singleHit-source
            }
            assertEquals(3, numHits);
            assertEquals(TotalHits.Relation.EQUAL_TO, relation);
            assertNotNull(hits.getHits()[0].getSourceAsString());
            assertNotNull(hits.getHits()[0].getSourceAsMap().get("title"));
            assertNotNull(hits.getHits()[0].getSourceAsMap().get("innerObject"));
            assertNull(hits.getHits()[0].getSourceAsMap().get("user"));
        }
    }

    @SuppressWarnings("unused")
    public void testBuildingSearchQueries() {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::search-query-builder-ctor
            MatchQueryBuilder matchQueryBuilder = new MatchQueryBuilder("user", "kimchy"); // <1>
            // end::search-query-builder-ctor
            // tag::search-query-builder-options
            matchQueryBuilder.fuzziness(Fuzziness.AUTO); // <1>
            matchQueryBuilder.prefixLength(3); // <2>
            matchQueryBuilder.maxExpansions(10); // <3>
            // end::search-query-builder-options
        }
        {
            // tag::search-query-builders
            QueryBuilder matchQueryBuilder = QueryBuilders.matchQuery("user", "kimchy")
                                                            .fuzziness(Fuzziness.AUTO)
                                                            .prefixLength(3)
                                                            .maxExpansions(10);
            // end::search-query-builders
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            // tag::search-query-setter
            searchSourceBuilder.query(matchQueryBuilder);
            // end::search-query-setter
        }
    }

    @SuppressWarnings({ "unused" })
    public void testSearchRequestAggregations() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest("posts").id("1")
                    .source(XContentType.JSON, "company", "Elastic", "age", 20));
            request.add(new IndexRequest("posts").id("2")
                    .source(XContentType.JSON, "company", "Elastic", "age", 30));
            request.add(new IndexRequest("posts").id("3")
                    .source(XContentType.JSON, "company", "Elastic", "age", 40));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            assertSame(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            // tag::search-request-aggregations
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            TermsAggregationBuilder aggregation = AggregationBuilders.terms("by_company")
                    .field("company.keyword");
            aggregation.subAggregation(AggregationBuilders.avg("average_age")
                    .field("age"));
            searchSourceBuilder.aggregation(aggregation);
            // end::search-request-aggregations
            searchSourceBuilder.query(QueryBuilders.matchAllQuery());
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            {
                // tag::search-request-aggregations-get
                Aggregations aggregations = searchResponse.getAggregations();
                Terms byCompanyAggregation = aggregations.get("by_company"); // <1>
                Bucket elasticBucket = byCompanyAggregation.getBucketByKey("Elastic"); // <2>
                Avg averageAge = elasticBucket.getAggregations().get("average_age"); // <3>
                double avg = averageAge.getValue();
                // end::search-request-aggregations-get

                try {
                    // tag::search-request-aggregations-get-wrongCast
                    Range range = aggregations.get("by_company"); // <1>
                    // end::search-request-aggregations-get-wrongCast
                } catch (ClassCastException ex) {
                    String message = ex.getMessage();
                    assertThat(message, containsString("org.elasticsearch.search.aggregations.bucket.terms.ParsedStringTerms"));
                    assertThat(message, containsString("org.elasticsearch.search.aggregations.bucket.range.Range"));
                }
                assertEquals(3, elasticBucket.getDocCount());
                assertEquals(30, avg, 0.0);
            }
            Aggregations aggregations = searchResponse.getAggregations();
            {
                // tag::search-request-aggregations-asMap
                Map<String, Aggregation> aggregationMap = aggregations.getAsMap();
                Terms companyAggregation = (Terms) aggregationMap.get("by_company");
                // end::search-request-aggregations-asMap
            }
            {
                // tag::search-request-aggregations-asList
                List<Aggregation> aggregationList = aggregations.asList();
                // end::search-request-aggregations-asList
            }
            {
                // tag::search-request-aggregations-iterator
                for (Aggregation agg : aggregations) {
                    String type = agg.getType();
                    if (type.equals(TermsAggregationBuilder.NAME)) {
                        Bucket elasticBucket = ((Terms) agg).getBucketByKey("Elastic");
                        long numberOfDocs = elasticBucket.getDocCount();
                    }
                }
                // end::search-request-aggregations-iterator
            }
        }
    }

    @SuppressWarnings({"unused", "rawtypes"})
    public void testSearchRequestSuggestions() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest("posts").id("1").source(XContentType.JSON, "user", "kimchy"));
            request.add(new IndexRequest("posts").id("2").source(XContentType.JSON, "user", "javanna"));
            request.add(new IndexRequest("posts").id("3").source(XContentType.JSON, "user", "tlrx"));
            request.add(new IndexRequest("posts").id("4").source(XContentType.JSON, "user", "cbuescher"));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            assertSame(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            // tag::search-request-suggestion
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            SuggestionBuilder termSuggestionBuilder =
                SuggestBuilders.termSuggestion("user").text("kmichy"); // <1>
            SuggestBuilder suggestBuilder = new SuggestBuilder();
            suggestBuilder.addSuggestion("suggest_user", termSuggestionBuilder); // <2>
            searchSourceBuilder.suggest(suggestBuilder);
            // end::search-request-suggestion
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            {
                // tag::search-request-suggestion-get
                Suggest suggest = searchResponse.getSuggest(); // <1>
                TermSuggestion termSuggestion = suggest.getSuggestion("suggest_user"); // <2>
                for (TermSuggestion.Entry entry : termSuggestion.getEntries()) { // <3>
                    for (TermSuggestion.Entry.Option option : entry) { // <4>
                        String suggestText = option.getText().string();
                    }
                }
                // end::search-request-suggestion-get
                assertEquals(1, termSuggestion.getEntries().size());
                assertEquals(1, termSuggestion.getEntries().get(0).getOptions().size());
                assertEquals("kimchy", termSuggestion.getEntries().get(0).getOptions().get(0).getText().string());
            }
        }
    }

    @SuppressWarnings("unused")
    public void testSearchRequestHighlighting() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest("posts").id("1")
                    .source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?", "user",
                            Arrays.asList("kimchy", "luca"), "innerObject", Collections.singletonMap("key", "value")));
            request.add(new IndexRequest("posts").id("2")
                    .source(XContentType.JSON, "title", "Current status and upcoming changes in Elasticsearch", "user",
                            Arrays.asList("kimchy", "christoph"), "innerObject", Collections.singletonMap("key", "value")));
            request.add(new IndexRequest("posts").id("3")
                    .source(XContentType.JSON, "title", "The Future of Federated Search in Elasticsearch", "user",
                            Arrays.asList("kimchy", "tanguy"), "innerObject", Collections.singletonMap("key", "value")));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            assertSame(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            // tag::search-request-highlighting
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            HighlightBuilder highlightBuilder = new HighlightBuilder(); // <1>
            HighlightBuilder.Field highlightTitle =
                    new HighlightBuilder.Field("title"); // <2>
            highlightTitle.highlighterType("unified");  // <3>
            highlightBuilder.field(highlightTitle);  // <4>
            HighlightBuilder.Field highlightUser = new HighlightBuilder.Field("user");
            highlightBuilder.field(highlightUser);
            searchSourceBuilder.highlighter(highlightBuilder);
            // end::search-request-highlighting
            searchSourceBuilder.query(QueryBuilders.boolQuery()
                    .should(matchQuery("title", "Elasticsearch"))
                    .should(matchQuery("user", "kimchy")));
            searchRequest.source(searchSourceBuilder);
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            {
                // tag::search-request-highlighting-get
                SearchHits hits = searchResponse.getHits();
                for (SearchHit hit : hits.getHits()) {
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    HighlightField highlight = highlightFields.get("title"); // <1>
                    Text[] fragments = highlight.fragments();  // <2>
                    String fragmentString = fragments[0].string();
                }
                // end::search-request-highlighting-get
                hits = searchResponse.getHits();
                for (SearchHit hit : hits.getHits()) {
                    Map<String, HighlightField> highlightFields = hit.getHighlightFields();
                    HighlightField highlight = highlightFields.get("title");
                    Text[] fragments = highlight.fragments();
                    assertEquals(1, fragments.length);
                    assertThat(fragments[0].string(), containsString("<em>Elasticsearch</em>"));
                    highlight = highlightFields.get("user");
                    fragments = highlight.fragments();
                    assertEquals(1, fragments.length);
                    assertThat(fragments[0].string(), containsString("<em>kimchy</em>"));
                }
            }

        }
    }

    @SuppressWarnings("unused")
    public void testSearchRequestProfiling() throws IOException {
        RestHighLevelClient client = highLevelClient();
        {
            IndexRequest request = new IndexRequest("posts").id("1")
                    .source(XContentType.JSON, "tags", "elasticsearch", "comments", 123);
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);
            IndexResponse indexResponse = client.index(request, RequestOptions.DEFAULT);
            assertSame(RestStatus.CREATED, indexResponse.status());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            // tag::search-request-profiling
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.profile(true);
            // end::search-request-profiling
            searchSourceBuilder.query(QueryBuilders.termQuery("tags", "elasticsearch"));
            searchSourceBuilder.aggregation(AggregationBuilders.histogram("by_comments").field("comments").interval(100));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            // tag::search-request-profiling-get
            Map<String, SearchProfileShardResult> profilingResults =
                    searchResponse.getProfileResults(); // <1>
            for (Map.Entry<String, SearchProfileShardResult> profilingResult : profilingResults.entrySet()) { // <2>
                String key = profilingResult.getKey(); // <3>
                SearchProfileShardResult profileShardResult = profilingResult.getValue(); // <4>
            }
            // end::search-request-profiling-get

            SearchProfileShardResult profileShardResult = profilingResults.values().iterator().next();
            assertNotNull(profileShardResult);

            // tag::search-request-profiling-queries
            List<QueryProfileShardResult> queryProfileShardResults =
                    profileShardResult.getQueryProfileResults(); // <1>
            for (QueryProfileShardResult queryProfileResult : queryProfileShardResults) { // <2>

            }
            // end::search-request-profiling-queries
            assertThat(queryProfileShardResults.size(), equalTo(1));

            for (QueryProfileShardResult queryProfileResult : queryProfileShardResults) {
                // tag::search-request-profiling-queries-results
                for (ProfileResult profileResult : queryProfileResult.getQueryResults()) { // <1>
                    String queryName = profileResult.getQueryName(); // <2>
                    long queryTimeInMillis = profileResult.getTime(); // <3>
                    List<ProfileResult> profiledChildren = profileResult.getProfiledChildren(); // <4>
                }
                // end::search-request-profiling-queries-results

                // tag::search-request-profiling-queries-collectors
                CollectorResult collectorResult = queryProfileResult.getCollectorResult();  // <1>
                String collectorName = collectorResult.getName();  // <2>
                Long collectorTimeInMillis = collectorResult.getTime(); // <3>
                List<CollectorResult> profiledChildren = collectorResult.getProfiledChildren(); // <4>
                // end::search-request-profiling-queries-collectors
            }

            // tag::search-request-profiling-aggs
            AggregationProfileShardResult aggsProfileResults =
                    profileShardResult.getAggregationProfileResults(); // <1>
            for (ProfileResult profileResult : aggsProfileResults.getProfileResults()) { // <2>
                String aggName = profileResult.getQueryName(); // <3>
                long aggTimeInMillis = profileResult.getTime(); // <4>
                List<ProfileResult> profiledChildren = profileResult.getProfiledChildren(); // <5>
            }
            // end::search-request-profiling-aggs
            assertThat(aggsProfileResults.getProfileResults().size(), equalTo(1));
        }
    }

    public void testScroll() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            BulkRequest request = new BulkRequest();
            request.add(new IndexRequest("posts").id("1")
                    .source(XContentType.JSON, "title", "In which order are my Elasticsearch queries executed?"));
            request.add(new IndexRequest("posts").id("2")
                    .source(XContentType.JSON, "title", "Current status and upcoming changes in Elasticsearch"));
            request.add(new IndexRequest("posts").id("3")
                    .source(XContentType.JSON, "title", "The Future of Federated Search in Elasticsearch"));
            request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
            assertSame(RestStatus.OK, bulkResponse.status());
            assertFalse(bulkResponse.hasFailures());
        }
        {
            int size = 1;
            // tag::search-scroll-init
            SearchRequest searchRequest = new SearchRequest("posts");
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQuery("title", "Elasticsearch"));
            searchSourceBuilder.size(size); // <1>
            searchRequest.source(searchSourceBuilder);
            searchRequest.scroll(TimeValue.timeValueMinutes(1L)); // <2>
            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = searchResponse.getScrollId(); // <3>
            SearchHits hits = searchResponse.getHits();  // <4>
            // end::search-scroll-init
            assertEquals(3, hits.getTotalHits().value);
            assertEquals(1, hits.getHits().length);
            assertNotNull(scrollId);

            // tag::search-scroll2
            SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); // <1>
            scrollRequest.scroll(TimeValue.timeValueSeconds(30));
            SearchResponse searchScrollResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            scrollId = searchScrollResponse.getScrollId();  // <2>
            hits = searchScrollResponse.getHits(); // <3>
            assertEquals(3, hits.getTotalHits().value);
            assertEquals(1, hits.getHits().length);
            assertNotNull(scrollId);
            // end::search-scroll2

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            assertTrue(clearScrollResponse.isSucceeded());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.scroll("60s");

            SearchResponse initialSearchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
            String scrollId = initialSearchResponse.getScrollId();

            SearchScrollRequest scrollRequest = new SearchScrollRequest();
            scrollRequest.scrollId(scrollId);

            // tag::scroll-request-arguments
            scrollRequest.scroll(TimeValue.timeValueSeconds(60L)); // <1>
            scrollRequest.scroll("60s"); // <2>
            // end::scroll-request-arguments

            // tag::search-scroll-execute-sync
            SearchResponse searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
            // end::search-scroll-execute-sync

            assertEquals(0, searchResponse.getFailedShards());
            assertEquals(3L, searchResponse.getHits().getTotalHits().value);

            // tag::search-scroll-execute-listener
            ActionListener<SearchResponse> scrollListener =
                    new ActionListener<SearchResponse>() {
                @Override
                public void onResponse(SearchResponse searchResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::search-scroll-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            scrollListener = new LatchedActionListener<>(scrollListener, latch);

            // tag::search-scroll-execute-async
            client.scrollAsync(scrollRequest, RequestOptions.DEFAULT, scrollListener); // <1>
            // end::search-scroll-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

            // tag::clear-scroll-request
            ClearScrollRequest request = new ClearScrollRequest(); // <1>
            request.addScrollId(scrollId); // <2>
            // end::clear-scroll-request

            // tag::clear-scroll-add-scroll-id
            request.addScrollId(scrollId);
            // end::clear-scroll-add-scroll-id

            List<String> scrollIds = Collections.singletonList(scrollId);

            // tag::clear-scroll-add-scroll-ids
            request.setScrollIds(scrollIds);
            // end::clear-scroll-add-scroll-ids

            // tag::clear-scroll-execute
            ClearScrollResponse response = client.clearScroll(request, RequestOptions.DEFAULT);
            // end::clear-scroll-execute

            // tag::clear-scroll-response
            boolean success = response.isSucceeded(); // <1>
            int released = response.getNumFreed(); // <2>
            // end::clear-scroll-response
            assertTrue(success);
            assertThat(released, greaterThan(0));

            // tag::clear-scroll-execute-listener
            ActionListener<ClearScrollResponse> listener =
                    new ActionListener<ClearScrollResponse>() {
                @Override
                public void onResponse(ClearScrollResponse clearScrollResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::clear-scroll-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch clearScrollLatch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, clearScrollLatch);

            // tag::clear-scroll-execute-async
            client.clearScrollAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::clear-scroll-execute-async

            assertTrue(clearScrollLatch.await(30L, TimeUnit.SECONDS));
        }
        {
            // tag::search-scroll-example
            final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
            SearchRequest searchRequest = new SearchRequest("posts");
            searchRequest.scroll(scroll);
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(matchQuery("title", "Elasticsearch"));
            searchRequest.source(searchSourceBuilder);

            SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT); // <1>
            String scrollId = searchResponse.getScrollId();
            SearchHit[] searchHits = searchResponse.getHits().getHits();

            while (searchHits != null && searchHits.length > 0) { // <2>
                // <3>
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId); // <4>
                scrollRequest.scroll(scroll);
                searchResponse = client.scroll(scrollRequest, RequestOptions.DEFAULT);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();
            }

            ClearScrollRequest clearScrollRequest = new ClearScrollRequest(); // <5>
            clearScrollRequest.addScrollId(scrollId);
            ClearScrollResponse clearScrollResponse = client.clearScroll(clearScrollRequest, RequestOptions.DEFAULT);
            boolean succeeded = clearScrollResponse.isSucceeded();
            // end::search-scroll-example
            assertTrue(succeeded);
        }
    }

    public void testPointInTime() throws Exception {
        RestHighLevelClient client = highLevelClient();
        BulkRequest request = new BulkRequest();
        request.add(new IndexRequest("posts").id("1").source(XContentType.JSON, "lang", "Java"));
        request.add(new IndexRequest("posts").id("2").source(XContentType.JSON, "lang", "Python"));
        request.add(new IndexRequest("posts").id("3").source(XContentType.JSON, "lang", "Go"));
        request.add(new IndexRequest("posts").id("4").source(XContentType.JSON, "lang", "Rust"));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse = client.bulk(request, RequestOptions.DEFAULT);
        assertSame(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());

        // tag::open-point-in-time
        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest("posts"); // <1>
        openRequest.keepAlive(TimeValue.timeValueMinutes(30)); // <2>
        OpenPointInTimeResponse openResponse = client.openPointInTime(openRequest, RequestOptions.DEFAULT);
        String pitId = openResponse.getPointInTimeId(); // <3>
        // end::open-point-in-time
        assertNotNull(pitId);

        // tag::search-point-in-time
        SearchRequest searchRequest = new SearchRequest();
        final PointInTimeBuilder pointInTimeBuilder = new PointInTimeBuilder(pitId); // <1>
        pointInTimeBuilder.setKeepAlive("2m"); // <2>
        searchRequest.source(new SearchSourceBuilder().pointInTimeBuilder(pointInTimeBuilder)); // <3>
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        // end::search-point-in-time
        assertThat(searchResponse.pointInTimeId(), equalTo(pitId));

        // tag::close-point-in-time
        ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest(pitId); // <1>
        ClearScrollResponse closeResponse = client.closePointInTime(closeRequest, RequestOptions.DEFAULT);
        boolean succeeded = closeResponse.isSucceeded();
        // end::close-point-in-time
        assertTrue(succeeded);

        // Open a point in time with optional arguments
        {
            openRequest = new OpenPointInTimeRequest("posts").keepAlive(TimeValue.timeValueMinutes(10));
            // tag::open-point-in-time-indices-option
            openRequest.indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED); // <1>
            // end::open-point-in-time-indices-option

            // tag::open-point-in-time-routing
            openRequest.routing("routing"); // <1>
            // end::open-point-in-time-routing

            // tag::open-point-in-time-preference
            openRequest.preference("_local"); // <1>
            // end::open-point-in-time-preference

            openResponse = client.openPointInTime(openRequest, RequestOptions.DEFAULT);
            pitId = openResponse.getPointInTimeId();
            client.closePointInTime(new ClosePointInTimeRequest(pitId), RequestOptions.DEFAULT);
        }
    }

    public void testSearchTemplateWithInlineScript() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();

        // tag::search-template-request-inline
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest("posts")); // <1>

        request.setScriptType(ScriptType.INLINE);
        request.setScript( // <2>
            "{" +
            "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
            "  \"size\" : \"{{size}}\"" +
            "}");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("field", "title");
        scriptParams.put("value", "elasticsearch");
        scriptParams.put("size", 5);
        request.setScriptParams(scriptParams); // <3>
        // end::search-template-request-inline

        // tag::search-template-response
        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        SearchResponse searchResponse = response.getResponse();
        // end::search-template-response

        assertNotNull(searchResponse);
        assertTrue(searchResponse.getHits().getTotalHits().value > 0);

        // tag::render-search-template-request
        request.setSimulate(true); // <1>
        // end::render-search-template-request

        // tag::render-search-template-response
        SearchTemplateResponse renderResponse = client.searchTemplate(request, RequestOptions.DEFAULT);
        BytesReference source = renderResponse.getSource(); // <1>
        // end::render-search-template-response

        assertNotNull(source);
        assertEquals(
            ("{  \"size\" : \"5\",  \"query\": { \"match\" : { \"title\" : \"elasticsearch\" } }}").replaceAll("\\s+", ""),
            source.utf8ToString()
        );
    }

    public void testSearchTemplateWithStoredScript() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();
        RestClient restClient = client();

        registerQueryScript(restClient);

        // tag::search-template-request-stored
        SearchTemplateRequest request = new SearchTemplateRequest();
        request.setRequest(new SearchRequest("posts"));

        request.setScriptType(ScriptType.STORED);
        request.setScript("title_search");

        Map<String, Object> params = new HashMap<>();
        params.put("field", "title");
        params.put("value", "elasticsearch");
        params.put("size", 5);
        request.setScriptParams(params);
        // end::search-template-request-stored

        // tag::search-template-request-options
        request.setExplain(true);
        request.setProfile(true);
        // end::search-template-request-options

        // tag::search-template-execute
        SearchTemplateResponse response = client.searchTemplate(request, RequestOptions.DEFAULT);
        // end::search-template-execute

        SearchResponse searchResponse = response.getResponse();
        assertNotNull(searchResponse);
        assertTrue(searchResponse.getHits().getTotalHits().value > 0);

        // tag::search-template-execute-listener
        ActionListener<SearchTemplateResponse> listener = new ActionListener<SearchTemplateResponse>() {
            @Override
            public void onResponse(SearchTemplateResponse response) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::search-template-execute-listener

        // Replace the empty listener by a blocking listener for tests.
        CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::search-template-execute-async
        client.searchTemplateAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::search-template-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }


    @SuppressWarnings("unused")
    public void testMultiSearchTemplateWithInlineScript() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();

        // tag::multi-search-template-request-inline
        String [] searchTerms = {"elasticsearch", "logstash", "kibana"};

        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest(); // <1>
        for (String searchTerm : searchTerms) {
            SearchTemplateRequest request = new SearchTemplateRequest();  // <2>
            request.setRequest(new SearchRequest("posts"));

            request.setScriptType(ScriptType.INLINE);
            request.setScript(
                "{" +
                "  \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
                "  \"size\" : \"{{size}}\"" +
                "}");

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "title");
            scriptParams.put("value", searchTerm);
            scriptParams.put("size", 5);
            request.setScriptParams(scriptParams);

            multiRequest.add(request);  // <3>
        }
        // end::multi-search-template-request-inline

        // tag::multi-search-template-request-sync
        MultiSearchTemplateResponse multiResponse = client.msearchTemplate(multiRequest, RequestOptions.DEFAULT);
        // end::multi-search-template-request-sync

        // tag::multi-search-template-response
        for (Item item : multiResponse.getResponses()) { // <1>
            if (item.isFailure()) {
                String error = item.getFailureMessage(); // <2>
            } else {
                SearchTemplateResponse searchTemplateResponse = item.getResponse(); // <3>
                SearchResponse searchResponse = searchTemplateResponse.getResponse();
                searchResponse.getHits();
            }
        }
        // end::multi-search-template-response

        assertNotNull(multiResponse);
        assertEquals(searchTerms.length, multiResponse.getResponses().length);
        assertNotNull(multiResponse.getResponses()[0]);
        SearchResponse searchResponse = multiResponse.getResponses()[0].getResponse().getResponse();
        assertTrue(searchResponse.getHits().getTotalHits().value > 0);

    }

    public void testMultiSearchTemplateWithStoredScript() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();
        RestClient restClient = client();

        registerQueryScript(restClient);

        // tag::multi-search-template-request-stored
        MultiSearchTemplateRequest multiRequest = new MultiSearchTemplateRequest();

        String [] searchTerms = {"elasticsearch", "logstash", "kibana"};
        for (String searchTerm : searchTerms) {

            SearchTemplateRequest request = new SearchTemplateRequest();
            request.setRequest(new SearchRequest("posts"));

            request.setScriptType(ScriptType.STORED);
            request.setScript("title_search");

            Map<String, Object> params = new HashMap<>();
            params.put("field", "title");
            params.put("value", searchTerm);
            params.put("size", 5);
            request.setScriptParams(params);
            multiRequest.add(request);
        }
        // end::multi-search-template-request-stored




        // tag::multi-search-template-execute
        MultiSearchTemplateResponse multiResponse = client.msearchTemplate(multiRequest, RequestOptions.DEFAULT);
        // end::multi-search-template-execute

        assertNotNull(multiResponse);
        assertEquals(searchTerms.length, multiResponse.getResponses().length);
        assertNotNull(multiResponse.getResponses()[0]);
        SearchResponse searchResponse = multiResponse.getResponses()[0].getResponse().getResponse();
        assertTrue(searchResponse.getHits().getTotalHits().value > 0);

        // tag::multi-search-template-execute-listener
        ActionListener<MultiSearchTemplateResponse> listener = new ActionListener<MultiSearchTemplateResponse>() {
            @Override
            public void onResponse(MultiSearchTemplateResponse response) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::multi-search-template-execute-listener

        // Replace the empty listener by a blocking listener for tests.
        CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::multi-search-template-execute-async
        client.msearchTemplateAsync(multiRequest, RequestOptions.DEFAULT, listener);
        // end::multi-search-template-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    protected void registerQueryScript(RestClient restClient) throws IOException {
        // tag::register-script
        Request scriptRequest = new Request("POST", "_scripts/title_search");
        scriptRequest.setJsonEntity(
            "{" +
            "  \"script\": {" +
            "    \"lang\": \"mustache\"," +
            "    \"source\": {" +
            "      \"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" } }," +
            "      \"size\" : \"{{size}}\"" +
            "    }" +
            "  }" +
            "}");
        Response scriptResponse = restClient.performRequest(scriptRequest);
        // end::register-script
        assertEquals(RestStatus.OK.getStatus(), scriptResponse.getStatusLine().getStatusCode());
    }


    public void testExplain() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();

        // tag::explain-request
        ExplainRequest request = new ExplainRequest("contributors", "1");
        request.query(QueryBuilders.termQuery("user", "tanguy"));
        // end::explain-request

        // tag::explain-request-routing
        request.routing("routing"); // <1>
        // end::explain-request-routing

        // tag::explain-request-preference
        request.preference("_local"); // <1>
        // end::explain-request-preference

        // tag::explain-request-source
        request.fetchSourceContext(new FetchSourceContext(true, new String[]{"user"}, null)); // <1>
        // end::explain-request-source

        // tag::explain-request-stored-field
        request.storedFields(new String[]{"user"}); // <1>
        // end::explain-request-stored-field

        // tag::explain-execute
        ExplainResponse response = client.explain(request, RequestOptions.DEFAULT);
        // end::explain-execute

        // tag::explain-response
        String index = response.getIndex(); // <1>
        String id = response.getId(); // <2>
        boolean exists = response.isExists(); // <3>
        boolean match = response.isMatch(); // <4>
        boolean hasExplanation = response.hasExplanation(); // <5>
        Explanation explanation = response.getExplanation(); // <6>
        GetResult getResult = response.getGetResult(); // <7>
        // end::explain-response
        assertThat(index, equalTo("contributors"));
        assertThat(id, equalTo("1"));
        assertTrue(exists);
        assertTrue(match);
        assertTrue(hasExplanation);
        assertNotNull(explanation);
        assertNotNull(getResult);

        // tag::get-result
        Map<String, Object> source = getResult.getSource(); // <1>
        Map<String, DocumentField> fields = getResult.getFields(); // <2>
        // end::get-result
        assertThat(source, equalTo(Collections.singletonMap("user", "tanguy")));
        assertThat(fields.get("user").getValue(), equalTo("tanguy"));

        // tag::explain-execute-listener
        ActionListener<ExplainResponse> listener = new ActionListener<ExplainResponse>() {
            @Override
            public void onResponse(ExplainResponse explainResponse) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::explain-execute-listener

        CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::explain-execute-async
        client.explainAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::explain-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testFieldCaps() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();

        // tag::field-caps-request
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .fields("user")
            .indices("posts", "authors", "contributors");
        // end::field-caps-request

        // tag::field-caps-request-indicesOptions
        request.indicesOptions(IndicesOptions.lenientExpandOpen()); // <1>
        // end::field-caps-request-indicesOptions

        // tag::field-caps-execute
        FieldCapabilitiesResponse response = client.fieldCaps(request, RequestOptions.DEFAULT);
        // end::field-caps-execute

        // tag::field-caps-response
        Map<String, FieldCapabilities> userResponse = response.getField("user");  // <1>
        FieldCapabilities textCapabilities = userResponse.get("keyword");

        boolean isSearchable = textCapabilities.isSearchable();
        boolean isAggregatable = textCapabilities.isAggregatable();

        String[] indices = textCapabilities.indices(); // <2>
        String[] nonSearchableIndices = textCapabilities.nonSearchableIndices(); // <3>
        String[] nonAggregatableIndices = textCapabilities.nonAggregatableIndices();//<4>
        // end::field-caps-response

        assertThat(userResponse.keySet(), containsInAnyOrder("keyword", "text"));

        assertTrue(isSearchable);
        assertFalse(isAggregatable);

        assertArrayEquals(indices, new String[]{"authors", "contributors"});
        assertNull(nonSearchableIndices);
        assertArrayEquals(nonAggregatableIndices, new String[]{"authors"});

        // tag::field-caps-execute-listener
        ActionListener<FieldCapabilitiesResponse> listener = new ActionListener<FieldCapabilitiesResponse>() {
            @Override
            public void onResponse(FieldCapabilitiesResponse response) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::field-caps-execute-listener

        // Replace the empty listener by a blocking listener for tests.
        CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::field-caps-execute-async
        client.fieldCapsAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::field-caps-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testRankEval() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();
        {
            // tag::rank-eval-request-basic
            EvaluationMetric metric = new PrecisionAtK();                 // <1>
            List<RatedDocument> ratedDocs = new ArrayList<>();
            ratedDocs.add(new RatedDocument("posts", "1", 1));            // <2>
            SearchSourceBuilder searchQuery = new SearchSourceBuilder();
            searchQuery.query(QueryBuilders.matchQuery("user", "kimchy"));// <3>
            RatedRequest ratedRequest =                                   // <4>
                    new RatedRequest("kimchy_query", ratedDocs, searchQuery);
            List<RatedRequest> ratedRequests = Arrays.asList(ratedRequest);
            RankEvalSpec specification =
                    new RankEvalSpec(ratedRequests, metric);              // <5>
            RankEvalRequest request =                                     // <6>
                    new RankEvalRequest(specification, new String[] { "posts" });
            // end::rank-eval-request-basic

            // tag::rank-eval-execute
            RankEvalResponse response = client.rankEval(request, RequestOptions.DEFAULT);
            // end::rank-eval-execute

            // tag::rank-eval-response
            double evaluationResult = response.getMetricScore();   // <1>
            assertEquals(1.0 / 3.0, evaluationResult, 0.0);
            Map<String, EvalQueryQuality> partialResults =
                    response.getPartialResults();
            EvalQueryQuality evalQuality =
                    partialResults.get("kimchy_query");                 // <2>
            assertEquals("kimchy_query", evalQuality.getId());
            double qualityLevel = evalQuality.metricScore();        // <3>
            assertEquals(1.0 / 3.0, qualityLevel, 0.0);
            List<RatedSearchHit> hitsAndRatings = evalQuality.getHitsAndRatings();
            RatedSearchHit ratedSearchHit = hitsAndRatings.get(2);
            assertEquals("3", ratedSearchHit.getSearchHit().getId());   // <4>
            assertFalse(ratedSearchHit.getRating().isPresent());        // <5>
            MetricDetail metricDetails = evalQuality.getMetricDetails();
            String metricName = metricDetails.getMetricName();
            assertEquals(PrecisionAtK.NAME, metricName);                // <6>
            PrecisionAtK.Detail detail = (PrecisionAtK.Detail) metricDetails;
            assertEquals(1, detail.getRelevantRetrieved());             // <7>
            assertEquals(3, detail.getRetrieved());
            // end::rank-eval-response

            // tag::rank-eval-execute-listener
            ActionListener<RankEvalResponse> listener = new ActionListener<RankEvalResponse>() {
                @Override
                public void onResponse(RankEvalResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::rank-eval-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::rank-eval-execute-async
            client.rankEvalAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::rank-eval-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testMultiSearch() throws Exception {
        indexSearchTestData();
        RestHighLevelClient client = highLevelClient();
        {
            // tag::multi-search-request-basic
            MultiSearchRequest request = new MultiSearchRequest();    // <1>
            SearchRequest firstSearchRequest = new SearchRequest();   // <2>
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("user", "kimchy"));
            firstSearchRequest.source(searchSourceBuilder);
            request.add(firstSearchRequest);                          // <3>
            SearchRequest secondSearchRequest = new SearchRequest();  // <4>
            searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(QueryBuilders.matchQuery("user", "luca"));
            secondSearchRequest.source(searchSourceBuilder);
            request.add(secondSearchRequest);
            // end::multi-search-request-basic
            // tag::multi-search-execute
            MultiSearchResponse response = client.msearch(request, RequestOptions.DEFAULT);
            // end::multi-search-execute
            // tag::multi-search-response
            MultiSearchResponse.Item firstResponse = response.getResponses()[0];   // <1>
            assertNull(firstResponse.getFailure());                                // <2>
            SearchResponse searchResponse = firstResponse.getResponse();           // <3>
            assertEquals(4, searchResponse.getHits().getTotalHits().value);
            MultiSearchResponse.Item secondResponse = response.getResponses()[1];  // <4>
            assertNull(secondResponse.getFailure());
            searchResponse = secondResponse.getResponse();
            assertEquals(1, searchResponse.getHits().getTotalHits().value);
            // end::multi-search-response

            // tag::multi-search-execute-listener
            ActionListener<MultiSearchResponse> listener = new ActionListener<MultiSearchResponse>() {
                @Override
                public void onResponse(MultiSearchResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::multi-search-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::multi-search-execute-async
            client.msearchAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::multi-search-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private void indexSearchTestData() throws IOException {
        CreateIndexRequest authorsRequest = new CreateIndexRequest("authors")
            .mapping(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("id")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("user")
                        .field("type", "keyword")
                        .field("doc_values", "false")
                    .endObject()
                .endObject()
            .endObject());
        CreateIndexResponse authorsResponse = highLevelClient().indices().create(authorsRequest, RequestOptions.DEFAULT);
        assertTrue(authorsResponse.isAcknowledged());

        CreateIndexRequest reviewersRequest = new CreateIndexRequest("contributors")
            .mapping(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("id")
                        .field("type", "keyword")
                    .endObject()
                    .startObject("user")
                        .field("type", "keyword")
                        .field("store", "true")
                    .endObject()
                .endObject()
            .endObject());
        CreateIndexResponse reviewersResponse = highLevelClient().indices().create(reviewersRequest, RequestOptions.DEFAULT);
        assertTrue(reviewersResponse.isAcknowledged());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("posts").id("1")
                .source(XContentType.JSON, "id", 1, "title", "In which order are my Elasticsearch queries executed?", "user",
                        Arrays.asList("kimchy", "luca"), "innerObject", Collections.singletonMap("key", "value")));
        bulkRequest.add(new IndexRequest("posts").id("2")
                .source(XContentType.JSON, "id", 2, "title", "Current status and upcoming changes in Elasticsearch", "user",
                        Arrays.asList("kimchy", "christoph"), "innerObject", Collections.singletonMap("key", "value")));
        bulkRequest.add(new IndexRequest("posts").id("3")
                .source(XContentType.JSON, "id", 3, "title", "The Future of Federated Search in Elasticsearch", "user",
                        Arrays.asList("kimchy", "tanguy"), "innerObject", Collections.singletonMap("key", "value")));

        bulkRequest.add(new IndexRequest("authors").id("1")
            .source(XContentType.JSON, "id", 1, "user", "kimchy"));
        bulkRequest.add(new IndexRequest("contributors").id("1")
            .source(XContentType.JSON, "id", 1, "user", "tanguy"));


        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertSame(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());
    }


    @SuppressWarnings({"unused", "unchecked"})
    public void testCount() throws Exception {
        indexCountTestData();
        RestHighLevelClient client = highLevelClient();
        {
            // tag::count-request-basic
            CountRequest countRequest = new CountRequest(); // <1>
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder(); // <2>
            searchSourceBuilder.query(QueryBuilders.matchAllQuery()); // <3>
            countRequest.source(searchSourceBuilder); // <4>
            // end::count-request-basic
        }
        {
            // tag::count-request-args
            CountRequest countRequest = new CountRequest("blog") // <1>
                .routing("routing") // <2>
                .indicesOptions(IndicesOptions.lenientExpandOpen()) // <3>
                .preference("_local"); // <4>
            // end::count-request-args
            assertNotNull(client.count(countRequest, RequestOptions.DEFAULT));
        }
        {
            // tag::count-source-basics
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder(); // <1>
            sourceBuilder.query(QueryBuilders.termQuery("user", "kimchy")); // <2>
            // end::count-source-basics

            // tag::count-source-setter
            CountRequest countRequest = new CountRequest();
            countRequest.indices("blog", "author");
            countRequest.source(sourceBuilder);
            // end::count-source-setter

            // tag::count-execute
            CountResponse countResponse = client
                .count(countRequest, RequestOptions.DEFAULT);
            // end::count-execute

            // tag::count-execute-listener
            ActionListener<CountResponse> listener =
                new ActionListener<CountResponse>() {

                    @Override
                    public void onResponse(CountResponse countResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::count-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::count-execute-async
            client.countAsync(countRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::count-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

            // tag::count-response-1
            long count = countResponse.getCount();
            RestStatus status = countResponse.status();
            Boolean terminatedEarly = countResponse.isTerminatedEarly();
            // end::count-response-1

            // tag::count-response-2
            int totalShards = countResponse.getTotalShards();
            int skippedShards = countResponse.getSkippedShards();
            int successfulShards = countResponse.getSuccessfulShards();
            int failedShards = countResponse.getFailedShards();
            for (ShardSearchFailure failure : countResponse.getShardFailures()) {
                // failures should be handled here
            }
            // end::count-response-2
            assertNotNull(countResponse);
            assertEquals(4, countResponse.getCount());
        }
    }

    private static void indexCountTestData() throws IOException {
        CreateIndexRequest authorsRequest = new CreateIndexRequest("author")
            .mapping(XContentFactory.jsonBuilder().startObject()
                .startObject("properties")
                    .startObject("user")
                        .field("type", "keyword")
                        .field("doc_values", "false")
                    .endObject()
                .endObject()
            .endObject());
        CreateIndexResponse authorsResponse = highLevelClient().indices().create(authorsRequest, RequestOptions.DEFAULT);
        assertTrue(authorsResponse.isAcknowledged());

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest("blog").id("1")
            .source(XContentType.JSON, "title", "Doubling Down on Open?", "user",
                Collections.singletonList("kimchy"), "innerObject", Collections.singletonMap("key", "value")));
        bulkRequest.add(new IndexRequest("blog").id("2")
            .source(XContentType.JSON, "title", "Swiftype Joins Forces with Elastic", "user",
                Arrays.asList("kimchy", "matt"), "innerObject", Collections.singletonMap("key", "value")));
        bulkRequest.add(new IndexRequest("blog").id("3")
            .source(XContentType.JSON, "title", "On Net Neutrality", "user",
                Arrays.asList("tyler", "kimchy"), "innerObject", Collections.singletonMap("key", "value")));

        bulkRequest.add(new IndexRequest("author").id("1")
            .source(XContentType.JSON, "user", "kimchy"));


        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        BulkResponse bulkResponse = highLevelClient().bulk(bulkRequest, RequestOptions.DEFAULT);
        assertSame(RestStatus.OK, bulkResponse.status());
        assertFalse(bulkResponse.hasFailures());
    }

}
