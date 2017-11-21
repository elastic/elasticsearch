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

package org.elasticsearch.client;

import org.apache.http.HttpEntity;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.nio.entity.NStringEntity;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.join.aggregations.Children;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;

public class SearchIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        StringEntity doc1 = new StringEntity("{\"type\":\"type1\", \"num\":10, \"num2\":50}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/1", Collections.emptyMap(), doc1);
        StringEntity doc2 = new StringEntity("{\"type\":\"type1\", \"num\":20, \"num2\":40}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/2", Collections.emptyMap(), doc2);
        StringEntity doc3 = new StringEntity("{\"type\":\"type1\", \"num\":50, \"num2\":35}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/3", Collections.emptyMap(), doc3);
        StringEntity doc4 = new StringEntity("{\"type\":\"type2\", \"num\":100, \"num2\":10}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/4", Collections.emptyMap(), doc4);
        StringEntity doc5 = new StringEntity("{\"type\":\"type2\", \"num\":100, \"num2\":10}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/5", Collections.emptyMap(), doc5);
        client().performRequest("POST", "/index/_refresh");
    }

    public void testSearchNoQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().totalHits);
        assertEquals(5, searchResponse.getHits().getHits().length);
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            assertEquals("index", searchHit.getIndex());
            assertEquals("type", searchHit.getType());
            assertThat(Integer.valueOf(searchHit.getId()), both(greaterThan(0)).and(lessThan(6)));
            assertEquals(1.0f, searchHit.getScore(), 0);
            assertEquals(-1L, searchHit.getVersion());
            assertNotNull(searchHit.getSourceAsMap());
            assertEquals(3, searchHit.getSourceAsMap().size());
            assertTrue(searchHit.getSourceAsMap().containsKey("type"));
            assertTrue(searchHit.getSourceAsMap().containsKey("num"));
            assertTrue(searchHit.getSourceAsMap().containsKey("num2"));
        }
    }

    public void testSearchMatchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(1, searchResponse.getHits().totalHits);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertThat(searchResponse.getHits().getMaxScore(), greaterThan(0f));
        SearchHit searchHit = searchResponse.getHits().getHits()[0];
        assertEquals("index", searchHit.getIndex());
        assertEquals("type", searchHit.getType());
        assertEquals("1", searchHit.getId());
        assertThat(searchHit.getScore(), greaterThan(0f));
        assertEquals(-1L, searchHit.getVersion());
        assertNotNull(searchHit.getSourceAsMap());
        assertEquals(3, searchHit.getSourceAsMap().size());
        assertEquals("type1", searchHit.getSourceAsMap().get("type"));
        assertEquals(50, searchHit.getSourceAsMap().get("num2"));
    }

    public void testSearchWithTermsAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1", ValueType.STRING).field("type.keyword"));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(0f, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket type1 = termsAgg.getBucketByKey("type1");
        assertEquals(3, type1.getDocCount());
        assertEquals(0, type1.getAggregations().asList().size());
        Terms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(0, type2.getAggregations().asList().size());
    }

    public void testSearchWithRangeAgg() throws IOException {
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(new RangeAggregationBuilder("agg1").field("num"));
            searchSourceBuilder.size(0);
            searchRequest.source(searchSourceBuilder);

            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
                    () -> execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync));
            assertEquals(RestStatus.BAD_REQUEST, exception.status());
        }

        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new RangeAggregationBuilder("agg1").field("num")
                .addRange("first", 0, 30).addRange("second", 31, 200));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().totalHits);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(0f, searchResponse.getHits().getMaxScore(), 0f);
        Range rangeAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", rangeAgg.getName());
        assertEquals(2, rangeAgg.getBuckets().size());
        {
            Range.Bucket bucket = rangeAgg.getBuckets().get(0);
            assertEquals("first", bucket.getKeyAsString());
            assertEquals(2, bucket.getDocCount());
        }
        {
            Range.Bucket bucket = rangeAgg.getBuckets().get(1);
            assertEquals("second", bucket.getKeyAsString());
            assertEquals(3, bucket.getDocCount());
        }
    }

    public void testSearchWithTermsAndRangeAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder agg = new TermsAggregationBuilder("agg1", ValueType.STRING).field("type.keyword");
        agg.subAggregation(new RangeAggregationBuilder("subagg").field("num")
                .addRange("first", 0, 30).addRange("second", 31, 200));
        searchSourceBuilder.aggregation(agg);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(0f, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket type1 = termsAgg.getBucketByKey("type1");
        assertEquals(3, type1.getDocCount());
        assertEquals(1, type1.getAggregations().asList().size());
        {
            Range rangeAgg = type1.getAggregations().get("subagg");
            assertEquals(2, rangeAgg.getBuckets().size());
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(0);
                assertEquals("first", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(1);
                assertEquals("second", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
            }
        }
        Terms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(1, type2.getAggregations().asList().size());
        {
            Range rangeAgg = type2.getAggregations().get("subagg");
            assertEquals(2, rangeAgg.getBuckets().size());
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(0);
                assertEquals("first", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
            }
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(1);
                assertEquals("second", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }
        }
    }

    public void testSearchWithMatrixStats() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new MatrixStatsAggregationBuilder("agg1").fields(Arrays.asList("num", "num2")));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().totalHits);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(0f, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(1, searchResponse.getAggregations().asList().size());
        MatrixStats matrixStats = searchResponse.getAggregations().get("agg1");
        assertEquals(5, matrixStats.getFieldCount("num"));
        assertEquals(56d, matrixStats.getMean("num"), 0d);
        assertEquals(1830d, matrixStats.getVariance("num"), 0d);
        assertEquals(0.09340198804973057, matrixStats.getSkewness("num"), 0d);
        assertEquals(1.2741646510794589, matrixStats.getKurtosis("num"), 0d);
        assertEquals(5, matrixStats.getFieldCount("num2"));
        assertEquals(29d, matrixStats.getMean("num2"), 0d);
        assertEquals(330d, matrixStats.getVariance("num2"), 0d);
        assertEquals(-0.13568039346585542, matrixStats.getSkewness("num2"), 1.0e-16);
        assertEquals(1.3517561983471074, matrixStats.getKurtosis("num2"), 0d);
        assertEquals(-767.5, matrixStats.getCovariance("num", "num2"), 0d);
        assertEquals(-0.9876336291667923, matrixStats.getCorrelation("num", "num2"), 0d);
    }

    public void testSearchWithParentJoin() throws IOException {
        final String indexName = "child_example";
        StringEntity parentMapping = new StringEntity("{\n" +
                "    \"mappings\": {\n" +
                "        \"qa\" : {\n" +
                "            \"properties\" : {\n" +
                "                \"qa_join_field\" : {\n" +
                "                    \"type\" : \"join\",\n" +
                "                    \"relations\" : { \"question\" : \"answer\" }\n" +
                "                }\n" +
                "            }\n" +
                "        }\n" +
                "    }" +
                "}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + indexName, Collections.emptyMap(), parentMapping);
        StringEntity questionDoc = new StringEntity("{\n" +
                "    \"body\": \"<p>I have Windows 2003 server and i bought a new Windows 2008 server...\",\n" +
                "    \"title\": \"Whats the best way to file transfer my site from server to a newer one?\",\n" +
                "    \"tags\": [\n" +
                "        \"windows-server-2003\",\n" +
                "        \"windows-server-2008\",\n" +
                "        \"file-transfer\"\n" +
                "    ],\n" +
                "    \"qa_join_field\" : \"question\"\n" +
                "}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + indexName + "/qa/1", Collections.emptyMap(), questionDoc);
        StringEntity answerDoc1 = new StringEntity("{\n" +
                "    \"owner\": {\n" +
                "        \"location\": \"Norfolk, United Kingdom\",\n" +
                "        \"display_name\": \"Sam\",\n" +
                "        \"id\": 48\n" +
                "    },\n" +
                "    \"body\": \"<p>Unfortunately you're pretty much limited to FTP...\",\n" +
                "    \"qa_join_field\" : {\n" +
                "        \"name\" : \"answer\",\n" +
                "        \"parent\" : \"1\"\n" +
                "    },\n" +
                "    \"creation_date\": \"2009-05-04T13:45:37.030\"\n" +
                "}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + indexName + "/qa/2", Collections.singletonMap("routing", "1"), answerDoc1);
        StringEntity answerDoc2 = new StringEntity("{\n" +
                "    \"owner\": {\n" +
                "        \"location\": \"Norfolk, United Kingdom\",\n" +
                "        \"display_name\": \"Troll\",\n" +
                "        \"id\": 49\n" +
                "    },\n" +
                "    \"body\": \"<p>Use Linux...\",\n" +
                "    \"qa_join_field\" : {\n" +
                "        \"name\" : \"answer\",\n" +
                "        \"parent\" : \"1\"\n" +
                "    },\n" +
                "    \"creation_date\": \"2009-05-05T13:45:37.030\"\n" +
                "}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/" + indexName + "/qa/3", Collections.singletonMap("routing", "1"), answerDoc2);
        client().performRequest("POST", "/_refresh");

        TermsAggregationBuilder leafTermAgg = new TermsAggregationBuilder("top-names", ValueType.STRING)
                .field("owner.display_name.keyword").size(10);
        ChildrenAggregationBuilder childrenAgg = new ChildrenAggregationBuilder("to-answers", "answer").subAggregation(leafTermAgg);
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("top-tags", ValueType.STRING).field("tags.keyword")
                .size(10).subAggregation(childrenAgg);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).aggregation(termsAgg);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(3, searchResponse.getHits().totalHits);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(0f, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(1, searchResponse.getAggregations().asList().size());
        Terms terms = searchResponse.getAggregations().get("top-tags");
        assertEquals(0, terms.getDocCountError());
        assertEquals(0, terms.getSumOfOtherDocCounts());
        assertEquals(3, terms.getBuckets().size());
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(bucket.getKeyAsString(),
                    either(equalTo("file-transfer")).or(equalTo("windows-server-2003")).or(equalTo("windows-server-2008")));
            assertEquals(1, bucket.getDocCount());
            assertEquals(1, bucket.getAggregations().asList().size());
            Children children = bucket.getAggregations().get("to-answers");
            assertEquals(2, children.getDocCount());
            assertEquals(1, children.getAggregations().asList().size());
            Terms leafTerms = children.getAggregations().get("top-names");
            assertEquals(0, leafTerms.getDocCountError());
            assertEquals(0, leafTerms.getSumOfOtherDocCounts());
            assertEquals(2, leafTerms.getBuckets().size());
            assertEquals(2, leafTerms.getBuckets().size());
            Terms.Bucket sam = leafTerms.getBucketByKey("Sam");
            assertEquals(1, sam.getDocCount());
            Terms.Bucket troll = leafTerms.getBucketByKey("Troll");
            assertEquals(1, troll.getDocCount());
        }
    }

    public void testSearchWithSuggest() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion("sugg1", new PhraseSuggestionBuilder("type"))
                .setGlobalText("type"));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().totalHits);
        assertEquals(0f, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(1, searchResponse.getSuggest().size());

        Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> sugg = searchResponse
                .getSuggest().iterator().next();
        assertEquals("sugg1", sugg.getName());
        for (Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> options : sugg) {
            assertEquals("type", options.getText().string());
            assertEquals(0, options.getOffset());
            assertEquals(4, options.getLength());
            assertEquals(2 ,options.getOptions().size());
            for (Suggest.Suggestion.Entry.Option option : options) {
                assertThat(option.getScore(), greaterThan(0f));
                assertThat(option.getText().string(), either(equalTo("type1")).or(equalTo("type2")));
            }
        }
    }

    public void testSearchScroll() throws Exception {

        for (int i = 0; i < 100; i++) {
            XContentBuilder builder = jsonBuilder().startObject().field("field", i).endObject();
            HttpEntity entity = new NStringEntity(builder.string(), ContentType.APPLICATION_JSON);
            client().performRequest("PUT", "test/type1/" + Integer.toString(i), Collections.emptyMap(), entity);
        }
        client().performRequest("POST", "/test/_refresh");

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(35).sort("field", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest("test").scroll(TimeValue.timeValueMinutes(2)).source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);

        try {
            long counter = 0;
            assertSearchHeader(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = execute(new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2)),
                    highLevelClient()::searchScroll, highLevelClient()::searchScrollAsync);

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertEquals(counter++, ((Number) hit.getSortValues()[0]).longValue());
            }

            searchResponse = execute(new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2)),
                    highLevelClient()::searchScroll, highLevelClient()::searchScrollAsync);

            assertThat(searchResponse.getHits().getTotalHits(), equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(30));
            for (SearchHit hit : searchResponse.getHits()) {
                assertEquals(counter++, ((Number) hit.getSortValues()[0]).longValue());
            }
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponse.getScrollId());
            ClearScrollResponse clearScrollResponse = execute(clearScrollRequest,
                    // Not using a method reference to work around https://bugs.eclipse.org/bugs/show_bug.cgi?id=517951
                    (request, headers) -> highLevelClient().clearScroll(request, headers),
                    (request, listener, headers) -> highLevelClient().clearScrollAsync(request, listener, headers));
            assertThat(clearScrollResponse.getNumFreed(), greaterThan(0));
            assertTrue(clearScrollResponse.isSucceeded());

            SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2));
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> execute(scrollRequest,
                    highLevelClient()::searchScroll, highLevelClient()::searchScrollAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertThat(exception.getRootCause(), instanceOf(ElasticsearchException.class));
            ElasticsearchException rootCause = (ElasticsearchException) exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("No search context found for"));
        }
    }

    private static void assertSearchHeader(SearchResponse searchResponse) {
        assertThat(searchResponse.getTook().nanos(), greaterThanOrEqualTo(0L));
        assertEquals(0, searchResponse.getFailedShards());
        assertThat(searchResponse.getTotalShards(), greaterThan(0));
        assertEquals(searchResponse.getTotalShards(), searchResponse.getSuccessfulShards());
        assertEquals(0, searchResponse.getShardFailures().length);
        assertEquals(SearchResponse.Clusters.EMPTY, searchResponse.getClusters());
    }
}
