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

import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.explain.ExplainRequest;
import org.elasticsearch.action.explain.ExplainResponse;
import org.elasticsearch.action.fieldcaps.FieldCapabilities;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.elasticsearch.action.search.ClearScrollRequest;
import org.elasticsearch.action.search.ClearScrollResponse;
import org.elasticsearch.action.search.MultiSearchRequest;
import org.elasticsearch.action.search.MultiSearchResponse;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchScrollRequest;
import org.elasticsearch.client.core.CountRequest;
import org.elasticsearch.client.core.CountResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.ScriptQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.join.aggregations.Children;
import org.elasticsearch.join.aggregations.ChildrenAggregationBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.action.document.RestIndexAction;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.mustache.MultiSearchTemplateRequest;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse;
import org.elasticsearch.script.mustache.MultiSearchTemplateResponse.Item;
import org.elasticsearch.script.mustache.SearchTemplateRequest;
import org.elasticsearch.script.mustache.SearchTemplateResponse;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.elasticsearch.search.aggregations.BucketOrder;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStats;
import org.elasticsearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.WeightedAvg;
import org.elasticsearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.elasticsearch.xpack.core.index.query.PinnedQueryBuilder;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertFirstHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertSecondHit;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertToXContentEquivalent;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.hasId;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class SearchIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/type/1");
            doc1.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc1.setJsonEntity("{\"type\":\"type1\", \"num\":10, \"num2\":50}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/type/2");
            doc2.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc2.setJsonEntity("{\"type\":\"type1\", \"num\":20, \"num2\":40}");
            client().performRequest(doc2);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/type/3");
            doc3.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc3.setJsonEntity("{\"type\":\"type1\", \"num\":50, \"num2\":35}");
            client().performRequest(doc3);
            Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/type/4");
            doc4.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc4.setJsonEntity("{\"type\":\"type2\", \"num\":100, \"num2\":10}");
            client().performRequest(doc4);
            Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/type/5");
            doc5.setOptions(expectWarnings(RestIndexAction.TYPES_DEPRECATION_MESSAGE));
            doc5.setJsonEntity("{\"type\":\"type2\", \"num\":100, \"num2\":10}");
            client().performRequest(doc5);
        }

        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index1/_doc/1");
            doc1.setJsonEntity("{\"field\":\"value1\", \"rating\": 7}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/index1/_doc/2");
            doc2.setJsonEntity("{\"field\":\"value2\"}");
            client().performRequest(doc2);
        }

        {
            Request create = new Request("PUT", "/index2");
            create.setJsonEntity(
                "{" +
                "  \"mappings\": {" +
                "    \"properties\": {" +
                "      \"rating\": {" +
                "        \"type\":  \"keyword\"" +
                "      }" +
                "    }" +
                "  }" +
                "}");
            client().performRequest(create);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/3");
            doc3.setJsonEntity("{\"field\":\"value1\", \"rating\": \"good\"}");
            client().performRequest(doc3);
            Request doc4 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/4");
            doc4.setJsonEntity("{\"field\":\"value2\"}");
            client().performRequest(doc4);
        }

        {
            Request doc5 = new Request(HttpPut.METHOD_NAME, "/index3/_doc/5");
            doc5.setJsonEntity("{\"field\":\"value1\"}");
            client().performRequest(doc5);
            Request doc6 = new Request(HttpPut.METHOD_NAME, "/index3/_doc/6");
            doc6.setJsonEntity("{\"field\":\"value2\"}");
            client().performRequest(doc6);
        }

        {
            Request create = new Request(HttpPut.METHOD_NAME, "/index4");
            create.setJsonEntity(
                    "{" +
                    "  \"mappings\": {" +
                    "    \"properties\": {" +
                    "      \"field1\": {" +
                    "        \"type\":  \"keyword\"," +
                    "        \"store\":  true" +
                    "      }," +
                    "      \"field2\": {" +
                    "        \"type\":  \"keyword\"," +
                    "        \"store\":  true" +
                    "      }" +
                    "    }" +
                    "  }" +
                    "}");
            client().performRequest(create);
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index4/_doc/1");
            doc1.setJsonEntity("{\"field1\":\"value1\", \"field2\":\"value2\"}");
            client().performRequest(doc1);

            Request createFilteredAlias = new Request(HttpPost.METHOD_NAME, "/_aliases");
            createFilteredAlias.setJsonEntity(
                    "{" +
                    "  \"actions\" : [" +
                    "    {" +
                    "      \"add\" : {" +
                    "        \"index\" : \"index4\"," +
                    "        \"alias\" : \"alias4\"," +
                    "        \"filter\" : { \"term\" : { \"field2\" : \"value1\" } }" +
                    "      }" +
                    "    }" +
                    "  ]" +
                    "}");
            client().performRequest(createFilteredAlias);
        }

        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testSearchNoQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
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
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
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
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
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

    public void testSearchWithCompositeAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        List<CompositeValuesSourceBuilder<?>> sources
            = Collections.singletonList(new TermsValuesSourceBuilder("terms").field("type.keyword").missingBucket(true).order("asc"));
        searchSourceBuilder.aggregation(AggregationBuilders.composite("composite", sources));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices("index");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        CompositeAggregation compositeAgg = searchResponse.getAggregations().get("composite");
        assertEquals("composite", compositeAgg.getName());
        assertEquals(2, compositeAgg.getBuckets().size());
        CompositeAggregation.Bucket bucket1 = compositeAgg.getBuckets().get(0);
        assertEquals(3, bucket1.getDocCount());
        assertEquals("{terms=type1}", bucket1.getKeyAsString());
        assertEquals(0, bucket1.getAggregations().asList().size());
        CompositeAggregation.Bucket bucket2 = compositeAgg.getBuckets().get(1);
        assertEquals(2, bucket2.getDocCount());
        assertEquals("{terms=type2}", bucket2.getKeyAsString());
        assertEquals(0, bucket2.getAggregations().asList().size());
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

        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new RangeAggregationBuilder("agg1").field("num")
                .addRange("first", 0, 30).addRange("second", 31, 200));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
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
        SearchRequest searchRequest = new SearchRequest("index");
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
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
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

    public void testSearchWithTermsAndWeightedAvg() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder agg = new TermsAggregationBuilder("agg1", ValueType.STRING).field("type.keyword");
        agg.subAggregation(new WeightedAvgAggregationBuilder("subagg")
            .value(new MultiValuesSourceFieldConfig.Builder().setFieldName("num").build())
            .weight(new MultiValuesSourceFieldConfig.Builder().setFieldName("num2").build())
        );
        searchSourceBuilder.aggregation(agg);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket type1 = termsAgg.getBucketByKey("type1");
        assertEquals(3, type1.getDocCount());
        assertEquals(1, type1.getAggregations().asList().size());
        {
            WeightedAvg weightedAvg = type1.getAggregations().get("subagg");
            assertEquals(24.4, weightedAvg.getValue(), 0f);
        }
        Terms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(1, type2.getAggregations().asList().size());
        {
            WeightedAvg weightedAvg = type2.getAggregations().get("subagg");
            assertEquals(100, weightedAvg.getValue(), 0f);
        }
    }

    public void testSearchWithMatrixStats() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new MatrixStatsAggregationBuilder("agg1").fields(Arrays.asList("num", "num2")));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(1, searchResponse.getAggregations().asList().size());
        MatrixStats matrixStats = searchResponse.getAggregations().get("agg1");
        assertEquals(5, matrixStats.getFieldCount("num"));
        assertEquals(56d, matrixStats.getMean("num"), 0d);
        assertEquals(1830.0000000000002, matrixStats.getVariance("num"), 0d);
        assertEquals(0.09340198804973039, matrixStats.getSkewness("num"), 0d);
        assertEquals(1.2741646510794589, matrixStats.getKurtosis("num"), 0d);
        assertEquals(5, matrixStats.getFieldCount("num2"));
        assertEquals(29d, matrixStats.getMean("num2"), 0d);
        assertEquals(330d, matrixStats.getVariance("num2"), 0d);
        assertEquals(-0.13568039346585542, matrixStats.getSkewness("num2"), 1.0e-16);
        assertEquals(1.3517561983471071, matrixStats.getKurtosis("num2"), 0d);
        assertEquals(-767.5, matrixStats.getCovariance("num", "num2"), 0d);
        assertEquals(-0.9876336291667923, matrixStats.getCorrelation("num", "num2"), 0d);
    }

    public void testSearchWithParentJoin() throws IOException {
        final String indexName = "child_example";
        Request createIndex = new Request(HttpPut.METHOD_NAME, "/" + indexName);
        createIndex.setJsonEntity(
                "{\n" +
                "    \"mappings\": {\n" +
                "        \"properties\" : {\n" +
                "            \"qa_join_field\" : {\n" +
                "                \"type\" : \"join\",\n" +
                "                \"relations\" : { \"question\" : \"answer\" }\n" +
                "            }\n" +
                "        }\n" +
                "    }" +
                "}");
        client().performRequest(createIndex);
        Request questionDoc = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/1");
        questionDoc.setJsonEntity(
                "{\n" +
                "    \"body\": \"<p>I have Windows 2003 server and i bought a new Windows 2008 server...\",\n" +
                "    \"title\": \"Whats the best way to file transfer my site from server to a newer one?\",\n" +
                "    \"tags\": [\n" +
                "        \"windows-server-2003\",\n" +
                "        \"windows-server-2008\",\n" +
                "        \"file-transfer\"\n" +
                "    ],\n" +
                "    \"qa_join_field\" : \"question\"\n" +
                "}");
        client().performRequest(questionDoc);
        Request answerDoc1 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/2");
        answerDoc1.addParameter("routing", "1");
        answerDoc1.setJsonEntity(
                "{\n" +
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
                "}");
        client().performRequest(answerDoc1);
        Request answerDoc2 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/3");
        answerDoc2.addParameter("routing", "1");
        answerDoc2.setJsonEntity(
                "{\n" +
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
                "}");
        client().performRequest(answerDoc2);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

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
        assertEquals(3, searchResponse.getHits().getTotalHits().value);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
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
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion("sugg1", new PhraseSuggestionBuilder("type"))
                .setGlobalText("type"));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getTotalHits().value);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
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

    public void testSearchWithWeirdScriptFields() throws Exception {
        Request doc = new Request("PUT", "test/_doc/1");
        doc.setJsonEntity("{\"field\":\"value\"}");
        client().performRequest(doc);
        client().performRequest(new Request("POST", "/test/_refresh"));

        {
            SearchRequest searchRequest = new SearchRequest("test").source(SearchSourceBuilder.searchSource()
                    .scriptField("result", new Script("null")));
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertNull(values.get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest("test").source(SearchSourceBuilder.searchSource()
                    .scriptField("result", new Script("new HashMap()")));
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertThat(values.get(0), instanceOf(Map.class));
            Map<?, ?> map = (Map<?, ?>) values.get(0);
            assertEquals(0, map.size());
        }
        {
            SearchRequest searchRequest = new SearchRequest("test").source(SearchSourceBuilder.searchSource()
                    .scriptField("result", new Script("new String[]{}")));
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertThat(values.get(0), instanceOf(List.class));
            List<?> list = (List<?>) values.get(0);
            assertEquals(0, list.size());
        }
    }

    public void testSearchScroll() throws Exception {
        for (int i = 0; i < 100; i++) {
            XContentBuilder builder = jsonBuilder().startObject().field("field", i).endObject();
            Request doc = new Request(HttpPut.METHOD_NAME, "/test/_doc/" + Integer.toString(i));
            doc.setJsonEntity(Strings.toString(builder));
            client().performRequest(doc);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/test/_refresh"));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(35).sort("field", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest("test").scroll(TimeValue.timeValueMinutes(2)).source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);

        try {
            long counter = 0;
            assertSearchHeader(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = execute(new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2)),
                    highLevelClient()::scroll, highLevelClient()::scrollAsync);

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertEquals(counter++, ((Number) hit.getSortValues()[0]).longValue());
            }

            searchResponse = execute(new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2)),
                    highLevelClient()::scroll, highLevelClient()::scrollAsync);

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(30));
            for (SearchHit hit : searchResponse.getHits()) {
                assertEquals(counter++, ((Number) hit.getSortValues()[0]).longValue());
            }
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponse.getScrollId());
            ClearScrollResponse clearScrollResponse = execute(clearScrollRequest,
                    highLevelClient()::clearScroll, highLevelClient()::clearScrollAsync);
            assertThat(clearScrollResponse.getNumFreed(), greaterThan(0));
            assertTrue(clearScrollResponse.isSucceeded());

            SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2));
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class, () -> execute(scrollRequest,
                    highLevelClient()::scroll, highLevelClient()::scrollAsync));
            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertThat(exception.getRootCause(), instanceOf(ElasticsearchException.class));
            ElasticsearchException rootCause = (ElasticsearchException) exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("No search context found for"));
        }
    }

    public void testMultiSearch() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().sort("_id", SortOrder.ASC);
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().sort("_id", SortOrder.ASC);
        multiSearchRequest.add(searchRequest2);
        SearchRequest searchRequest3 = new SearchRequest("index3");
        searchRequest3.source().sort("_id", SortOrder.ASC);
        multiSearchRequest.add(searchRequest3);

        MultiSearchResponse multiSearchResponse =
                execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("1"));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(1).getId(), Matchers.equalTo("2"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[1].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("3"));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(1).getId(), Matchers.equalTo("4"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[2].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("5"));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(1).getId(), Matchers.equalTo("6"));
    }

    public void testMultiSearch_withAgg() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().size(0).aggregation(new TermsAggregationBuilder("name", ValueType.STRING).field("field.keyword")
                .order(BucketOrder.key(true)));
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().size(0).aggregation(new TermsAggregationBuilder("name", ValueType.STRING).field("field.keyword")
                .order(BucketOrder.key(true)));
        multiSearchRequest.add(searchRequest2);
        SearchRequest searchRequest3 = new SearchRequest("index3");
        searchRequest3.source().size(0).aggregation(new TermsAggregationBuilder("name", ValueType.STRING).field("field.keyword")
                .order(BucketOrder.key(true)));
        multiSearchRequest.add(searchRequest3);

        MultiSearchResponse multiSearchResponse =
                execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getHits().length, Matchers.equalTo(0));
        Terms terms = multiSearchResponse.getResponses()[0].getResponse().getAggregations().get("name");
        assertThat(terms.getBuckets().size(), Matchers.equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), Matchers.equalTo("value1"));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), Matchers.equalTo("value2"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getHits().length, Matchers.equalTo(0));
        terms = multiSearchResponse.getResponses()[1].getResponse().getAggregations().get("name");
        assertThat(terms.getBuckets().size(), Matchers.equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), Matchers.equalTo("value1"));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), Matchers.equalTo("value2"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getHits().length, Matchers.equalTo(0));
        terms = multiSearchResponse.getResponses()[2].getResponse().getAggregations().get("name");
        assertThat(terms.getBuckets().size(), Matchers.equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), Matchers.equalTo("value1"));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), Matchers.equalTo("value2"));
    }

    public void testMultiSearch_withQuery() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().query(new TermsQueryBuilder("field", "value2"));
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().query(new TermsQueryBuilder("field", "value2"));
        multiSearchRequest.add(searchRequest2);
        SearchRequest searchRequest3 = new SearchRequest("index3");
        searchRequest3.source().query(new TermsQueryBuilder("field", "value2"));
        multiSearchRequest.add(searchRequest3);

        MultiSearchResponse multiSearchResponse =
                execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("2"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[1].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("4"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[2].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("6"));

        searchRequest1.source().highlighter(new HighlightBuilder().field("field"));
        searchRequest2.source().highlighter(new HighlightBuilder().field("field"));
        searchRequest3.source().highlighter(new HighlightBuilder().field("field"));
        multiSearchResponse = execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(0).getHighlightFields()
                .get("field").fragments()[0].string(), Matchers.equalTo("<em>value2</em>"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[1].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("4"));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getHighlightFields()
                .get("field").fragments()[0].string(), Matchers.equalTo("<em>value2</em>"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[2].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("6"));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getHighlightFields()
                .get("field").fragments()[0].string(), Matchers.equalTo("<em>value2</em>"));
    }

    public void testMultiSearch_failure() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().query(new ScriptQueryBuilder(new Script(ScriptType.INLINE, "invalid", "code", Collections.emptyMap())));
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().query(new ScriptQueryBuilder(new Script(ScriptType.INLINE, "invalid", "code", Collections.emptyMap())));
        multiSearchRequest.add(searchRequest2);

        MultiSearchResponse multiSearchResponse =
                execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(2));

        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(true));
        assertThat(multiSearchResponse.getResponses()[0].getFailure().getMessage(), containsString("search_phase_execution_exception"));
        assertThat(multiSearchResponse.getResponses()[0].getResponse(), nullValue());

        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(true));
        assertThat(multiSearchResponse.getResponses()[1].getFailure().getMessage(), containsString("search_phase_execution_exception"));
        assertThat(multiSearchResponse.getResponses()[1].getResponse(), nullValue());
    }

    public void testSearchTemplate() throws IOException {
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
        searchTemplateRequest.setRequest(new SearchRequest("index"));

        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript(
            "{" +
            "  \"query\": {" +
            "    \"match\": {" +
            "      \"num\": {{number}}" +
            "    }" +
            "  }" +
            "}");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        searchTemplateRequest.setScriptParams(scriptParams);

        searchTemplateRequest.setExplain(true);
        searchTemplateRequest.setProfile(true);

        SearchTemplateResponse searchTemplateResponse = execute(searchTemplateRequest,
            highLevelClient()::searchTemplate,
            highLevelClient()::searchTemplateAsync);

        assertNull(searchTemplateResponse.getSource());

        SearchResponse searchResponse = searchTemplateResponse.getResponse();
        assertNotNull(searchResponse);

        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertThat(searchResponse.getHits().getMaxScore(), greaterThan(0f));

        SearchHit hit = searchResponse.getHits().getHits()[0];
        assertNotNull(hit.getExplanation());

        assertFalse(searchResponse.getProfileResults().isEmpty());
    }

    public void testNonExistentSearchTemplate() {
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
        searchTemplateRequest.setRequest(new SearchRequest("index"));

        searchTemplateRequest.setScriptType(ScriptType.STORED);
        searchTemplateRequest.setScript("non-existent");
        searchTemplateRequest.setScriptParams(Collections.emptyMap());

        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(searchTemplateRequest,
                highLevelClient()::searchTemplate,
                highLevelClient()::searchTemplateAsync));

        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testRenderSearchTemplate() throws IOException {
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();

        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript(
            "{" +
            "  \"query\": {" +
            "    \"match\": {" +
            "      \"num\": {{number}}" +
            "    }" +
            "  }" +
            "}");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        searchTemplateRequest.setScriptParams(scriptParams);

        // Setting simulate true causes the template to only be rendered.
        searchTemplateRequest.setSimulate(true);

        SearchTemplateResponse searchTemplateResponse = execute(searchTemplateRequest,
            highLevelClient()::searchTemplate,
            highLevelClient()::searchTemplateAsync);
        assertNull(searchTemplateResponse.getResponse());

        BytesReference expectedSource = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                    .startObject("query")
                        .startObject("match")
                            .field("num", 10)
                        .endObject()
                    .endObject()
                .endObject());

        BytesReference actualSource = searchTemplateResponse.getSource();
        assertNotNull(actualSource);

        assertToXContentEquivalent(expectedSource, actualSource, XContentType.JSON);
    }


    public void testMultiSearchTemplate() throws Exception {
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();

        SearchTemplateRequest goodRequest = new SearchTemplateRequest();
        goodRequest.setRequest(new SearchRequest("index"));
        goodRequest.setScriptType(ScriptType.INLINE);
        goodRequest.setScript(
            "{" +
            "  \"query\": {" +
            "    \"match\": {" +
            "      \"num\": {{number}}" +
            "    }" +
            "  }" +
            "}");
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        goodRequest.setScriptParams(scriptParams);
        goodRequest.setExplain(true);
        goodRequest.setProfile(true);
        multiSearchTemplateRequest.add(goodRequest);


        SearchTemplateRequest badRequest = new SearchTemplateRequest();
        badRequest.setRequest(new SearchRequest("index"));
        badRequest.setScriptType(ScriptType.INLINE);
        badRequest.setScript("{ NOT VALID JSON {{number}} }");
        scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        badRequest.setScriptParams(scriptParams);

        multiSearchTemplateRequest.add(badRequest);

        MultiSearchTemplateResponse multiSearchTemplateResponse =
                execute(multiSearchTemplateRequest, highLevelClient()::msearchTemplate,
                        highLevelClient()::msearchTemplateAsync);

        Item[] responses = multiSearchTemplateResponse.getResponses();

        assertEquals(2, responses.length);


        assertNull(responses[0].getResponse().getSource());
        SearchResponse goodResponse =responses[0].getResponse().getResponse();
        assertNotNull(goodResponse);
        assertThat(responses[0].isFailure(), Matchers.is(false));
        assertEquals(1, goodResponse.getHits().getTotalHits().value);
        assertEquals(1, goodResponse.getHits().getHits().length);
        assertThat(goodResponse.getHits().getMaxScore(), greaterThan(0f));
        SearchHit hit = goodResponse.getHits().getHits()[0];
        assertNotNull(hit.getExplanation());
        assertFalse(goodResponse.getProfileResults().isEmpty());


        assertNull(responses[0].getResponse().getSource());
        assertThat(responses[1].isFailure(), Matchers.is(true));
        assertNotNull(responses[1].getFailureMessage());
        assertThat(responses[1].getFailureMessage(), containsString("json_parse_exception"));
    }

    public void testMultiSearchTemplateAllBad() throws Exception {
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();

        SearchTemplateRequest badRequest1 = new SearchTemplateRequest();
        badRequest1.setRequest(new SearchRequest("index"));
        badRequest1.setScriptType(ScriptType.INLINE);
        badRequest1.setScript(
                "{" +
                        "  \"query\": {" +
                        "    \"match\": {" +
                        "      \"num\": {{number}}" +
                        "    }" +
                        "  }" +
                        "}");
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", "BAD NUMBER");
        badRequest1.setScriptParams(scriptParams);
        multiSearchTemplateRequest.add(badRequest1);


        SearchTemplateRequest badRequest2 = new SearchTemplateRequest();
        badRequest2.setRequest(new SearchRequest("index"));
        badRequest2.setScriptType(ScriptType.INLINE);
        badRequest2.setScript("BAD QUERY TEMPLATE");
        scriptParams = new HashMap<>();
        scriptParams.put("number", "BAD NUMBER");
        badRequest2.setScriptParams(scriptParams);

        multiSearchTemplateRequest.add(badRequest2);

        // The whole HTTP request should fail if no nested search requests are valid
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
                () -> execute(multiSearchTemplateRequest, highLevelClient()::msearchTemplate,
                        highLevelClient()::msearchTemplateAsync));

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertThat(exception.getMessage(), containsString("no requests added"));
    }

    public void testExplain() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(explainResponse.getType(), equalTo("_doc"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertNull(explainResponse.getGetResult());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.termQuery("field", "value1"));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(explainResponse.getType(), equalTo("_doc"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue().floatValue(), greaterThan(0.0f));
            assertNull(explainResponse.getGetResult());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.termQuery("field", "value2"));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(explainResponse.getType(), equalTo("_doc"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertFalse(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertNull(explainResponse.getGetResult());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.boolQuery()
                .must(QueryBuilders.termQuery("field", "value1"))
                .must(QueryBuilders.termQuery("field", "value2")));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(explainResponse.getType(), equalTo("_doc"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertFalse(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getDetails().length, equalTo(2));
            assertNull(explainResponse.getGetResult());
        }
    }

    public void testExplainNonExistent() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("non_existent_index", "1");
            explainRequest.query(QueryBuilders.matchQuery("field", "value"));
            ElasticsearchException exception = expectThrows(ElasticsearchException.class,
                () -> execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync));
            assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(exception.getIndex().getName(), equalTo("non_existent_index"));
            assertThat(exception.getDetailedMessage(),
                containsString("Elasticsearch exception [type=index_not_found_exception, reason=no such index [non_existent_index]]"));
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "999");
            explainRequest.query(QueryBuilders.matchQuery("field", "value1"));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(explainResponse.getType(), equalTo("_doc"));
            assertThat(explainResponse.getId(), equalTo("999"));
            assertFalse(explainResponse.isExists());
            assertFalse(explainResponse.isMatch());
            assertFalse(explainResponse.hasExplanation());
            assertNull(explainResponse.getGetResult());
        }
    }

    public void testExplainWithStoredFields() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.storedFields(new String[]{"field1"});

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getFields().keySet(), equalTo(Collections.singleton("field1")));
            assertThat(explainResponse.getGetResult().getFields().get("field1").getValue().toString(), equalTo("value1"));
            assertTrue(explainResponse.getGetResult().isSourceEmpty());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.storedFields(new String[]{"field1", "field2"});

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getFields().keySet().size(), equalTo(2));
            assertThat(explainResponse.getGetResult().getFields().get("field1").getValue().toString(), equalTo("value1"));
            assertThat(explainResponse.getGetResult().getFields().get("field2").getValue().toString(), equalTo("value2"));
            assertTrue(explainResponse.getGetResult().isSourceEmpty());
        }
    }

    public void testExplainWithFetchSource() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.fetchSourceContext(new FetchSourceContext(true, new String[]{"field1"}, null));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getSource(), equalTo(Collections.singletonMap("field1", "value1")));
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.fetchSourceContext(new FetchSourceContext(true, null, new String[] {"field2"}));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getSource(), equalTo(Collections.singletonMap("field1", "value1")));
        }
    }

    public void testExplainWithAliasFilter() throws IOException {
        ExplainRequest explainRequest = new ExplainRequest("alias4", "1");
        explainRequest.query(QueryBuilders.matchAllQuery());

        ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

        assertTrue(explainResponse.isExists());
        assertFalse(explainResponse.isMatch());
    }

    public void testFieldCaps() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index1", "index2")
            .fields("rating", "field");

        FieldCapabilitiesResponse response = execute(request,
            highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync);

        assertEquals(new String[] {"index1", "index2"}, response.getIndices());

        // Check the capabilities for the 'rating' field.
        assertTrue(response.get().containsKey("rating"));
        Map<String, FieldCapabilities> ratingResponse = response.getField("rating");
        assertEquals(2, ratingResponse.size());

        FieldCapabilities expectedKeywordCapabilities = new FieldCapabilities(
            "rating", "keyword", true, true, new String[]{"index2"}, null, null);
        assertEquals(expectedKeywordCapabilities, ratingResponse.get("keyword"));

        FieldCapabilities expectedLongCapabilities = new FieldCapabilities(
            "rating", "long", true, true, new String[]{"index1"}, null, null);
        assertEquals(expectedLongCapabilities, ratingResponse.get("long"));

        // Check the capabilities for the 'field' field.
        assertTrue(response.get().containsKey("field"));
        Map<String, FieldCapabilities> fieldResponse = response.getField("field");
        assertEquals(1, fieldResponse.size());

        FieldCapabilities expectedTextCapabilities = new FieldCapabilities(
            "field", "text", true, false);
        assertEquals(expectedTextCapabilities, fieldResponse.get("text"));
    }

    public void testFieldCapsWithNonExistentFields() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("index2")
            .fields("nonexistent");

        FieldCapabilitiesResponse response = execute(request,
            highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync);
        assertTrue(response.get().isEmpty());
    }

    public void testFieldCapsWithNonExistentIndices() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest()
            .indices("non-existent")
            .fields("rating");

        ElasticsearchException exception = expectThrows(ElasticsearchException.class,
            () -> execute(request, highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync));
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    private static void assertSearchHeader(SearchResponse searchResponse) {
        assertThat(searchResponse.getTook().nanos(), greaterThanOrEqualTo(0L));
        assertEquals(0, searchResponse.getFailedShards());
        assertThat(searchResponse.getTotalShards(), greaterThan(0));
        assertEquals(searchResponse.getTotalShards(), searchResponse.getSuccessfulShards());
        assertEquals(0, searchResponse.getShardFailures().length);
        assertEquals(SearchResponse.Clusters.EMPTY, searchResponse.getClusters());
    }

    public void testCountOneIndexNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index");
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(5, countResponse.getCount());
    }

    public void testCountMultipleIndicesNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index", "index1");
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(7, countResponse.getCount());
    }

    public void testCountAllIndicesNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest();
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        // add logging to get more info about why https://github.com/elastic/elasticsearch/issues/35644 is failing
        // TODO remove this once #35644 is fixed
        if (countResponse.getCount() != 12) {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.source(new SearchSourceBuilder().size(20));
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            logger.info("Unexpected hit count, was expecting 12 hits but got: " + searchResponse.toString());
        }
        assertEquals(12, countResponse.getCount());
    }

    public void testCountOneIndexMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index");
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(1, countResponse.getCount());
    }

    public void testCountMultipleIndicesMatchQueryUsingConstructor() throws IOException {

        SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1"));
        CountRequest countRequest = new CountRequest(new String[]{"index1", "index2", "index3"}, sourceBuilder);
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(3, countResponse.getCount());

    }

    public void testCountMultipleIndicesMatchQuery() throws IOException {

        CountRequest countRequest = new CountRequest("index1", "index2", "index3");
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1")));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(3, countResponse.getCount());
    }

    public void testCountAllIndicesMatchQuery() throws IOException {

        CountRequest countRequest = new CountRequest();
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1")));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(3, countResponse.getCount());
    }
    
    public void testSearchWithBasicLicensedQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        PinnedQueryBuilder pinnedQuery = new PinnedQueryBuilder(new MatchAllQueryBuilder(), "2", "1");
        searchSourceBuilder.query(pinnedQuery);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertFirstHit(searchResponse, hasId("2"));
        assertSecondHit(searchResponse, hasId("1"));
    }
    
    private static void assertCountHeader(CountResponse countResponse) {
        assertEquals(0, countResponse.getSkippedShards());
        assertEquals(0, countResponse.getFailedShards());
        assertThat(countResponse.getTotalShards(), greaterThan(0));
        assertEquals(countResponse.getTotalShards(), countResponse.getSuccessfulShards());
        assertEquals(0, countResponse.getShardFailures().length);
    }
}
