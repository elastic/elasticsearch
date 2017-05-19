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

import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.MatchQueryBuilder;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.aggregations.bucket.range.Range;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.Terms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.suggest.Suggest;
import org.elasticsearch.search.suggest.SuggestBuilder;
import org.elasticsearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.lessThan;

public class SearchIT extends ESRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        StringEntity doc1 = new StringEntity("{\"field\":\"value1\", \"num\":10}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/1", Collections.emptyMap(), doc1);
        StringEntity doc2 = new StringEntity("{\"field\":\"value2\", \"num\":20}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/2", Collections.emptyMap(), doc2);
        StringEntity doc3 = new StringEntity("{\"field\":\"value3\", \"num\":50}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/3", Collections.emptyMap(), doc3);
        StringEntity doc4 = new StringEntity("{\"field\":\"value4\", \"num\":100}", ContentType.APPLICATION_JSON);
        client().performRequest("PUT", "/index/type/4", Collections.emptyMap(), doc4);
        StringEntity doc5 = new StringEntity("{\"field\":\"value5\", \"num\":100}", ContentType.APPLICATION_JSON);
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
            assertEquals(2, searchHit.getSourceAsMap().size());
            assertTrue(searchHit.getSourceAsMap().containsKey("field"));
        }
    }

    public void testSearchMatchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1")));
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
        assertEquals(2, searchHit.getSourceAsMap().size());
        assertEquals("value1", searchHit.getSourceAsMap().get("field"));
    }

    public void testSearchWithTermsAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1", ValueType.STRING).field("field.keyword"));
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
        assertEquals(5, termsAgg.getBuckets().size());
        for (Terms.Bucket bucket : termsAgg.getBuckets()) {
            assertThat(bucket.getKeyAsString(), either(equalTo("value1")).or(equalTo("value2"))
                    .or(equalTo("value3")).or(equalTo("value4")).or(equalTo("value5")));
            assertEquals(1, bucket.getDocCount());
        }
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
        assertThat(searchResponse.getTook().nanos(), greaterThan(0L));
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

    public void testSearchWithSuggest() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion("sugg1", new PhraseSuggestionBuilder("field"))
                .setGlobalText("value"));
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
            assertEquals("value", options.getText().string());
            assertEquals(0, options.getOffset());
            assertEquals(5, options.getLength());
            for (Suggest.Suggestion.Entry.Option option : options) {
                assertThat(option.getScore(), greaterThan(0f));
                assertThat(option.getText().string(), either(equalTo("value1")).or(equalTo("value2"))
                        .or(equalTo("value3")).or(equalTo("value4")).or(equalTo("value5")));
            }
        }
    }

    private static void assertSearchHeader(SearchResponse searchResponse) {
        assertThat(searchResponse.getTook().nanos(), greaterThan(0L));
        assertEquals(0, searchResponse.getFailedShards());
        assertThat(searchResponse.getTotalShards(), greaterThan(0));
        assertEquals(searchResponse.getTotalShards(), searchResponse.getSuccessfulShards());
        assertEquals(0, searchResponse.getShardFailures().length);
    }
}
