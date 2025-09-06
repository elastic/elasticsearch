/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.search;

import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.BoostingQueryBuilder;
import org.elasticsearch.index.query.ConstantScoreQueryBuilder;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.test.ESTestCase;

public class SearchRequestIntrospectorTests extends ESTestCase {

    public void testIndexIntrospectionSingleIndex() {
        assertEquals(".kibana", SearchRequestIntrospector.introspectIndices(new String[] { ".kibana_task_manager" }));
        assertEquals(".kibana", SearchRequestIntrospector.introspectIndices(new String[] { ".kibana_9_2_0" }));
        assertEquals(".kibana", SearchRequestIntrospector.introspectIndices(new String[] { ".kibana_ingest_9_2_0" }));
        assertEquals(".kibana", SearchRequestIntrospector.introspectIndices(new String[] { ".kibana_security_solution" }));
        assertEquals(".fleet", SearchRequestIntrospector.introspectIndices(new String[] { ".fleet-agents" }));
        assertEquals(".ml", SearchRequestIntrospector.introspectIndices(new String[] { ".ml-anomalies" }));
        assertEquals(".ml", SearchRequestIntrospector.introspectIndices(new String[] { ".ml-notifications" }));
        assertEquals(".slo", SearchRequestIntrospector.introspectIndices(new String[] { ".slo" }));
        assertEquals(".alerts", SearchRequestIntrospector.introspectIndices(new String[] { ".alerts" }));
        assertEquals(".elastic", SearchRequestIntrospector.introspectIndices(new String[] { ".elastic" }));
        assertEquals(".ds-", SearchRequestIntrospector.introspectIndices(new String[] { ".ds-test1" }));
        assertEquals(".ds-", SearchRequestIntrospector.introspectIndices(new String[] { ".ds-test2" }));
        assertEquals(".ds-", SearchRequestIntrospector.introspectIndices(new String[] { ".ds-test3" }));

        assertEquals(".others", SearchRequestIntrospector.introspectIndices(new String[] { ".a" }));
        assertEquals(".others", SearchRequestIntrospector.introspectIndices(new String[] { ".abcde" }));

        assertEquals("user", SearchRequestIntrospector.introspectIndices(new String[] { "a" }));
        assertEquals("user", SearchRequestIntrospector.introspectIndices(new String[] { "ab" }));
        assertEquals("user", SearchRequestIntrospector.introspectIndices(new String[] { "abc" }));
        assertEquals("user", SearchRequestIntrospector.introspectIndices(new String[] { "abcd" }));

        String indexName = "a" + randomAlphaOfLengthBetween(3, 10);
        assertEquals("user", SearchRequestIntrospector.introspectIndices(new String[] { indexName }));
    }

    public void testIndexIntrospectionMultipleIndices() {
        int length = randomIntBetween(2, 10);
        String[] indices = new String[length];
        for (int i = 0; i < length; i++) {
            indices[i] = randomAlphaOfLengthBetween(3, 10);
        }
        assertEquals("user", SearchRequestIntrospector.introspectIndices(indices));
    }

    public void testPrimarySortIntrospection() {
        assertEquals("@timestamp", SearchRequestIntrospector.introspectPrimarySort(new FieldSortBuilder("@timestamp")));
        assertEquals("event.ingested", SearchRequestIntrospector.introspectPrimarySort(new FieldSortBuilder("event.ingested")));
        assertEquals("_doc", SearchRequestIntrospector.introspectPrimarySort(new FieldSortBuilder("_doc")));
        assertEquals("field", SearchRequestIntrospector.introspectPrimarySort(new FieldSortBuilder(randomAlphaOfLengthBetween(3, 10))));
        assertEquals("_score", SearchRequestIntrospector.introspectPrimarySort(new ScoreSortBuilder()));
        assertEquals(
            "_geo_distance",
            SearchRequestIntrospector.introspectPrimarySort(new GeoDistanceSortBuilder(randomAlphaOfLengthBetween(3, 10), 1d, 1d))
        );
        assertEquals(
            "_script",
            SearchRequestIntrospector.introspectPrimarySort(
                new ScriptSortBuilder(new Script("id"), randomFrom(ScriptSortBuilder.ScriptSortType.values()))
            )
        );
    }

    public void testQueryTypeIntrospection() {
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            assertEquals("hits_only", SearchRequestIntrospector.introspectQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(randomIntBetween(1, 100));
            assertEquals("hits_only", SearchRequestIntrospector.introspectQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            assertEquals("count_only", SearchRequestIntrospector.introspectQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(new TermsAggregationBuilder("test"));
            assertEquals("hits_and_aggs", SearchRequestIntrospector.introspectQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(randomIntBetween(1, 100));
            searchSourceBuilder.aggregation(new TermsAggregationBuilder("test"));
            assertEquals("hits_and_aggs", SearchRequestIntrospector.introspectQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(new TermsAggregationBuilder("test"));
            assertEquals("aggs_only", SearchRequestIntrospector.introspectQueryType(searchSourceBuilder));
        }
    }

    public void testIntrospectSearchRequest() {
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("_score", queryMetadata.primarySort());
            assertEquals(0, queryMetadata.rangeFields().length);
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY));
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("_score", queryMetadata.primarySort());
            assertEquals(0, queryMetadata.rangeFields().length);
            assertEquals("pit", queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest();
            searchRequest.scroll(new TimeValue(randomIntBetween(1, 10)));
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("_score", queryMetadata.primarySort());
            assertEquals(0, queryMetadata.rangeFields().length);
            assertEquals("scroll", queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            searchSourceBuilder.query(new RangeQueryBuilder("@timestamp"));
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            int numBool = randomIntBetween(2, 10);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (int i = 0; i < numBool; i++) {
                BoolQueryBuilder boolQueryBuilderNew = new BoolQueryBuilder();
                boolQueryBuilder.must(boolQueryBuilderNew);
                boolQueryBuilder = boolQueryBuilderNew;
            }
            boolQueryBuilder.must(new RangeQueryBuilder("@timestamp"));
            searchSourceBuilder.query(boolQueryBuilder);
            if (randomBoolean()) {
                boolQueryBuilder.should(new RangeQueryBuilder("event.ingested"));
            }
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            int numBool = randomIntBetween(2, 10);
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            for (int i = 0; i < numBool; i++) {
                BoolQueryBuilder boolQueryBuilderNew = new BoolQueryBuilder();
                boolQueryBuilder.filter(boolQueryBuilderNew);
                boolQueryBuilder = boolQueryBuilderNew;
            }
            if (randomBoolean()) {
                boolQueryBuilder.should(new RangeQueryBuilder("event.ingested"));
            }

            boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp"));
            searchSourceBuilder.query(boolQueryBuilder);
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(new RangeQueryBuilder("@timestamp"));
            boolQueryBuilder.must(new RangeQueryBuilder("event.ingested"));
            boolQueryBuilder.must(new RangeQueryBuilder(randomAlphaOfLengthBetween(3, 10)));
            searchSourceBuilder.query(boolQueryBuilder);
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp", "event.ingested", "field" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(new RangeQueryBuilder("@timestamp"));
            searchSourceBuilder.query(boolQueryBuilder);
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(new RangeQueryBuilder(randomAlphaOfLengthBetween(3, 10)));
            searchSourceBuilder.query(boolQueryBuilder);
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "field" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            searchSourceBuilder.query(new ConstantScoreQueryBuilder(new RangeQueryBuilder("@timestamp")));
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            searchSourceBuilder.query(new BoostingQueryBuilder(new RangeQueryBuilder("@timestamp"), new MatchAllQueryBuilder()));
            SearchRequestIntrospector.QueryMetadata queryMetadata = SearchRequestIntrospector.introspectSearchRequest(searchRequest);
            assertEquals("user", queryMetadata.target());
            assertEquals("hits_only", queryMetadata.queryType());
            assertFalse(queryMetadata.knn());
            assertEquals("@timestamp", queryMetadata.primarySort());
            assertArrayEquals(new String[] { "@timestamp" }, queryMetadata.rangeFields());
            assertNull(queryMetadata.pitOrScroll());
        }
    }
}
