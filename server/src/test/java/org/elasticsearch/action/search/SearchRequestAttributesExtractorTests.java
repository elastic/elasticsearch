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
import org.elasticsearch.search.retriever.KnnRetrieverBuilder;
import org.elasticsearch.search.retriever.StandardRetrieverBuilder;
import org.elasticsearch.search.retriever.TestCompoundRetrieverBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.GeoDistanceSortBuilder;
import org.elasticsearch.search.sort.ScoreSortBuilder;
import org.elasticsearch.search.sort.ScriptSortBuilder;
import org.elasticsearch.search.vectors.KnnSearchBuilder;
import org.elasticsearch.search.vectors.KnnVectorQueryBuilder;
import org.elasticsearch.test.ESTestCase;

import java.util.List;
import java.util.Map;

public class SearchRequestAttributesExtractorTests extends ESTestCase {

    public void testIndexIntrospectionSingleIndex() {
        assertEquals(".kibana", SearchRequestAttributesExtractor.extractIndices(new String[] { ".kibana_task_manager" }));
        assertEquals(".kibana", SearchRequestAttributesExtractor.extractIndices(new String[] { ".kibana_9_2_0" }));
        assertEquals(".kibana", SearchRequestAttributesExtractor.extractIndices(new String[] { ".kibana_ingest_9_2_0" }));
        assertEquals(".kibana", SearchRequestAttributesExtractor.extractIndices(new String[] { ".kibana_security_solution" }));
        assertEquals(".fleet", SearchRequestAttributesExtractor.extractIndices(new String[] { ".fleet-agents" }));
        assertEquals(".ml", SearchRequestAttributesExtractor.extractIndices(new String[] { ".ml-anomalies" }));
        assertEquals(".ml", SearchRequestAttributesExtractor.extractIndices(new String[] { ".ml-notifications" }));
        assertEquals(".slo", SearchRequestAttributesExtractor.extractIndices(new String[] { ".slo" }));
        assertEquals(".alerts", SearchRequestAttributesExtractor.extractIndices(new String[] { ".alerts" }));
        assertEquals(".elastic", SearchRequestAttributesExtractor.extractIndices(new String[] { ".elastic" }));
        assertEquals(".ds-", SearchRequestAttributesExtractor.extractIndices(new String[] { ".ds-test1" }));
        assertEquals(".ds-", SearchRequestAttributesExtractor.extractIndices(new String[] { ".ds-test2" }));
        assertEquals(".ds-", SearchRequestAttributesExtractor.extractIndices(new String[] { ".ds-test3" }));

        assertEquals(".others", SearchRequestAttributesExtractor.extractIndices(new String[] { ".a" }));
        assertEquals(".others", SearchRequestAttributesExtractor.extractIndices(new String[] { ".abcde" }));

        assertEquals("user", SearchRequestAttributesExtractor.extractIndices(new String[] { "a" }));
        assertEquals("user", SearchRequestAttributesExtractor.extractIndices(new String[] { "ab" }));
        assertEquals("user", SearchRequestAttributesExtractor.extractIndices(new String[] { "abc" }));
        assertEquals("user", SearchRequestAttributesExtractor.extractIndices(new String[] { "abcd" }));

        String indexName = "a" + randomAlphaOfLengthBetween(3, 10);
        assertEquals("user", SearchRequestAttributesExtractor.extractIndices(new String[] { indexName }));
    }

    public void testIndexIntrospectionMultipleIndices() {
        int length = randomIntBetween(2, 10);
        String[] indices = new String[length];
        for (int i = 0; i < length; i++) {
            indices[i] = randomAlphaOfLengthBetween(3, 10);
        }
        assertEquals("user", SearchRequestAttributesExtractor.extractIndices(indices));
    }

    public void testPrimarySortIntrospection() {
        assertEquals("@timestamp", SearchRequestAttributesExtractor.extractPrimarySort(new FieldSortBuilder("@timestamp")));
        assertEquals("event.ingested", SearchRequestAttributesExtractor.extractPrimarySort(new FieldSortBuilder("event.ingested")));
        assertEquals("_doc", SearchRequestAttributesExtractor.extractPrimarySort(new FieldSortBuilder("_doc")));
        assertEquals("field", SearchRequestAttributesExtractor.extractPrimarySort(new FieldSortBuilder(randomAlphaOfLengthBetween(3, 10))));
        assertEquals("_score", SearchRequestAttributesExtractor.extractPrimarySort(new ScoreSortBuilder()));
        assertEquals(
            "_geo_distance",
            SearchRequestAttributesExtractor.extractPrimarySort(new GeoDistanceSortBuilder(randomAlphaOfLengthBetween(3, 10), 1d, 1d))
        );
        assertEquals(
            "_script",
            SearchRequestAttributesExtractor.extractPrimarySort(
                new ScriptSortBuilder(new Script("id"), randomFrom(ScriptSortBuilder.ScriptSortType.values()))
            )
        );
    }

    public void testQueryTypeIntrospection() {
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            assertEquals("hits_only", SearchRequestAttributesExtractor.extractQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(randomIntBetween(1, 100));
            assertEquals("hits_only", SearchRequestAttributesExtractor.extractQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            assertEquals("count_only", SearchRequestAttributesExtractor.extractQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(new TermsAggregationBuilder("test"));
            assertEquals("hits_and_aggs", SearchRequestAttributesExtractor.extractQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(randomIntBetween(1, 100));
            searchSourceBuilder.aggregation(new TermsAggregationBuilder("test"));
            assertEquals("hits_and_aggs", SearchRequestAttributesExtractor.extractQueryType(searchSourceBuilder));
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.size(0);
            searchSourceBuilder.aggregation(new TermsAggregationBuilder("test"));
            assertEquals("aggs_only", SearchRequestAttributesExtractor.extractQueryType(searchSourceBuilder));
        }
    }

    private static void assertAttributes(
        Map<String, Object> attributes,
        String target,
        String primarySort,
        String queryType,
        boolean knn,
        boolean rangeOnTimestamp,
        boolean rangeOnEventIngested,
        String pitOrScroll
    ) {
        assertEquals(target, attributes.get(SearchRequestAttributesExtractor.TARGET_ATTRIBUTE));
        assertEquals(primarySort, attributes.get(SearchRequestAttributesExtractor.SORT_ATTRIBUTE));
        assertEquals(queryType, attributes.get(SearchRequestAttributesExtractor.QUERY_TYPE_ATTRIBUTE));
        assertEquals(pitOrScroll, attributes.get(SearchRequestAttributesExtractor.PIT_SCROLL_ATTRIBUTE));
        if (knn) {
            assertEquals(knn, attributes.get(SearchRequestAttributesExtractor.KNN_ATTRIBUTE));
        } else {
            assertNull(attributes.get(SearchRequestAttributesExtractor.KNN_ATTRIBUTE));
        }
        if (rangeOnTimestamp && rangeOnEventIngested) {
            assertEquals(
                "@timestamp_AND_event.ingested",
                attributes.get(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FIELD_ATTRIBUTE)
            );
        } else if (rangeOnTimestamp) {
            assertEquals("@timestamp", attributes.get(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FIELD_ATTRIBUTE));
        } else if (rangeOnEventIngested) {
            assertEquals("event.ingested", attributes.get(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FIELD_ATTRIBUTE));
        } else {
            assertNull(attributes.get(SearchRequestAttributesExtractor.TIME_RANGE_FILTER_FIELD_ATTRIBUTE));
        }
    }

    public void testExtractAttributesTargetOnly() {
        SearchRequest searchRequest = new SearchRequest();
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, false, false, null);
    }

    public void testExtractAttributesTopLevelKnn() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.knnSearch(List.of(new KnnSearchBuilder("field", new float[] {}, 2, 5, 10f, null, null, null)));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "_score", "hits_only", true, false, false, null);
    }

    public void testExtractAttributesKnnQuery() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.query(new KnnVectorQueryBuilder("field", new float[] {}, 2, 5, 10f, null, null, null));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "_score", "hits_only", true, false, false, null);
    }

    public void testExtractAttributesKnnRetriever() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.retriever(new KnnRetrieverBuilder("field", new float[] {}, null, 2, 5, 10f, null, null, null));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "_score", "hits_only", true, false, false, null);
    }

    public void testExtractAttributesPit() {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.pointInTimeBuilder(new PointInTimeBuilder(BytesArray.EMPTY));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, false, false, "pit");
    }

    public void testExtractAttributesScroll() {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.scroll(new TimeValue(randomIntBetween(1, 10)));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, false, false, "scroll");
    }

    public void testExtractAttributesTimestampSorted() {
        SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.sort("@timestamp");
        searchSourceBuilder.query(new RangeQueryBuilder("@timestamp"));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, false, false, null);
    }

    public void testExtractAttributesTimestampSortedTimeRangeFilter() {
        SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.sort("@timestamp");
        searchSourceBuilder.query(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
    }

    public void testExtractAttributesTimestampSortedTimeRangeFilterShouldClauses() {
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
        boolQueryBuilder.must(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
        searchSourceBuilder.query(boolQueryBuilder);
        if (randomBoolean()) {
            boolQueryBuilder.should(new RangeQueryBuilder("event.ingested").from("2021-11-11"));
        }
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
    }

    public void testExtractAttributesTimestampSortedTimeRangeFilters() {
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

            boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            searchSourceBuilder.query(boolQueryBuilder);
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            boolQueryBuilder.must(new RangeQueryBuilder("event.ingested").from("2021-11-11"));
            boolQueryBuilder.must(new RangeQueryBuilder(randomAlphaOfLengthBetween(3, 10)));
            searchSourceBuilder.query(boolQueryBuilder);
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, true, null);
        }
    }

    public void testExtractAttributesTimestampSortedRetriever() {
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

            boolQueryBuilder.filter(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            searchSourceBuilder.retriever(new StandardRetrieverBuilder(boolQueryBuilder));
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            TestCompoundRetrieverBuilder compoundRetrieverBuilder = new TestCompoundRetrieverBuilder(10);
            searchSourceBuilder.retriever(compoundRetrieverBuilder);
            int numCompound = randomIntBetween(2, 10);
            for (int i = 0; i < numCompound; i++) {
                TestCompoundRetrieverBuilder innerCompoundRetriever = new TestCompoundRetrieverBuilder(10);
                compoundRetrieverBuilder.addChild(innerCompoundRetriever);
                compoundRetrieverBuilder = innerCompoundRetriever;
            }
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            boolQueryBuilder.must(new RangeQueryBuilder("event.ingested").from("2021-11-11"));
            boolQueryBuilder.must(new RangeQueryBuilder(randomAlphaOfLengthBetween(3, 10)));
            compoundRetrieverBuilder.addChild(new StandardRetrieverBuilder(boolQueryBuilder));
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, true, null);
        }
    }

    public void testExtractAttributesTimestampSortedTimeRangeFilterOneShouldClause() {
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.should(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            searchSourceBuilder.query(boolQueryBuilder);
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
        }
        {
            SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchRequest.source(searchSourceBuilder);
            searchSourceBuilder.sort("@timestamp");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            // range on some other field
            boolQueryBuilder.should(new RangeQueryBuilder(randomAlphaOfLengthBetween(3, 10)).from("2021-11-11"));
            searchSourceBuilder.query(boolQueryBuilder);
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, false, false, null);
        }
    }

    public void testExtractAttributesTimestampSortedTimeRangeFilterConstantScore() {
        SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.sort("@timestamp");
        searchSourceBuilder.query(new ConstantScoreQueryBuilder(new RangeQueryBuilder("@timestamp").from("2021-11-11")));
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
    }

    public void testExtractAttributesTimestampSortedTimeRangeFilterBoostingQuery() {
        SearchRequest searchRequest = new SearchRequest(randomAlphaOfLengthBetween(3, 10));
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchRequest.source(searchSourceBuilder);
        searchSourceBuilder.sort("@timestamp");
        searchSourceBuilder.query(
            new BoostingQueryBuilder(new RangeQueryBuilder("@timestamp").from("2021-11-11"), new MatchAllQueryBuilder())
        );
        Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(searchRequest, searchRequest.indices());
        assertAttributes(stringObjectMap, "user", "@timestamp", "hits_only", false, true, false, null);
    }

    public void testIntrospectQueryBuilderDepthLimit() {
        {
            SearchRequest searchRequest = new SearchRequest("index");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));
            BoolQueryBuilder newBoolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(newBoolQueryBuilder);
            int depth = randomIntBetween(5, 18);
            for (int i = 0; i < depth; i++) {
                BoolQueryBuilder innerBoolQueryBuilder = new BoolQueryBuilder();
                newBoolQueryBuilder.must(innerBoolQueryBuilder);
                newBoolQueryBuilder = innerBoolQueryBuilder;
            }
            newBoolQueryBuilder.must(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, true, false, null);
        }
        {
            SearchRequest searchRequest = new SearchRequest("index");
            BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
            searchRequest.source(new SearchSourceBuilder().query(boolQueryBuilder));
            BoolQueryBuilder newBoolQueryBuilder = new BoolQueryBuilder();
            boolQueryBuilder.must(newBoolQueryBuilder);
            int depth = randomIntBetween(19, 50);
            for (int i = 0; i < depth; i++) {
                BoolQueryBuilder innerBoolQueryBuilder = new BoolQueryBuilder();
                newBoolQueryBuilder.must(innerBoolQueryBuilder);
                newBoolQueryBuilder = innerBoolQueryBuilder;
            }
            newBoolQueryBuilder.must(new RangeQueryBuilder("@timestamp").from("2021-11-11"));
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, false, false, null);
        }
    }

    public void testIntrospectCompoundRetrieverBuilderDepthLimit() {
        {
            SearchRequest searchRequest = new SearchRequest("index");
            TestCompoundRetrieverBuilder compoundRetrieverBuilder = new TestCompoundRetrieverBuilder(10);
            searchRequest.source(new SearchSourceBuilder().retriever(compoundRetrieverBuilder));
            int depth = randomIntBetween(5, 18);
            for (int i = 0; i < depth; i++) {
                TestCompoundRetrieverBuilder innerCmpoundRetrieverBuilder = new TestCompoundRetrieverBuilder(10);
                compoundRetrieverBuilder.addChild(innerCmpoundRetrieverBuilder);
                compoundRetrieverBuilder = innerCmpoundRetrieverBuilder;
            }
            compoundRetrieverBuilder.addChild(new StandardRetrieverBuilder(new RangeQueryBuilder("@timestamp").from("2021-11-11")));
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, true, false, null);
        }
        {
            SearchRequest searchRequest = new SearchRequest("index");
            TestCompoundRetrieverBuilder compoundRetrieverBuilder = new TestCompoundRetrieverBuilder(10);
            searchRequest.source(new SearchSourceBuilder().retriever(compoundRetrieverBuilder));
            int depth = randomIntBetween(19, 50);
            for (int i = 0; i < depth; i++) {
                TestCompoundRetrieverBuilder innerCmpoundRetrieverBuilder = new TestCompoundRetrieverBuilder(10);
                compoundRetrieverBuilder.addChild(innerCmpoundRetrieverBuilder);
                compoundRetrieverBuilder = innerCmpoundRetrieverBuilder;
            }
            compoundRetrieverBuilder.addChild(new StandardRetrieverBuilder(new RangeQueryBuilder("@timestamp").from("2021-11-11")));
            Map<String, Object> stringObjectMap = SearchRequestAttributesExtractor.extractAttributes(
                searchRequest,
                searchRequest.indices()
            );
            assertAttributes(stringObjectMap, "user", "_score", "hits_only", false, false, false, null);
        }
    }

    public void testIntrospectTimeRange() {
        long nowInMillis = System.currentTimeMillis();
        assertEquals("15_minutes", SearchRequestAttributesExtractor.introspectTimeRange(nowInMillis, nowInMillis));

        long fifteenMinutesAgo = nowInMillis - (15 * 60 * 1000);
        assertEquals(
            "15_minutes",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(fifteenMinutesAgo, nowInMillis), nowInMillis)
        );

        long oneHourAgo = nowInMillis - (60 * 60 * 1000);
        assertEquals(
            "1_hour",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(oneHourAgo, fifteenMinutesAgo), nowInMillis)
        );

        long twelveHoursAgo = nowInMillis - (12 * 60 * 60 * 1000);
        assertEquals(
            "12_hours",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(twelveHoursAgo, oneHourAgo), nowInMillis)
        );

        long oneDayAgo = nowInMillis - (24 * 60 * 60 * 1000);
        assertEquals(
            "1_day",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(oneDayAgo, twelveHoursAgo), nowInMillis)
        );

        long threeDaysAgo = nowInMillis - (3 * 24 * 60 * 60 * 1000);
        assertEquals(
            "3_days",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(threeDaysAgo, oneDayAgo), nowInMillis)
        );

        long sevenDaysAgo = nowInMillis - (7 * 24 * 60 * 60 * 1000);
        assertEquals(
            "7_days",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(sevenDaysAgo, threeDaysAgo), nowInMillis)
        );

        long fourteenDaysAgo = nowInMillis - (14 * 24 * 60 * 60 * 1000);
        assertEquals(
            "14_days",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(fourteenDaysAgo, sevenDaysAgo), nowInMillis)
        );

        assertEquals(
            "older_than_14_days",
            SearchRequestAttributesExtractor.introspectTimeRange(randomLongBetween(0, fourteenDaysAgo), nowInMillis)
        );
    }
}
