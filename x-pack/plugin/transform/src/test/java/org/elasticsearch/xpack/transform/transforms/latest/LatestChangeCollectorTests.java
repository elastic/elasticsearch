/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.latest;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.SearchResponseUtils;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.composite.InternalComposite;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.TransformCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LatestChangeCollectorTests extends ESTestCase {

    private static final TransformCheckpoint CHECKPOINT_OLD = new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 0L);
    private static final TransformCheckpoint CHECKPOINT_NEW = new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 123456789L);

    public void testQueryForChanges() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));
        assertTrue(changeCollector.queryForChanges());
    }

    public void testIsOptimized() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));
        assertTrue(changeCollector.isOptimized());
    }

    public void testBuildChangesQuerySingleField() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));
        SearchSourceBuilder sourceBuilder = changeCollector.buildChangesQuery(new SearchSourceBuilder(), null, 500);

        assertThat(sourceBuilder.size(), is(equalTo(0)));
        CompositeAggregationBuilder compositeAgg = getCompositeAggregationBuilder(sourceBuilder);
        assertThat(compositeAgg.getName(), is(equalTo(LatestChangeCollector.COMPOSITE_AGGREGATION_NAME)));
        assertThat(compositeAgg.size(), is(equalTo(500)));
    }

    public void testBuildChangesQueryMultipleFields() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId", "region"));
        SearchSourceBuilder sourceBuilder = changeCollector.buildChangesQuery(new SearchSourceBuilder(), null, 1000);

        CompositeAggregationBuilder compositeAgg = getCompositeAggregationBuilder(sourceBuilder);
        assertThat(compositeAgg.getName(), is(equalTo(LatestChangeCollector.COMPOSITE_AGGREGATION_NAME)));
        assertThat(compositeAgg.size(), is(equalTo(1000)));
    }

    public void testBuildChangesQueryWithAfterKey() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));
        Map<String, Object> afterKey = Map.of("orderId", "id99");
        SearchSourceBuilder sourceBuilder = changeCollector.buildChangesQuery(new SearchSourceBuilder(), afterKey, 500);

        CompositeAggregationBuilder compositeAgg = getCompositeAggregationBuilder(sourceBuilder);
        assertThat(compositeAgg.size(), is(equalTo(500)));
        String searchSource = sourceBuilder.toString();
        assertTrue("after key should be set in the composite aggregation", searchSource.contains("id99"));
    }

    public void testProcessSearchResponseCollectsKeys() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));
        // Set page size to match bucket count so we get afterKey (not last page)
        changeCollector.buildChangesQuery(new SearchSourceBuilder(), null, 3);

        SearchResponse response = createSearchResponse(
            List.of(Map.of("orderId", "id1"), Map.of("orderId", "id2"), Map.of("orderId", "id3")),
            Map.of("orderId", "id3")
        );
        try {
            Map<String, Object> afterKey = changeCollector.processSearchResponse(response);
            assertThat(afterKey, is(notNullValue()));

            QueryBuilder filterQuery = changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW);
            assertThat(filterQuery, instanceOf(TermsQueryBuilder.class));
            assertThat(((TermsQueryBuilder) filterQuery).values(), containsInAnyOrder("id1", "id2", "id3"));
        } finally {
            response.decRef();
        }
    }

    public void testProcessSearchResponseReturnsNullForNoAggregations() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        SearchResponse response = SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS).build();
        try {
            Map<String, Object> afterKey = changeCollector.processSearchResponse(response);
            assertThat(afterKey, is(nullValue()));
        } finally {
            response.decRef();
        }
    }

    public void testProcessSearchResponseReturnsNullForEmptyBuckets() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        SearchResponse response = createSearchResponse(List.of(), null);
        try {
            Map<String, Object> afterKey = changeCollector.processSearchResponse(response);
            assertThat(afterKey, is(nullValue()));
        } finally {
            response.decRef();
        }
    }

    public void testBuildFilterQuerySingleField() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        SearchResponse response = createSearchResponse(
            List.of(Map.of("orderId", "order-A"), Map.of("orderId", "order-B")),
            Map.of("orderId", "order-B")
        );
        try {
            changeCollector.processSearchResponse(response);

            QueryBuilder filterQuery = changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW);
            assertThat(filterQuery, instanceOf(TermsQueryBuilder.class));
            TermsQueryBuilder termsQuery = (TermsQueryBuilder) filterQuery;
            assertThat(termsQuery.fieldName(), is(equalTo("orderId")));
            assertThat(termsQuery.values(), containsInAnyOrder("order-A", "order-B"));
        } finally {
            response.decRef();
        }
    }

    public void testBuildFilterQueryMultipleFields() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId", "region"));

        SearchResponse response = createSearchResponse(
            List.of(Map.of("orderId", "order-A", "region", "US"), Map.of("orderId", "order-B", "region", "EU")),
            Map.of("orderId", "order-B", "region", "EU")
        );
        try {
            changeCollector.processSearchResponse(response);

            QueryBuilder filterQuery = changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW);
            assertThat(filterQuery, instanceOf(BoolQueryBuilder.class));
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) filterQuery;
            assertThat(boolQuery.filter().size(), is(equalTo(2)));

            TermsQueryBuilder orderFilter = (TermsQueryBuilder) boolQuery.filter().get(0);
            assertThat(orderFilter.fieldName(), is(equalTo("orderId")));
            assertThat(orderFilter.values(), containsInAnyOrder("order-A", "order-B"));

            TermsQueryBuilder regionFilter = (TermsQueryBuilder) boolQuery.filter().get(1);
            assertThat(regionFilter.fieldName(), is(equalTo("region")));
            assertThat(regionFilter.values(), containsInAnyOrder("US", "EU"));
        } finally {
            response.decRef();
        }
    }

    public void testBuildFilterQueryWithNullBucket() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        Map<String, Object> bucketWithNull = new java.util.HashMap<>();
        bucketWithNull.put("orderId", null);

        SearchResponse response = createSearchResponse(List.of(Map.of("orderId", "order-A"), bucketWithNull), Map.of("orderId", "order-A"));
        try {
            changeCollector.processSearchResponse(response);

            QueryBuilder filterQuery = changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW);
            assertThat(filterQuery, instanceOf(BoolQueryBuilder.class));
            BoolQueryBuilder boolQuery = (BoolQueryBuilder) filterQuery;
            assertThat(boolQuery.should().size(), is(equalTo(2)));
        } finally {
            response.decRef();
        }
    }

    public void testClearResetsState() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        SearchResponse response = createSearchResponse(List.of(Map.of("orderId", "order-A")), Map.of("orderId", "order-A"));
        try {
            changeCollector.processSearchResponse(response);
            assertThat(changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW), is(notNullValue()));

            changeCollector.clear();
            assertThat(changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW), is(nullValue()));
        } finally {
            response.decRef();
        }
    }

    public void testProcessSearchResponseClearsPreviousPageKeys() throws IOException {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        SearchResponse firstPage = createSearchResponse(List.of(Map.of("orderId", "page1-id")), Map.of("orderId", "page1-id"));
        try {
            changeCollector.processSearchResponse(firstPage);
        } finally {
            firstPage.decRef();
        }

        SearchResponse secondPage = createSearchResponse(List.of(Map.of("orderId", "page2-id")), Map.of("orderId", "page2-id"));
        try {
            changeCollector.processSearchResponse(secondPage);

            QueryBuilder filterQuery = changeCollector.buildFilterQuery(CHECKPOINT_OLD, CHECKPOINT_NEW);
            assertThat(filterQuery, instanceOf(TermsQueryBuilder.class));
            TermsQueryBuilder termsQuery = (TermsQueryBuilder) filterQuery;
            assertThat(termsQuery.values(), containsInAnyOrder("page2-id"));
        } finally {
            secondPage.decRef();
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/77329")
    public void testGetIndicesToQuery() {
        LatestChangeCollector changeCollector = new LatestChangeCollector("timestamp", List.of("orderId"));

        long[] indexSequenceIds1 = { 25L, 25L, 25L };
        long[] indexSequenceIds2 = { 324L, 2425L, 2225L };
        long[] indexSequenceIds3 = { 244L, 225L, 2425L };
        long[] indexSequenceIds4 = { 2005L, 2445L, 2425L };

        long[] indexSequenceIds3_1 = { 246L, 255L, 2485L };
        long[] indexSequenceIds4_1 = { 2105L, 2545L, 2525L };

        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123456789L
                )
            ),
            equalTo(Collections.emptySet())
        );

        assertThat(
            changeCollector.getIndicesToQuery(
                new TransformCheckpoint(
                    "t_id",
                    123513L,
                    42L,
                    Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3,
                        "index-4",
                        indexSequenceIds4
                    ),
                    123543L
                ),
                new TransformCheckpoint(
                    "t_id",
                    123456759L,
                    43L,
                    Map.of(
                        "index-1",
                        indexSequenceIds1,
                        "index-2",
                        indexSequenceIds2,
                        "index-3",
                        indexSequenceIds3_1,
                        "index-4",
                        indexSequenceIds4_1
                    ),
                    123456789L
                )
            ),
            equalTo(Set.of("index-3", "index-4"))
        );
    }

    private static SearchResponse createSearchResponse(List<Map<String, Object>> bucketKeys, Map<String, Object> afterKey) {
        InternalComposite composite = mock(InternalComposite.class);
        when(composite.getName()).thenReturn(LatestChangeCollector.COMPOSITE_AGGREGATION_NAME);
        when(composite.afterKey()).thenReturn(afterKey);

        List<InternalComposite.InternalBucket> compositeBuckets = new ArrayList<>();
        for (Map<String, Object> key : bucketKeys) {
            InternalComposite.InternalBucket bucket = mock(InternalComposite.InternalBucket.class);
            when(bucket.getKey()).thenReturn(key);
            compositeBuckets.add(bucket);
        }
        when(composite.getBuckets()).thenReturn(compositeBuckets);

        return SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS).aggregations(InternalAggregations.from(composite)).build();
    }

    private static CompositeAggregationBuilder getCompositeAggregationBuilder(SearchSourceBuilder builder) {
        return (CompositeAggregationBuilder) builder.aggregations().getAggregatorFactories().iterator().next();
    }
}
