/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.search.SearchResponse;
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
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSourceTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.ScriptConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.pivot.CompositeBucketsChangeCollector.FieldCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class CompositeBucketsChangeCollectorTests extends ESTestCase {

    /**
     * Simple unit tests to check that a field collector has been implemented for any single source type
     */
    public void testCreateFieldCollector() {
        GroupConfig groupConfig = GroupConfigTests.randomGroupConfig();

        Map<String, FieldCollector> changeCollector = CompositeBucketsChangeCollector.createFieldCollectors(
            groupConfig.getGroups(),
            randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null
        );

        assertNotNull(changeCollector);
    }

    public void testPageSize() throws IOException {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // a terms group_by is limited by terms query (use null maxTerms so it contributes a composite source)
        SingleGroupSource termsGroupBy = new TermsGroupSource(randomAlphaOfLengthBetween(1, 20), null, randomBoolean());
        groups.put("terms", termsGroupBy);

        ChangeCollector collector = CompositeBucketsChangeCollector.buildChangeCollector(groups, null);
        assertEquals(1_000, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 1_000)).size());
        assertEquals(10_000, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 10_000)).size());
        assertEquals(10, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 10)).size());
        assertEquals(65536, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 100_000)).size());
        assertEquals(
            65536,
            getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, Integer.MAX_VALUE)).size()
        );

        // a geo tile group_by is limited by query clauses
        SingleGroupSource geoGroupBy = GeoTileGroupSourceTests.randomGeoTileGroupSource();
        groups.put("geo_tile", geoGroupBy);

        collector = CompositeBucketsChangeCollector.buildChangeCollector(groups, null);
        assertEquals(1_000, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 1_000)).size());
        assertEquals(1_024, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 10_000)).size());
    }

    public void testTermsFieldCollector() throws IOException {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // a terms group_by is limited by terms query
        SingleGroupSource termsGroupBy = new TermsGroupSource("id", null, false);
        groups.put("id", termsGroupBy);

        ChangeCollector collector = CompositeBucketsChangeCollector.buildChangeCollector(groups, null);

        InternalComposite composite = mock(InternalComposite.class);
        when(composite.getName()).thenReturn("_transform_change_collector");
        when(composite.getBuckets()).thenAnswer(invocationOnMock -> {
            List<InternalComposite.InternalBucket> compositeBuckets = new ArrayList<>();
            InternalComposite.InternalBucket bucket = mock(InternalComposite.InternalBucket.class);
            when(bucket.getKey()).thenReturn(Collections.singletonMap("id", "id1"));
            compositeBuckets.add(bucket);

            bucket = mock(InternalComposite.InternalBucket.class);
            when(bucket.getKey()).thenReturn(Collections.singletonMap("id", "id2"));
            compositeBuckets.add(bucket);

            bucket = mock(InternalComposite.InternalBucket.class);
            when(bucket.getKey()).thenReturn(Collections.singletonMap("id", "id3"));
            compositeBuckets.add(bucket);

            return compositeBuckets;
        });

        SearchResponse response = SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS)
            .aggregations(InternalAggregations.from(composite))
            .build();
        try {
            collector.processSearchResponse(response);

            QueryBuilder queryBuilder = collector.buildFilterQuery(
                new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 0L),
                new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 0L)
            );
            assertNotNull(queryBuilder);
            assertThat(queryBuilder, instanceOf(TermsQueryBuilder.class));
            assertThat(((TermsQueryBuilder) queryBuilder).values(), containsInAnyOrder("id1", "id2", "id3"));
        } finally {
            response.decRef();
        }
    }

    public void testNoTermsFieldCollectorForScripts() throws IOException {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // terms with value script
        SingleGroupSource termsGroupBy = new TermsGroupSource("id", ScriptConfigTests.randomScriptConfig(), false);
        groups.put("id", termsGroupBy);

        Map<String, FieldCollector> fieldCollectors = CompositeBucketsChangeCollector.createFieldCollectors(groups, null);
        assertTrue(fieldCollectors.isEmpty());

        // terms with only a script
        termsGroupBy = new TermsGroupSource(null, ScriptConfigTests.randomScriptConfig(), false);
        groups.put("id", termsGroupBy);

        fieldCollectors = CompositeBucketsChangeCollector.createFieldCollectors(groups, null);
        assertTrue(fieldCollectors.isEmpty());
    }

    public void testTermsFieldCollectorOverflow() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(10_000);
        assertTrue(collector.isOptimized());

        processWithTerms(collector, 10_001);
        assertFalse(collector.isOptimized());
        assertNull(buildFilterQuery(collector));
    }

    public void testTermsFieldCollectorWithThresholdZeroIsDisabledFromStart() {
        ChangeCollector collector = buildTermsChangeCollector(0);
        assertFalse(collector.isOptimized());
        assertFalse(collector.queryForChanges());
        assertNull(buildFilterQuery(collector));
    }

    public void testTermsFieldCollectorWithThresholdZeroStaysDisabledAfterReset() {
        ChangeCollector collector = buildTermsChangeCollector(0);
        assertFalse(collector.isOptimized());

        collector.buildChangesQuery(new SearchSourceBuilder(), null, 1000);
        assertFalse(collector.isOptimized());
        assertFalse(collector.queryForChanges());
    }

    public void testTermsFieldCollectorWithThresholdZeroHasNoCompositeSource() {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();
        groups.put("id", new TermsGroupSource("id", null, false, 0));
        Map<String, CompositeBucketsChangeCollector.FieldCollector> fieldCollectors = CompositeBucketsChangeCollector.createFieldCollectors(
            groups,
            null
        );
        assertFalse(fieldCollectors.isEmpty());
        assertNull(fieldCollectors.get("id").getCompositeValueSourceBuilder());
    }

    public void testTermsFieldCollectorBelowThreshold() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(10_000);

        processWithTerms(collector, 10_000);
        assertTrue(collector.isOptimized());

        QueryBuilder queryBuilder = buildFilterQuery(collector);
        assertNotNull(queryBuilder);
        assertThat(queryBuilder, instanceOf(TermsQueryBuilder.class));
        assertEquals(10_000, ((TermsQueryBuilder) queryBuilder).values().size());
    }

    public void testOverflowResetOnNewCheckpoint() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(10_000);

        processWithTerms(collector, 10_001);
        assertFalse(collector.isOptimized());

        collector.buildChangesQuery(new SearchSourceBuilder(), null, 1000);
        assertTrue(collector.isOptimized());
    }

    public void testOverflowPersistsThroughClear() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(10_000);

        processWithTerms(collector, 10_001);
        assertFalse(collector.isOptimized());

        collector.clear();
        assertFalse(collector.isOptimized());
    }

    public void testOverflowedCollectorReturnsDoneOnSubsequentCalls() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(10_000);

        Map<String, Object> position = processWithTerms(collector, 10_001);
        assertNotNull(position);
        assertFalse(collector.isOptimized());

        // after APPLY_RESULTS, the indexer returns to IDENTIFY_CHANGES
        // with more composite data; the overflowed collector should return done immediately
        position = processWithTerms(collector, 3);
        assertNull(position);
    }

    public void testTermsFieldCollectorMultiPageOverflow() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(100);
        assertTrue(collector.isOptimized());

        processWithTerms(collector, 60);
        assertTrue(collector.isOptimized());

        collector.clear();

        processWithTerms(collector, 50);
        assertFalse(collector.isOptimized());
        assertNull(buildFilterQuery(collector));
    }

    public void testTermsFieldCollectorMultiPageBelowThreshold() throws IOException {
        ChangeCollector collector = buildTermsChangeCollector(100);

        processWithTerms(collector, 50);
        assertTrue(collector.isOptimized());

        collector.clear();

        processWithTerms(collector, 50);
        assertTrue(collector.isOptimized());

        QueryBuilder queryBuilder = buildFilterQuery(collector);
        assertNotNull(queryBuilder);
        assertThat(queryBuilder, instanceOf(TermsQueryBuilder.class));
        assertEquals(50, ((TermsQueryBuilder) queryBuilder).values().size());
    }

    private static ChangeCollector buildTermsChangeCollector(int maxTermsForChangeDetection) {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();
        groups.put("id", new TermsGroupSource("id", null, false, maxTermsForChangeDetection));
        return CompositeBucketsChangeCollector.buildChangeCollector(groups, null);
    }

    private Map<String, Object> processWithTerms(ChangeCollector collector, int termCount) throws IOException {
        InternalComposite composite = createMockCompositeWithTerms("id", termCount);
        SearchResponse response = SearchResponseUtils.response(SearchHits.EMPTY_WITH_TOTAL_HITS)
            .aggregations(InternalAggregations.from(composite))
            .build();
        try {
            return collector.processSearchResponse(response);
        } finally {
            response.decRef();
        }
    }

    private static QueryBuilder buildFilterQuery(ChangeCollector collector) {
        return collector.buildFilterQuery(
            new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 0L),
            new TransformCheckpoint("t_id", 42L, 42L, Collections.emptyMap(), 0L)
        );
    }

    private InternalComposite createMockCompositeWithTerms(String fieldName, int termCount) {
        InternalComposite composite = mock(InternalComposite.class);
        when(composite.getName()).thenReturn("_transform_change_collector");
        when(composite.getBuckets()).thenAnswer(invocationOnMock -> {
            List<InternalComposite.InternalBucket> buckets = new ArrayList<>();
            for (int i = 0; i < termCount; i++) {
                InternalComposite.InternalBucket bucket = mock(InternalComposite.InternalBucket.class);
                when(bucket.getKey()).thenReturn(Collections.singletonMap(fieldName, "term_" + i));
                buckets.add(bucket);
            }
            return buckets;
        });
        Map<String, Object> afterKey = new HashMap<>();
        afterKey.put(fieldName, "term_" + (termCount - 1));
        when(composite.afterKey()).thenReturn(afterKey);
        return composite;
    }

    private static CompositeAggregationBuilder getCompositeAggregationBuilder(SearchSourceBuilder builder) {
        return (CompositeAggregationBuilder) builder.aggregations().getAggregatorFactories().iterator().next();
    }
}
