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
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSourceTests;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.pivot.CompositeBucketsChangeCollector.FieldCollector;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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

        // a terms group_by is limited by terms query
        SingleGroupSource termsGroupBy = TermsGroupSourceTests.randomTermsGroupSourceNoScript();
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

    private static CompositeAggregationBuilder getCompositeAggregationBuilder(SearchSourceBuilder builder) {
        return (CompositeAggregationBuilder) builder.aggregations().getAggregatorFactories().iterator().next();
    }
}
