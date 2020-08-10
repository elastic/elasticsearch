/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchResponseSections;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;
import org.elasticsearch.index.query.TermsQueryBuilder;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.DateHistogramGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSourceTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSourceTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.ScriptConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSourceTests;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.pivot.CompositeBucketsChangeCollector.FieldCollector;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
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

        // a histogram group_by has no limits
        SingleGroupSource histogramGroupBy = HistogramGroupSourceTests.randomHistogramGroupSource();
        groups.put("hist", histogramGroupBy);

        ChangeCollector collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, null);
        collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, null);
        assertEquals(1_000, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 1_000)).size());
        assertEquals(100_000, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 100_000)).size());
        assertEquals(10, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 10)).size());

        // a terms group_by is limited by terms query
        SingleGroupSource termsGroupBy = TermsGroupSourceTests.randomTermsGroupSourceNoScript();
        groups.put("terms", termsGroupBy);

        collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, null);
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

        collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, null);
        assertEquals(1_000, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 1_000)).size());
        assertEquals(1_024, getCompositeAggregationBuilder(collector.buildChangesQuery(new SearchSourceBuilder(), null, 10_000)).size());
    }

    public void testTermsFieldCollector() throws IOException {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        // a terms group_by is limited by terms query
        SingleGroupSource termsGroupBy = new TermsGroupSource("id", null, false);
        groups.put("id", termsGroupBy);

        ChangeCollector collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, null);

        CompositeAggregation composite = mock(CompositeAggregation.class);
        when(composite.getName()).thenReturn("_transform");
        when(composite.getBuckets()).thenAnswer((Answer<List<CompositeAggregation.Bucket>>) invocationOnMock -> {
            List<CompositeAggregation.Bucket> compositeBuckets = new ArrayList<>();
            CompositeAggregation.Bucket bucket = mock(CompositeAggregation.Bucket.class);
            when(bucket.getKey()).thenReturn(Collections.singletonMap("id", "id1"));
            compositeBuckets.add(bucket);

            bucket = mock(CompositeAggregation.Bucket.class);
            when(bucket.getKey()).thenReturn(Collections.singletonMap("id", "id2"));
            compositeBuckets.add(bucket);

            bucket = mock(CompositeAggregation.Bucket.class);
            when(bucket.getKey()).thenReturn(Collections.singletonMap("id", "id3"));
            compositeBuckets.add(bucket);

            return compositeBuckets;
        });
        Aggregations aggs = new Aggregations(Collections.singletonList(composite));

        SearchResponseSections sections = new SearchResponseSections(null, aggs, null, false, null, null, 1);
        SearchResponse response = new SearchResponse(sections, null, 1, 1, 0, 0, ShardSearchFailure.EMPTY_ARRAY, null);

        collector.processSearchResponse(response);

        QueryBuilder queryBuilder = collector.buildFilterQuery(0, 0);
        assertNotNull(queryBuilder);
        assertThat(queryBuilder, instanceOf(TermsQueryBuilder.class));
        assertThat(((TermsQueryBuilder) queryBuilder).values(), containsInAnyOrder("id1", "id2", "id3"));
    }

    public void testDateHistogramFieldCollector() throws IOException {
        Map<String, SingleGroupSource> groups = new LinkedHashMap<>();

        SingleGroupSource groupBy = new DateHistogramGroupSource(
            "timestamp",
            null,
            false,
            new DateHistogramGroupSource.FixedInterval(DateHistogramInterval.MINUTE),
            null
        );
        groups.put("output_timestamp", groupBy);

        ChangeCollector collector = CompositeBucketsChangeCollector.buildChangeCollector(
            getCompositeAggregation(groups),
            groups,
            "timestamp"
        );

        QueryBuilder queryBuilder = collector.buildFilterQuery(66_666, 200_222);
        assertNotNull(queryBuilder);
        assertThat(queryBuilder, instanceOf(RangeQueryBuilder.class));
        // rounded down
        assertThat(((RangeQueryBuilder) queryBuilder).from(), equalTo(Long.valueOf(60_000)));
        assertTrue(((RangeQueryBuilder) queryBuilder).includeLower());
        assertThat(((RangeQueryBuilder) queryBuilder).fieldName(), equalTo("timestamp"));

        // timestamp field does not match
        collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, "sync_timestamp");

        queryBuilder = collector.buildFilterQuery(66_666, 200_222);
        assertNull(queryBuilder);

        // field does not match, but output field equals sync field
        collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, "output_timestamp");

        queryBuilder = collector.buildFilterQuery(66_666, 200_222);
        assertNull(queryBuilder);

        // missing bucket disables optimization
        groupBy = new DateHistogramGroupSource(
            "timestamp",
            null,
            true,
            new DateHistogramGroupSource.FixedInterval(DateHistogramInterval.MINUTE),
            null
        );
        groups.put("output_timestamp", groupBy);

        collector = CompositeBucketsChangeCollector.buildChangeCollector(getCompositeAggregation(groups), groups, "timestamp");

        queryBuilder = collector.buildFilterQuery(66_666, 200_222);
        assertNull(queryBuilder);
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

    private static CompositeAggregationBuilder getCompositeAggregation(Map<String, SingleGroupSource> groups) throws IOException {
        CompositeAggregationBuilder compositeAggregation;
        try (XContentBuilder builder = jsonBuilder()) {
            builder.startObject();
            builder.field(CompositeAggregationBuilder.SOURCES_FIELD_NAME.getPreferredName());
            builder.startArray();
            for (Entry<String, SingleGroupSource> groupBy : groups.entrySet()) {
                builder.startObject();
                builder.startObject(groupBy.getKey());
                builder.field(groupBy.getValue().getType().value(), groupBy.getValue());
                builder.endObject();
                builder.endObject();
            }
            builder.endArray();
            builder.endObject(); // sources

            XContentParser parser = builder.generator()
                .contentType()
                .xContent()
                .createParser(NamedXContentRegistry.EMPTY, LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
            compositeAggregation = CompositeAggregationBuilder.PARSER.parse(parser, "_transform");
        }

        return compositeAggregation;
    }

    private static CompositeAggregationBuilder getCompositeAggregationBuilder(SearchSourceBuilder builder) {
        return (CompositeAggregationBuilder) builder.aggregations().getAggregatorFactories().iterator().next();
    }
}
