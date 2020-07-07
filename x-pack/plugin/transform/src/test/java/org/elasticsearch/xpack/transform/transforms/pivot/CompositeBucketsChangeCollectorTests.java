/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.transform.transforms.pivot;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.bucket.composite.CompositeAggregationBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSourceTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfig;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GroupConfigTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.HistogramGroupSourceTests;
import org.elasticsearch.xpack.core.transform.transforms.pivot.SingleGroupSource;
import org.elasticsearch.xpack.core.transform.transforms.pivot.TermsGroupSourceTests;
import org.elasticsearch.xpack.transform.transforms.Function.ChangeCollector;
import org.elasticsearch.xpack.transform.transforms.pivot.CompositeBucketsChangeCollector.FieldCollector;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

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
        SingleGroupSource termsGroupBy = TermsGroupSourceTests.randomTermsGroupSource();
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
