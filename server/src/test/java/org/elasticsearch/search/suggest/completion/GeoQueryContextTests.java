/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.suggest.completion;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.search.suggest.completion.context.GeoQueryContext;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.equalTo;

public class GeoQueryContextTests extends QueryContextTestCase<GeoQueryContext> {

    public static GeoQueryContext randomGeoQueryContext() {
        final GeoQueryContext.Builder builder = GeoQueryContext.builder();
        builder.setGeoPoint(new GeoPoint(randomDouble(), randomDouble()));
        maybeSet(builder::setBoost, randomIntBetween(1, 10));
        maybeSet(builder::setPrecision, randomIntBetween(1, 12));
        List<Integer> neighbours = new ArrayList<>();
        for (int i = 0; i < randomIntBetween(1, 12); i++) {
            neighbours.add(randomIntBetween(1, 12));
        }
        maybeSet(builder::setNeighbours, neighbours);
        return builder.build();
    }

    @Override
    protected GeoQueryContext createTestModel() {
        return randomGeoQueryContext();
    }

    @Override
    protected GeoQueryContext fromXContent(XContentParser parser) throws IOException {
        return GeoQueryContext.fromXContent(parser);
    }

    public void testNullGeoPointIsIllegal() {
        final GeoQueryContext geoQueryContext = randomGeoQueryContext();
        final GeoQueryContext.Builder builder = GeoQueryContext.builder()
            .setNeighbours(geoQueryContext.getNeighbours())
            .setPrecision(geoQueryContext.getPrecision())
            .setBoost(geoQueryContext.getBoost());
        try {
            builder.build();
            fail("null geo point is illegal");
        } catch (NullPointerException e) {
            assertThat(e.getMessage(), equalTo("geoPoint must not be null"));
        }
    }

    public void testIllegalArguments() {
        final GeoQueryContext.Builder builder = GeoQueryContext.builder();

        try {
            builder.setGeoPoint(null);
            fail("geoPoint must not be null");
        } catch (NullPointerException e) {
            assertEquals(e.getMessage(), "geoPoint must not be null");
        }
        try {
            builder.setBoost(-randomIntBetween(1, Integer.MAX_VALUE));
            fail("boost must be positive");
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "boost must be greater than 0");
        }
        int precision = 0;
        try {
            do {
                precision = randomInt();
            } while (precision >= 1 && precision <= 12);
            builder.setPrecision(precision);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "precision must be between 1 and 12");
        }
        try {
            List<Integer> neighbours = new ArrayList<>();
            neighbours.add(precision);
            for (int i = 1; i < randomIntBetween(1, 11); i++) {
                neighbours.add(i);
            }
            Collections.shuffle(neighbours, random());
            builder.setNeighbours(neighbours);
        } catch (IllegalArgumentException e) {
            assertEquals(e.getMessage(), "neighbour value must be between 1 and 12");
        }
    }

    public void testStringPrecision() throws IOException {
        XContentBuilder builder = jsonBuilder().startObject();
        {
            builder.startObject("context").field("lat", 23.654242).field("lon", 90.047153).endObject();
            builder.field("boost", 10);
            builder.field("precision", 12);
            builder.array("neighbours", 1, 2);
        }
        builder.endObject();
        XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken();
        GeoQueryContext queryContext = fromXContent(parser);
        assertEquals(10, queryContext.getBoost());
        assertEquals(12, queryContext.getPrecision());
        assertEquals(Arrays.asList(1, 2), queryContext.getNeighbours());

        builder = jsonBuilder().startObject();
        {
            builder.startObject("context").field("lat", 23.654242).field("lon", 90.047153).endObject();
            builder.field("boost", 10);
            builder.field("precision", "12m");
            builder.array("neighbours", "4km", "10km");
        }
        builder.endObject();
        parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder));
        parser.nextToken();
        queryContext = fromXContent(parser);
        assertEquals(10, queryContext.getBoost());
        assertEquals(9, queryContext.getPrecision());
        assertEquals(Arrays.asList(6, 5), queryContext.getNeighbours());
    }
}
