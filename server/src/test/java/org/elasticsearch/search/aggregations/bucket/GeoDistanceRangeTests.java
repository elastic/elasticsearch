/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.GeoDistanceAggregationBuilder.Range;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class GeoDistanceRangeTests extends BaseAggregationTestCase<GeoDistanceAggregationBuilder> {

    @Override
    protected GeoDistanceAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        Point origin = GeometryTestUtils.randomPoint();
        GeoDistanceAggregationBuilder factory = new GeoDistanceAggregationBuilder(
            randomAlphaOfLengthBetween(3, 10),
            new GeoPoint(origin.getLat(), origin.getLon())
        );
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAlphaOfLengthBetween(1, 20);
            }
            double from = randomBoolean() ? 0 : randomIntBetween(0, Integer.MAX_VALUE - 1000);
            double to = randomBoolean()
                ? Double.POSITIVE_INFINITY
                : (Double.compare(from, 0) == 0 ? randomIntBetween(0, Integer.MAX_VALUE) : randomIntBetween((int) from, Integer.MAX_VALUE));
            factory.addRange(new Range(key, from, to));
        }
        factory.field(randomAlphaOfLengthBetween(1, 20));
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.missing("0, 0");
        }
        if (randomBoolean()) {
            factory.unit(randomFrom(DistanceUnit.values()));
        }
        if (randomBoolean()) {
            factory.distanceType(randomFrom(GeoDistance.values()));
        }
        return factory;
    }

    public void testParsingRangeStrict() throws IOException {
        final String rangeAggregation = """
            {
            "field" : "location",
            "origin" : "52.3760, 4.894",
            "unit" : "m",
            "ranges" : [
                { "from" : 10000, "to" : 20000, "badField" : "abcd" }
            ]
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        XContentParseException ex = expectThrows(
            XContentParseException.class,
            () -> GeoDistanceAggregationBuilder.parse("aggregationName", parser)
        );
        assertThat(ex.getCause(), notNullValue());
        assertThat(ex.getCause().getMessage(), containsString("badField"));
    }

    /**
     * We never render "null" values to xContent, but we should test that we can parse them (and they return correct defaults)
     */
    public void testParsingNull() throws IOException {
        final String rangeAggregation = """
            {
            "field" : "location",
            "origin" : "52.3760, 4.894",
            "unit" : "m",
            "ranges" : [
                { "from" : null, "to" : null }
            ]
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        GeoDistanceAggregationBuilder aggregationBuilder = (GeoDistanceAggregationBuilder) GeoDistanceAggregationBuilder.parse(
            "aggregationName",
            parser
        );
        assertEquals(1, aggregationBuilder.range().size());
        assertEquals(0.0, aggregationBuilder.range().get(0).getFrom(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, aggregationBuilder.range().get(0).getTo(), 0.0);
    }
}
