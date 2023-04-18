/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class RangeTests extends BaseAggregationTestCase<RangeAggregationBuilder> {

    @Override
    protected RangeAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        RangeAggregationBuilder factory = new RangeAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAlphaOfLengthBetween(1, 20);
            }
            double from = randomBoolean() ? Double.NEGATIVE_INFINITY : randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - 1000);
            double to = randomBoolean()
                ? Double.POSITIVE_INFINITY
                : (Double.isInfinite(from)
                    ? randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE)
                    : randomIntBetween((int) from, Integer.MAX_VALUE));
            if (randomBoolean()) {
                factory.addRange(new Range(key, from, to));
            } else {
                String fromAsStr = Double.isInfinite(from) ? null : String.valueOf(from);
                String toAsStr = Double.isInfinite(to) ? null : String.valueOf(to);
                factory.addRange(new Range(key, fromAsStr, toAsStr));
            }
        }
        factory.field(INT_FIELD_NAME);
        if (randomBoolean()) {
            factory.format("###.##");
        }
        if (randomBoolean()) {
            factory.keyed(randomBoolean());
        }
        if (randomBoolean()) {
            factory.missing(randomIntBetween(0, 10));
        }
        return factory;
    }

    public void testParsingRangeStrict() throws IOException {
        final String rangeAggregation = """
            {
            "field" : "price",
            "ranges" : [
                { "from" : 50, "to" : 100, "badField" : "abcd" }
            ]
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        XContentParseException ex = expectThrows(
            XContentParseException.class,
            () -> RangeAggregationBuilder.PARSER.parse(parser, "aggregationName")
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
            "field" : "price",
            "ranges" : [
                { "from" : null, "to" : null }
            ]
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        RangeAggregationBuilder aggregationBuilder = RangeAggregationBuilder.PARSER.parse(parser, "aggregationName");
        assertEquals(1, aggregationBuilder.ranges().size());
        assertEquals(Double.NEGATIVE_INFINITY, aggregationBuilder.ranges().get(0).getFrom(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, aggregationBuilder.ranges().get(0).getTo(), 0.0);
    }
}
