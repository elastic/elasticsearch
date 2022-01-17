/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.DateRangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;
import org.elasticsearch.xcontent.XContentParseException;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.notNullValue;

public class DateRangeTests extends BaseAggregationTestCase<DateRangeAggregationBuilder> {

    @Override
    protected DateRangeAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        DateRangeAggregationBuilder factory = new DateRangeAggregationBuilder(randomAlphaOfLengthBetween(3, 10));
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
        if (randomBoolean()) {
            factory.timeZone(randomZone());
        }
        return factory;
    }

    public void testParsingRangeStrict() throws IOException {
        final String rangeAggregation = """
            {
            "field" : "date",
            "format" : "yyyy-MM-dd",
            "ranges" : [
                { "from" : "2017-01-01", "to" : "2017-01-02", "badField" : "abcd" }
            ]
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        XContentParseException ex = expectThrows(
            XContentParseException.class,
            () -> DateRangeAggregationBuilder.PARSER.parse(parser, "aggregationName")
        );
        assertThat(ex.getCause(), notNullValue());
        assertThat(ex.getCause().getMessage(), containsString("badField"));
    }

}
