/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.range.RangeAggregator.Range;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;

public class RangeTests extends BaseAggregationTestCase<RangeAggregationBuilder> {

    @Override
    protected RangeAggregationBuilder createTestAggregatorBuilder() {
        int numRanges = randomIntBetween(1, 10);
        RangeAggregationBuilder factory = new RangeAggregationBuilder("foo");
        for (int i = 0; i < numRanges; i++) {
            String key = null;
            if (randomBoolean()) {
                key = randomAlphaOfLengthBetween(1, 20);
            }
            double from = randomBoolean() ? Double.NEGATIVE_INFINITY : randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE - 1000);
            double to = randomBoolean() ? Double.POSITIVE_INFINITY
                    : (Double.isInfinite(from) ? randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE)
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
        final String rangeAggregation = "{\n" +
                "\"field\" : \"price\",\n" +
                "\"ranges\" : [\n" +
                "    { \"from\" : 50, \"to\" : 100, \"badField\" : \"abcd\" }\n" +
                "]\n" +
            "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        ParsingException ex = expectThrows(ParsingException.class, () -> RangeAggregationBuilder.parse("aggregationName", parser));
        assertThat(ex.getDetailedMessage(), containsString("badField"));
    }

    /**
     * We never render "null" values to xContent, but we should test that we can parse them (and they return correct defaults)
     */
    public void testParsingNull() throws IOException {
        final String rangeAggregation = "{\n" +
                "\"field\" : \"price\",\n" +
                "\"ranges\" : [\n" +
                "    { \"from\" : null, \"to\" : null }\n" +
                "]\n" +
            "}";
        XContentParser parser = createParser(JsonXContent.jsonXContent, rangeAggregation);
        RangeAggregationBuilder aggregationBuilder = (RangeAggregationBuilder) RangeAggregationBuilder.parse("aggregationName", parser);
        assertEquals(1, aggregationBuilder.ranges().size());
        assertEquals(Double.NEGATIVE_INFINITY, aggregationBuilder.ranges().get(0).getFrom(), 0.0);
        assertEquals(Double.POSITIVE_INFINITY, aggregationBuilder.ranges().get(0).getTo(), 0.0);
    }
}
