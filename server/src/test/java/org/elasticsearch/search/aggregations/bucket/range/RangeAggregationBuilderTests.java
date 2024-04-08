/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.range;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class RangeAggregationBuilderTests extends AbstractXContentSerializingTestCase<RangeAggregationBuilder> {
    @Override
    protected RangeAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        String name = parser.currentName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("range"));
        RangeAggregationBuilder parsed = RangeAggregationBuilder.PARSER.apply(parser, name);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        return parsed;
    }

    @Override
    protected Reader<RangeAggregationBuilder> instanceReader() {
        return RangeAggregationBuilder::new;
    }

    @Override
    protected RangeAggregationBuilder createTestInstance() {
        RangeAggregationBuilder builder = new RangeAggregationBuilder(randomAlphaOfLength(5));
        builder.keyed(randomBoolean());
        builder.field(randomAlphaOfLength(4));
        int rangeCount = between(1, 10);
        double r = 0;
        for (int i = 0; i < rangeCount; i++) {
            switch (between(0, 2)) {
                case 0 -> builder.addUnboundedFrom(randomAlphaOfLength(2), r);
                case 1 -> builder.addUnboundedTo(randomAlphaOfLength(2), r);
                case 2 -> {
                    double from = r;
                    r += randomDouble(); // less than 1
                    double to = r;
                    builder.addRange(randomAlphaOfLength(2), from, to);
                }
                default -> fail();
            }
            r += randomDouble(); // less than 1
        }
        return builder;
    }

    @Override
    protected RangeAggregationBuilder mutateInstance(RangeAggregationBuilder builder) {
        String name = builder.getName();
        boolean keyed = builder.keyed();
        String field = builder.field();
        List<RangeAggregator.Range> ranges = builder.ranges();
        switch (between(0, 3)) {
            case 0 -> name += randomAlphaOfLength(1);
            case 1 -> keyed = keyed == false;
            case 2 -> field += randomAlphaOfLength(1);
            case 3 -> {
                ranges = new ArrayList<>(ranges);
                double from = ranges.get(ranges.size() - 1).from;
                double to = from + randomDouble();
                ranges.add(new RangeAggregator.Range(randomAlphaOfLength(2), from, to));
            }
            default -> fail();
        }
        RangeAggregationBuilder mutant = new RangeAggregationBuilder(name).keyed(keyed).field(field);
        ranges.forEach(mutant::addRange);
        return mutant;
    }

    public void testNumericKeys() throws IOException {
        RangeAggregationBuilder builder = doParseInstance(createParser(JsonXContent.jsonXContent, """
            {"test":{"range":{"field":"f","ranges":[{"key":1,"to":0}]}}}"""));
        assertThat(builder.getName(), equalTo("test"));
        assertThat(builder.field(), equalTo("f"));
        assertThat(builder.ranges, equalTo(List.of(new RangeAggregator.Range("1", null, 0d))));
    }
}
