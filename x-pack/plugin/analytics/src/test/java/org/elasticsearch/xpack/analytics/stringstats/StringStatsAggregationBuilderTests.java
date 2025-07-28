/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.stringstats;

import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.util.Arrays;

import static org.hamcrest.Matchers.equalTo;

public class StringStatsAggregationBuilderTests extends AbstractXContentSerializingTestCase<StringStatsAggregationBuilder> {
    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            Arrays.asList(
                new NamedXContentRegistry.Entry(
                    BaseAggregationBuilder.class,
                    new ParseField(StringStatsAggregationBuilder.NAME),
                    (p, c) -> StringStatsAggregationBuilder.PARSER.parse(p, (String) c)
                )
            )
        );
    }

    @Override
    protected StringStatsAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        String name = parser.currentName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("string_stats"));
        StringStatsAggregationBuilder parsed = StringStatsAggregationBuilder.PARSER.apply(parser, name);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        return parsed;
    }

    @Override
    protected Reader<StringStatsAggregationBuilder> instanceReader() {
        return StringStatsAggregationBuilder::new;
    }

    @Override
    protected StringStatsAggregationBuilder createTestInstance() {
        StringStatsAggregationBuilder builder = new StringStatsAggregationBuilder(randomAlphaOfLength(5));
        builder.field("foo");
        builder.showDistribution(randomBoolean());
        return builder;
    }

    @Override
    protected StringStatsAggregationBuilder mutateInstance(StringStatsAggregationBuilder instance) {
        if (randomBoolean()) {
            StringStatsAggregationBuilder mutant = new StringStatsAggregationBuilder(instance.getName());
            mutant.showDistribution(instance.showDistribution() == false);
            return mutant;
        }
        StringStatsAggregationBuilder mutant = new StringStatsAggregationBuilder(randomAlphaOfLength(4));
        mutant.showDistribution(instance.showDistribution());
        return mutant;
    }
}
