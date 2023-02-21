/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasSize;

public class BoxplotAggregationBuilderTests extends AbstractXContentSerializingTestCase<BoxplotAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(
            singletonList(
                new NamedXContentRegistry.Entry(
                    BaseAggregationBuilder.class,
                    new ParseField(BoxplotAggregationBuilder.NAME),
                    (p, n) -> BoxplotAggregationBuilder.PARSER.apply(p, (String) n)
                )
            )
        );
    }

    @Override
    protected BoxplotAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        BoxplotAggregationBuilder agg = (BoxplotAggregationBuilder) parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(agg);
        return agg;
    }

    @Override
    protected BoxplotAggregationBuilder createTestInstance() {
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder(aggregationName).field(randomAlphaOfLength(10));
        if (randomBoolean()) {
            aggregationBuilder.compression(randomDoubleBetween(0, 100, true));
        }
        return aggregationBuilder;
    }

    @Override
    protected BoxplotAggregationBuilder mutateInstance(BoxplotAggregationBuilder instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<BoxplotAggregationBuilder> instanceReader() {
        return BoxplotAggregationBuilder::new;
    }
}
