/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.hasSize;

public class TTestAggregationBuilderTests extends AbstractSerializingTestCase<TTestAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(singletonList(new NamedXContentRegistry.Entry(
            BaseAggregationBuilder.class,
            new ParseField(TTestAggregationBuilder.NAME),
            (p, n) -> TTestAggregationBuilder.PARSER.apply(p, (String) n))));
    }

    @Override
    protected TTestAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        TTestAggregationBuilder agg = (TTestAggregationBuilder) parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(agg);
        return agg;
    }

    @Override
    protected TTestAggregationBuilder createTestInstance() {
        MultiValuesSourceFieldConfig aConfig;
        if (randomBoolean()) {
            aConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("a_field").build();
        } else {
            aConfig = new MultiValuesSourceFieldConfig.Builder().setScript(new Script(randomAlphaOfLength(10))).build();
        }
        MultiValuesSourceFieldConfig bConfig;
        if (randomBoolean()) {
            bConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("b_field").build();
        } else {
            bConfig = new MultiValuesSourceFieldConfig.Builder().setScript(new Script(randomAlphaOfLength(10))).build();
        }
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder(aggregationName)
            .a(aConfig)
            .b(bConfig);
        if (randomBoolean()) {
            aggregationBuilder.tails(randomIntBetween(1, 2));
        }
        if (randomBoolean()) {
            aggregationBuilder.testType(randomFrom(TTestType.values()));
        }
        return aggregationBuilder;
    }

    @Override
    protected Writeable.Reader<TTestAggregationBuilder> instanceReader() {
        return TTestAggregationBuilder::new;
    }
}

