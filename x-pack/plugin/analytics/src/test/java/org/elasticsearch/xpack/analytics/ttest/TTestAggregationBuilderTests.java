/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.ttest;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;

public class TTestAggregationBuilderTests extends AbstractSerializingTestCase<TTestAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
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
        MultiValuesSourceFieldConfig.Builder aConfig;
        TTestType tTestType = randomFrom(TTestType.values());
        if (randomBoolean()) {
            aConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("a_field");
        } else {
            aConfig = new MultiValuesSourceFieldConfig.Builder().setScript(new Script(randomAlphaOfLength(10)));
        }
        MultiValuesSourceFieldConfig.Builder bConfig;
        if (randomBoolean()) {
            bConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName("b_field");
        } else {
            bConfig = new MultiValuesSourceFieldConfig.Builder().setScript(new Script(randomAlphaOfLength(10)));
        }
        if (tTestType != TTestType.PAIRED && randomBoolean()) {
            aConfig.setFilter(QueryBuilders.queryStringQuery(randomAlphaOfLength(10)));
        }
        if (tTestType != TTestType.PAIRED && randomBoolean()) {
            bConfig.setFilter(QueryBuilders.queryStringQuery(randomAlphaOfLength(10)));
        }
        TTestAggregationBuilder aggregationBuilder = new TTestAggregationBuilder(aggregationName)
            .a(aConfig.build())
            .b(bConfig.build());
        if (randomBoolean()) {
            aggregationBuilder.tails(randomIntBetween(1, 2));
        }
        if (tTestType != TTestType.HETEROSCEDASTIC || randomBoolean()) {
            aggregationBuilder.testType(randomFrom(tTestType));
        }
        return aggregationBuilder;
    }

    @Override
    protected Writeable.Reader<TTestAggregationBuilder> instanceReader() {
        return TTestAggregationBuilder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList())
            .getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(new NamedXContentRegistry.Entry(
            BaseAggregationBuilder.class,
            new ParseField(TTestAggregationBuilder.NAME),
            (p, n) -> TTestAggregationBuilder.PARSER.apply(p, (String) n)));
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }
}

