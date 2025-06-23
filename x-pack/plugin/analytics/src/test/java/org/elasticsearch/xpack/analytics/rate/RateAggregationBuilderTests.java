/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.analytics.rate;

import org.elasticsearch.common.Rounding;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.Script;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BaseAggregationBuilder;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.hasSize;

public class RateAggregationBuilderTests extends AbstractXContentSerializingTestCase<RateAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
    }

    @Override
    protected RateAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertSame(XContentParser.Token.START_OBJECT, parser.nextToken());
        AggregatorFactories.Builder parsed = AggregatorFactories.parseAggregators(parser);
        assertThat(parsed.getAggregatorFactories(), hasSize(1));
        assertThat(parsed.getPipelineAggregatorFactories(), hasSize(0));
        RateAggregationBuilder agg = (RateAggregationBuilder) parsed.getAggregatorFactories().iterator().next();
        assertNull(parser.nextToken());
        assertNotNull(agg);
        return agg;
    }

    @Override
    protected RateAggregationBuilder createTestInstance() {
        RateAggregationBuilder aggregationBuilder = new RateAggregationBuilder(aggregationName);
        if (randomBoolean()) {
            if (randomBoolean()) {
                aggregationBuilder.field(randomAlphaOfLength(10));
            } else {
                aggregationBuilder.script(new Script(randomAlphaOfLength(10)));
            }
            if (randomBoolean()) {
                aggregationBuilder.rateMode(randomFrom(RateMode.values()));
            }
        }
        if (randomBoolean()) {
            aggregationBuilder.rateUnit(randomFrom(Rounding.DateTimeUnit.values()));
        }
        return aggregationBuilder;
    }

    @Override
    protected RateAggregationBuilder mutateInstance(RateAggregationBuilder instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<RateAggregationBuilder> instanceReader() {
        return RateAggregationBuilder::new;
    }

    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedWriteables());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.add(
            new NamedXContentRegistry.Entry(
                BaseAggregationBuilder.class,
                new ParseField(RateAggregationBuilder.NAME),
                (p, n) -> RateAggregationBuilder.PARSER.apply(p, (String) n)
            )
        );
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        return new NamedXContentRegistry(namedXContent);
    }
}
