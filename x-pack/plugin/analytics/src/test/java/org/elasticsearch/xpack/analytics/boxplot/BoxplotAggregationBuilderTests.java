/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.boxplot;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xpack.analytics.AnalyticsPlugin;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.hasSize;

public class BoxplotAggregationBuilderTests extends AbstractSerializingTestCase<BoxplotAggregationBuilder> {
    String aggregationName;

    @Before
    public void setupName() {
        aggregationName = randomAlphaOfLength(10);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, Collections.singletonList(new AnalyticsPlugin()));
        return new NamedXContentRegistry(searchModule.getNamedXContents());
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
        BoxplotAggregationBuilder aggregationBuilder = new BoxplotAggregationBuilder(aggregationName)
            .field(randomAlphaOfLength(10));
        if (randomBoolean()) {
            aggregationBuilder.compression(randomDoubleBetween(0, 100, true));
        }
        return aggregationBuilder;
    }

    @Override
    protected Writeable.Reader<BoxplotAggregationBuilder> instanceReader() {
        return BoxplotAggregationBuilder::new;
    }
}

