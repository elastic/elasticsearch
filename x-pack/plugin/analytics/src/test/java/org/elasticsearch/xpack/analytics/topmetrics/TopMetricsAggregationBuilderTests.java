/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.analytics.topmetrics;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static java.util.Collections.singletonList;
import static org.hamcrest.Matchers.equalTo;

public class TopMetricsAggregationBuilderTests extends AbstractSerializingTestCase<TopMetricsAggregationBuilder> {
    @Override
    protected NamedWriteableRegistry getNamedWriteableRegistry() {
        return new NamedWriteableRegistry(Arrays.asList(
                new NamedWriteableRegistry.Entry(SortBuilder.class, FieldSortBuilder.NAME, FieldSortBuilder::new)));
    }

    @Override
    protected TopMetricsAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        String name = parser.currentName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo("top_metrics"));
        TopMetricsAggregationBuilder parsed = TopMetricsAggregationBuilder.PARSER.apply(parser, name);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        return parsed;
    }

    @Override
    protected Reader<TopMetricsAggregationBuilder> instanceReader() {
        return TopMetricsAggregationBuilder::new;
    }

    @Override
    protected TopMetricsAggregationBuilder createTestInstance() {
        List<SortBuilder<?>> sortBuilders = singletonList(
                new FieldSortBuilder(randomAlphaOfLength(5)).order(randomFrom(SortOrder.values())));
        MultiValuesSourceFieldConfig.Builder metricField = new MultiValuesSourceFieldConfig.Builder();
        metricField.setFieldName(randomAlphaOfLength(5)).setMissing(1.0);
        return new TopMetricsAggregationBuilder(randomAlphaOfLength(5), sortBuilders, metricField.build());
    }

}
