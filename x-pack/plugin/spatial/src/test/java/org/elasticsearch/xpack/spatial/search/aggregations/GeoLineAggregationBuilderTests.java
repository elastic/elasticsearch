/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GeoLineAggregationBuilderTests extends AbstractSerializingTestCase<GeoLineAggregationBuilder> {

    @Override
    protected GeoLineAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        String name = parser.currentName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo(GeoLineAggregationBuilder.NAME));
        GeoLineAggregationBuilder parsed = GeoLineAggregationBuilder.PARSER.apply(parser, name);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        return parsed;
    }

    @Override
    protected Writeable.Reader<GeoLineAggregationBuilder> instanceReader() {
        return GeoLineAggregationBuilder::new;
    }

    @Override
    protected GeoLineAggregationBuilder createTestInstance() {
        MultiValuesSourceFieldConfig pointConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(randomAlphaOfLength(5)).build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(randomAlphaOfLength(6)).build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(pointConfig).sort(sortConfig);
        if (randomBoolean()) {
            SortOrder sortOrder = randomFrom(SortOrder.values());
            lineAggregationBuilder.sortOrder(sortOrder);
        }
        if (randomBoolean()) {
            lineAggregationBuilder.size(randomIntBetween(1, GeoLineAggregationBuilder.MAX_PATH_SIZE));
        }
        if (randomBoolean()) {
            lineAggregationBuilder.includeSort(randomBoolean());
        }
        return lineAggregationBuilder;
    }

    public void testInvalidSize() {
        MultiValuesSourceFieldConfig pointConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(randomAlphaOfLength(5)).build();
        MultiValuesSourceFieldConfig sortConfig = new MultiValuesSourceFieldConfig.Builder().setFieldName(randomAlphaOfLength(6)).build();
        GeoLineAggregationBuilder lineAggregationBuilder = new GeoLineAggregationBuilder("_name").point(pointConfig).sort(sortConfig);
        expectThrows(IllegalArgumentException.class, () -> lineAggregationBuilder.size(0));
        expectThrows(
            IllegalArgumentException.class,
            () -> lineAggregationBuilder.size(GeoLineAggregationBuilder.MAX_PATH_SIZE + randomIntBetween(1, 10))
        );
    }
}
