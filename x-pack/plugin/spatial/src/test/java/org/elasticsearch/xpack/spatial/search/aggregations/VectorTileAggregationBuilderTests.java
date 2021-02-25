/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class VectorTileAggregationBuilderTests extends AbstractSerializingTestCase<VectorTileAggregationBuilder> {

    @Override
    protected VectorTileAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        String name = parser.currentName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo(VectorTileAggregationBuilder.NAME));
        VectorTileAggregationBuilder parsed = VectorTileAggregationBuilder.PARSER.apply(parser, name);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        return parsed;
    }

    @Override
    protected Writeable.Reader<VectorTileAggregationBuilder> instanceReader() {
        return VectorTileAggregationBuilder::new;
    }

    @Override
    protected VectorTileAggregationBuilder createTestInstance() {
        VectorTileAggregationBuilder vectorTileAggregationBuilder = new VectorTileAggregationBuilder("_name");
        vectorTileAggregationBuilder.z(0).x(0).y(0);
        return vectorTileAggregationBuilder;
    }
}
