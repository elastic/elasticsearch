/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.h3.H3;
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

import static org.hamcrest.Matchers.equalTo;

public class GeoHexAggregationBuilderTests extends AbstractXContentSerializingTestCase<GeoHexGridAggregationBuilder> {

    @Override
    protected GeoHexGridAggregationBuilder doParseInstance(XContentParser parser) throws IOException {
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        String name = parser.currentName();
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.START_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.FIELD_NAME));
        assertThat(parser.currentName(), equalTo(GeoHexGridAggregationBuilder.NAME));
        GeoHexGridAggregationBuilder parsed = GeoHexGridAggregationBuilder.PARSER.apply(parser, name);
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        assertThat(parser.nextToken(), equalTo(XContentParser.Token.END_OBJECT));
        return parsed;
    }

    @Override
    protected Writeable.Reader<GeoHexGridAggregationBuilder> instanceReader() {
        return GeoHexGridAggregationBuilder::new;
    }

    @Override
    protected GeoHexGridAggregationBuilder createTestInstance() {
        GeoHexGridAggregationBuilder geoHexGridAggregationBuilder = new GeoHexGridAggregationBuilder("_name");
        geoHexGridAggregationBuilder.field("field");
        if (randomBoolean()) {
            geoHexGridAggregationBuilder.precision(randomIntBetween(0, H3.MAX_H3_RES));
        }
        if (randomBoolean()) {
            geoHexGridAggregationBuilder.size(randomIntBetween(1, 256 * 256));
        }
        if (randomBoolean()) {
            geoHexGridAggregationBuilder.shardSize(randomIntBetween(1, 256 * 256));
        }
        if (randomBoolean()) {
            geoHexGridAggregationBuilder.setGeoBoundingBox(GeoTestUtils.randomBBox());
        }
        return geoHexGridAggregationBuilder;
    }

    @Override
    protected GeoHexGridAggregationBuilder mutateInstance(GeoHexGridAggregationBuilder instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    public void testInvalidPrecision() {
        GeoHexGridAggregationBuilder geoHexGridAggregationBuilder = new GeoHexGridAggregationBuilder("_name");
        expectThrows(IllegalArgumentException.class, () -> geoHexGridAggregationBuilder.precision(16));
        expectThrows(IllegalArgumentException.class, () -> geoHexGridAggregationBuilder.precision(-1));
    }
}
