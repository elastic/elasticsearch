/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.VersionUtils;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGridTests extends BaseAggregationTestCase<GeoGridAggregationBuilder> {

    @Override
    protected GeoTileGridAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        GeoTileGridAggregationBuilder factory = new GeoTileGridAggregationBuilder(name);
        factory.field("foo");
        if (randomBoolean()) {
            factory.precision(randomIntBetween(0, GeoTileUtils.MAX_ZOOM));
        }
        if (randomBoolean()) {
            factory.size(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.shardSize(randomIntBetween(1, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            factory.setGeoBoundingBox(GeoBoundingBoxTests.randomBBox());
        }
        return factory;
    }

    public void testSerializationPreBounds() throws Exception {
        Version noBoundsSupportVersion = VersionUtils.randomVersionBetween(random(), Version.V_7_0_0, Version.V_7_5_0);
        GeoTileGridAggregationBuilder builder = createTestAggregatorBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_6_0);
            builder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(),
                new NamedWriteableRegistry(Collections.emptyList()))) {
                in.setVersion(noBoundsSupportVersion);
                GeoTileGridAggregationBuilder readBuilder = new GeoTileGridAggregationBuilder(in);
                assertThat(readBuilder.geoBoundingBox(), equalTo(new GeoBoundingBox(
                    new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN))));
            }
        }
    }
}
