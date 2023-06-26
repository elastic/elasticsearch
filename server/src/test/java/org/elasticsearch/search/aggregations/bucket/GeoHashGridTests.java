/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.search.aggregations.BaseAggregationTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.test.TransportVersionUtils;

import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class GeoHashGridTests extends BaseAggregationTestCase<GeoGridAggregationBuilder> {

    @Override
    protected GeoHashGridAggregationBuilder createTestAggregatorBuilder() {
        String name = randomAlphaOfLengthBetween(3, 20);
        GeoHashGridAggregationBuilder factory = new GeoHashGridAggregationBuilder(name);
        factory.field("foo");
        if (randomBoolean()) {
            int precision = randomIntBetween(1, 12);
            factory.precision(precision);
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
        TransportVersion noBoundsSupportVersion = TransportVersionUtils.randomVersionBetween(
            random(),
            TransportVersion.V_7_0_0,
            TransportVersion.V_7_5_0
        );
        GeoHashGridAggregationBuilder builder = createTestAggregatorBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setTransportVersion(TransportVersion.V_7_6_0);
            builder.writeTo(output);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    output.bytes().streamInput(),
                    new NamedWriteableRegistry(Collections.emptyList())
                )
            ) {
                in.setTransportVersion(noBoundsSupportVersion);
                GeoHashGridAggregationBuilder readBuilder = new GeoHashGridAggregationBuilder(in);
                assertThat(
                    readBuilder.geoBoundingBox(),
                    equalTo(new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN)))
                );
            }
        }
    }
}
