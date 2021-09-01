/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoBoundingBoxTests;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGridValuesSourceBuilderTests extends ESTestCase {

    public void testSetFormat() {
        CompositeValuesSourceBuilder<?> builder = new GeoTileGridValuesSourceBuilder("name");
        expectThrows(IllegalArgumentException.class, () -> builder.format("format"));
    }

    public void testBWCBounds() throws IOException {
        Version noBoundsSupportVersion = VersionUtils.randomVersionBetween(random(), Version.V_6_0_0, Version.V_7_5_0);
        GeoTileGridValuesSourceBuilder builder = new GeoTileGridValuesSourceBuilder("name");
        if (randomBoolean()) {
            builder.geoBoundingBox(GeoBoundingBoxTests.randomBBox());
        }
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            output.setVersion(Version.V_7_6_0);
            builder.writeTo(output);
            try (
                StreamInput in = new NamedWriteableAwareStreamInput(
                    output.bytes().streamInput(),
                    new NamedWriteableRegistry(Collections.emptyList())
                )
            ) {
                in.setVersion(noBoundsSupportVersion);
                GeoTileGridValuesSourceBuilder readBuilder = new GeoTileGridValuesSourceBuilder(in);
                assertThat(
                    readBuilder.geoBoundingBox(),
                    equalTo(new GeoBoundingBox(new GeoPoint(Double.NaN, Double.NaN), new GeoPoint(Double.NaN, Double.NaN)))
                );
            }
        }
    }
}
