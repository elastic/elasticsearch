/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.Version;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;

public class GeoTileGroupSourceTests extends AbstractSerializingTestCase<GeoTileGroupSource> {

    public static GeoTileGroupSource randomGeoTileGroupSource() {
        return randomGeoTileGroupSource(Version.CURRENT);
    }

    public static GeoTileGroupSource randomGeoTileGroupSource(Version version) {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        boolean missingBucket = version.onOrAfter(Version.V_7_10_0) ? randomBoolean() : false;
        return new GeoTileGroupSource(
            randomAlphaOfLength(10),
            missingBucket,
            randomBoolean() ? null : randomIntBetween(1, GeoTileUtils.MAX_ZOOM),
            randomBoolean()
                ? null
                : new GeoBoundingBox(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                )
        );
    }

    @Override
    protected GeoTileGroupSource doParseInstance(XContentParser parser) throws IOException {
        return GeoTileGroupSource.fromXContent(parser, false);
    }

    @Override
    protected GeoTileGroupSource createTestInstance() {
        return randomGeoTileGroupSource();
    }

    @Override
    protected Reader<GeoTileGroupSource> instanceReader() {
        return GeoTileGroupSource::new;
    }

}
