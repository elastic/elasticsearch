/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.transform.transforms.pivot;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.io.stream.Writeable.Reader;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.AbstractSerializingTestCase;

import java.io.IOException;

public class GeoTileGroupSourceTests extends AbstractSerializingTestCase<GeoTileGroupSource> {

    public static GeoTileGroupSource randomGeoTileGroupSource() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoTileGroupSource(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean() ? null : randomIntBetween(1, GeoTileUtils.MAX_ZOOM),
            randomBoolean() ? null : new GeoBoundingBox(
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
