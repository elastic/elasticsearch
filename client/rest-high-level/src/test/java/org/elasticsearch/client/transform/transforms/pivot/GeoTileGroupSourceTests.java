/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;

public class GeoTileGroupSourceTests extends AbstractXContentTestCase<GeoTileGroupSource> {

    public static GeoTileGroupSource randomGeoTileGroupSource() {
        Rectangle rectangle = GeometryTestUtils.randomRectangle();
        return new GeoTileGroupSource(
            randomBoolean() ? null : randomAlphaOfLength(10),
            randomBoolean(),
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
    protected GeoTileGroupSource createTestInstance() {
        return randomGeoTileGroupSource();
    }

    @Override
    protected GeoTileGroupSource doParseInstance(XContentParser parser) throws IOException {
        return GeoTileGroupSource.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected Predicate<String> getRandomFieldsExcludeFilter() {
        // allow unknown fields in the root of the object only
        return field -> field.isEmpty() == false;
    }
}
