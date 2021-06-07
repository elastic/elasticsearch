/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.transform.transforms.pivot.hlrc;

import org.elasticsearch.client.AbstractResponseTestCase;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.core.transform.transforms.pivot.GeoTileGroupSource;

import static org.hamcrest.Matchers.equalTo;

public class GeoTileGroupSourceTests extends AbstractResponseTestCase<
    GeoTileGroupSource,
    org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource> {

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
    protected GeoTileGroupSource createServerTestInstance(XContentType xContentType) {
        return randomGeoTileGroupSource();
    }

    @Override
    protected org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource doParseToClientInstance(XContentParser parser) {
        return org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource.fromXContent(parser);
    }

    @Override
    protected void assertInstances(
        GeoTileGroupSource serverTestInstance,
        org.elasticsearch.client.transform.transforms.pivot.GeoTileGroupSource clientInstance
    ) {
        assertThat(serverTestInstance.getField(), equalTo(clientInstance.getField()));
        assertNull(clientInstance.getScript());
        assertThat(serverTestInstance.getPrecision(), equalTo(clientInstance.getPrecision()));
        assertThat(serverTestInstance.getGeoBoundingBox(), equalTo(clientInstance.getGeoBoundingBox()));
    }

}
