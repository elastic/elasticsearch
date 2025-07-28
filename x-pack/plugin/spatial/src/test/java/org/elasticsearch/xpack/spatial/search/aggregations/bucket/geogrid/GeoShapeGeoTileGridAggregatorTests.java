/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoTileGridBucket;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.GeoGridQueryBuilder;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

public class GeoShapeGeoTileGridAggregatorTests extends GeoShapeGeoGridTestCase<InternalGeoTileGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(0, GeoTileUtils.MAX_ZOOM);
    }

    @Override
    protected String[] hashAsStrings(double lng, double lat, int precision) {
        return new String[] { GeoTileUtils.stringEncode(GeoTileUtils.longEncode(lng, lat, precision)) };
    }

    @Override
    protected Point randomPoint() {
        return new Point(
            randomDoubleBetween(GeoUtils.MIN_LON, GeoUtils.MAX_LON, true),
            randomDoubleBetween(-GeoTileUtils.LATITUDE_MASK, GeoTileUtils.LATITUDE_MASK, false)
        );
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        GeoBoundingBox bbox = randomValueOtherThanMany(
            (b) -> b.top() > GeoTileUtils.LATITUDE_MASK || b.bottom() < -GeoTileUtils.LATITUDE_MASK,
            GeoTestUtils::randomBBox
        );
        // Avoid numerical errors for sub-atomic values
        double left = GeoTestUtils.encodeDecodeLon(bbox.left());
        double right = GeoTestUtils.encodeDecodeLon(bbox.right());
        double top = GeoTestUtils.encodeDecodeLat(bbox.top());
        double bottom = GeoTestUtils.encodeDecodeLat(bbox.bottom());
        bbox.topLeft().reset(top, left);
        bbox.bottomRight().reset(bottom, right);
        return bbox;
    }

    @Override
    protected boolean intersects(String hash, GeoShapeValues.GeoShapeValue value) throws IOException {
        final Rectangle r = GeoGridQueryBuilder.getQueryTile(GeoTileUtils.stringEncode(GeoTileUtils.longEncode(hash)));
        return value.relate(
            GeoEncodingUtils.encodeLongitude(r.getMinLon()),
            GeoEncodingUtils.encodeLongitude(r.getMaxLon()),
            GeoEncodingUtils.encodeLatitude(r.getMinLat()),
            GeoEncodingUtils.encodeLatitude(r.getMaxLat())
        ) != GeoRelation.QUERY_DISJOINT;
    }

    @Override
    protected boolean intersectsBounds(String hash, GeoBoundingBox box) {
        final int[] values = GeoTileUtils.parseHash(hash);
        final GeoTileBoundedPredicate predicate = new GeoTileBoundedPredicate(values[0], box);
        return predicate.validTile(values[1], values[2], values[0]);
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoTileGridAggregationBuilder(name);
    }

    public void testPrecision() {
        final GeoGridAggregationBuilder builder = createBuilder("_name");

        expectThrows(IllegalArgumentException.class, () -> builder.precision(-1));
        expectThrows(IllegalArgumentException.class, () -> builder.precision(30));

        int precision = randomIntBetween(0, 29);
        builder.precision(precision);
        assertEquals(precision, builder.precision());
    }
}
