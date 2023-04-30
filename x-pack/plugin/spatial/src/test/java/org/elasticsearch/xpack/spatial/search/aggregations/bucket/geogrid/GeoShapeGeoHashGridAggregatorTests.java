/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoHashGridBucket;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.GeoGridQueryBuilder;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;

import static org.elasticsearch.geometry.utils.Geohash.stringEncode;

public class GeoShapeGeoHashGridAggregatorTests extends GeoShapeGeoGridTestCase<InternalGeoHashGridBucket> {

    @Override
    protected int randomPrecision() {
        return randomIntBetween(1, 12);
    }

    @Override
    protected String[] hashAsStrings(double lng, double lat, int precision) {
        return new String[] { stringEncode(lng, lat, precision) };
    }

    @Override
    protected Point randomPoint() {
        return GeometryTestUtils.randomPoint(false);
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        return GeoTestUtils.randomBBox();
    }

    @Override
    protected boolean intersects(String hash, GeoShapeValues.GeoShapeValue value) throws IOException {
        final Rectangle boundingBox = GeoGridQueryBuilder.getQueryHash(hash);
        return value.relate(
            GeoEncodingUtils.encodeLongitude(boundingBox.getMinLon()),
            GeoEncodingUtils.encodeLongitude(boundingBox.getMaxLon()),
            GeoEncodingUtils.encodeLatitude(boundingBox.getMinLat()),
            GeoEncodingUtils.encodeLatitude(boundingBox.getMaxLat())
        ) != GeoRelation.QUERY_DISJOINT;
    }

    @Override
    protected boolean intersectsBounds(String hash, GeoBoundingBox box) {
        final GeoHashBoundedPredicate predicate = new GeoHashBoundedPredicate(hash.length(), box);
        return predicate.validHash(hash);
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHashGridAggregationBuilder(name);
    }
}
