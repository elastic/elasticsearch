/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.SortedSetDocValuesField;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.h3.H3;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.xpack.spatial.geom.TestGeometryCollector;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;
import org.elasticsearch.xpack.spatial.index.query.H3LatLonGeometry;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.greaterThan;

public class GeoShapeGeoHexGridAggregatorTests extends GeoShapeGeoGridTestCase<InternalGeoHexGridBucket> {

    protected final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createGeometryCollector();
    // Uncomment the following to enable export of WKT output for specific tests when debugging
    // private final TestGeometryCollector testGeometryCollector = TestGeometryCollector.createWKTExporter(getTestClass().getSimpleName());

    @Override
    protected int randomPrecision() {
        return randomIntBetween(0, H3.MAX_H3_RES);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return H3.geoToH3Address(lat, lng, precision);
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
        return randomValueOtherThanMany(
            (b) -> b.top() > GeoTileUtils.LATITUDE_MASK || b.bottom() < -GeoTileUtils.LATITUDE_MASK,
            GeoTestUtils::randomBBox
        );
    }

    @Override
    protected boolean intersects(double lng, double lat, int precision, GeoShapeValues.GeoShapeValue value) throws IOException {
        H3LatLonGeometry geometry = new H3LatLonGeometry.Planar(H3.geoToH3Address(lat, lng, precision));
        return value.relate(geometry) != GeoRelation.QUERY_DISJOINT;
    }

    @Override
    protected boolean intersectsBounds(double lng, double lat, int precision, GeoBoundingBox box) {
        GeoHexBoundedPredicate predicate = new GeoHexBoundedPredicate(precision, box);
        return predicate.validAddress(hashAsString(lng, lat, precision));
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHexGridAggregationBuilder(name);
    }

    @Override
    public void testMappedMissingGeoShape() throws IOException {
        testGeometryCollector.start("testMappedMissingGeoShape");
        TestGeometryCollector.Collector collector = testGeometryCollector.normal();
        String lineString = "LINESTRING (30 10, 10 30, 40 40)";
        collector.addWKT(lineString);
        GeoGridAggregationBuilder builder = createBuilder("_name").field(FIELD_NAME).missing(lineString);
        testCase(
            new MatchAllDocsQuery(),
            1,
            null,
            iw -> { iw.addDocument(Collections.singleton(new SortedSetDocValuesField("string", new BytesRef("a")))); },
            geoGrid -> {
                for (InternalGeoGridBucket bucket : geoGrid.getBuckets()) {
                    collector.addH3Cell(bucket.getKeyAsString());
                }
                assertEquals(8, geoGrid.getBuckets().size());
            },
            builder
        );
        testGeometryCollector.stop((normal, special) -> assertThat("Line and match", normal.size(), greaterThan(1)));
    }
}
