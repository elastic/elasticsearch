/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeoUtils;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregatorTestCase;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.H3SphericalUtil;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

public class GeoHexAggregatorTests extends GeoGridAggregatorTestCase<InternalGeoHexGridBucket> {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        // TODO: why is shape here already, it is not supported yet
        return List.of(GeoShapeValuesSourceType.instance(), CoreValuesSourceType.GEOPOINT);
    }

    @Override
    protected int randomPrecision() {
        // avoid too big cells, so we don't go over the pole
        return randomIntBetween(2, H3.MAX_H3_RES);
    }

    @Override
    protected String hashAsString(double lng, double lat, int precision) {
        return H3.geoToH3Address(lat, lng, precision);
    }

    @Override
    protected GeoGridAggregationBuilder createBuilder(String name) {
        return new GeoHexGridAggregationBuilder(name);
    }

    @Override
    protected Point randomPoint() {
        // don't go close to the poles
        return new Point(
            randomDoubleBetween(GeoUtils.MIN_LON, GeoUtils.MAX_LON, true),
            randomDoubleBetween(-GeoTileUtils.LATITUDE_MASK, GeoTileUtils.LATITUDE_MASK, false)
        );
    }

    @Override
    protected GeoBoundingBox randomBBox() {
        GeoBoundingBox bbox = randomValueOtherThanMany(
            (b) -> b.top() > GeoTileUtils.LATITUDE_MASK || b.bottom() < -GeoTileUtils.LATITUDE_MASK,
            () -> {
                Rectangle rectangle = GeometryTestUtils.randomRectangle();
                return new GeoBoundingBox(
                    new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
                    new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
                );
            }
        );
        // Avoid numerical errors for sub-atomic values
        double left = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(bbox.left()));
        double right = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(bbox.right()));
        double top = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(bbox.top()));
        double bottom = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(bbox.bottom()));
        bbox.topLeft().reset(top, left);
        bbox.bottomRight().reset(bottom, right);
        return bbox;
    }

    @Override
    protected Rectangle getTile(double lng, double lat, int precision) {
        final GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(), new GeoPoint());
        H3SphericalUtil.computeGeoBounds(H3.stringToH3(hashAsString(lng, lat, precision)), boundingBox);
        return new Rectangle(boundingBox.left(), boundingBox.right(), boundingBox.top(), boundingBox.bottom());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return createBuilder("foo").field(fieldName);
    }

    public void testHexCrossesDateline() throws IOException {
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(10, 179.5), new GeoPoint(0, 179.6));
        double y = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(5));
        double x = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(179.9));
        LatLonDocValuesField field = new LatLonDocValuesField("bar", y, x);
        testCase(
            new MatchAllDocsQuery(),
            "bar",
            0,
            bbox,
            geoGrid -> assertTrue(AggregationInspectionHelper.hasValue(geoGrid)),
            iw -> iw.addDocument(Collections.singletonList(field))
        );
    }

    public void testHexContainsNorthPole() throws IOException {
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(90, 0), new GeoPoint(89, 10));
        LatLonDocValuesField fieldNorth = new LatLonDocValuesField("bar", 90, -5);
        LatLonDocValuesField fieldSouth = new LatLonDocValuesField("bar", -90, -5);
        testCase(new MatchAllDocsQuery(), "bar", 0, bbox, geoGrid -> {
            assertTrue(AggregationInspectionHelper.hasValue(geoGrid));
            assertEquals(1, geoGrid.getBuckets().size());
            assertEquals(1, geoGrid.getBuckets().get(0).getDocCount());
        }, iw -> iw.addDocument(List.of(fieldNorth, fieldSouth)));
    }

    public void testHexContainsSouthPole() throws IOException {
        GeoBoundingBox bbox = new GeoBoundingBox(new GeoPoint(-89, 0), new GeoPoint(-90, 10));
        LatLonDocValuesField fieldNorth = new LatLonDocValuesField("bar", 90, -5);
        LatLonDocValuesField fieldSouth = new LatLonDocValuesField("bar", -90, -5);
        testCase(new MatchAllDocsQuery(), "bar", 0, bbox, geoGrid -> {
            assertTrue(AggregationInspectionHelper.hasValue(geoGrid));
            assertEquals(1, geoGrid.getBuckets().size());
            assertEquals(1, geoGrid.getBuckets().get(0).getDocCount());
        }, iw -> iw.addDocument(List.of(fieldNorth, fieldSouth)));
    }
}
