/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.search;

import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.h3.CellBoundary;
import org.elasticsearch.h3.H3;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoHashGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileGridAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGrid;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.H3CartesianUtil;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper;
import org.elasticsearch.xpack.spatial.index.query.GeoGridQueryBuilder;
import org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid.GeoHexGridAggregationBuilder;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;

public class GeoGridAggAndQueryConsistencyIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(LocalStateSpatialPlugin.class);
    }

    public void testGeoPointGeoHash() throws IOException {
        doTestGeohashGrid(GeoPointFieldMapper.CONTENT_TYPE, GeometryTestUtils::randomPoint);
    }

    public void testGeoPointGeoTile() throws IOException {
        doTestGeotileGrid(
            GeoPointFieldMapper.CONTENT_TYPE,
            GeoTileUtils.MAX_ZOOM - 4,  // levels 26 and above have some rounding errors, but this is past the index resolution
            // just generate points on bounds
            () -> randomValueOtherThanMany(
                p -> p.getLat() > GeoTileUtils.NORMALIZED_LATITUDE_MASK || p.getLat() < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK,
                GeometryTestUtils::randomPoint
            )
        );
    }

    public void testGeoPointGeoHex() throws IOException {
        doTestGeohexGrid(GeoPointFieldMapper.CONTENT_TYPE, GeometryTestUtils::randomPoint);
    }

    public void testGeoShapeGeoHash() throws IOException {
        doTestGeohashGrid(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, () -> GeometryTestUtils.randomGeometryWithoutCircle(0, false));
    }

    public void testGeoShapeGeoTile() throws IOException {
        doTestGeotileGrid(
            GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE,
            GeoTileUtils.MAX_ZOOM - 1,
            () -> GeometryTestUtils.randomGeometryWithoutCircle(0, false)
        );
    }

    public void testGeoShapeGeoHex() throws IOException {
        doTestGeohexGrid(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, () -> GeometryTestUtils.randomGeometryWithoutCircle(0, false));
    }

    public void testKnownIssueWithCellLeftOfDatelineTouchingPolygonOnRightOfDateline() throws IOException {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geometry")
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        indicesAdmin().prepareCreate("test").setMapping(xcb).get();

        BulkRequestBuilder builder = client().prepareBulk();
        builder.add(
            new IndexRequest("test").source("{\"geometry\" : \"BBOX (179.99999, 180.0, -11.29550, -11.29552)\"}", XContentType.JSON)
        );
        builder.add(
            new IndexRequest("test").source("{\"geometry\" : \"BBOX (-180.0, -179.99999, -11.29550, -11.29552)\"}", XContentType.JSON)
        );

        assertFalse(builder.get().hasFailures());
        indicesAdmin().prepareRefresh("test").get();

        GeoBoundingBox boundingBox = new GeoBoundingBox(new GeoPoint(-11.29550, 179.999992), new GeoPoint(-11.29552, -179.999992));

        GeoGridAggregationBuilder builderPoint = new GeoHexGridAggregationBuilder("geometry").field("geometry")
            .precision(15)
            .setGeoBoundingBox(boundingBox)
            .size(256 * 256);
        SearchResponse response = client().prepareSearch("test").addAggregation(builderPoint).setSize(0).get();
        InternalGeoGrid<?> gridPoint = response.getAggregations().get("geometry");
        for (InternalGeoGridBucket bucket : gridPoint.getBuckets()) {
            assertThat(bucket.getDocCount(), Matchers.greaterThan(0L));
            QueryBuilder queryBuilder = new GeoGridQueryBuilder("geometry").setGridId(
                GeoGridQueryBuilder.Grid.GEOHEX,
                bucket.getKeyAsString()
            );
            response = client().prepareSearch("test").setTrackTotalHits(true).setQuery(queryBuilder).get();
            assertThat(
                "Bucket " + bucket.getKeyAsString(),
                response.getHits().getTotalHits().value,
                Matchers.equalTo(bucket.getDocCount())
            );
        }
    }

    public void testKnownIssueWithCellIntersectingPolygonAndBoundingBox() throws IOException {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geometry")
            .field("type", "geo_shape")
            .endObject()
            .endObject()
            .endObject();
        indicesAdmin().prepareCreate("test").setMapping(xcb).get();

        BulkRequestBuilder builder = client().prepareBulk();
        builder.add(
            new IndexRequest("test").source("{\"geometry\" : \"POINT (169.12088680200193 86.17678739494652)\"}", XContentType.JSON)
        );
        builder.add(
            new IndexRequest("test").source("{\"geometry\" : \"POINT (169.12088680200193 86.17678739494652)\"}", XContentType.JSON)
        );
        String mp = "POLYGON ((150.0 70.0, 150.0 85.91811374669217, 168.77544806565834 85.91811374669217, 150.0 70.0))";
        builder.add(new IndexRequest("test").source("{\"geometry\" : \"" + mp + "\"}", XContentType.JSON));

        assertFalse(builder.get().hasFailures());
        indicesAdmin().prepareRefresh("test").get();

        // BBOX (172.21916569181505, -173.17785081207947, 86.17678739494652, 83.01600086049713)
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(86.17678739494652, 172.21916569181505),
            new GeoPoint(83.01600086049713, 179)
        );
        int precision = 4;
        GeoGridAggregationBuilder builderPoint = new GeoHexGridAggregationBuilder("geometry").field("geometry")
            .precision(precision)
            .setGeoBoundingBox(boundingBox)
            .size(256 * 256);
        SearchResponse response = client().prepareSearch("test").addAggregation(builderPoint).setSize(0).get();
        InternalGeoGrid<?> gridPoint = response.getAggregations().get("geometry");
        for (InternalGeoGridBucket bucket : gridPoint.getBuckets()) {
            assertThat(bucket.getDocCount(), Matchers.greaterThan(0L));
            QueryBuilder queryBuilder = new GeoGridQueryBuilder("geometry").setGridId(
                GeoGridQueryBuilder.Grid.GEOHEX,
                bucket.getKeyAsString()
            );
            response = client().prepareSearch("test").setTrackTotalHits(true).setQuery(queryBuilder).get();
            assertThat(response.getHits().getTotalHits().value, Matchers.equalTo(bucket.getDocCount()));
        }
    }

    private void doTestGeohashGrid(String fieldType, Supplier<Geometry> randomGeometriesSupplier) throws IOException {
        doTestGrid(
            1,
            Geohash.PRECISION,
            fieldType,
            (precision, point) -> Geohash.stringEncode(point.getLon(), point.getLat(), precision),
            hash -> toPoints(Geohash.toBoundingBox(hash)),
            Geohash::toBoundingBox,
            GeoHashGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOHASH, s2),
            randomGeometriesSupplier
        );
    }

    private void doTestGeotileGrid(String fieldType, int maxPrecision, Supplier<Geometry> randomGeometriesSupplier) throws IOException {
        doTestGrid(
            0,
            maxPrecision,
            fieldType,
            (precision, point) -> GeoTileUtils.stringEncode(GeoTileUtils.longEncode(point.getLon(), point.getLat(), precision)),
            tile -> toPoints(GeoTileUtils.toBoundingBox(tile)),
            GeoTileUtils::toBoundingBox,
            GeoTileGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOTILE, s2),
            randomGeometriesSupplier
        );
    }

    private void doTestGeohexGrid(String fieldType, Supplier<Geometry> randomGeometriesSupplier) throws IOException {
        doTestGrid(1, H3.MAX_H3_RES, fieldType, (precision, point) -> H3.geoToH3Address(point.getLat(), point.getLon(), precision), h3 -> {
            final CellBoundary boundary = H3.h3ToGeoBoundary(h3);
            final List<Point> points = new ArrayList<>(boundary.numPoints());
            for (int i = 0; i < boundary.numPoints(); i++) {
                points.add(new Point(boundary.getLatLon(i).getLonDeg(), boundary.getLatLon(i).getLatDeg()));
            }
            return points;
        },
            h3 -> H3CartesianUtil.toBoundingBox(H3.stringToH3(h3)),
            GeoHexGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOHEX, s2),
            randomGeometriesSupplier
        );
    }

    private void doTestGrid(
        int minPrecision,
        int maxPrecision,
        String fieldType,
        BiFunction<Integer, Point, String> pointEncoder,
        Function<String, List<Point>> toPoints,
        Function<String, Rectangle> toBoundingBox,
        Function<String, GeoGridAggregationBuilder> aggBuilder,
        BiFunction<String, String, QueryBuilder> queryBuilder,
        Supplier<Geometry> randomGeometriesSupplier
    ) throws IOException {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geometry")
            .field("type", fieldType)
            .endObject()
            .endObject()
            .endObject();
        indicesAdmin().prepareCreate("test").setMapping(xcb).get();

        Point queryPoint = GeometryTestUtils.randomPoint();
        String[] tiles = new String[maxPrecision + 1];
        for (int zoom = minPrecision; zoom < tiles.length; zoom++) {
            tiles[zoom] = pointEncoder.apply(zoom, queryPoint);
        }

        BulkRequestBuilder builder = client().prepareBulk();
        for (int zoom = minPrecision; zoom < tiles.length; zoom++) {
            List<Point> edgePoints = toPoints.apply(tiles[zoom]);
            String[] multiPoint = new String[edgePoints.size()];
            for (int i = 0; i < edgePoints.size(); i++) {
                String wkt = WellKnownText.toWKT(edgePoints.get(i));
                String doc = "{\"geometry\" : \"" + wkt + "\"}";
                builder.add(new IndexRequest("test").source(doc, XContentType.JSON));
                multiPoint[i] = "\"" + wkt + "\"";
            }
            String doc = "{\"geometry\" : " + Arrays.toString(multiPoint) + "}";
            builder.add(new IndexRequest("test").source(doc, XContentType.JSON));

        }
        assertFalse(builder.get().hasFailures());
        indicesAdmin().prepareRefresh("test").get();

        for (int i = minPrecision; i <= maxPrecision; i++) {
            GeoGridAggregationBuilder builderPoint = aggBuilder.apply("geometry").field("geometry").precision(i);
            SearchResponse response = client().prepareSearch("test").addAggregation(builderPoint).setSize(0).get();
            InternalGeoGrid<?> gridPoint = response.getAggregations().get("geometry");
            assertQuery(gridPoint.getBuckets(), queryBuilder, i);
        }

        builder = client().prepareBulk();
        final int numDocs = randomIntBetween(10, 20);
        for (int id = 0; id < numDocs; id++) {
            String wkt = WellKnownText.toWKT(randomGeometriesSupplier.get());
            String doc = "{\"geometry\" : \"" + wkt + "\"}";
            builder.add(new IndexRequest("test").source(doc, XContentType.JSON));
        }
        assertFalse(builder.get().hasFailures());
        indicesAdmin().prepareRefresh("test").get();

        int zoom = randomIntBetween(minPrecision, maxPrecision);
        Rectangle rectangle = toBoundingBox.apply(tiles[zoom]);
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );

        for (int i = minPrecision; i <= Math.min(maxPrecision, zoom + 3); i++) {
            GeoGridAggregationBuilder builderPoint = aggBuilder.apply("geometry")
                .field("geometry")
                .precision(i)
                .setGeoBoundingBox(boundingBox)
                .size(256 * 256);
            SearchResponse response = client().prepareSearch("test").addAggregation(builderPoint).setSize(0).get();
            InternalGeoGrid<?> gridPoint = response.getAggregations().get("geometry");
            assertQuery(gridPoint.getBuckets(), queryBuilder, i);
        }
    }

    private void assertQuery(List<InternalGeoGridBucket> buckets, BiFunction<String, String, QueryBuilder> queryFunction, int precision) {
        for (InternalGeoGridBucket bucket : buckets) {
            assertThat(bucket.getDocCount(), Matchers.greaterThan(0L));
            QueryBuilder queryBuilder = queryFunction.apply("geometry", bucket.getKeyAsString());
            SearchResponse response = client().prepareSearch("test").setTrackTotalHits(true).setQuery(queryBuilder).get();
            assertThat(
                "Expected hits at precision " + precision + " for H3 cell " + bucket.getKeyAsString(),
                response.getHits().getTotalHits().value,
                Matchers.equalTo(bucket.getDocCount())
            );
        }
    }

    private static List<Point> toPoints(Rectangle rectangle) {
        List<Point> points = new ArrayList<>();
        points.add(new Point(rectangle.getMinX(), rectangle.getMinY()));
        points.add(new Point(rectangle.getMaxX(), rectangle.getMinY()));
        points.add(new Point(rectangle.getMinX(), rectangle.getMaxY()));
        points.add(new Point(rectangle.getMaxX(), rectangle.getMaxY()));
        return points;
    }
}
