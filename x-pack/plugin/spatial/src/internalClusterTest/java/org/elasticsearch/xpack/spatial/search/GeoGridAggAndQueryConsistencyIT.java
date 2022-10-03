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
import org.elasticsearch.common.geo.GeoUtils;
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

    public void testGeoPointGeoHex_Point0() throws IOException {
        doTestGeohexGrid(GeoPointFieldMapper.CONTENT_TYPE, () -> new Point(0, 0));
    }

    public void testGeoPointGeoHex_Point25() throws IOException {
        doTestGeohexGrid(GeoPointFieldMapper.CONTENT_TYPE, () -> new Point(25, 25));
    }

    public void testGeoShapeGeoHex_Point() throws IOException {
        doTestGeohexGrid(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, GeometryTestUtils::randomPoint);
    }

    public void testGeoShapeGeoHex_Point0() throws IOException {
        doTestGeohexGrid(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, () -> new Point(0, 0));
    }

    public void testGeoShapeGeoHex_Point25() throws IOException {
        doTestGeohexGrid(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, () -> new Point(25, 25));
    }

    public void testGeohexGrid() throws IOException {
        Point queryPoint = new Point(25, 25);
        TestGridConfig config = new TestGridConfig(1, 1, GeoPointFieldMapper.CONTENT_TYPE);
        doValidateGrid(
            config,
            queryPoint,
            (precision, point) -> H3.geoToH3Address(point.getLat(), point.getLon(), precision),
            h3 -> toPoints(H3.h3ToGeoBoundary(h3)),
            GeoHexGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOHEX, s2)
        );
        config = new TestGridConfig(1, 1, GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE);
        doValidateGrid(
            config,
            queryPoint,
            (precision, point) -> H3.geoToH3Address(point.getLat(), point.getLon(), precision),
            h3 -> toPoints(H3.h3ToGeoBoundary(h3)),
            GeoHexGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOHEX, s2)
        );
    }

    public void testGeoShapeGeoHex() throws IOException {
        doTestGeohexGrid(GeoShapeWithDocValuesFieldMapper.CONTENT_TYPE, () -> GeometryTestUtils.randomGeometryWithoutCircle(0, false));
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

    private void doTestGeohashGrid(String fieldType, Supplier<Geometry> randomGeometriesSupplier) throws IOException {
        TestGridConfig config = new TestGridConfig(1, Geohash.PRECISION, fieldType);
        doTestGrid(
            config,
            (precision, point) -> Geohash.stringEncode(point.getLon(), point.getLat(), precision),
            hash -> toPoints(Geohash.toBoundingBox(hash)),
            Geohash::toBoundingBox,
            GeoHashGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOHASH, s2),
            randomGeometriesSupplier
        );
    }

    private void doTestGeotileGrid(String fieldType, int maxPrecision, Supplier<Geometry> randomGeometriesSupplier) throws IOException {
        TestGridConfig config = new TestGridConfig(0, maxPrecision, fieldType);
        doTestGrid(
            config,
            (precision, point) -> GeoTileUtils.stringEncode(GeoTileUtils.longEncode(point.getLon(), point.getLat(), precision)),
            tile -> toPoints(GeoTileUtils.toBoundingBox(tile)),
            GeoTileUtils::toBoundingBox,
            GeoTileGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOTILE, s2),
            randomGeometriesSupplier
        );
    }

    private void doTestGeohexGrid(String fieldType, Supplier<Geometry> randomGeometriesSupplier) throws IOException {
        TestGridConfig config = new TestGridConfig(1, H3.MAX_H3_RES, fieldType);
        doTestGrid(
            config,
            (precision, point) -> H3.geoToH3Address(point.getLat(), point.getLon(), precision),
            h3 -> toPoints(H3.h3ToGeoBoundary(h3)),
            h3 -> new Rectangle(GeoUtils.MIN_LON, GeoUtils.MAX_LON, GeoUtils.MAX_LAT, GeoUtils.MAX_LAT),
            GeoHexGridAggregationBuilder::new,
            (s1, s2) -> new GeoGridQueryBuilder(s1).setGridId(GeoGridQueryBuilder.Grid.GEOHEX, s2),
            randomGeometriesSupplier
        );
    }

    private static class TestGridConfig {
        private final int minPrecision;
        private final int maxPrecision;
        private final String fieldType;
        private final String indexName;
        private final String aggName;
        private final String fieldName;

        private TestGridConfig(int minPrecision, int maxPrecision, String fieldType) {
            this.minPrecision = minPrecision;
            this.maxPrecision = maxPrecision;
            this.fieldType = fieldType;
            this.indexName = "test_" + fieldType;
            this.fieldName = "geometry";
            this.aggName = "geometry_agg";
        }
    }

    private void doValidateGrid(
        TestGridConfig config,
        Point queryPoint,
        BiFunction<Integer, Point, String> pointEncoder,
        Function<String, List<Point>> toPoints,
        Function<String, GeoGridAggregationBuilder> aggBuilder,
        BiFunction<String, String, QueryBuilder> queryBuilder
    ) throws IOException {
        createIndex(config);
        BulkRequestBuilder builder = client().prepareBulk();
        prepareDataInTiles(builder, config, queryPoint, pointEncoder, toPoints);
        validateGrid(config, aggBuilder, queryBuilder);
    }

    private void doTestGrid(
        TestGridConfig config,
        BiFunction<Integer, Point, String> pointEncoder,
        Function<String, List<Point>> toPoints,
        Function<String, Rectangle> toBoundingBox,
        Function<String, GeoGridAggregationBuilder> aggBuilder,
        BiFunction<String, String, QueryBuilder> queryBuilder,
        Supplier<Geometry> randomGeometriesSupplier
    ) throws IOException {
        createIndex(config);
        BulkRequestBuilder builder = client().prepareBulk();

        Point queryPoint = new Point(25, 25);// GeometryTestUtils.randomPoint();
        String[] tiles = prepareDataInTiles(builder, config, queryPoint, pointEncoder, toPoints);

        validateGrid(config, aggBuilder, queryBuilder);

        builder = client().prepareBulk();
        final int numDocs = randomIntBetween(10, 20);
        for (int id = 0; id < numDocs; id++) {
            String wkt = WellKnownText.toWKT(randomGeometriesSupplier.get());
            String doc = "{\"" + config.fieldName + "\" : \"" + wkt + "\"}";
            builder.add(new IndexRequest(config.indexName).source(doc, XContentType.JSON));
        }
        assertFalse(builder.get().hasFailures());
        client().admin().indices().prepareRefresh(config.indexName).get();

        int zoom = randomIntBetween(config.minPrecision, config.maxPrecision);
        Rectangle rectangle = toBoundingBox.apply(tiles[zoom]);
        GeoBoundingBox boundingBox = new GeoBoundingBox(
            new GeoPoint(rectangle.getMaxLat(), rectangle.getMinLon()),
            new GeoPoint(rectangle.getMinLat(), rectangle.getMaxLon())
        );

        for (int i = config.minPrecision; i <= Math.min(config.maxPrecision, zoom + 3); i++) {
            GeoGridAggregationBuilder builderPoint = aggBuilder.apply(config.aggName)
                .field(config.fieldName)
                .precision(i)
                .setGeoBoundingBox(boundingBox)
                .size(256 * 256);
            SearchResponse response = client().prepareSearch(config.indexName).addAggregation(builderPoint).setSize(0).get();
            InternalGeoGrid<?> gridPoint = response.getAggregations().get(config.aggName);
            assertQuery(gridPoint.getBuckets(), queryBuilder, config.fieldName, config.indexName, i);
        }
    }

    private void createIndex(TestGridConfig config) throws IOException {
        XContentBuilder xcb = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject(config.fieldName)
            .field("type", config.fieldType)
            .endObject()
            .endObject()
            .endObject();
        client().admin().indices().prepareCreate(config.indexName).setMapping(xcb).get();
    }

    private String[] prepareDataInTiles(
        BulkRequestBuilder builder,
        TestGridConfig config,
        Point queryPoint,
        BiFunction<Integer, Point, String> pointEncoder,
        Function<String, List<Point>> toPoints
    ) {
        String[] tiles = new String[config.maxPrecision + 1];
        for (int zoom = config.minPrecision; zoom < tiles.length; zoom++) {
            tiles[zoom] = pointEncoder.apply(zoom, queryPoint);
        }

        for (int zoom = config.minPrecision; zoom < tiles.length; zoom++) {
            List<Point> edgePoints = toPoints.apply(tiles[zoom]);
            String[] multiPoint = new String[edgePoints.size()];
            for (int i = 0; i < edgePoints.size(); i++) {
                String wkt = WellKnownText.toWKT(edgePoints.get(i));
                String doc = "{\"" + config.fieldName + "\" : \"" + wkt + "\"}";
                builder.add(new IndexRequest(config.indexName).source(doc, XContentType.JSON));
                multiPoint[i] = "\"" + wkt + "\"";
            }
            String doc = "{\"" + config.fieldName + "\" : " + Arrays.toString(multiPoint) + "}";
            builder.add(new IndexRequest(config.indexName).source(doc, XContentType.JSON));
        }
        assertFalse(builder.get().hasFailures());
        client().admin().indices().prepareRefresh(config.indexName).get();
        return tiles;
    }

    private void validateGrid(
        TestGridConfig config,
        Function<String, GeoGridAggregationBuilder> aggBuilder,
        BiFunction<String, String, QueryBuilder> queryBuilder
    ) {
        for (int i = config.minPrecision; i <= config.maxPrecision; i++) {
            GeoGridAggregationBuilder builderPoint = aggBuilder.apply(config.aggName).field(config.fieldName).precision(i);
            SearchResponse response = client().prepareSearch(config.indexName).addAggregation(builderPoint).setSize(0).get();
            InternalGeoGrid<?> gridPoint = response.getAggregations().get(config.aggName);
            assertQuery(gridPoint.getBuckets(), queryBuilder, config.fieldName, config.indexName, i);
        }
    }

    private void assertQuery(
        List<InternalGeoGridBucket> buckets,
        BiFunction<String, String, QueryBuilder> queryFunction,
        String fieldName,
        String indexName,
        int precision
    ) {
        for (InternalGeoGridBucket bucket : buckets) {
            assertThat(bucket.getDocCount(), Matchers.greaterThan(0L));
            QueryBuilder queryBuilder = queryFunction.apply(fieldName, bucket.getKeyAsString());
            SearchResponse response = client().prepareSearch(indexName).setTrackTotalHits(true).setQuery(queryBuilder).get();
            assertThat(
                "Precision:" + precision + ", Bucket '" + bucket.getKeyAsString() + "' Expected hits at precision " + precision,
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

    private static List<Point> toPoints(CellBoundary boundary) {
        final List<Point> points = new ArrayList<>(boundary.numPoints());
        for (int i = 0; i < boundary.numPoints(); i++) {
            points.add(new Point(boundary.getLatLon(i).getLonDeg(), boundary.getLatLon(i).getLatDeg()));
        }
        return points;
    }
}
