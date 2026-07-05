/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.geo.GeoEncodingUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.lucene.spatial.CentroidCalculator;
import org.elasticsearch.lucene.spatial.DimensionalShapeType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.metrics.GeoCentroidAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.CoreValuesSourceType;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.hamcrest.Matchers.equalTo;

public class GeoShapeCentroidAggregatorTests extends AggregatorTestCase {

    private static final double GEOHASH_TOLERANCE = 1E-6D;

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                "field",
                true,
                true,
                randomBoolean(),
                Orientation.RIGHT,
                null,
                null,
                null,
                false,
                Map.of()
            );
            try (IndexReader reader = w.getReader()) {
                InternalGeoCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("another_field");

            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                    "another_field",
                    true,
                    true,
                    randomBoolean(),
                    Orientation.RIGHT,
                    null,
                    null,
                    null,
                    false,
                    Map.of()
                );
                InternalGeoCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());

                fieldType = new GeoShapeWithDocValuesFieldType(
                    "field",
                    true,
                    true,
                    randomBoolean(),
                    Orientation.RIGHT,
                    null,
                    null,
                    null,
                    false,
                    Map.of()
                );
                result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmappedWithMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("another_field")
                .missing("POINT(6.475031 53.69437)");

            double normalizedLat = GeoEncodingUtils.decodeLatitude(GeoEncodingUtils.encodeLatitude(53.69437));
            double normalizedLon = GeoEncodingUtils.decodeLongitude(GeoEncodingUtils.encodeLongitude(6.475031));
            GeoPoint expectedCentroid = new GeoPoint(normalizedLat, normalizedLon);
            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                    "another_field",
                    true,
                    true,
                    randomBoolean(),
                    Orientation.RIGHT,
                    null,
                    null,
                    null,
                    false,
                    Map.of()
                );
                InternalGeoCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertThat(result.centroid(), equalTo(expectedCentroid));
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    @SuppressWarnings("unchecked")
    public void testSingleValuedField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        List<Geometry> geometries = new ArrayList<>();
        DimensionalShapeType targetShapeType = DimensionalShapeType.POINT;
        for (int i = 0; i < numDocs; i++) {
            Function<Boolean, Geometry> geometryGenerator = ESTestCase.randomFrom(
                GeometryTestUtils::randomLine,
                GeometryTestUtils::randomPoint,
                GeometryTestUtils::randomPolygon,
                GeometryTestUtils::randomMultiLine,
                GeometryTestUtils::randomMultiPoint,
                GeometryTestUtils::randomMultiPolygon
            );
            Geometry geometry = geometryGenerator.apply(false);
            try {
                geometry = GeometryNormalizer.apply(Orientation.CCW, geometry);
                // make sure we can index the geometry
                GeoTestUtils.binaryGeoShapeDocValuesField("field", geometry);
            } catch (IllegalArgumentException e) {
                // do not include geometry.
                assumeNoException("The geometry[" + geometry.toString() + "] is not supported", e);
            }
            geometries.add(geometry);
            // find dimensional-shape-type of geometry
            CentroidCalculator centroidCalculator = new CentroidCalculator();
            centroidCalculator.add(geometry);
            DimensionalShapeType geometryShapeType = centroidCalculator.getDimensionalShapeType();
            targetShapeType = targetShapeType.compareTo(geometryShapeType) >= 0 ? targetShapeType : geometryShapeType;
        }
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CompensatedSum compensatedSumLon = new CompensatedSum(0, 0);
            CompensatedSum compensatedSumLat = new CompensatedSum(0, 0);
            CompensatedSum compensatedSumWeight = new CompensatedSum(0, 0);
            for (Geometry geometry : geometries) {
                Document document = new Document();
                CentroidCalculator calculator = new CentroidCalculator();
                calculator.add(geometry);
                document.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", geometry));
                w.addDocument(document);
                if (targetShapeType.compareTo(calculator.getDimensionalShapeType()) == 0) {
                    double weight = calculator.sumWeight();
                    compensatedSumLat.add(weight * calculator.getY());
                    compensatedSumLon.add(weight * calculator.getX());
                    compensatedSumWeight.add(weight);
                }
            }
            // force using a single aggregator to compute the centroid
            w.forceMerge(1);
            GeoPoint expectedCentroid = new GeoPoint(
                compensatedSumLat.value() / compensatedSumWeight.value(),
                compensatedSumLon.value() / compensatedSumWeight.value()
            );
            assertCentroid(w, expectedCentroid);
        }
    }

    /**
     * Tests that when shapes with very different areas are in different segments (simulating different shards),
     * the aggregation produces an area-weighted centroid rather than a count-weighted one.
     *
     * <p>One large polygon (area ≈ 100 sq-deg, centroid at lat=60) and 100 small polygons
     * (area ≈ 0.01 sq-deg each, centroid at lat≈10) are placed in separate segments.
     * The area-weighted centroid should be near lat=59.5 (dominated by the large polygon).
     * The incorrect count-weighted centroid would be near lat=10.5 (dominated by 100 small docs).
     */
    public void testMultiSegmentAreaWeightedReduction() throws Exception {
        // Large polygon: 10×10 degree square, centroid lat=60, lon=0, area=100 sq-deg
        Polygon largePolygon = new Polygon(new LinearRing(new double[] { -5, 5, 5, -5, -5 }, new double[] { 55, 55, 65, 65, 55 }));

        // Small polygon: 0.1×0.1 degree square, centroid lat=10.05, lon=0.05, area=0.01 sq-deg
        Polygon smallPolygon = new Polygon(new LinearRing(new double[] { 0, 0.1, 0.1, 0, 0 }, new double[] { 10, 10, 10.1, 10.1, 10 }));
        int numSmallPolygons = 100;

        // Compute expected area-weighted centroid from CentroidCalculator
        CentroidCalculator largeCalc = new CentroidCalculator();
        largeCalc.add(largePolygon);
        CentroidCalculator smallCalc = new CentroidCalculator();
        smallCalc.add(smallPolygon);

        double largeWeight = largeCalc.sumWeight();
        double smallWeight = smallCalc.sumWeight();
        double totalWeight = largeWeight + numSmallPolygons * smallWeight;
        double expectedLat = (largeWeight * largeCalc.getY() + numSmallPolygons * smallWeight * smallCalc.getY()) / totalWeight;
        double expectedLon = (largeWeight * largeCalc.getX() + numSmallPolygons * smallWeight * smallCalc.getX()) / totalWeight;

        // The count-weighted centroid that old (buggy) code would produce in a multi-shard scenario:
        // lat = (1 * largeCalc.getY() + numSmallPolygons * smallCalc.getY()) / (1 + numSmallPolygons) ≈ 10.5
        // We verify the correct result is close to expectedLat (≈ 59.5), far from 10.5.
        assertTrue("Area-weighted centroid lat " + expectedLat + " should be far from count-weighted lat", expectedLat > 50);

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            // Segment 1: large polygon
            Document doc = new Document();
            doc.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", largePolygon));
            w.addDocument(doc);
            w.flush(); // force a segment boundary

            // Segment 2: many small polygons
            for (int i = 0; i < numSmallPolygons; i++) {
                Document smallDoc = new Document();
                smallDoc.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", smallPolygon));
                w.addDocument(smallDoc);
            }
            w.flush();

            MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                "field",
                true,
                true,
                randomBoolean(),
                Orientation.RIGHT,
                null,
                null,
                null,
                false,
                Map.of()
            );
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("field");
            try (IndexReader reader = w.getReader()) {
                InternalGeoCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNotNull(result.centroid());
                assertEquals(numSmallPolygons + 1, result.count());
                assertEquals("lat (area-weighted)", expectedLat, result.centroid().getY(), GEOHASH_TOLERANCE);
                assertEquals("lon (area-weighted)", expectedLon, result.centroid().getX(), GEOHASH_TOLERANCE);
            }
        }
    }

    /**
     * Tests that when points and polygons are in different segments (simulating different shards),
     * only the highest-dimensional type (polygons) contributes to the centroid. Points in a
     * separate shard should be discarded during cross-shard reduction.
     */
    public void testMultiSegmentDimensionalShapeTypePriority() throws Exception {
        // Polygon at lat=60, lon=0
        Polygon polygon = new Polygon(new LinearRing(new double[] { -5, 5, 5, -5, -5 }, new double[] { 55, 55, 65, 65, 55 }));

        // Point at lat=10, lon=0 — should be discarded since polygon is higher dimension
        org.elasticsearch.geometry.Point point = new org.elasticsearch.geometry.Point(0, 10);

        CentroidCalculator polyCalc = new CentroidCalculator();
        polyCalc.add(polygon);
        double expectedLat = polyCalc.getY();
        double expectedLon = polyCalc.getX();

        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            // Segment 1: polygon
            Document doc = new Document();
            doc.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", polygon));
            w.addDocument(doc);
            w.flush();

            // Segment 2: many points (different shard — these should be ignored)
            for (int i = 0; i < 100; i++) {
                Document pointDoc = new Document();
                pointDoc.add(GeoTestUtils.binaryGeoShapeDocValuesField("field", point));
                w.addDocument(pointDoc);
            }
            w.flush();

            MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                "field",
                true,
                true,
                randomBoolean(),
                Orientation.RIGHT,
                null,
                null,
                null,
                false,
                Map.of()
            );
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("field");
            try (IndexReader reader = w.getReader()) {
                InternalGeoCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNotNull(result.centroid());
                // The centroid should be the polygon centroid, not pulled toward the points
                assertEquals("lat (polygon only)", expectedLat, result.centroid().getY(), GEOHASH_TOLERANCE);
                assertEquals("lon (polygon only)", expectedLon, result.centroid().getX(), GEOHASH_TOLERANCE);
            }
        }
    }

    private void assertCentroid(RandomIndexWriter w, GeoPoint expectedCentroid) throws IOException {
        MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
            "field",
            true,
            true,
            randomBoolean(),
            Orientation.RIGHT,
            null,
            null,
            null,
            false,
            Map.of()
        );
        GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("field");
        try (IndexReader reader = w.getReader()) {
            InternalGeoCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));

            assertEquals("my_agg", result.getName());
            SpatialPoint centroid = result.centroid();
            assertNotNull(centroid);
            assertEquals(expectedCentroid.getX(), centroid.getX(), GEOHASH_TOLERANCE);
            assertEquals(expectedCentroid.getY(), centroid.getY(), GEOHASH_TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(result));
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new GeoCentroidAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CoreValuesSourceType.GEOPOINT, GeoShapeValuesSourceType.instance());
    }
}
