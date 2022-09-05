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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.GeometryNormalizer;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.MappedFieldType;
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
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType;
import org.elasticsearch.xpack.spatial.index.mapper.GeoShapeWithDocValuesFieldMapper.GeoShapeWithDocValuesFieldType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.GeoShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
                Orientation.RIGHT,
                null,
                null,
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                    "another_field",
                    true,
                    true,
                    Orientation.RIGHT,
                    null,
                    null,
                    Collections.emptyMap()
                );
                InternalGeoCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());

                fieldType = new GeoShapeWithDocValuesFieldType("field", true, true, Orientation.RIGHT, null, null, Collections.emptyMap());
                result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
                    "another_field",
                    true,
                    true,
                    Orientation.RIGHT,
                    null,
                    null,
                    Collections.emptyMap()
                );
                InternalGeoCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
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

    private void assertCentroid(RandomIndexWriter w, GeoPoint expectedCentroid) throws IOException {
        MappedFieldType fieldType = new GeoShapeWithDocValuesFieldType(
            "field",
            true,
            true,
            Orientation.RIGHT,
            null,
            null,
            Collections.emptyMap()
        );
        GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg").field("field");
        try (IndexReader reader = w.getReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            InternalGeoCentroid result = searchAndReduce(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

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
