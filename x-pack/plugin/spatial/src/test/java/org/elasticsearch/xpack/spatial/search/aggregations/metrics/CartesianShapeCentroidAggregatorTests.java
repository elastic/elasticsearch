/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.XYPointField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.common.geo.SpatialPoint;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.metrics.CompensatedSum;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.fielddata.CentroidCalculator;
import org.elasticsearch.xpack.spatial.index.fielddata.DimensionalShapeType;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static java.lang.Math.signum;
import static org.hamcrest.Matchers.equalTo;

public class CartesianShapeCentroidAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                "field",
                true,
                true,
                Orientation.RIGHT,
                null,
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                InternalCartesianCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("another_field");

            Document document = new Document();
            document.add(new XYPointField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                    "another_field",
                    true,
                    true,
                    Orientation.RIGHT,
                    null,
                    Collections.emptyMap()
                );
                InternalCartesianCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());

                fieldType = new ShapeFieldMapper.ShapeFieldType("field", true, true, Orientation.RIGHT, null, Collections.emptyMap());
                result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmappedWithMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("another_field")
                .missing("POINT(6.475031 53.69437)");

            // Cast to float to deal with the XYEncodingUtils use of floats
            CartesianPoint expectedCentroid = new CartesianPoint((float) 6.475031, (float) 53.69437);
            Document document = new Document();
            document.add(new XYPointField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                    "another_field",
                    true,
                    true,
                    Orientation.RIGHT,
                    null,
                    Collections.emptyMap()
                );
                InternalCartesianCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
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
                ShapeTestUtils::randomLine,
                ShapeTestUtils::randomPoint,
                ShapeTestUtils::randomPolygon,
                ShapeTestUtils::randomMultiLine,
                ShapeTestUtils::randomMultiPoint,
                ShapeTestUtils::randomMultiPolygon
            );
            Geometry geometry = geometryGenerator.apply(false);
            try {
                // make sure we can index the geometry
                GeoTestUtils.binaryCartesianShapeDocValuesField("field", geometry);
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
            TestExpected xExpected = new TestExpected();
            TestExpected yExpected = new TestExpected();
            for (Geometry geometry : geometries) {
                Document document = new Document();
                CentroidCalculator calculator = new CentroidCalculator();
                calculator.add(geometry);
                document.add(GeoTestUtils.binaryCartesianShapeDocValuesField("field", geometry));
                w.addDocument(document);
                if (targetShapeType.compareTo(calculator.getDimensionalShapeType()) == 0) {
                    double weight = calculator.sumWeight();
                    compensatedSumLat.add(weight * calculator.getY());
                    compensatedSumLon.add(weight * calculator.getX());
                    compensatedSumWeight.add(weight);
                    xExpected.setValue(compensatedSumLon.value() / compensatedSumWeight.value());
                    yExpected.setValue(compensatedSumLat.value() / compensatedSumWeight.value());
                }
            }
            // force using a single aggregator to compute the centroid
            w.forceMerge(1);
            assertCentroid(w, xExpected, yExpected);
        }
    }

    private void assertCentroid(RandomIndexWriter w, TestExpected xExpected, TestExpected yExpected) throws IOException {
        MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
            "field",
            true,
            true,
            Orientation.RIGHT,
            null,
            Collections.emptyMap()
        );
        CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("field");
        try (IndexReader reader = w.getReader()) {
            InternalCartesianCentroid result = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));

            assertEquals("my_agg", result.getName());
            SpatialPoint centroid = result.centroid();
            assertNotNull(centroid);
            assertCentroid("x-value", centroid.getX(), xExpected);
            assertCentroid("y-value", centroid.getY(), yExpected);
            assertTrue(AggregationInspectionHelper.hasValue(result));
        }
    }

    private void assertCentroid(String name, double value, TestExpected expected) {
        assertEquals("Centroid over " + expected.count + " had incorrect " + name, expected.value, value, expected.tolerance());
    }

    private static class TestExpected {
        private double value;
        private double previous;
        private int count;

        private void setValue(double value) {
            this.previous = this.value;
            this.value = value;
            this.count++;
        }

        private double tolerance() {
            // When last numbers change sign, they usually end up as much smaller numbers (but retain high errors)
            double tolerance = signum(value) != signum(previous) ? Math.abs(value / 1e3) : Math.abs(value / 1e5);
            // Very large numbers have more floating point error, also increasing with count
            return tolerance > 1e25 ? tolerance * count : tolerance;
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CartesianCentroidAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CartesianPointValuesSourceType.instance(), CartesianShapeValuesSourceType.instance());
    }
}
