/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.geo.XYEncodingUtils;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.geo.Orientation;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.index.mapper.ShapeFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianShapeValuesSourceType;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class CartesianShapeBoundsAggregatorTests extends AggregatorTestCase {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CartesianBoundsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CartesianPointValuesSourceType.instance(), CartesianShapeValuesSourceType.instance());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                "field",
                true,
                true,
                Orientation.RIGHT,
                null,
                false,
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                InternalCartesianBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.left));
                assertTrue(Double.isInfinite(bounds.right));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testUnmappedFieldWithDocs() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            if (randomBoolean()) {
                Document doc = new Document();
                doc.add(new XYDocValuesField("field", 0.0f, 0.0f));
                w.addDocument(doc);
            }

            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("non_existent");

            MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                "field",
                true,
                true,
                Orientation.RIGHT,
                null,
                false,
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                InternalCartesianBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.left));
                assertTrue(Double.isInfinite(bounds.right));
                assertFalse(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    public void testMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                "field",
                true,
                true,
                Orientation.RIGHT,
                null,
                false,
                Collections.emptyMap()
            );

            Point point = ShapeTestUtils.randomPointNotExtreme(false);
            double x = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getX()));
            double y = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getY()));
            Object missingVal = "POINT(" + x + " " + y + ")";

            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field")
                .missing(missingVal);

            try (IndexReader reader = w.getReader()) {
                InternalCartesianBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertThat(bounds.top, equalTo(y));
                assertThat(bounds.bottom, equalTo(y));
                assertThat(bounds.left, equalTo(x));
                assertThat(bounds.right, equalTo(x));
            }
        }
    }

    public void testInvalidMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                "field",
                true,
                true,
                Orientation.RIGHT,
                null,
                false,
                Collections.emptyMap()
            );

            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field")
                .missing("invalid");
            try (IndexReader reader = w.getReader()) {
                IllegalArgumentException exception = expectThrows(
                    IllegalArgumentException.class,
                    () -> searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType))
                );
                assertThat(exception.getMessage(), startsWith("Unknown geometry type"));
            }
        }
    }

    public void testRandomShapes() throws Exception {
        TestPointCollection expectedExtent = new TestPointCollection();
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            addRandomMultiPointDocs(w, expectedExtent);
            readAndAssertExtent(w, expectedExtent);
        }
    }

    public void testSpecificMultiPointNegNeg() throws Exception {
        double value = Randomness.get().nextDouble(0, 1000);
        doTestSpecificMultiPoint(new Point(-value, -value));
        doTestSpecificMultiPoint(new Point(-value, -value), new Point(-value / 2, -value / 2));
    }

    public void testSpecificMultiPointNegPos() throws Exception {
        double value = Randomness.get().nextDouble(0, 1000);
        doTestSpecificMultiPoint(new Point(-value, value));
        doTestSpecificMultiPoint(new Point(-value, value), new Point(-value / 2, value / 2));
    }

    public void testSpecificMultiPointPosPos() throws Exception {
        double value = Randomness.get().nextDouble(0, 1000);
        doTestSpecificMultiPoint(new Point(value, value));
        doTestSpecificMultiPoint(new Point(value, value), new Point(value / 2, value / 2));
    }

    public void testSpecificMultiPointPosNeg() throws Exception {
        double value = Randomness.get().nextDouble(0, 1000);
        doTestSpecificMultiPoint(new Point(value, -value));
        doTestSpecificMultiPoint(new Point(value, -value), new Point(value / 2, -value / 2));
    }

    public void testSpecificMultiPoint() throws Exception {
        double value = Randomness.get().nextDouble(100, 1000);
        doTestSpecificMultiPoint(new Point(-value, value), new Point(-value, -value), new Point(value, value), new Point(value, -value));
    }

    private void doTestSpecificMultiPoint(Point... data) throws Exception {
        TestPointCollection collection = new TestPointCollection();
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            for (Point point : data) {
                collection.add(point);
            }
            Geometry geometry = new MultiPoint(collection.points);
            doc.add(GeoTestUtils.binaryCartesianShapeDocValuesField("field", geometry));
            w.addDocument(doc);
            readAndAssertExtent(w, collection);
        }
    }

    private void addRandomMultiPointDocs(RandomIndexWriter w, TestPointCollection points) throws IOException {
        int numDocs = randomIntBetween(50, 100);
        for (int i = 0; i < numDocs; i++) {
            Document doc = new Document();
            int numValues = randomIntBetween(1, 5);
            for (int j = 0; j < numValues; j++) {
                points.add(ShapeTestUtils.randomPointNotExtreme(false));
            }
            Geometry geometry = new MultiPoint(points.points);
            doc.add(GeoTestUtils.binaryCartesianShapeDocValuesField("field", geometry));
            w.addDocument(doc);
        }
    }

    private void readAndAssertExtent(RandomIndexWriter w, TestPointCollection points) throws IOException {
        String description = "Bounds over " + points.points.size() + " points";
        CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field");

        MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
            "field",
            true,
            true,
            Orientation.RIGHT,
            null,
            false,
            Collections.emptyMap()
        );
        try (IndexReader reader = w.getReader()) {
            InternalCartesianBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
            assertThat(description + ": top", bounds.top, closeTo(points.top, GEOHASH_TOLERANCE));
            assertThat(description + ": bottom", bounds.bottom, closeTo(points.bottom, GEOHASH_TOLERANCE));
            assertThat(description + ": left", bounds.left, closeTo(points.left, GEOHASH_TOLERANCE));
            assertThat(description + ": right", bounds.right, closeTo(points.right, GEOHASH_TOLERANCE));
            assertTrue(description + ": hasValue(bounds)", AggregationInspectionHelper.hasValue(bounds));
        }
    }

    private static class TestPointCollection {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;
        ArrayList<Point> points = new ArrayList<>();

        private void add(Point point) {
            top = max((float) top, (float) point.getY());
            bottom = min((float) bottom, (float) point.getY());
            left = min((float) left, (float) point.getX());
            right = max((float) right, (float) point.getX());
            points.add(point);
        }
    }
}
