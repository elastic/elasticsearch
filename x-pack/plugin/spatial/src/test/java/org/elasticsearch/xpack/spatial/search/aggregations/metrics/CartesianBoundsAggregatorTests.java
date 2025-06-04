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
import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.geo.ShapeTestUtils;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.plugins.SearchPlugin;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.search.aggregations.support.ValuesSourceType;
import org.elasticsearch.xpack.spatial.LocalStateSpatialPlugin;
import org.elasticsearch.xpack.spatial.common.CartesianPoint;
import org.elasticsearch.xpack.spatial.index.mapper.PointFieldMapper;
import org.elasticsearch.xpack.spatial.search.aggregations.support.CartesianPointValuesSourceType;

import java.io.IOException;
import java.util.List;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.elasticsearch.xpack.spatial.search.aggregations.metrics.InternalCartesianBoundsTests.GEOHASH_TOLERANCE;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.startsWith;

public class CartesianBoundsAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
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

            MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
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

            Point point = ShapeTestUtils.randomPointNotExtreme(false);
            double x = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getX()));
            double y = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getY()));

            // valid missing values
            for (Object missingVal : List.of("POINT(" + x + " " + y + ")", x + ", " + y, new CartesianPoint(x, y))) {
                readAndAssertMissing(w, missingVal, (float) x, (float) y);
            }
        }
    }

    private void readAndAssertMissing(RandomIndexWriter w, Object missingVal, float x, float y) throws IOException {
        CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field").missing(missingVal);
        MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");

        String description = "Bounds aggregation with missing=" + missingVal;
        try (IndexReader reader = w.getReader()) {
            InternalCartesianBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
            assertThat(description + ": top", bounds.top, closeTo(y, GEOHASH_TOLERANCE));
            assertThat(description + ": bottom", bounds.bottom, closeTo(y, GEOHASH_TOLERANCE));
            assertThat(description + ": left", bounds.left, closeTo(x, GEOHASH_TOLERANCE));
            assertThat(description + ": right", bounds.right, closeTo(x, GEOHASH_TOLERANCE));
        }
    }

    public void testInvalidMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            Document doc = new Document();
            doc.add(new NumericDocValuesField("not_field", 1000L));
            w.addDocument(doc);

            MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");

            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field")
                .missing("invalid");
            try (IndexReader reader = w.getReader()) {
                ElasticsearchParseException exception = expectThrows(
                    ElasticsearchParseException.class,
                    () -> searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType))
                );
                assertThat(exception.getMessage(), startsWith("unsupported symbol"));
            }
        }
    }

    public void testRandom() throws Exception {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double left = Double.POSITIVE_INFINITY;
        double right = Double.NEGATIVE_INFINITY;
        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                for (int j = 0; j < numValues; j++) {
                    Point point = ShapeTestUtils.randomPointNotExtreme(false);
                    doc.add(new XYDocValuesField("field", (float) point.getX(), (float) point.getY()));

                    // To determine expected values we should imitate the internal encoding behaviour
                    double x = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getX()));
                    double y = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getY()));
                    top = max(top, y);
                    bottom = min(bottom, y);
                    left = min(left, x);
                    right = max(right, x);
                }
                w.addDocument(doc);
            }
            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                InternalCartesianBounds bounds = searchAndReduce(reader, new AggTestConfig(aggBuilder, fieldType));
                assertCloseTo("top", numDocs, bounds.top, top);
                assertCloseTo("bottom", numDocs, bounds.bottom, bottom);
                assertCloseTo("left", numDocs, bounds.left, left);
                assertCloseTo("right", numDocs, bounds.right, right);
                assertTrue(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    private void assertCloseTo(String name, long count, double value, double expected) {
        assertEquals("Bounds over " + count + " points had incorrect " + name, expected, value, tolerance(value, expected, count));
    }

    private double tolerance(double value, double expected, long count) {
        double tolerance = max(Math.abs(expected / 1e5), Math.abs(value / 1e5));
        // Very large numbers have more floating point error, also increasing with count
        return tolerance > 1e25 ? tolerance * count : tolerance;
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CartesianBoundsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CartesianPointValuesSourceType.instance());
    }
}
