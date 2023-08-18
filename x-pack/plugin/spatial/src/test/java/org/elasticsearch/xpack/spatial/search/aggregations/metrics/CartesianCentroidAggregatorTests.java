/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.document.XYDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.geo.SpatialPoint;
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
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.io.IOException;
import java.util.List;

public class CartesianCentroidAggregatorTests extends AggregatorTestCase {

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
    }

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalCartesianCentroid result = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("another_field");

            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new PointFieldMapper.PointFieldType("another_field");
                InternalCartesianCentroid result = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());

                fieldType = new PointFieldMapper.PointFieldType("another_field");
                result = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmappedWithMissing() throws Exception {
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("another_field")
                .missing("6.475030899047852, 53.69437026977539");

            CartesianPoint expectedCentroid = new CartesianPoint(6.475030899047852, 53.69437026977539);
            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new PointFieldMapper.PointFieldType("another_field");
                InternalCartesianCentroid result = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertEquals(expectedCentroid, result.centroid());
                assertTrue(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testSingleValuedField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        int numUniqueCartesianPoints = randomIntBetween(1, numDocs);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            CartesianPoint expectedCentroid = new CartesianPoint(0, 0);
            CartesianPoint[] singleValues = new CartesianPoint[numUniqueCartesianPoints];
            for (int i = 0; i < singleValues.length; i++) {
                Point point = ShapeTestUtils.randomPointNotExtreme(false);
                singleValues[i] = new CartesianPoint(point.getX(), point.getY());
            }
            for (int i = 0; i < numDocs; i++) {
                CartesianPoint singleVal = singleValues[i % numUniqueCartesianPoints];
                Document document = new Document();
                document.add(new XYDocValuesField("field", (float) singleVal.getX(), (float) singleVal.getY()));
                w.addDocument(document);
                expectedCentroid = expectedCentroid.reset(
                    expectedCentroid.getX() + ((float) singleVal.getX() - expectedCentroid.getX()) / (i + 1),
                    expectedCentroid.getY() + ((float) singleVal.getY() - expectedCentroid.getY()) / (i + 1)
                );
            }
            assertCentroid(w, expectedCentroid);
        }
    }

    public void testMultiValuedField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        int numUniqueCartesianPoints = randomIntBetween(1, numDocs);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            CartesianPoint expectedCentroid = new CartesianPoint(0, 0);
            CartesianPoint[] multiValues = new CartesianPoint[numUniqueCartesianPoints];
            for (int i = 0; i < multiValues.length; i++) {
                Point point = ShapeTestUtils.randomPointNotExtreme(false);
                multiValues[i] = new CartesianPoint(point.getX(), point.getY());
            }
            final CartesianPoint[] multiVal = new CartesianPoint[2];
            for (int i = 0; i < numDocs; i++) {
                multiVal[0] = multiValues[i % numUniqueCartesianPoints];
                multiVal[1] = multiValues[(i + 1) % numUniqueCartesianPoints];
                Document document = new Document();
                document.add(new XYDocValuesField("field", (float) multiVal[0].getX(), (float) multiVal[0].getY()));
                document.add(new XYDocValuesField("field", (float) multiVal[1].getX(), (float) multiVal[1].getY()));
                w.addDocument(document);
                double newMVx = ((float) multiVal[0].getX() + (float) multiVal[1].getX()) / 2d;
                double newMVy = ((float) multiVal[0].getY() + (float) multiVal[1].getY()) / 2d;
                expectedCentroid = expectedCentroid.reset(
                    expectedCentroid.getX() + (newMVx - expectedCentroid.getX()) / (i + 1),
                    expectedCentroid.getY() + (newMVy - expectedCentroid.getY()) / (i + 1)
                );
            }
            assertCentroid(w, expectedCentroid);
        }
    }

    // TODO Consider merging this code with the almost identical code in the CartesianShapeCentroidAggregatorTests
    private void assertCentroid(RandomIndexWriter w, CartesianPoint expectedCentroid) throws IOException {
        MappedFieldType fieldType = new PointFieldMapper.PointFieldType("field");
        CartesianCentroidAggregationBuilder aggBuilder = new CartesianCentroidAggregationBuilder("my_agg").field("field");
        try (IndexReader reader = w.getReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            InternalCartesianCentroid result = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));

            assertEquals("my_agg", result.getName());
            SpatialPoint centroid = result.centroid();
            assertNotNull(centroid);
            assertCentroid("x-value", result.count(), centroid.getX(), expectedCentroid.getX());
            assertCentroid("y-value", result.count(), centroid.getY(), expectedCentroid.getY());
            assertTrue(AggregationInspectionHelper.hasValue(result));
        }
    }

    private void assertCentroid(String name, long count, double value, double expected) {
        assertEquals("Centroid over " + count + " had incorrect " + name, expected, value, tolerance(value, expected, count));
    }

    /**
     * Due to cartesian ranging from very large numbers to very small, we need different ways of calculating accuracy for the extremes
     */
    private double tolerance(double a, double b, long count) {
        double coordinate = (Math.abs(a) + Math.abs(b)) / 2;
        if (coordinate < 1.0) {
            // When dealing with small numbers, we use an absolute tolerance, similar to the geo case
            return 1e-6D;
        } else {
            // For large numbers we use a tolerance based on a faction of the expected value
            double tolerance = coordinate / 1e6D;
            // For very large numbers the floating point error is worse for large counts
            return tolerance > 1e25 ? tolerance * count : tolerance;
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CartesianCentroidAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CartesianPointValuesSourceType.instance());
    }
}
