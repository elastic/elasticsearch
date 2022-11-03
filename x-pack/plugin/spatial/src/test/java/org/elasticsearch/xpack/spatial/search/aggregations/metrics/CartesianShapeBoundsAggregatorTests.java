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
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.index.RandomIndexWriter;
import org.elasticsearch.common.geo.Orientation;
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
import org.elasticsearch.xpack.spatial.util.ShapeTestUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;

public class CartesianShapeBoundsAggregatorTests extends AggregatorTestCase {
    static final double GEOHASH_TOLERANCE = 1E-5D;

    @Override
    protected List<SearchPlugin> getSearchPlugins() {
        return List.of(new LocalStateSpatialPlugin());
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
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalCartesianBounds bounds = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
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
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalCartesianBounds bounds = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertTrue(Double.isInfinite(bounds.top));
                assertTrue(Double.isInfinite(bounds.bottom));
                assertTrue(Double.isInfinite(bounds.posLeft));
                assertTrue(Double.isInfinite(bounds.posRight));
                assertTrue(Double.isInfinite(bounds.negLeft));
                assertTrue(Double.isInfinite(bounds.negRight));
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
                Collections.emptyMap()
            );

            Point point = ShapeTestUtils.randomPoint(false);
            double x = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getX()));
            double y = XYEncodingUtils.decode(XYEncodingUtils.encode((float) point.getY()));
            Object missingVal = "POINT(" + x + " " + y + ")";

            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field")
                .missing(missingVal);

            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalCartesianBounds bounds = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertThat(bounds.top, equalTo(y));
                assertThat(bounds.bottom, equalTo(y));
                assertThat(bounds.posLeft, equalTo(x >= 0 ? x : Double.POSITIVE_INFINITY));
                assertThat(bounds.posRight, equalTo(x >= 0 ? x : Double.NEGATIVE_INFINITY));
                assertThat(bounds.negLeft, equalTo(x >= 0 ? Double.POSITIVE_INFINITY : x));
                assertThat(bounds.negRight, equalTo(x >= 0 ? Double.NEGATIVE_INFINITY : x));
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
                Collections.emptyMap()
            );

            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field")
                .missing("invalid");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                IllegalArgumentException exception = expectThrows(
                    IllegalArgumentException.class,
                    () -> { searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType)); }
                );
                assertThat(exception.getMessage(), startsWith("Unknown geometry type"));
            }
        }
    }

    public void testRandomShapes() throws Exception {
        double top = Double.NEGATIVE_INFINITY;
        double bottom = Double.POSITIVE_INFINITY;
        double posLeft = Double.POSITIVE_INFINITY;
        double posRight = Double.NEGATIVE_INFINITY;
        double negLeft = Double.POSITIVE_INFINITY;
        double negRight = Double.NEGATIVE_INFINITY;
        int numDocs = randomIntBetween(50, 100);
        try (Directory dir = newDirectory(); RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            for (int i = 0; i < numDocs; i++) {
                Document doc = new Document();
                int numValues = randomIntBetween(1, 5);
                List<Point> points = new ArrayList<>();
                for (int j = 0; j < numValues; j++) {
                    Point point = ShapeTestUtils.randomPoint(false);
                    points.add(point);
                    if (point.getLat() > top) {
                        top = point.getLat();
                    }
                    if (point.getLat() < bottom) {
                        bottom = point.getLat();
                    }
                    if (point.getLon() >= 0 && point.getLon() < posLeft) {
                        posLeft = point.getLon();
                    }
                    if (point.getLon() >= 0 && point.getLon() > posRight) {
                        posRight = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() < negLeft) {
                        negLeft = point.getLon();
                    }
                    if (point.getLon() < 0 && point.getLon() > negRight) {
                        negRight = point.getLon();
                    }
                }
                Geometry geometry = new MultiPoint(points);
                doc.add(GeoTestUtils.binaryCartesianShapeDocValuesField("field", geometry));
                w.addDocument(doc);
            }
            CartesianBoundsAggregationBuilder aggBuilder = new CartesianBoundsAggregationBuilder("my_agg").field("field");

            MappedFieldType fieldType = new ShapeFieldMapper.ShapeFieldType(
                "field",
                true,
                true,
                Orientation.RIGHT,
                null,
                Collections.emptyMap()
            );
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalCartesianBounds bounds = searchAndReduce(searcher, new AggTestConfig(aggBuilder, fieldType));
                assertThat(bounds.top, closeTo(top, GEOHASH_TOLERANCE));
                assertThat(bounds.bottom, closeTo(bottom, GEOHASH_TOLERANCE));
                assertThat(bounds.posLeft, closeTo(posLeft, GEOHASH_TOLERANCE));
                assertThat(bounds.posRight, closeTo(posRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negRight, closeTo(negRight, GEOHASH_TOLERANCE));
                assertThat(bounds.negLeft, closeTo(negLeft, GEOHASH_TOLERANCE));
                assertTrue(AggregationInspectionHelper.hasValue(bounds));
            }
        }
    }

    @Override
    protected AggregationBuilder createAggBuilderForTypeTest(MappedFieldType fieldType, String fieldName) {
        return new CartesianBoundsAggregationBuilder("foo").field(fieldName);
    }

    @Override
    protected List<ValuesSourceType> getSupportedValuesSourceTypes() {
        return List.of(CartesianPointValuesSourceType.instance(), CartesianShapeValuesSourceType.instance());
    }
}
