/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.search.aggregations.metrics;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.LatLonDocValuesField;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.RandomIndexWriter;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.Directory;
import org.elasticsearch.common.geo.CentroidCalculator;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geo.GeometryTestUtils;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.GeometryVisitor;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.mapper.BinaryGeoShapeDocValuesField;
import org.elasticsearch.index.mapper.GeoPointFieldMapper;
import org.elasticsearch.index.mapper.GeoShapeFieldMapper;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.aggregations.AggregatorTestCase;
import org.elasticsearch.search.aggregations.support.AggregationInspectionHelper;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.geo.RandomGeoGenerator;

import java.io.IOException;
import java.util.function.Function;

public class GeoCentroidAggregatorTests extends AggregatorTestCase {

    private static final double GEOHASH_TOLERANCE = 1E-6D;

    public void testEmpty() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg")
                    .field("field");

            MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
            fieldType.setHasDocValues(true);
            fieldType.setName("field");
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);
                InternalGeoCentroid result = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testUnmapped() throws Exception {
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg")
                    .field("another_field");

            Document document = new Document();
            document.add(new LatLonDocValuesField("field", 10, 10));
            w.addDocument(document);
            try (IndexReader reader = w.getReader()) {
                IndexSearcher searcher = new IndexSearcher(reader);

                MappedFieldType fieldType = new GeoPointFieldMapper.GeoPointFieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName("another_field");
                InternalGeoCentroid result = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());

                fieldType = new GeoPointFieldMapper.GeoPointFieldType();
                fieldType.setHasDocValues(true);
                fieldType.setName("field");
                result = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);
                assertNull(result.centroid());
                assertFalse(AggregationInspectionHelper.hasValue(result));
            }
        }
    }

    public void testSingleValuedGeoPointField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        int numUniqueGeoPoints = randomIntBetween(1, numDocs);
        try (Directory dir = newDirectory();
            RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoPoint expectedCentroid = new GeoPoint(0, 0);
            GeoPoint[] singleValues = new GeoPoint[numUniqueGeoPoints];
            for (int i = 0 ; i < singleValues.length; i++) {
                singleValues[i] = RandomGeoGenerator.randomPoint(random());
            }
            GeoPoint singleVal;
            for (int i = 0; i < numDocs; i++) {
                singleVal = singleValues[i % numUniqueGeoPoints];
                Document document = new Document();
                document.add(new LatLonDocValuesField("field", singleVal.getLat(), singleVal.getLon()));
                w.addDocument(document);
                expectedCentroid = expectedCentroid.reset(expectedCentroid.lat() + (singleVal.lat() - expectedCentroid.lat()) / (i + 1),
                        expectedCentroid.lon() + (singleVal.lon() - expectedCentroid.lon()) / (i + 1));
            }
            assertCentroid(w, expectedCentroid, new GeoPointFieldMapper.GeoPointFieldType());
        }
    }

    public void testMultiValuedGeoPointField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        int numUniqueGeoPoints = randomIntBetween(1, numDocs);
        try (Directory dir = newDirectory();
            RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {

            GeoPoint expectedCentroid = new GeoPoint(0, 0);
            GeoPoint[] multiValues = new GeoPoint[numUniqueGeoPoints];
            for (int i = 0 ; i < multiValues.length; i++) {
                multiValues[i] = RandomGeoGenerator.randomPoint(random());
            }
            final GeoPoint[] multiVal = new GeoPoint[2];
            for (int i = 0; i < numDocs; i++) {
                multiVal[0] = multiValues[i % numUniqueGeoPoints];
                multiVal[1] = multiValues[(i+1) % numUniqueGeoPoints];
                Document document = new Document();
                document.add(new LatLonDocValuesField("field", multiVal[0].getLat(), multiVal[0].getLon()));
                document.add(new LatLonDocValuesField("field", multiVal[1].getLat(), multiVal[1].getLon()));
                w.addDocument(document);
                double newMVLat = (multiVal[0].lat() + multiVal[1].lat())/2d;
                double newMVLon = (multiVal[0].lon() + multiVal[1].lon())/2d;
                expectedCentroid = expectedCentroid.reset(expectedCentroid.lat() + (newMVLat - expectedCentroid.lat()) / (i + 1),
                        expectedCentroid.lon() + (newMVLon - expectedCentroid.lon()) / (i + 1));
            }
            assertCentroid(w, expectedCentroid, new GeoPointFieldMapper.GeoPointFieldType());
        }
    }

    @SuppressWarnings("unchecked")
    public void testGeoShapeField() throws Exception {
        int numDocs = scaledRandomIntBetween(64, 256);
        Function<Boolean, Geometry> geometryGenerator = ESTestCase.randomFrom(
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint,
            (hasAlt) -> GeometryTestUtils.randomRectangle(),
            GeometryTestUtils::randomMultiPolygon
        );
        try (Directory dir = newDirectory();
             RandomIndexWriter w = new RandomIndexWriter(random(), dir)) {
            GeoPoint expectedCentroid = new GeoPoint(0, 0);
            CentroidCalculator centroidOfCentroidsCalculator = new CentroidCalculator();
            for (int i = 0; i < numDocs; i++) {
                CentroidCalculator calculator = new CentroidCalculator();
                Document document = new Document();
                Geometry geometry = geometryGenerator.apply(false);
                geometry.visit(new GeometryVisitor<Void, Exception>() {
                    @Override
                    public Void visit(Circle circle) throws Exception {
                        calculator.addCoordinate(circle.getX(), circle.getY());
                        return null;
                    }

                    @Override
                    public Void visit(GeometryCollection<?> collection) throws Exception {
                        for (Geometry shape : collection) {
                            shape.visit(this);
                        }
                        return null;
                    }

                    @Override
                    public Void visit(Line line) throws Exception {
                        for (int i = 0; i < line.length(); i++) {
                            calculator.addCoordinate(line.getX(i), line.getY(i));
                        }
                        return null;
                    }

                    @Override
                    public Void visit(LinearRing ring) throws Exception {
                        for (int i = 0; i < ring.length() - 1; i++) {
                            calculator.addCoordinate(ring.getX(i), ring.getY(i));
                        }
                        return null;
                    }

                    @Override
                    public Void visit(MultiLine multiLine) throws Exception {
                        for (Line line : multiLine) {
                            visit(line);
                        }
                        return null;
                    }

                    @Override
                    public Void visit(MultiPoint multiPoint) throws Exception {
                        for (Point point : multiPoint) {
                            visit(point);
                        }
                        return null;
                    }

                    @Override
                    public Void visit(MultiPolygon multiPolygon) throws Exception {
                        for (Polygon polygon : multiPolygon) {
                            visit(polygon);
                        }
                        return null;
                    }

                    @Override
                    public Void visit(Point point) throws Exception {
                        calculator.addCoordinate(point.getX(), point.getY());
                        return null;
                    }

                    @Override
                    public Void visit(Polygon polygon) throws Exception {
                        return visit(polygon.getPolygon());
                    }

                    @Override
                    public Void visit(Rectangle rectangle) throws Exception {
                        calculator.addCoordinate(rectangle.getMinX(), rectangle.getMinY());
                        calculator.addCoordinate(rectangle.getMinX(), rectangle.getMaxY());
                        calculator.addCoordinate(rectangle.getMaxX(), rectangle.getMinY());
                        calculator.addCoordinate(rectangle.getMaxX(), rectangle.getMaxY());
                        return null;
                    }
                });
                document.add(new BinaryGeoShapeDocValuesField("field", geometry));
                w.addDocument(document);
                centroidOfCentroidsCalculator.addCoordinate(calculator.getX(), calculator.getY());
            }
            expectedCentroid.reset(centroidOfCentroidsCalculator.getY(), centroidOfCentroidsCalculator.getX());
            assertCentroid(w, expectedCentroid, new GeoShapeFieldMapper.GeoShapeFieldType());
        }
    }

    private void assertCentroid(RandomIndexWriter w, GeoPoint expectedCentroid, MappedFieldType fieldType) throws IOException {
        fieldType.setHasDocValues(true);
        fieldType.setName("field");
        GeoCentroidAggregationBuilder aggBuilder = new GeoCentroidAggregationBuilder("my_agg")
                .field("field");
        try (IndexReader reader = w.getReader()) {
            IndexSearcher searcher = new IndexSearcher(reader);
            InternalGeoCentroid result = search(searcher, new MatchAllDocsQuery(), aggBuilder, fieldType);

            assertEquals("my_agg", result.getName());
            GeoPoint centroid = result.centroid();
            assertNotNull(centroid);
            assertEquals(expectedCentroid.getLat(), centroid.getLat(), GEOHASH_TOLERANCE);
            assertEquals(expectedCentroid.getLon(), centroid.getLon(), GEOHASH_TOLERANCE);
            assertTrue(AggregationInspectionHelper.hasValue(result));
        }
    }

}
