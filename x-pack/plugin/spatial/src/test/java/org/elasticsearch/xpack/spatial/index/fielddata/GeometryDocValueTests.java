/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.index.fielddata;

import org.apache.lucene.geo.LatLonGeometry;
import org.elasticsearch.common.geo.GeoShapeUtils;
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
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.index.mapper.GeoShapeIndexer;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.spatial.util.GeoTestUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.elasticsearch.geo.GeometryTestUtils.randomLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiLine;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomMultiPolygon;
import static org.elasticsearch.geo.GeometryTestUtils.randomPoint;
import static org.elasticsearch.geo.GeometryTestUtils.randomPolygon;
import static org.hamcrest.Matchers.equalTo;

public class GeometryDocValueTests extends ESTestCase {

    @SuppressWarnings("unchecked")
    public void testDimensionalShapeType() throws IOException {
        GeoShapeIndexer indexer = new GeoShapeIndexer(true, "test");
        assertDimensionalShapeType(randomPoint(false), DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomMultiPoint(false), DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomLine(false), DimensionalShapeType.LINE);
        assertDimensionalShapeType(randomMultiLine(false), DimensionalShapeType.LINE);
        Geometry randoPoly = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
            try {
                Geometry newGeo = indexer.prepareForIndexing(g);
                return newGeo.type() != ShapeType.POLYGON;
            } catch (Exception e) {
                return true;
            }
        }, () -> randomPolygon(false)));
        Geometry randoMultiPoly = indexer.prepareForIndexing(randomValueOtherThanMany(g -> {
            try {
                Geometry newGeo = indexer.prepareForIndexing(g);
                return newGeo.type() != ShapeType.MULTIPOLYGON;
            } catch (Exception e) {
                return true;
            }
        }, () -> randomMultiPolygon(false)));
        assertDimensionalShapeType(randoPoly, DimensionalShapeType.POLYGON);
        assertDimensionalShapeType(randoMultiPoly, DimensionalShapeType.POLYGON);
        assertDimensionalShapeType(randomFrom(
            new GeometryCollection<>(List.of(randomPoint(false))),
            new GeometryCollection<>(List.of(randomMultiPoint(false))),
            new GeometryCollection<>(Collections.singletonList(
                new GeometryCollection<>(List.of(randomPoint(false), randomMultiPoint(false))))))
            , DimensionalShapeType.POINT);
        assertDimensionalShapeType(randomFrom(
            new GeometryCollection<>(List.of(randomPoint(false), randomLine(false))),
            new GeometryCollection<>(List.of(randomMultiPoint(false), randomMultiLine(false))),
            new GeometryCollection<>(Collections.singletonList(
                new GeometryCollection<>(List.of(randomPoint(false), randomLine(false))))))
            , DimensionalShapeType.LINE);
        assertDimensionalShapeType(randomFrom(
            new GeometryCollection<>(List.of(randomPoint(false), indexer.prepareForIndexing(randomLine(false)), randoPoly)),
            new GeometryCollection<>(List.of(randomMultiPoint(false), randoMultiPoly)),
            new GeometryCollection<>(Collections.singletonList(
                new GeometryCollection<>(List.of(indexer.prepareForIndexing(randomLine(false)),
                    indexer.prepareForIndexing(randoPoly))))))
            , DimensionalShapeType.POLYGON);
    }

    public void testRectangleShape() throws IOException {
        for (int i = 0; i < 1000; i++) {
            int minX = randomIntBetween(-40, -1);
            int maxX = randomIntBetween(1, 40);
            int minY = randomIntBetween(-40, -1);
            int maxY = randomIntBetween(1, 40);
            Geometry rectangle = new Rectangle(minX, maxX, maxY, minY);
            GeometryDocValueReader reader = GeoTestUtils.GeometryDocValueReader(rectangle, CoordinateEncoder.GEO);

            Extent expectedExtent  = getExtentFromBox(minX, minY, maxX, maxY);
            assertThat(expectedExtent, equalTo(reader.getExtent()));
            // centroid is calculated using original double values but then loses precision as it is serialized as an integer
            int encodedCentroidX = CoordinateEncoder.GEO.encodeX(((double) minX + maxX) / 2);
            int encodedCentroidY = CoordinateEncoder.GEO.encodeY(((double) minY + maxY) / 2);
            assertEquals(encodedCentroidX, reader.getCentroidX());
            assertEquals(encodedCentroidY, reader.getCentroidY());
        }
    }

    private static Extent getExtentFromBox(double bottomLeftX, double bottomLeftY, double topRightX, double topRightY) {
        return Extent.fromPoints(CoordinateEncoder.GEO.encodeX(bottomLeftX),
            CoordinateEncoder.GEO.encodeY(bottomLeftY),
            CoordinateEncoder.GEO.encodeX(topRightX),
            CoordinateEncoder.GEO.encodeY(topRightY));

    }

    private static void assertDimensionalShapeType(Geometry geometry, DimensionalShapeType expected) throws IOException {
        GeometryDocValueReader reader = GeoTestUtils.GeometryDocValueReader(geometry, CoordinateEncoder.GEO);
        assertThat(reader.getDimensionalShapeType(), equalTo(expected));
    }

    private static LatLonGeometry[] toLuceneGeometry(Geometry geometry) {
        List<LatLonGeometry> luceneGeometries = new ArrayList<>();
        geometry.visit(new GeometryVisitor<Void, RuntimeException>() {
            @Override
            public Void visit(Circle circle)  {
                throw new UnsupportedOperationException();
            }

            @Override
            public Void visit(GeometryCollection<?> collection)  {
                for (Geometry g : collection) {
                    g.visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Line line)  {
                luceneGeometries.add(GeoShapeUtils.toLuceneLine(line));
                return null;
            }

            @Override
            public Void visit(LinearRing ring)  {
                throw new UnsupportedOperationException();
            }

            @Override
            public Void visit(MultiLine multiLine) {
                for(Line line : multiLine) {
                    visit(line);
                }
                return null;
            }

            @Override
            public Void visit(MultiPoint multiPoint) {
                for(Point point : multiPoint) {
                    visit(point);
                }
                return null;
            }

            @Override
            public Void visit(MultiPolygon multiPolygon)  {
                for(Polygon polygon : multiPolygon) {
                    visit(polygon);
                }
                return null;
            }

            @Override
            public Void visit(Point point)  {
                luceneGeometries.add(GeoShapeUtils.toLucenePoint(point));
                return null;
            }

            @Override
            public Void visit(Polygon polygon) {
                luceneGeometries.add(GeoShapeUtils.toLucenePolygon(polygon));
                return null;
            }

            @Override
            public Void visit(Rectangle rectangle) {
                luceneGeometries.add(GeoShapeUtils.toLuceneRectangle(rectangle));
                return null;
            }
        });
        return luceneGeometries.toArray(new LatLonGeometry[luceneGeometries.size()]);
    }
}
