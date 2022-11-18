/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.spatial.util;

import org.apache.lucene.geo.XShapeTestUtil;
import org.apache.lucene.geo.XYCircle;
import org.apache.lucene.geo.XYPolygon;
import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.geometry.MultiPoint;
import org.elasticsearch.geometry.MultiPolygon;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.test.ESTestCase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static org.elasticsearch.geo.GeometryTestUtils.linearRing;
import static org.elasticsearch.test.ESTestCase.randomValueOtherThanMany;

/** generates random cartesian shapes */
public class ShapeTestUtils {

    public static final double MIN_VALID_AREA = 1e-10;

    public static double randomValue() {
        return XShapeTestUtil.nextDouble();
    }

    public static Point randomPoint() {
        return randomPoint(ESTestCase.randomBoolean());
    }

    public static Point randomPoint(boolean hasAlt) {
        if (hasAlt) {
            return new Point(randomValue(), randomValue(), randomAlt());
        }
        return new Point(randomValue(), randomValue());
    }

    public static Point randomPointNotExtreme(boolean hasAlt) {
        return ESTestCase.randomValueOtherThanMany(ShapeTestUtils::extremePoint, () -> ShapeTestUtils.randomPoint(hasAlt));
    }

    public static Point randomPointNotExtreme() {
        return ShapeTestUtils.randomPointNotExtreme(ESTestCase.randomBoolean());
    }

    /**
     * Since cartesian centroid is stored in Float values, and calculations perform averages over many,
     * We cannot support points at the very edge of the range.
     */
    public static boolean extremePoint(Point point) {
        double max = Float.MAX_VALUE / 100;
        double min = -Float.MAX_VALUE / 100;
        return point.getLon() > max || point.getLon() < min || point.getLat() > max || point.getLat() < min;
    }

    public static double randomAlt() {
        return ESTestCase.randomDouble() * XShapeTestUtil.CENTER_SCALE_FACTOR;
    }

    public static Circle randomCircle(boolean hasAlt) {
        XYCircle luceneCircle = XShapeTestUtil.nextCircle();
        if (hasAlt) {
            return new Circle(luceneCircle.getX(), luceneCircle.getY(), randomAlt(), luceneCircle.getRadius());
        } else {
            return new Circle(luceneCircle.getX(), luceneCircle.getY(), luceneCircle.getRadius());
        }
    }

    public static Line randomLine(boolean hasAlts) {
        // we use nextPolygon because it guarantees no duplicate points
        XYPolygon lucenePolygon = XShapeTestUtil.nextPolygon();
        int size = lucenePolygon.numPoints() - 1;
        double[] x = new double[size];
        double[] y = new double[size];
        double[] alts = hasAlts ? new double[size] : null;
        for (int i = 0; i < size; i++) {
            x[i] = lucenePolygon.getPolyX(i);
            y[i] = lucenePolygon.getPolyY(i);
            if (hasAlts) {
                alts[i] = randomAlt();
            }
        }
        if (hasAlts) {
            return new Line(x, y, alts);
        }
        return new Line(x, y);
    }

    public static Polygon randomPolygon(boolean hasAlt) {
        XYPolygon lucenePolygon = randomValueOtherThanMany(p -> area(p) <= MIN_VALID_AREA, XShapeTestUtil::nextPolygon);
        if (lucenePolygon.numHoles() > 0) {
            XYPolygon[] luceneHoles = lucenePolygon.getHoles();
            List<LinearRing> holes = new ArrayList<>();
            for (int i = 0; i < lucenePolygon.numHoles(); i++) {
                XYPolygon poly = luceneHoles[i];
                holes.add(linearRing(floatsToDoubles(poly.getPolyX()), floatsToDoubles(poly.getPolyY()), hasAlt));
            }
            return new Polygon(
                linearRing(floatsToDoubles(lucenePolygon.getPolyX()), floatsToDoubles(lucenePolygon.getPolyY()), hasAlt),
                holes
            );
        }
        return new Polygon(linearRing(floatsToDoubles(lucenePolygon.getPolyX()), floatsToDoubles(lucenePolygon.getPolyY()), hasAlt));
    }

    static double area(XYPolygon p) {
        double windingSum = 0;
        final int numPts = p.numPoints() - 1;
        for (int i = 0; i < numPts; i++) {
            // compute signed area
            windingSum += p.getPolyX(i) * p.getPolyY(i + 1) - p.getPolyY(i) * p.getPolyX(i + 1);
        }
        return Math.abs(windingSum / 2);
    }

    static double[] floatsToDoubles(float[] f) {
        double[] d = new double[f.length];
        for (int i = 0; i < f.length; i++) {
            d[i] = f[i];
        }
        return d;
    }

    public static Rectangle randomRectangle() {
        org.apache.lucene.geo.XYRectangle rectangle = XShapeTestUtil.nextBox();
        return new Rectangle(rectangle.minX, rectangle.maxX, rectangle.maxY, rectangle.minY);
    }

    public static MultiPoint randomMultiPoint(boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(3, 10);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            points.add(randomPoint(hasAlt));
        }
        return new MultiPoint(points);
    }

    public static MultiLine randomMultiLine(boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(3, 10);
        List<Line> lines = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            lines.add(randomLine(hasAlt));
        }
        return new MultiLine(lines);
    }

    public static MultiPolygon randomMultiPolygon(boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(3, 10);
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            polygons.add(randomPolygon(hasAlt));
        }
        return new MultiPolygon(polygons);
    }

    public static GeometryCollection<Geometry> randomGeometryCollection(boolean hasAlt) {
        return randomGeometryCollection(0, hasAlt);
    }

    private static GeometryCollection<Geometry> randomGeometryCollection(int level, boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometry(level, hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }

    public static Geometry randomGeometry(boolean hasAlt) {
        return randomGeometry(0, hasAlt);
    }

    protected static Geometry randomGeometry(int level, boolean hasAlt) {
        @SuppressWarnings("unchecked")
        Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            ShapeTestUtils::randomLine,
            ShapeTestUtils::randomPoint,
            ShapeTestUtils::randomPolygon,
            ShapeTestUtils::randomMultiLine,
            ShapeTestUtils::randomMultiPoint,
            ShapeTestUtils::randomMultiPolygon,
            hasAlt ? ShapeTestUtils::randomPoint : (b) -> randomRectangle(),
            level < 3 ? (b) -> randomGeometryCollection(level + 1, b) : ShapeTestUtils::randomPoint // don't build too deep
        );
        return geometry.apply(hasAlt);
    }

    public static Geometry randomGeometryWithoutCircle(boolean hasAlt) {
        return randomGeometryWithoutCircle(0, hasAlt);
    }

    public static Geometry randomGeometryWithoutCircle(int level, boolean hasAlt) {
        @SuppressWarnings("unchecked")
        Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            ShapeTestUtils::randomPoint,
            ShapeTestUtils::randomMultiPoint,
            ShapeTestUtils::randomLine,
            ShapeTestUtils::randomMultiLine,
            ShapeTestUtils::randomPolygon,
            ShapeTestUtils::randomMultiPolygon,
            hasAlt ? ShapeTestUtils::randomPoint : (b) -> randomRectangle(),
            level < 3 ? (b) -> randomGeometryCollectionWithoutCircle(level + 1, hasAlt) : ShapeTestUtils::randomPoint // don't build too
            // deep
        );
        return geometry.apply(hasAlt);
    }

    public static GeometryCollection<Geometry> randomGeometryCollectionWithoutCircle(boolean hasAlt) {
        return randomGeometryCollectionWithoutCircle(0, hasAlt);
    }

    protected static GeometryCollection<Geometry> randomGeometryCollectionWithoutCircle(int level, boolean hasAlt) {
        int size = ESTestCase.randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            shapes.add(randomGeometryWithoutCircle(level, hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }
}
