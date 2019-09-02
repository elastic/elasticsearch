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

package org.elasticsearch.geo;

import org.apache.lucene.geo.GeoTestUtil;
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

public class GeometryTestUtils {

    public static double randomLat() {
        return GeoTestUtil.nextLatitude();
    }

    public static double randomLon() {
        return GeoTestUtil.nextLongitude();
    }

    public static double randomAlt() {
        return ESTestCase.randomDouble();
    }

    public static Circle randomCircle(boolean hasAlt) {
        if (hasAlt) {
            return new Circle(randomLon(), randomLat(), ESTestCase.randomDouble(),
                ESTestCase.randomDoubleBetween(0, 100, false));
        } else {
            return new Circle(randomLon(), randomLat(), ESTestCase.randomDoubleBetween(0, 100, false));
        }
    }

    public static Line randomLine(boolean hasAlts) {
        // we use nextPolygon because it guarantees no duplicate points
        org.apache.lucene.geo.Polygon lucenePolygon = GeoTestUtil.nextPolygon();
        int size = lucenePolygon.numPoints() - 1;
        double[] lats = new double[size];
        double[] lons = new double[size];
        double[] alts = hasAlts ? new double[size] : null;
        for (int i = 0; i < size; i++) {
            lats[i] = lucenePolygon.getPolyLat(i);
            lons[i] = lucenePolygon.getPolyLon(i);
            if (hasAlts) {
                alts[i] = randomAlt();
            }
        }
        if (hasAlts) {
            return new Line(lons, lats, alts);
        }
        return new Line(lons, lats);
    }

    public static Point randomPoint() {
        return randomPoint(ESTestCase.randomBoolean());
    }

    public static Point randomPoint(boolean hasAlt) {
        if (hasAlt) {
            return new Point(randomLon(), randomLat(), randomAlt());
        } else {
            return new Point(randomLon(), randomLat());
        }
    }

    public static Polygon randomPolygon(boolean hasAlt) {
        org.apache.lucene.geo.Polygon lucenePolygon = GeoTestUtil.nextPolygon();
        if (lucenePolygon.numHoles() > 0) {
            org.apache.lucene.geo.Polygon[] luceneHoles = lucenePolygon.getHoles();
            List<LinearRing> holes = new ArrayList<>();
            for (int i = 0; i < lucenePolygon.numHoles(); i++) {
                org.apache.lucene.geo.Polygon poly = luceneHoles[i];
                holes.add(linearRing(poly.getPolyLons(), poly.getPolyLats(), hasAlt));
            }
            return new Polygon(linearRing(lucenePolygon.getPolyLons(), lucenePolygon.getPolyLats(), hasAlt), holes);
        }
        return new Polygon(linearRing(lucenePolygon.getPolyLons(), lucenePolygon.getPolyLats(), hasAlt));
    }


    private static double[] randomAltRing(int size) {
        double[] alts = new double[size];
        for (int i = 0; i < size - 1; i++) {
            alts[i] = randomAlt();
        }
        alts[size - 1] = alts[0];
        return alts;
    }

    public static LinearRing linearRing(double[] lons, double[] lats,boolean generateAlts) {
        if (generateAlts) {
            return new LinearRing(lons, lats, randomAltRing(lats.length));
        }
        return new LinearRing(lons, lats);
    }

    public static Rectangle randomRectangle() {
        org.apache.lucene.geo.Rectangle rectangle = GeoTestUtil.nextBox();
        return new Rectangle(rectangle.minLon, rectangle.maxLon, rectangle.maxLat, rectangle.minLat);
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
        @SuppressWarnings("unchecked") Function<Boolean, Geometry> geometry = ESTestCase.randomFrom(
            GeometryTestUtils::randomCircle,
            GeometryTestUtils::randomLine,
            GeometryTestUtils::randomPoint,
            GeometryTestUtils::randomPolygon,
            GeometryTestUtils::randomMultiLine,
            GeometryTestUtils::randomMultiPoint,
            GeometryTestUtils::randomMultiPolygon,
            hasAlt ? GeometryTestUtils::randomPoint : (b) -> randomRectangle(),
            level < 3 ? (b) -> randomGeometryCollection(level + 1, b) : GeometryTestUtils::randomPoint // don't build too deep
        );
        return geometry.apply(hasAlt);
    }
}
