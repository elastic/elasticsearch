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

package org.elasticsearch.common.geo;

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.geo.geometry.Circle;
import org.elasticsearch.geo.geometry.Geometry;
import org.elasticsearch.geo.geometry.GeometryCollection;
import org.elasticsearch.geo.geometry.Line;
import org.elasticsearch.geo.geometry.LinearRing;
import org.elasticsearch.geo.geometry.MultiLine;
import org.elasticsearch.geo.geometry.MultiPoint;
import org.elasticsearch.geo.geometry.MultiPolygon;
import org.elasticsearch.geo.geometry.Point;
import org.elasticsearch.geo.geometry.Polygon;
import org.elasticsearch.geo.geometry.Rectangle;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Supplier;

public class GeoJsonSerializationTests extends ESTestCase {

    private static class GeometryWrapper implements ToXContentObject {

        private Geometry geometry;

        GeometryWrapper(Geometry geometry) {
            this.geometry = geometry;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return GeoJson.toXContent(geometry, builder, params);
        }

        public static GeometryWrapper fromXContent(XContentParser parser) throws IOException {
            parser.nextToken();
            return new GeometryWrapper(GeoJson.fromXContent(parser, true, false, true));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            GeometryWrapper that = (GeometryWrapper) o;
            return Objects.equals(geometry, that.geometry);
        }

        @Override
        public int hashCode() {
            return Objects.hash(geometry);
        }
    }


    private void xContentTest(Supplier<Geometry> instanceSupplier) throws IOException {
        AbstractXContentTestCase.xContentTester(
            this::createParser,
            () -> new GeometryWrapper(instanceSupplier.get()),
            (geometryWrapper, xContentBuilder) -> {
                geometryWrapper.toXContent(xContentBuilder, ToXContent.EMPTY_PARAMS);
            },
            GeometryWrapper::fromXContent)
            .supportsUnknownFields(true)
            .test();
    }


    public void testPoint() throws IOException {
        xContentTest(() -> randomPoint(randomBoolean()));
    }

    public void testMultiPoint() throws IOException {
        xContentTest(() -> randomMultiPoint(randomBoolean()));
    }

    public void testLineString() throws IOException {
        xContentTest(() -> randomLine(randomBoolean()));
    }

    public void testMultiLineString() throws IOException {
        xContentTest(() -> randomMultiLine(randomBoolean()));
    }

    public void testPolygon() throws IOException {
        xContentTest(() -> randomPolygon(randomBoolean()));
    }

    public void testMultiPolygon() throws IOException {
        xContentTest(() -> randomMultiPolygon(randomBoolean()));
    }

    public void testEnvelope() throws IOException {
        xContentTest(GeoJsonSerializationTests::randomRectangle);
    }

    public void testGeometryCollection() throws IOException {
        xContentTest(() -> randomGeometryCollection(randomBoolean()));
    }

    public void testCircle() throws IOException {
        xContentTest(() -> randomCircle(randomBoolean()));
    }

    public static double randomLat() {
        return randomDoubleBetween(-90, 90, true);
    }

    public static double randomLon() {
        return randomDoubleBetween(-180, 180, true);
    }

    public static Circle randomCircle(boolean hasAlt) {
        if (hasAlt) {
            return new Circle(randomDoubleBetween(-90, 90, true), randomDoubleBetween(-180, 180, true), randomDouble(),
                randomDoubleBetween(0, 100, false));
        } else {
            return new Circle(randomDoubleBetween(-90, 90, true), randomDoubleBetween(-180, 180, true), randomDoubleBetween(0, 100, false));
        }
    }

    public static Line randomLine(boolean hasAlts) {
        int size = randomIntBetween(2, 10);
        double[] lats = new double[size];
        double[] lons = new double[size];
        double[] alts = hasAlts ? new double[size] : null;
        for (int i = 0; i < size; i++) {
            lats[i] = randomLat();
            lons[i] = randomLon();
            if (hasAlts) {
                alts[i] = randomDouble();
            }
        }
        if (hasAlts) {
            return new Line(lats, lons, alts);
        }
        return new Line(lats, lons);
    }

    public static Point randomPoint(boolean hasAlt) {
        if (hasAlt) {
            return new Point(randomLat(), randomLon(), randomDouble());
        } else {
            return new Point(randomLat(), randomLon());
        }
    }

    public static MultiPoint randomMultiPoint(boolean hasAlt) {
        int size = randomIntBetween(3, 10);
        List<Point> points = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            points.add(randomPoint(hasAlt));
        }
        return new MultiPoint(points);
    }

    public static MultiLine randomMultiLine(boolean hasAlt) {
        int size = randomIntBetween(3, 10);
        List<Line> lines = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            lines.add(randomLine(hasAlt));
        }
        return new MultiLine(lines);
    }

    public static MultiPolygon randomMultiPolygon(boolean hasAlt) {
        int size = randomIntBetween(3, 10);
        List<Polygon> polygons = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            polygons.add(randomPolygon(hasAlt));
        }
        return new MultiPolygon(polygons);
    }

    public static LinearRing randomLinearRing(boolean hasAlt) {
        int size = randomIntBetween(3, 10);
        double[] lats = new double[size + 1];
        double[] lons = new double[size + 1];
        double[] alts;
        if (hasAlt) {
            alts = new double[size + 1];
        } else {
            alts = null;
        }
        for (int i = 0; i < size; i++) {
            lats[i] = randomLat();
            lons[i] = randomLon();
            if (hasAlt) {
                alts[i] = randomDouble();
            }
        }
        lats[size] = lats[0];
        lons[size] = lons[0];
        if (hasAlt) {
            alts[size] = alts[0];
            return new LinearRing(lats, lons, alts);
        } else {
            return new LinearRing(lats, lons);
        }
    }

    public static Polygon randomPolygon(boolean hasAlt) {
        int size = randomIntBetween(0, 10);
        List<LinearRing> holes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            holes.add(randomLinearRing(hasAlt));
        }
        if (holes.size() > 0) {
            return new Polygon(randomLinearRing(hasAlt), holes);
        } else {
            return new Polygon(randomLinearRing(hasAlt));
        }
    }

    public static Rectangle randomRectangle() {
        double lat1 = randomLat();
        double lat2 = randomLat();
        double minLon = randomLon();
        double maxLon = randomLon();
        return new Rectangle(Math.min(lat1, lat2), Math.max(lat1, lat2), minLon, maxLon);
    }

    public static GeometryCollection<Geometry> randomGeometryCollection(boolean hasAlt) {
        return randomGeometryCollection(0, hasAlt);
    }

    private static GeometryCollection<Geometry> randomGeometryCollection(int level, boolean hasAlt) {
        int size = randomIntBetween(1, 10);
        List<Geometry> shapes = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            @SuppressWarnings("unchecked") Function<Boolean, Geometry> geometry = randomFrom(
                GeoJsonSerializationTests::randomCircle,
                GeoJsonSerializationTests::randomLine,
                GeoJsonSerializationTests::randomPoint,
                GeoJsonSerializationTests::randomPolygon,
                hasAlt ? GeoJsonSerializationTests::randomPoint : (b) -> randomRectangle(),
                level < 3 ? (b) -> randomGeometryCollection(level + 1, b) : GeoJsonSerializationTests::randomPoint // don't build too deep
            );
            shapes.add(geometry.apply(hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }
}
