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

package org.elasticsearch.geo.geometry;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.Version;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.geo.utils.WellKnownText;
import org.elasticsearch.test.AbstractWireTestCase;

import java.io.IOException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

abstract class BaseGeometryTestCase<T extends Geometry> extends AbstractWireTestCase<T> {

    @Override
    protected final T createTestInstance() {
        boolean hasAlt = randomBoolean();
        T obj = createTestInstance(hasAlt);
        assertEquals(hasAlt, obj.hasAlt());
        return obj;
    }

    protected abstract T createTestInstance(boolean hasAlt);

    @Override
    protected Writeable.Reader<T> instanceReader() {
        throw new IllegalStateException("shouldn't be called in this test");
    }


    @SuppressWarnings("unchecked")
    @Override
    protected T copyInstance(T instance, Version version) throws IOException {
        WellKnownText wkt = new WellKnownText(true, true);
        String text = wkt.toWKT(instance);
        try {
            return (T) wkt.fromWKT(text);
        } catch (ParseException e) {
            throw new ElasticsearchException(e);
        }
    }

    public void testVisitor() {
        testVisitor(createTestInstance());
    }

    public static void testVisitor(Geometry geom) {
        AtomicBoolean called = new AtomicBoolean(false);
        Object result = geom.visit(new GeometryVisitor<Object, RuntimeException>() {
            private Object verify(Geometry geometry, String expectedClass) {
                assertFalse("Visitor should be called only once", called.getAndSet(true));
                assertSame(geom, geometry);
                assertEquals(geometry.getClass().getName(), "org.elasticsearch.geo.geometry." + expectedClass);
                return "result";
            }

            @Override
            public Object visit(Circle circle) {
                return verify(circle, "Circle");
            }

            @Override
            public Object visit(GeometryCollection<?> collection) {
                return verify(collection, "GeometryCollection");            }

            @Override
            public Object visit(Line line) {
                return verify(line, "Line");
            }

            @Override
            public Object visit(LinearRing ring) {
                return verify(ring, "LinearRing");
            }

            @Override
            public Object visit(MultiLine multiLine) {
                return verify(multiLine, "MultiLine");
            }

            @Override
            public Object visit(MultiPoint multiPoint) {
                return verify(multiPoint, "MultiPoint");
            }

            @Override
            public Object visit(MultiPolygon multiPolygon) {
                return verify(multiPolygon, "MultiPolygon");
            }

            @Override
            public Object visit(Point point) {
                return verify(point, "Point");
            }

            @Override
            public Object visit(Polygon polygon) {
                return verify(polygon, "Polygon");
            }

            @Override
            public Object visit(Rectangle rectangle) {
                return verify(rectangle, "Rectangle");
            }
        });

        assertTrue("visitor wasn't called", called.get());
        assertEquals("result", result);
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

    public static Line randomLine() {
        return randomLine(randomBoolean());
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

    public static Point randomPoint() {
        return randomPoint(randomBoolean());
    }

    public static Point randomPoint(boolean hasAlt) {
        if (hasAlt) {
            return new Point(randomLat(), randomLon(), randomDouble());
        } else {
            return new Point(randomLat(), randomLon());
        }
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
                BaseGeometryTestCase::randomCircle,
                BaseGeometryTestCase::randomLine,
                BaseGeometryTestCase::randomPoint,
                BaseGeometryTestCase::randomPolygon,
                hasAlt ? BaseGeometryTestCase::randomPoint : (b) -> randomRectangle(),
                level < 3 ? (b) -> randomGeometryCollection(level + 1, b) : BaseGeometryTestCase::randomPoint // don't build too deep
            );
            shapes.add(geometry.apply(hasAlt));
        }
        return new GeometryCollection<>(shapes);
    }
}
