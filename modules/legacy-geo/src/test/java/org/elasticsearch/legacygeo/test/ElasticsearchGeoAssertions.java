/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.legacygeo.test;

import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.MultiLine;
import org.elasticsearch.legacygeo.parsers.ShapeParser;
import org.elasticsearch.xcontent.XContentParser;
import org.hamcrest.Matcher;
import org.junit.Assert;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.impl.GeoCircle;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ElasticsearchGeoAssertions {

    private static int top(Coordinate... points) {
        int top = 0;
        for (int i = 1; i < points.length; i++) {
            if (points[i].y < points[top].y) {
                top = i;
            } else if (points[i].y == points[top].y) {
                if (points[i].x <= points[top].x) {
                    top = i;
                }
            }
        }
        return top;
    }

    private static int prev(int top, Coordinate... points) {
        for (int i = 1; i < points.length; i++) {
            int p = (top + points.length - i) % points.length;
            if ((points[p].x != points[top].x) || (points[p].y != points[top].y)) {
                return p;
            }
        }
        return -1;
    }

    private static int next(int top, Coordinate... points) {
        for (int i = 1; i < points.length; i++) {
            int n = (top + i) % points.length;
            if ((points[n].x != points[top].x) || (points[n].y != points[top].y)) {
                return n;
            }
        }
        return -1;
    }

    private static Coordinate[] fixedOrderedRing(List<Coordinate> coordinates, boolean direction) {
        return fixedOrderedRing(coordinates.toArray(new Coordinate[coordinates.size()]), direction);
    }

    private static Coordinate[] fixedOrderedRing(Coordinate[] points, boolean direction) {

        final int top = top(points);
        final int next = next(top, points);
        final int prev = prev(top, points);
        final boolean orientation = points[next].x < points[prev].x;

        if (orientation != direction) {
            List<Coordinate> asList = Arrays.asList(points);
            Collections.reverse(asList);
            return fixedOrderedRing(asList, direction);
        } else {
            if (top > 0) {
                Coordinate[] aligned = new Coordinate[points.length];
                System.arraycopy(points, top, aligned, 0, points.length - top - 1);
                System.arraycopy(points, 0, aligned, points.length - top - 1, top);
                aligned[aligned.length - 1] = aligned[0];
                return aligned;
            } else {
                return points;
            }
        }

    }

    public static void assertEquals(Coordinate c1, Coordinate c2) {
        assertTrue("expected coordinate " + c1 + " but found " + c2, c1.x == c2.x && c1.y == c2.y);
    }

    private static boolean isRing(Coordinate[] c) {
        return (c[0].x == c[c.length - 1].x) && (c[0].y == c[c.length - 1].y);
    }

    public static void assertEquals(Coordinate[] c1, Coordinate[] c2) {
        Assert.assertEquals(c1.length, c2.length);

        if (isRing(c1) && isRing(c2)) {
            c1 = fixedOrderedRing(c1, true);
            c2 = fixedOrderedRing(c2, true);
        }

        for (int i = 0; i < c2.length; i++) {
            assertEquals(c1[i], c2[i]);
        }
    }

    public static void assertEquals(LineString l1, LineString l2) {
        assertEquals(l1.getCoordinates(), l2.getCoordinates());
    }

    public static void assertEquals(MultiLineString l1, MultiLineString l2) {
        assertEquals(l1.getCoordinates(), l2.getCoordinates());
    }

    public static void assertEquals(Polygon p1, Polygon p2) {
        Assert.assertEquals(p1.getNumInteriorRing(), p2.getNumInteriorRing());

        assertEquals(p1.getExteriorRing(), p2.getExteriorRing());

        // TODO: This test do not check all permutations of linestrings. So the test
        // fails if the holes of the polygons are not ordered the same way
        for (int i = 0; i < p1.getNumInteriorRing(); i++) {
            assertEquals(p1.getInteriorRingN(i), p2.getInteriorRingN(i));
        }
    }

    public static void assertEquals(MultiPolygon p1, MultiPolygon p2) {
        Assert.assertEquals(p1.getNumGeometries(), p2.getNumGeometries());

        // TODO: This test do not check all permutations. So the Test fails
        // if the inner polygons are not ordered the same way in both Multipolygons
        for (int i = 0; i < p1.getNumGeometries(); i++) {
            Geometry a = p1.getGeometryN(i);
            Geometry b = p2.getGeometryN(i);
            assertEquals(a, b);
        }
    }

    public static void assertEquals(Geometry s1, Geometry s2) {
        if (s1 instanceof LineString && s2 instanceof LineString) {
            assertEquals((LineString) s1, (LineString) s2);

        } else if (s1 instanceof Polygon && s2 instanceof Polygon) {
            assertEquals((Polygon) s1, (Polygon) s2);

        } else if (s1 instanceof MultiPoint && s2 instanceof MultiPoint) {
            Assert.assertEquals(s1, s2);

        } else if (s1 instanceof MultiPolygon && s2 instanceof MultiPolygon) {
            assertEquals((MultiPolygon) s1, (MultiPolygon) s2);

        } else if (s1 instanceof MultiLineString && s2 instanceof MultiLineString) {
            assertEquals((MultiLineString) s1, (MultiLineString) s2);

        } else {
            throw new RuntimeException(
                "equality of shape types not supported [" + s1.getClass().getName() + " and " + s2.getClass().getName() + "]"
            );
        }
    }

    public static void assertEquals(JtsGeometry g1, JtsGeometry g2) {
        assertEquals(g1.getGeom(), g2.getGeom());
    }

    public static void assertEquals(ShapeCollection<?> s1, ShapeCollection<?> s2) {
        Assert.assertEquals(s1.size(), s2.size());
        for (int i = 0; i < s1.size(); i++) {
            assertEquals(s1.get(i), s2.get(i));
        }
    }

    public static void assertEquals(Object s1, Object s2) {
        if (s1 instanceof JtsGeometry && s2 instanceof JtsGeometry) {
            assertEquals((JtsGeometry) s1, (JtsGeometry) s2);
        } else if (s1 instanceof JtsPoint p1 && s2 instanceof JtsPoint p2) {
            Assert.assertEquals(p1, p2);
        } else if (s1 instanceof ShapeCollection && s2 instanceof ShapeCollection) {
            assertEquals((ShapeCollection<?>) s1, (ShapeCollection<?>) s2);
        } else if (s1 instanceof GeoCircle && s2 instanceof GeoCircle) {
            Assert.assertEquals(s1, s2);
        } else if (s1 instanceof RectangleImpl && s2 instanceof RectangleImpl) {
            Assert.assertEquals(s1, s2);
        } else if (s1 instanceof org.apache.lucene.geo.Line[] lines1 && s2 instanceof org.apache.lucene.geo.Line[] lines2) {
            Assert.assertArrayEquals(lines1, lines2);
        } else if (s1 instanceof org.apache.lucene.geo.Polygon[] poly1 && s2 instanceof org.apache.lucene.geo.Polygon[] poly2) {
            Assert.assertArrayEquals(poly1, poly2);
        } else if ((s1 instanceof org.apache.lucene.geo.Line && s2 instanceof org.apache.lucene.geo.Line)
            || (s1 instanceof org.apache.lucene.geo.Polygon && s2 instanceof org.apache.lucene.geo.Polygon)
            || (s1 instanceof org.apache.lucene.geo.Rectangle && s2 instanceof org.apache.lucene.geo.Rectangle)
            || (s1 instanceof GeoPoint && s2 instanceof GeoPoint)) {
                Assert.assertEquals(s1, s2);
            } else if (s1 instanceof Object[] objects1 && s2 instanceof Object[] objects2) {
                Assert.assertArrayEquals(objects1, objects2);
            } else if (s1 instanceof org.elasticsearch.geometry.Geometry && s2 instanceof org.elasticsearch.geometry.Geometry) {
                Assert.assertEquals(s1, s2);
            } else {
                // We want to know the type of the shape because we test shape equality in a special way...
                // ... in particular we test that one ring is equivalent to another ring even if the points are rotated or reversed.
                throw new RuntimeException(
                    "equality of shape types not supported [" + s1.getClass().getName() + " and " + s2.getClass().getName() + "]"
                );
            }
    }

    @Deprecated
    private static Geometry unwrapJTS(Object shape) {
        assertThat(shape, instanceOf(JtsGeometry.class));
        return ((JtsGeometry) shape).getGeom();
    }

    public static void assertMultiPolygon(Object shape, boolean useJTS) {
        if (useJTS) {
            assertTrue(
                "expected MultiPolygon but found " + unwrapJTS(shape).getClass().getName(),
                unwrapJTS(shape) instanceof MultiPolygon
            );
        } else {
            assertTrue(
                "expected Polygon[] but found " + shape.getClass().getName(),
                shape instanceof org.elasticsearch.geometry.MultiPolygon
            );
        }
    }

    public static void assertPolygon(Object shape, boolean useJTS) {
        if (useJTS) {
            assertTrue("expected Polygon but found " + unwrapJTS(shape).getClass().getName(), unwrapJTS(shape) instanceof Polygon);
        } else {
            assertTrue("expected Polygon but found " + shape.getClass().getName(), shape instanceof org.elasticsearch.geometry.Polygon);
        }
    }

    public static void assertLineString(Object shape, boolean useJTS) {
        if (useJTS) {
            assertTrue("expected LineString but found " + unwrapJTS(shape).getClass().getName(), unwrapJTS(shape) instanceof LineString);
        } else {
            assertTrue("expected Line but found " + shape.getClass().getName(), shape instanceof Line);
        }
    }

    public static void assertMultiLineString(Object shape, boolean useJTS) {
        if (useJTS) {
            assertTrue(
                "expected MultiLineString but found " + unwrapJTS(shape).getClass().getName(),
                unwrapJTS(shape) instanceof MultiLineString
            );
        } else {
            assertTrue("expected Line[] but found " + shape.getClass().getName(), shape instanceof MultiLine);
        }
    }

    public static void assertDistance(String geohash1, String geohash2, Matcher<Double> match) {
        GeoPoint p1 = new GeoPoint(geohash1);
        GeoPoint p2 = new GeoPoint(geohash2);
        assertDistance(p1.lat(), p1.lon(), p2.lat(), p2.lon(), match);
    }

    public static void assertDistance(double lat1, double lon1, double lat2, double lon2, Matcher<Double> match) {
        assertThat(distance(lat1, lon1, lat2, lon2), match);
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        return GeoDistance.ARC.calculate(lat1, lon1, lat2, lon2, DistanceUnit.DEFAULT);
    }

    public static void assertValidException(XContentParser parser, Class<?> expectedException) {
        try {
            ShapeParser.parse(parser).buildS4J();
            Assert.fail("process completed successfully when " + expectedException.getName() + " expected");
        } catch (Exception e) {
            assertTrue(
                "expected " + expectedException.getName() + " but found " + e.getClass().getName(),
                e.getClass().equals(expectedException)
            );
        }
    }
}
