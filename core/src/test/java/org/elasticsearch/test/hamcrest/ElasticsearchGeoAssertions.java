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

package org.elasticsearch.test.hamcrest;

import org.locationtech.spatial4j.shape.Shape;
import org.locationtech.spatial4j.shape.ShapeCollection;
import org.locationtech.spatial4j.shape.impl.GeoCircle;
import org.locationtech.spatial4j.shape.impl.RectangleImpl;
import org.locationtech.spatial4j.shape.jts.JtsGeometry;
import org.locationtech.spatial4j.shape.jts.JtsPoint;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import com.vividsolutions.jts.geom.LineString;
import com.vividsolutions.jts.geom.MultiLineString;
import com.vividsolutions.jts.geom.MultiPoint;
import com.vividsolutions.jts.geom.MultiPolygon;
import com.vividsolutions.jts.geom.Polygon;
import org.elasticsearch.common.geo.GeoDistance;
import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.common.geo.builders.ShapeBuilder;
import org.elasticsearch.common.unit.DistanceUnit;
import org.elasticsearch.common.xcontent.XContentParser;
import org.hamcrest.Matcher;
import org.junit.Assert;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class ElasticsearchGeoAssertions {

    private static int top(Coordinate...points) {
        int top = 0;
        for (int i = 1; i < points.length; i++) {
            if(points[i].y < points[top].y) {
                top = i;
            } else if(points[i].y == points[top].y) {
                if(points[i].x <= points[top].x) {
                    top = i;
                }
            }
        }
        return top;
    }

    private static int prev(int top, Coordinate...points) {
        for (int i = 1; i < points.length; i++) {
            int p = (top + points.length - i) % points.length;
            if((points[p].x != points[top].x) || (points[p].y != points[top].y)) {
                return p;
            }
        }
        return -1;
    }

    private static int next(int top, Coordinate...points) {
        for (int i = 1; i < points.length; i++) {
            int n = (top + i) % points.length;
            if((points[n].x != points[top].x) || (points[n].y != points[top].y)) {
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

        if(orientation != direction) {
            List<Coordinate> asList = Arrays.asList(points);
            Collections.reverse(asList);
            return fixedOrderedRing(asList, direction);
        } else {
            if(top>0) {
                Coordinate[] aligned = new Coordinate[points.length];
                System.arraycopy(points, top, aligned, 0, points.length-top-1);
                System.arraycopy(points, 0, aligned, points.length-top-1, top);
                aligned[aligned.length-1] = aligned[0];
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
        return (c[0].x == c[c.length-1].x) && (c[0].y == c[c.length-1].y);
    }

    public static void assertEquals(Coordinate[] c1, Coordinate[] c2) {
        Assert.assertEquals(c1.length, c2.length);

        if(isRing(c1) && isRing(c2)) {
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
        if(s1 instanceof LineString && s2 instanceof LineString) {
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
            throw new RuntimeException("equality of shape types not supported [" + s1.getClass().getName() + " and " + s2.getClass().getName() + "]");
        }
    }

    public static void assertEquals(JtsGeometry g1, JtsGeometry g2) {
        assertEquals(g1.getGeom(), g2.getGeom());
    }

    public static void assertEquals(ShapeCollection s1, ShapeCollection s2) {
        Assert.assertEquals(s1.size(), s2.size());
        for (int i = 0; i < s1.size(); i++) {
            assertEquals(s1.get(i), s2.get(i));
        }
    }

    public static void assertEquals(Shape s1, Shape s2) {
        if(s1 instanceof JtsGeometry && s2 instanceof JtsGeometry) {
            assertEquals((JtsGeometry) s1, (JtsGeometry) s2);
        } else if(s1 instanceof JtsPoint && s2 instanceof JtsPoint) {
            JtsPoint p1 = (JtsPoint) s1;
            JtsPoint p2 = (JtsPoint) s2;
            Assert.assertEquals(p1, p2);
        } else if (s1 instanceof ShapeCollection && s2 instanceof ShapeCollection) {
            assertEquals((ShapeCollection)s1, (ShapeCollection)s2);
        } else if (s1 instanceof GeoCircle && s2 instanceof GeoCircle) {
            Assert.assertEquals((GeoCircle)s1, (GeoCircle)s2);
        } else if (s1 instanceof RectangleImpl && s2 instanceof RectangleImpl) {
            Assert.assertEquals((RectangleImpl)s1, (RectangleImpl)s2);
        } else {
            //We want to know the type of the shape because we test shape equality in a special way...
            //... in particular we test that one ring is equivalent to another ring even if the points are rotated or reversed.
            throw new RuntimeException(
                    "equality of shape types not supported [" + s1.getClass().getName() + " and " + s2.getClass().getName() + "]");
        }
    }

    private static Geometry unwrap(Shape shape) {
        assertThat(shape, instanceOf(JtsGeometry.class));
        return ((JtsGeometry)shape).getGeom();
    }

    public static void assertMultiPolygon(Shape shape) {
        assert(unwrap(shape) instanceof MultiPolygon): "expected MultiPolygon but found " + unwrap(shape).getClass().getName();
    }

    public static void assertPolygon(Shape shape) {
        assert(unwrap(shape) instanceof Polygon): "expected Polygon but found " + unwrap(shape).getClass().getName();
    }

    public static void assertLineString(Shape shape) {
        assert(unwrap(shape) instanceof LineString): "expected LineString but found " + unwrap(shape).getClass().getName();
    }

    public static void assertMultiLineString(Shape shape) {
        assert(unwrap(shape) instanceof MultiLineString): "expected MultiLineString but found " + unwrap(shape).getClass().getName();
    }

    public static void assertDistance(String geohash1, String geohash2, Matcher<Double> match) {
        GeoPoint p1 = new GeoPoint(geohash1);
        GeoPoint p2 = new GeoPoint(geohash2);
        assertDistance(p1.lat(), p1.lon(), p2.lat(),p2.lon(), match);
    }

    public static void assertDistance(double lat1, double lon1, double lat2, double lon2, Matcher<Double> match) {
        assertThat(distance(lat1, lon1, lat2, lon2), match);
    }

    private static double distance(double lat1, double lon1, double lat2, double lon2) {
        return GeoDistance.ARC.calculate(lat1, lon1, lat2, lon2, DistanceUnit.DEFAULT);
    }

    public static void assertValidException(XContentParser parser, Class expectedException) {
        try {
            ShapeBuilder.parse(parser).build();
            Assert.fail("process completed successfully when " + expectedException.getName() + " expected");
        } catch (Exception e) {
            assert(e.getClass().equals(expectedException)):
                    "expected " + expectedException.getName() + " but found " + e.getClass().getName();
        }
    }
}
