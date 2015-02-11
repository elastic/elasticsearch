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

package org.elasticsearch.test.geo;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.spatial4j.core.context.jts.JtsSpatialContext;
import com.spatial4j.core.distance.DistanceUtils;
import com.spatial4j.core.exception.InvalidShapeException;
import com.spatial4j.core.shape.Point;
import com.spatial4j.core.shape.Rectangle;
import com.spatial4j.core.shape.impl.Range;
import com.vividsolutions.jts.algorithm.ConvexHull;
import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Geometry;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.geo.builders.BaseLineStringBuilder;
import org.elasticsearch.common.geo.builders.GeometryCollectionBuilder;
import org.elasticsearch.common.geo.builders.LineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiLineStringBuilder;
import org.elasticsearch.common.geo.builders.MultiPointBuilder;
import org.elasticsearch.common.geo.builders.PointBuilder;
import org.elasticsearch.common.geo.builders.PointCollection;
import org.elasticsearch.common.geo.builders.PolygonBuilder;
import org.elasticsearch.common.geo.builders.ShapeBuilder;

import static com.spatial4j.core.shape.SpatialRelation.CONTAINS;

/**
 * Random geoshape generation utilities for randomized Geospatial testing
 */
public class RandomShapeGenerator extends RandomizedTest {

    protected static JtsSpatialContext ctx = ShapeBuilder.SPATIAL_CONTEXT;
    protected static final double xDIVISIBLE = 2;
    protected static boolean ST_VALIDATE = true;

    public static enum ShapeType {
        POINT, MULTIPOINT, LINESTRING, MULTILINESTRING, POLYGON;
        private static final ShapeType[] types = values();

        public static ShapeType randomType() {
            return types[randomIntBetween(0, types.length-1)];
        }
    }

    public static ShapeBuilder createShapeNear(Point nearPoint) throws InvalidShapeException {
        return createShape(nearPoint, null, null);
    }

    public static ShapeBuilder createShapeNear(Point nearPoint, ShapeType st) throws InvalidShapeException {
        return createShape(nearPoint, null, st);
    }

    public static ShapeBuilder createShapeWithin(Rectangle bbox) throws InvalidShapeException {
        return createShape(null, bbox, null);
    }

    public static ShapeBuilder createShapeWithin(Rectangle bbox, ShapeType st) throws InvalidShapeException {
        return createShape(null, bbox, st);
    }

    public static GeometryCollectionBuilder createGeometryCollection() throws InvalidShapeException {
        return createGeometryCollection(null, null, 0);
    }

    public static GeometryCollectionBuilder createGeometryCollectionNear(Point nearPoint) throws InvalidShapeException {
        return createGeometryCollection(nearPoint, null, 0);
    }

    public static GeometryCollectionBuilder createGeometryCollectionNear(Point nearPoint, int size) throws InvalidShapeException {
        return createGeometryCollection(nearPoint, null, size);
    }

    public static GeometryCollectionBuilder createGeometryCollectionWithin(Rectangle within) throws InvalidShapeException {
        return createGeometryCollection(null, within, 0);
    }

    public static GeometryCollectionBuilder createGeometryCollectionWithin(Rectangle within, int size) throws InvalidShapeException {
        return createGeometryCollection(null, within, size);
    }

    protected static GeometryCollectionBuilder createGeometryCollection(Point nearPoint, Rectangle bounds, int numGeometries) throws
            InvalidShapeException {
        if (numGeometries <= 0) {
            // cap geometry collection at 4 shapes (to save test time)
            numGeometries = randomIntBetween(2, 5);
        }

        if (nearPoint == null) {
            nearPoint = xRandomPoint();
        }

        if (bounds == null) {
            bounds = xRandomRectangle(nearPoint);
        }

        GeometryCollectionBuilder gcb = new GeometryCollectionBuilder();
        for (int i=0; i<numGeometries;) {
            ShapeBuilder builder = createShapeWithin(bounds);
            // due to world wrapping, and the possibility for ambiguous polygons, the random shape generation could bail with
            // a null shape. We catch that situation here, and only increment the counter when a valid shape is returned.
            // Not the most efficient but its the lesser of the evil alternatives
            if (builder != null) {
                gcb.shape(builder);
                ++i;
            }
        }
        return gcb;
    }

    private static ShapeBuilder createShape(Point nearPoint, Rectangle within, ShapeType st) throws InvalidShapeException {
        return createShape(nearPoint, within, st, ST_VALIDATE);
    }

    /**
     * Creates a random shape useful for randomized testing, NOTE: exercise caution when using this to build random GeometryCollections
     * as creating a large random number of random shapes can result in massive resource consumption
     * see: {@link org.elasticsearch.search.geo.GeoShapeIntegrationTests#testShapeFilterWithRandomGeoCollection}
     *
     * The following options are included
     * @param nearPoint Create a shape near a provided point
     * @param within Create a shape within the provided rectangle (note: if not null this will override the provided point)
     * @param st Create a random shape of the provided type
     * @return the ShapeBuilder for a random shape
     */
    private static ShapeBuilder createShape(Point nearPoint, Rectangle within, ShapeType st, boolean validate) throws InvalidShapeException {

        if (st == null) {
            st = ShapeType.randomType();
        }

        if (within == null) {
            within = xRandomRectangle(nearPoint);
        }

        // NOTE: multipolygon not yet supported. Overlapping polygons are invalid so randomization
        // requires an approach to avoid overlaps. This could be approached by creating polygons
        // inside non overlapping bounding rectangles
        switch (st) {
            case POINT:
                Point p = xRandomPointIn(within);
                PointBuilder pb = new PointBuilder().coordinate(new Coordinate(p.getX(), p.getY(), Double.NaN));
                return pb;
            case MULTIPOINT:
            case LINESTRING:
                // for random testing having a maximum number of 10 points for a line string is more than sufficient
                // if this number gets out of hand, the number of self intersections for a linestring can become
                // (n^2-n)/2 and computing the relation intersection matrix will become NP-Hard
                int numPoints = randomIntBetween(3, 10);
                PointCollection pcb = (st == ShapeType.MULTIPOINT) ? new MultiPointBuilder() : new LineStringBuilder();
                for (int i=0; i<numPoints; ++i) {
                    p = xRandomPointIn(within);
                    pcb.point(p.getX(), p.getY());
                }
                return pcb;
            case MULTILINESTRING:
                MultiLineStringBuilder mlsb = new MultiLineStringBuilder();
                for (int i=0; i<randomIntBetween(1, 10); ++i) {
                    mlsb.linestring((BaseLineStringBuilder) createShape(nearPoint, within, ShapeType.LINESTRING, false));
                }
                return mlsb;
            case POLYGON:
                numPoints = randomIntBetween(5, 25);
                Coordinate[] coordinates = new Coordinate[numPoints];
                for (int i=0; i<numPoints; ++i) {
                    p = (Point) createShape(nearPoint, within, ShapeType.POINT, false).build();
                    coordinates[i] = new Coordinate(p.getX(), p.getY());
                }
                // random point order or random linestrings can lead to invalid self-crossing polygons,
                // compute the convex hull for a set of points to ensure polygon does not self cross
                Geometry shell = new ConvexHull(coordinates, ctx.getGeometryFactory()).getConvexHull();
                Coordinate[] shellCoords = shell.getCoordinates();
                // if points are in a line the convex hull will be 2 points which will also lead to an invalid polygon
                // when all else fails, use the bounding box as the polygon
                if (shellCoords.length < 3) {
                    shellCoords = new Coordinate[4];
                    shellCoords[0] = new Coordinate(within.getMinX(), within.getMinY());
                    shellCoords[1] = new Coordinate(within.getMinX(), within.getMaxY());
                    shellCoords[2] = new Coordinate(within.getMaxX(), within.getMaxY());
                    shellCoords[3] = new Coordinate(within.getMaxX(), within.getMinY());
                }
                PolygonBuilder pgb = new PolygonBuilder().points(shellCoords).close();
                if (validate) {
                    // This test framework builds semi-random geometry (in the sense that points are not truly random due to spatial
                    // auto-correlation) As a result of the semi-random nature of the geometry, one can not predict the orientation
                    // intent for ambiguous polygons. Therefore, an invalid oriented dateline crossing polygon could be built.
                    // The validate flag will check for these possibilities and bail if an incorrect geometry is created
                    try {
                        pgb.build();
                    } catch (InvalidShapeException e) {
                        return null;
                    }
                }
                return pgb;
            default:
                throw new ElasticsearchException("Unable to create shape of type [" + st + "]");
        }
    }

    protected static Point xRandomPoint() {
        return xRandomPointIn(ctx.getWorldBounds());
    }

    protected static Point xRandomPointIn(Rectangle r) {
        double x = r.getMinX() + randomDouble()*r.getWidth();
        double y = r.getMinY() + randomDouble()*r.getHeight();
        x = xNormX(x);
        y = xNormY(y);
        Point p = ctx.makePoint(x,y);
        assertEquals(CONTAINS,r.relate(p));
        return p;
    }

    protected static Rectangle xRandomRectangle(Point nearP) {
        Rectangle bounds = ctx.getWorldBounds();
        if (nearP == null)
            nearP = xRandomPointIn(bounds);

        Range xRange = xRandomRange(rarely() ? 0 : nearP.getX(), Range.xRange(bounds, ctx));
        Range yRange = xRandomRange(rarely() ? 0 : nearP.getY(), Range.yRange(bounds, ctx));

        return xMakeNormRect(
                xDivisible(xRange.getMin()*10e3)/10e3,
                xDivisible(xRange.getMax()*10e3)/10e3,
                xDivisible(yRange.getMin()*10e3)/10e3,
                xDivisible(yRange.getMax()*10e3)/10e3);
    }

    private static Range xRandomRange(double near, Range bounds) {
        double mid = near + randomGaussian() * bounds.getWidth() / 6;
        double width = Math.abs(randomGaussian()) * bounds.getWidth() / 6;//1/3rd
        return new Range(mid - width / 2, mid + width / 2);
    }

    private static double xDivisible(double v, double divisible) {
        return (int) (Math.round(v / divisible) * divisible);
    }

    private static double xDivisible(double v) {
        return xDivisible(v, xDIVISIBLE);
    }

    protected static Rectangle xMakeNormRect(double minX, double maxX, double minY, double maxY) {
        minX = DistanceUtils.normLonDEG(minX);
        maxX = DistanceUtils.normLonDEG(maxX);

        if (maxX < minX) {
            double t = minX;
            minX = maxX;
            maxX = t;
        }

        double minWorldY = ctx.getWorldBounds().getMinY();
        double maxWorldY = ctx.getWorldBounds().getMaxY();
        if (minY < minWorldY || minY > maxWorldY) {
            minY = DistanceUtils.normLatDEG(minY);
        }
        if (maxY < minWorldY || maxY > maxWorldY) {
            maxY = DistanceUtils.normLatDEG(maxY);
        }
        if (maxY < minY) {
            double t = minY;
            minY = maxY;
            maxY = t;
        }
        return ctx.makeRectangle(minX, maxX, minY, maxY);
    }

    protected static double xNormX(double x) {
        return ctx.isGeo() ? DistanceUtils.normLonDEG(x) : x;
    }

    protected static double xNormY(double y) {
        return ctx.isGeo() ? DistanceUtils.normLatDEG(y) : y;
    }
}
