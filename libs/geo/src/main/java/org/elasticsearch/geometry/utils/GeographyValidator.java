/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.geometry.utils;

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

/**
 * Validator that checks that lats are between -90 and +90 and lons are between -180 and +180 and altitude is present only if
 * ignoreZValue is set to true
 */
public class GeographyValidator implements GeometryValidator {

    private static final GeometryValidator TRUE = new GeographyValidator(true);
    private static final GeometryValidator FALSE = new GeographyValidator(false);

    /**
     * Minimum longitude value.
     */
    private static final double MIN_LON_INCL = -180.0D;

    /**
     * Maximum longitude value.
     */
    private static final double MAX_LON_INCL = 180.0D;

    /**
     * Minimum latitude value.
     */
    private static final double MIN_LAT_INCL = -90.0D;

    /**
     * Maximum latitude value.
     */
    private static final double MAX_LAT_INCL = 90.0D;

    private final boolean ignoreZValue;

    protected GeographyValidator(boolean ignoreZValue) {
        this.ignoreZValue = ignoreZValue;
    }

    public static GeometryValidator instance(boolean ignoreZValue) {
        return ignoreZValue ? TRUE : FALSE;
    }

    /**
     * validates latitude value is within standard +/-90 coordinate bounds
     */
    protected void checkLatitude(double latitude) {
        if (Double.isNaN(latitude) || latitude < MIN_LAT_INCL || latitude > MAX_LAT_INCL) {
            throw new IllegalArgumentException(
                "invalid latitude " + latitude + "; must be between " + MIN_LAT_INCL + " and " + MAX_LAT_INCL);
        }
    }

    /**
     * validates longitude value is within standard +/-180 coordinate bounds
     */
    protected void checkLongitude(double longitude) {
        if (Double.isNaN(longitude) || longitude < MIN_LON_INCL || longitude > MAX_LON_INCL) {
            throw new IllegalArgumentException(
                "invalid longitude " + longitude + "; must be between " + MIN_LON_INCL + " and " + MAX_LON_INCL);
        }
    }

    protected void checkAltitude(double zValue) {
        if (ignoreZValue == false && Double.isNaN(zValue) == false) {
            throw new IllegalArgumentException("found Z value [" + zValue + "] but [ignore_z_value] "
                + "parameter is [" + ignoreZValue + "]");
        }
    }

    @Override
    public void validate(Geometry geometry) {
        geometry.visit(new GeometryVisitor<Void, RuntimeException>() {

            @Override
            public Void visit(Circle circle) throws RuntimeException {
                checkLatitude(circle.getY());
                checkLongitude(circle.getX());
                checkAltitude(circle.getZ());
                return null;
            }

            @Override
            public Void visit(GeometryCollection<?> collection) throws RuntimeException {
                for (Geometry g : collection) {
                    g.visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Line line) throws RuntimeException {
                for (int i = 0; i < line.length(); i++) {
                    checkLatitude(line.getY(i));
                    checkLongitude(line.getX(i));
                    checkAltitude(line.getZ(i));
                }
                return null;
            }

            @Override
            public Void visit(LinearRing ring) throws RuntimeException {
                for (int i = 0; i < ring.length(); i++) {
                    checkLatitude(ring.getY(i));
                    checkLongitude(ring.getX(i));
                    checkAltitude(ring.getZ(i));
                }
                return null;
            }

            @Override
            public Void visit(MultiLine multiLine) throws RuntimeException {
                return visit((GeometryCollection<?>) multiLine);
            }

            @Override
            public Void visit(MultiPoint multiPoint) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPoint);
            }

            @Override
            public Void visit(MultiPolygon multiPolygon) throws RuntimeException {
                return visit((GeometryCollection<?>) multiPolygon);
            }

            @Override
            public Void visit(Point point) throws RuntimeException {
                checkLatitude(point.getY());
                checkLongitude(point.getX());
                checkAltitude(point.getZ());
                return null;
            }

            @Override
            public Void visit(Polygon polygon) throws RuntimeException {
                polygon.getPolygon().visit(this);
                for (int i = 0; i < polygon.getNumberOfHoles(); i++) {
                    polygon.getHole(i).visit(this);
                }
                return null;
            }

            @Override
            public Void visit(Rectangle rectangle) throws RuntimeException {
                checkLatitude(rectangle.getMinY());
                checkLatitude(rectangle.getMaxY());
                checkLongitude(rectangle.getMinX());
                checkLongitude(rectangle.getMaxX());
                checkAltitude(rectangle.getMinZ());
                checkAltitude(rectangle.getMaxZ());
                return null;
            }
        });
    }
}
