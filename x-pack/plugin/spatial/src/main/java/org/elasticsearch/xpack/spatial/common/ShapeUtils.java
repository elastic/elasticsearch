/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.common;

import org.elasticsearch.geometry.Circle;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

/**
 * Utility class that transforms Elasticsearch geometry objects to the Lucene representation
 */
public class ShapeUtils {
    // no instance:
    private ShapeUtils() {
    }

    public static org.apache.lucene.geo.XYPolygon toLuceneXYPolygon(Polygon polygon) {
        org.apache.lucene.geo.XYPolygon[] holes = new org.apache.lucene.geo.XYPolygon[polygon.getNumberOfHoles()];
        for(int i = 0; i<holes.length; i++) {
            holes[i] = new org.apache.lucene.geo.XYPolygon(
                doubleArrayToFloatArray(polygon.getHole(i).getX()),
                doubleArrayToFloatArray(polygon.getHole(i).getY()));
        }
        return new org.apache.lucene.geo.XYPolygon(
            doubleArrayToFloatArray(polygon.getPolygon().getX()),
            doubleArrayToFloatArray(polygon.getPolygon().getY()), holes);
    }

    public static org.apache.lucene.geo.XYPolygon toLuceneXYPolygon(Rectangle r) {
        return new org.apache.lucene.geo.XYPolygon(
            new float[]{(float) r.getMinX(), (float) r.getMaxX(), (float) r.getMaxX(), (float) r.getMinX(), (float) r.getMinX()},
            new float[]{(float) r.getMinY(), (float) r.getMinY(), (float) r.getMaxY(), (float) r.getMaxY(), (float) r.getMinY()});
    }

    public static org.apache.lucene.geo.XYRectangle toLuceneXYRectangle(Rectangle r) {
        return new org.apache.lucene.geo.XYRectangle((float) r.getMinX(), (float) r.getMaxX(),
                                                     (float) r.getMinY(), (float) r.getMaxY());
    }

    public static org.apache.lucene.geo.XYPoint toLuceneXYPoint(Point point) {
        return new org.apache.lucene.geo.XYPoint((float) point.getX(), (float) point.getY());
    }

    public static org.apache.lucene.geo.XYLine toLuceneXYLine(Line line) {
        return new org.apache.lucene.geo.XYLine(
            doubleArrayToFloatArray(line.getX()),
            doubleArrayToFloatArray(line.getY()));
    }

    public static org.apache.lucene.geo.XYCircle toLuceneXYCircle(Circle circle) {
        return new org.apache.lucene.geo.XYCircle((float) circle.getX(), (float) circle.getY(), (float) circle.getRadiusMeters());
    }

    private static float[] doubleArrayToFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = (float) array[i];
        }
        return result;
    }

}
