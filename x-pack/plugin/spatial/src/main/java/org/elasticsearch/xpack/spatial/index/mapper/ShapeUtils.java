/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.spatial.index.mapper;

import org.apache.lucene.geo.XYLine;
import org.apache.lucene.geo.XYPoint;
import org.apache.lucene.geo.XYPolygon;
import org.apache.lucene.geo.XYRectangle;
import org.elasticsearch.geometry.Line;
import org.elasticsearch.geometry.LinearRing;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.Polygon;
import org.elasticsearch.geometry.Rectangle;

/**
 * Utility class that transforms Elasticsearch geometry objects to to Lucene XYGeometries
 */
class ShapeUtils {

   static XYPolygon toLucenePolygon(Polygon polygon) {
        XYPolygon[] holes = new XYPolygon[polygon.getNumberOfHoles()];
        for(int i = 0; i<holes.length; i++) {
            LinearRing ring = polygon.getHole(i);
            holes[i] = new XYPolygon(doubleArrayToFloatArray(ring.getX()),
                                     doubleArrayToFloatArray(ring.getY()));
        }
        LinearRing ring = polygon.getPolygon();
        return new XYPolygon(doubleArrayToFloatArray(ring.getX()),
                             doubleArrayToFloatArray(ring.getY()),
                             holes);
    }

    static XYPolygon toLucenePolygon(Rectangle r) {
       return new XYPolygon(
           new float[]{(float)r.getMinX(), (float)r.getMaxX(), (float)r.getMaxX(), (float)r.getMinX(), (float)r.getMinX()},
           new float[]{(float)r.getMinY(), (float)r.getMinY(), (float)r.getMaxY(), (float)r.getMaxY(), (float)r.getMinY()});
    }

    static XYRectangle toLuceneRectangle(Rectangle r) {
        return new XYRectangle((float) r.getMinX(), (float) r.getMaxX(), (float) r.getMinY(), (float) r.getMaxY());
    }

    static XYPoint toLucenePoint(Point point) {
        return new XYPoint((float) point.getX(), (float) point.getY());
    }

    static XYLine toLuceneLine(Line line) {
        return new XYLine(doubleArrayToFloatArray(line.getX()),
                          doubleArrayToFloatArray(line.getY()));
    }

    private static float[] doubleArrayToFloatArray(double[] array) {
        float[] result = new float[array.length];
        for (int i = 0; i < array.length; ++i) {
            result[i] = (float) array[i];
        }
        return result;
    }

    private ShapeUtils() {
    }
}
