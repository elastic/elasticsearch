/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.spatial;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;
import org.apache.lucene.geo.Rectangle;
import org.apache.lucene.geo.XYGeometry;

import java.util.ArrayList;
import java.util.List;

/**
 * This utilities class provides access to protected methods in Lucene using alternative APIs.
 * For example, the 'create' method returns the original Component2D array, instead of a Component2D containing
 * a component tree of potentially multiple components. This is particularly useful for algorithms that need to
 * operate on each component individually.
 */
public class LuceneComponent2DUtils {
    /**
     * This method is based on LatLonGeometry.create, but returns an array of Component2D objects for multi-component geometries.
     */
    public static Component2D[] createLatLonComponents(LatLonGeometry... latLonGeometries) {
        if (latLonGeometries == null) {
            throw new IllegalArgumentException("geometries must not be null");
        } else if (latLonGeometries.length == 0) {
            throw new IllegalArgumentException("geometries must not be empty");
        } else {
            final List<Component2D> components = new ArrayList<>(latLonGeometries.length);

            for (int i = 0; i < latLonGeometries.length; ++i) {
                if (latLonGeometries[i] == null) {
                    throw new IllegalArgumentException("geometries[" + i + "] must not be null");
                }

                if (latLonGeometries[i] instanceof Rectangle rectangle && rectangle.crossesDateline()) {
                    addRectangle(components, rectangle);
                } else {
                    components.add(LatLonGeometry.create(latLonGeometries[i]));
                }
            }

            return components.toArray(new Component2D[0]);
        }
    }

    private static void addRectangle(List<Component2D> components, Rectangle rectangle) {
        double minLongitude = rectangle.minLon;
        boolean crossesDateline = rectangle.minLon > rectangle.maxLon;
        if (minLongitude == 180.0 && crossesDateline) {
            minLongitude = -180.0;
            crossesDateline = false;
        }
        if (crossesDateline) {
            Rectangle left = new Rectangle(rectangle.minLat, rectangle.maxLat, -180.0, rectangle.maxLon);
            Rectangle right = new Rectangle(rectangle.minLat, rectangle.maxLat, minLongitude, 180.0);
            components.add(LatLonGeometry.create(left));
            components.add(LatLonGeometry.create(right));
        } else {
            components.add(LatLonGeometry.create(rectangle));
        }
    }

    /**
     * This method is based on XYGeometry.create, but returns an array of Component2D objects for multi-component geometries.
     */
    public static Component2D[] createXYComponents(XYGeometry... xyGeometries) {
        if (xyGeometries == null) {
            throw new IllegalArgumentException("geometries must not be null");
        } else if (xyGeometries.length == 0) {
            throw new IllegalArgumentException("geometries must not be empty");
        } else {
            Component2D[] components = new Component2D[xyGeometries.length];

            for (int i = 0; i < xyGeometries.length; ++i) {
                if (xyGeometries[i] == null) {
                    throw new IllegalArgumentException("geometries[" + i + "] must not be null");
                }

                components[i] = XYGeometry.create(xyGeometries[i]);
            }

            return components;
        }
    }
}
