/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.index.query;

import org.apache.lucene.geo.Component2D;
import org.apache.lucene.geo.LatLonGeometry;

import java.util.Objects;

/**
 * Implementation of a lucene {@link LatLonGeometry} that covers the extent of a provided H3 bin.
 *
 * Since H3 bins are polygons on the sphere, we internally represent them as polygons using the Lucene spatial3d package.
 * As such this class represents an interface between the Lucene 2D world of Component2D and the Lucene spatial3d world
 * which performs all the mathematics on the sphere (using 3d x, y, z coordinates). In particular the toComponent2D method
 * will return an object with all methods implemented using spatial3d functions, which are geometrically accurate in that they
 * use great circles to model straight lines between points.
 *
 * This makes it possible to compare H3 cells to existing indexed (and triangulated) lucene geometries, like Polygon2D for example.
 * To achieve this, we make use of a model of a hexagon that implements the org.apache.lucene.spatial3d.geom.GeoPolygon interface.
 * The methods implemented for the Component2D interface are essentially copies of the same methods in Polygon2D,
 * but with the internal logic modified in two specific directions:
 * <ul>
 *     <li>Intersections between lines make use of great circles</li>
 *     <li>The fact that this object is a simple convex polygon with no holes allows for some optimizations</li>
 * </ul>
 *
 * Note that H3 cells are simple convex polygons except where they intersect the edges of the original icosohedron triangles from
 * which the H3 model is derived. Those edge cells are more complex to work with. As such we divide the H3 cells into two groups:
 * <ol>
 *     <li>Plain hexagons that are convex polygons, and we use a simple optimized implementation of GeoRegularConvexPolygon</li>
 *     <li>Other cells revert to the spatial3d class GeoPolygon created using GeoPolygonFactory</li>
 * </ol>
 */
public abstract class H3LatLonGeometry extends LatLonGeometry {

    protected final String h3Address;

    private H3LatLonGeometry(String h3Address) {
        this.h3Address = h3Address;
    }

    @Override
    protected abstract Component2D toComponent2D();

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o instanceof H3LatLonGeometry h3) {
            return Objects.equals(h3Address, h3.h3Address);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(h3Address);
    }

    @Override
    public String toString() {
        return "H3 : " + "\"" + h3Address + "\"";
    }

    /**
     * The H3 grid itself is designed to approximate spherical coordinates. When we wish to match the results of the H3 library,
     * we should use the Spherical implementation. When we wish to display results on a planar map, we should use the
     * Planar implementation. The key difference is in the definition of edges between vertices. With Spherical, these are
     * great circles, with Planar they are lines on an equi-rectangular projection.
     */
    public static class Spherical extends H3LatLonGeometry {
        public Spherical(String h3Address) {
            super(h3Address);
        }

        @Override
        protected Component2D toComponent2D() {
            return new H3Polygon2D.Spherical.Unscaled(h3Address);
        }

        /**
         * When there is need to create an H3 cell scaled to a new size, this class provides that capability.
         * For example, since child cells are not fully contained in parent cells, we can create a fake parent
         * cell scaled to contain all child cells. This would be of use in depth-first searches of the parent-child
         * tree.
         */
        protected static class Scaled extends Spherical {
            private final double scaleFactor;

            public Scaled(String h3Address, double scaleFactor) {
                super(h3Address);
                this.scaleFactor = scaleFactor;
            }

            @Override
            protected Component2D toComponent2D() {
                return new H3Polygon2D.Spherical.Scaled(h3Address, scaleFactor);
            }
        }
    }

    /**
     * The H3 grid itself is designed to approximate spherical coordinates. When we wish to match the results of the H3 library,
     * we should use the Spherical implementation. When we wish to display results on a planar map, we should use the
     * Planar implementation. The key difference is in the definition of edges between vertices. With Spherical, these are
     * great circles, with Planar they are lines on an equi-rectangular projection.
     */
    public static class Planar extends H3LatLonGeometry {
        public Planar(String h3Address) {
            super(h3Address);
        }

        @Override
        protected Component2D toComponent2D() {
            return new H3Polygon2D.Planar.Unscaled(h3Address);
        }

        /**
         * When there is need to create an H3 cell scaled to a new size, this class provides that capability.
         * For example, since child cells are not fully contained in parent cells, we can create a fake parent
         * cell scaled to contain all child cells. This would be of use in depth-first searches of the parent-child
         * tree.
         */
        protected static class Scaled extends Planar {
            private final double scaleFactor;

            public Scaled(String h3Address, double scaleFactor) {
                super(h3Address);
                this.scaleFactor = scaleFactor;
            }

            @Override
            protected Component2D toComponent2D() {
                return new H3Polygon2D.Planar.Scaled(h3Address, scaleFactor);
            }
        }
    }
}
