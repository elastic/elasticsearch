/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.geom;

import org.apache.lucene.spatial3d.geom.GeoArea;
import org.apache.lucene.spatial3d.geom.GeoAreaShape;
import org.apache.lucene.spatial3d.geom.GeoBaseMembershipShape;
import org.apache.lucene.spatial3d.geom.GeoPoint;
import org.apache.lucene.spatial3d.geom.GeoPolygon;
import org.apache.lucene.spatial3d.geom.GeoShape;
import org.apache.lucene.spatial3d.geom.PlanetModel;

/**
 * This is a copy of org.apache.lucene.spatial3d.geom.GeoBaseAreaShape, modified to be a drop-in replacement for GeoBasePolygon
 * both of which are currently package-private and therefor not available to be used/extended by ES code.
 * We need to extend GeoBasePolygon for use in GeoRegularConvexPolygon for use in H3 aggregations over shapes.
 *
 * We have two ways forward to not need this copy of Lucene code:
 * <ul>
 *     <li>Either we make Lucene's GeoBasePolygon (and BaseBaseAreaShape) public</li>
 *     <li>Or we move GeoRegularConvexPolygon into Lucene codebase</li>
 * </ul>
 * Or we do both of the above.
 */
abstract class LuceneGeoBaseAreaShape extends GeoBaseMembershipShape implements GeoAreaShape, GeoPolygon {

    /**
     * Constructor.
     *
     * @param planetModel is the planet model to use.
     */
    LuceneGeoBaseAreaShape(final PlanetModel planetModel) {
        super(planetModel);
    }

    /** All edgepoints inside shape */
    protected static final int ALL_INSIDE = 0;
    /** Some edgepoints inside shape */
    protected static final int SOME_INSIDE = 1;
    /** No edgepoints inside shape */
    protected static final int NONE_INSIDE = 2;

    /**
     * Determine the relationship between the GeoAreShape and the shape's edgepoints.
     *
     * @param geoShape is the shape.
     * @return the relationship.
     */
    protected int isShapeInsideGeoAreaShape(final GeoShape geoShape) {
        boolean foundOutside = false;
        boolean foundInside = false;
        for (GeoPoint p : geoShape.getEdgePoints()) {
            if (isWithin(p)) {
                foundInside = true;
            } else {
                foundOutside = true;
            }
            if (foundInside && foundOutside) {
                return SOME_INSIDE;
            }
        }
        return determineRelationship(foundOutside, foundInside);
    }

    /**
     * Determine the relationship between the GeoAreaShape's edgepoints and the provided shape.
     *
     * @param geoshape is the shape.
     * @return the relationship.
     */
    protected int isGeoAreaShapeInsideShape(final GeoShape geoshape) {
        boolean foundOutside = false;
        boolean foundInside = false;
        for (GeoPoint p : getEdgePoints()) {
            if (geoshape.isWithin(p)) {
                foundInside = true;
            } else {
                foundOutside = true;
            }
            if (foundInside && foundOutside) {
                return SOME_INSIDE;
            }
        }
        return determineRelationship(foundOutside, foundInside);
    }

    private int determineRelationship(boolean foundOutside, boolean foundInside) {
        if (foundInside == false && foundOutside == false) {
            return NONE_INSIDE;
        }
        if (foundInside && foundOutside == false) {
            return ALL_INSIDE;
        }
        if (foundOutside && foundInside == false) {
            return NONE_INSIDE;
        }
        return SOME_INSIDE;
    }

    @Override
    public int getRelationship(GeoShape geoShape) {
        if (geoShape.getPlanetModel().equals(planetModel) == false) {
            throw new IllegalArgumentException("Cannot relate shapes with different planet models.");
        }
        final int insideGeoAreaShape = isShapeInsideGeoAreaShape(geoShape);
        if (insideGeoAreaShape == SOME_INSIDE) {
            return GeoArea.OVERLAPS;
        }

        final int insideShape = isGeoAreaShapeInsideShape(geoShape);
        if (insideShape == SOME_INSIDE) {
            return GeoArea.OVERLAPS;
        }

        if (insideGeoAreaShape == ALL_INSIDE && insideShape == ALL_INSIDE) {
            return GeoArea.OVERLAPS;
        }

        if (intersects(geoShape)) {
            return GeoArea.OVERLAPS;
        }

        if (insideGeoAreaShape == ALL_INSIDE) {
            return GeoArea.WITHIN;
        }

        if (insideShape == ALL_INSIDE) {
            return GeoArea.CONTAINS;
        }

        return GeoArea.DISJOINT;
    }
}
