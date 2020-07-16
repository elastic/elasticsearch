/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.geometry.utils.Geohash;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;

public class BoundedGeoHashGridTiler extends GeoHashGridTiler {
    private final double boundsTop;
    private final double boundsBottom;
    private final double boundsWestLeft;
    private final double boundsWestRight;
    private final double boundsEastLeft;
    private final double boundsEastRight;
    private final boolean crossesDateline;

    public BoundedGeoHashGridTiler(GeoBoundingBox geoBoundingBox) {
        // split geoBoundingBox into west and east boxes
        boundsTop = geoBoundingBox.top();
        boundsBottom = geoBoundingBox.bottom();
        if (geoBoundingBox.right() < geoBoundingBox.left()) {
            boundsWestLeft = -180;
            boundsWestRight = geoBoundingBox.right();
            boundsEastLeft = geoBoundingBox.left();
            boundsEastRight = 180;
            crossesDateline = true;
        } else { // only set east bounds
            boundsEastLeft = geoBoundingBox.left();
            boundsEastRight = geoBoundingBox.right();
            boundsWestLeft = 0;
            boundsWestRight = 0;
            crossesDateline = false;
        }
    }

    boolean cellIntersectsGeoBoundingBox(Rectangle rectangle) {
        return (boundsTop >= rectangle.getMinY() && boundsBottom <= rectangle.getMaxY()
            && (boundsEastLeft <= rectangle.getMaxX() && boundsEastRight >= rectangle.getMinX()
            || (crossesDateline && boundsWestLeft <= rectangle.getMaxX() && boundsWestRight >= rectangle.getMinX())));
    }

    @Override
    protected int setValue(GeoShapeCellValues docValues, MultiGeoShapeValues.GeoShapeValue geoValue, MultiGeoShapeValues.BoundingBox bounds,
                           int precision) {
        String hash = Geohash.stringEncode(bounds.minX(), bounds.minY(), precision);
        GeoRelation relation = relateTile(geoValue, hash);
        if (relation != GeoRelation.QUERY_DISJOINT) {
            docValues.resizeCell(1);
            docValues.add(0, Geohash.longEncode(hash));
            return 1;
        }
        return 0;
    }

    @Override
    protected GeoRelation relateTile(MultiGeoShapeValues.GeoShapeValue geoValue, String hash) {
        Rectangle rectangle = Geohash.toBoundingBox(hash);
        if (cellIntersectsGeoBoundingBox(rectangle)) {
            return geoValue.relate(rectangle);
        } else {
            return GeoRelation.QUERY_DISJOINT;
        }
    }

    @Override
    protected int setValuesForFullyContainedTile(String hash, GeoShapeCellValues values, int valuesIndex, int targetPrecision) {
        String[] hashes = Geohash.getSubGeohashes(hash);
        for (int i = 0; i < hashes.length; i++) {
            if (hashes[i].length() == targetPrecision ) {
                if (cellIntersectsGeoBoundingBox(Geohash.toBoundingBox(hashes[i]))) {
                    values.add(valuesIndex++, Geohash.longEncode(hashes[i]));
                }
            } else {
                valuesIndex = setValuesForFullyContainedTile(hashes[i], values, valuesIndex, targetPrecision);
            }
        }
        return valuesIndex;
    }
}
