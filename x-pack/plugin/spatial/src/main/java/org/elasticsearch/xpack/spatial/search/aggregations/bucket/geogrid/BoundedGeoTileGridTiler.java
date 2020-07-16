/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.MultiGeoShapeValues;

public class BoundedGeoTileGridTiler extends GeoTileGridTiler {
    private final double boundsTop;
    private final double boundsBottom;
    private final double boundsWestLeft;
    private final double boundsWestRight;
    private final double boundsEastLeft;
    private final double boundsEastRight;
    private final boolean crossesDateline;

    public BoundedGeoTileGridTiler(GeoBoundingBox geoBoundingBox) {
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

    public int advancePointValue(long[] values, double x, double y, int precision, int valuesIdx) {
        long hash = encode(x, y, precision);
        if (cellIntersectsGeoBoundingBox(GeoTileUtils.toBoundingBox(hash))) {
            values[valuesIdx] = hash;
            return valuesIdx + 1;
        }
        return valuesIdx;
    }

    boolean cellIntersectsGeoBoundingBox(Rectangle rectangle) {
        return (boundsTop >= rectangle.getMinY() && boundsBottom <= rectangle.getMaxY()
            && (boundsEastLeft <= rectangle.getMaxX() && boundsEastRight >= rectangle.getMinX()
            || (crossesDateline && boundsWestLeft <= rectangle.getMaxX() && boundsWestRight >= rectangle.getMinX())));
    }

    @Override
    public GeoRelation relateTile(MultiGeoShapeValues.GeoShapeValue geoValue, int xTile, int yTile, int precision) {
        Rectangle rectangle = GeoTileUtils.toBoundingBox(xTile, yTile, precision);
        if (cellIntersectsGeoBoundingBox(rectangle)) {
            return geoValue.relate(rectangle);
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    @Override
    protected int setValue(GeoShapeCellValues docValues, MultiGeoShapeValues.GeoShapeValue geoValue, int xTile, int yTile, int precision) {
        if (cellIntersectsGeoBoundingBox(GeoTileUtils.toBoundingBox(xTile, yTile, precision))) {
            docValues.resizeCell(1);
            docValues.add(0, GeoTileUtils.longEncodeTiles(precision, xTile, yTile));
            return 1;
        }
        return 0;
    }

    @Override
    protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex,
                                                 int targetPrecision) {
        zTile++;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                int nextX = 2 * xTile + i;
                int nextY = 2 * yTile + j;
                if (zTile == targetPrecision) {
                    if (cellIntersectsGeoBoundingBox(GeoTileUtils.toBoundingBox(nextX, nextY, zTile))) {
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    }
                } else {
                    valuesIndex = setValuesForFullyContainedTile(nextX, nextY, zTile, values, valuesIndex, targetPrecision);
                }
            }
        }
        return valuesIndex;
    }
}
