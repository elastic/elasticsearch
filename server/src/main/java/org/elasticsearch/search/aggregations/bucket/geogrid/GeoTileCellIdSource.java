/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.search.aggregations.bucket.geogrid;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedNumericDocValues;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.index.fielddata.MultiGeoPointValues;
import org.elasticsearch.index.fielddata.SortedBinaryDocValues;
import org.elasticsearch.index.fielddata.SortedNumericDoubleValues;
import org.elasticsearch.search.aggregations.support.ValuesSource;

/**
 * Class to help convert {@link MultiGeoPointValues}
 * to GeoTile bucketing.
 */
public class GeoTileCellIdSource extends ValuesSource.Numeric {
    private final GeoPoint valuesSource;
    private final int precision;
    private final GeoBoundingBox geoBoundingBox;

    public GeoTileCellIdSource(GeoPoint valuesSource, int precision, GeoBoundingBox geoBoundingBox) {
        this.valuesSource = valuesSource;
        this.precision = precision;
        this.geoBoundingBox = geoBoundingBox;
    }

    public int precision() {
        return precision;
    }

    @Override
    public boolean isFloatingPoint() {
        return false;
    }

    @Override
    public SortedNumericDocValues longValues(LeafReaderContext ctx) {
        return geoBoundingBox.isUnbounded()
            ? new UnboundedCellValues(valuesSource.geoPointValues(ctx), precision)
            : new BoundedCellValues(valuesSource.geoPointValues(ctx), precision, geoBoundingBox);
    }

    @Override
    public SortedNumericDoubleValues doubleValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    @Override
    public SortedBinaryDocValues bytesValues(LeafReaderContext ctx) {
        throw new UnsupportedOperationException();
    }

    private static class UnboundedCellValues extends CellValues {

        UnboundedCellValues(MultiGeoPointValues geoValues, int precision) {
            super(geoValues, precision);
        }

        @Override
        protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
            values[valuesIdx] = GeoTileUtils.longEncode(target.getLon(), target.getLat(), precision);
            return valuesIdx + 1;
        }
    }

    private static class BoundedCellValues extends CellValues {

        private final boolean crossesDateline;
        private final long tiles;
        private final int minX, maxX, minY, maxY;

        protected BoundedCellValues(MultiGeoPointValues geoValues, int precision, GeoBoundingBox bbox) {
            super(geoValues, precision);
            this.crossesDateline = bbox.right() < bbox.left();
            this.tiles = 1L << precision;
            // compute minX, minY
            final int minX = GeoTileUtils.getXTile(bbox.left(), this.tiles);
            final int minY = GeoTileUtils.getYTile(bbox.top(), this.tiles);
            final Rectangle minTile = GeoTileUtils.toBoundingBox(minX, minY, precision);
            // touching tiles are excluded, they need to share at least one interior point
            this.minX = minTile.getMaxX() == bbox.left() ? minX + 1 : minX;
            this.minY = minTile.getMinY() == bbox.top() ? minY + 1 : minY;
            // compute maxX, maxY
            final int maxX = GeoTileUtils.getXTile(bbox.right(), this.tiles);
            final int maxY = GeoTileUtils.getYTile(bbox.bottom(), this.tiles);
            final Rectangle maxTile = GeoTileUtils.toBoundingBox(maxX, maxY, precision);
            // touching tiles are excluded, they need to share at least one interior point
            this.maxX = maxTile.getMinX() == bbox.right() ? maxX - 1 : maxX;
            this.maxY = maxTile.getMaxY() == bbox.bottom() ? maxY - 1 : maxY;
        }

        @Override
        protected int advanceValue(org.elasticsearch.common.geo.GeoPoint target, int valuesIdx) {
            final int x = GeoTileUtils.getXTile(target.getLon(), this.tiles);
            final int y = GeoTileUtils.getYTile(target.getLat(), this.tiles);
            if (validTile(x, y)) {
                values[valuesIdx] = GeoTileUtils.longEncodeTiles(precision, x, y);
                return valuesIdx + 1;
            }
            return valuesIdx;
        }

        private boolean validTile(int x, int y) {
            if (maxY >= y && minY <= y) {
                if (crossesDateline) {
                    return maxX >= x || minX <= x;
                } else {
                    return maxX >= x && minX <= x;
                }
            }
            return false;
        }
    }
}
