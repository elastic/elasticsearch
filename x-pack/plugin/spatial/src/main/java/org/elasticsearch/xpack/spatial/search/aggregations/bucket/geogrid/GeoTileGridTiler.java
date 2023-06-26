/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.apache.lucene.geo.GeoEncodingUtils;
import org.elasticsearch.common.geo.GeoBoundingBox;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileBoundedPredicate;
import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

import java.io.IOException;

/**
 * Implements the logic for the GeoTile aggregation over a geoshape doc value.
 */
public abstract class GeoTileGridTiler extends GeoGridTiler {

    protected final int tiles;

    private GeoTileGridTiler(int precision) {
        super(precision);
        tiles = 1 << precision;
    }

    /** Factory method to create GeoTileGridTiler objects */
    public static GeoTileGridTiler makeGridTiler(int precision, GeoBoundingBox geoBoundingBox) {
        return geoBoundingBox == null || geoBoundingBox.isUnbounded()
            ? new GeoTileGridTiler.UnboundedGeoTileGridTiler(precision)
            : new GeoTileGridTiler.BoundedGeoTileGridTiler(precision, geoBoundingBox);
    }

    /** check if the provided tile is in the solution space of this tiler */
    protected abstract boolean validTile(int x, int y, int z);

    @Override
    public long encode(double x, double y) {
        return GeoTileUtils.longEncode(x, y, precision);
    }

    /**
     * Sets the values of the long[] underlying {@link GeoShapeCellValues}.
     *
     * If the shape resides between <code>GeoTileUtils.NORMALIZED_LATITUDE_MASK</code> and 90 or
     * between <code>GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK</code> and -90 degree latitudes, then
     * the shape is not accounted for since geo-tiles are only defined within those bounds.
     *
     * @param values           the bucket values
     * @param geoValue         the input shape
     *
     * @return the number of tiles set by the shape
     */
    @Override
    public int setValues(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) throws IOException {
        final GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        // geo tiles are not defined at the extreme latitudes due to them
        // tiling the world as a square.
        if (bounds.bottom > GeoTileUtils.NORMALIZED_LATITUDE_MASK || bounds.top < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK) {
            return 0;
        }

        if (precision == 0) {
            return setValuesByBruteForceScan(values, geoValue, 0, 0, 0, 0);
        }

        final int minXTile = GeoTileUtils.getXTile(bounds.minX(), tiles);
        final int minYTile = GeoTileUtils.getYTile(bounds.maxY(), tiles);
        final int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), tiles);
        final int maxYTile = GeoTileUtils.getYTile(bounds.minY(), tiles);
        final long count = (long) (maxXTile - minXTile + 1) * (maxYTile - minYTile + 1);
        if (count == 1) {
            return setValue(values, minXTile, minYTile);
        } else if (count <= 8L * precision) {
            return setValuesByBruteForceScan(values, geoValue, minXTile, minYTile, maxXTile, maxYTile);
        } else {
            return setValuesByRasterization(0, 0, 0, values, 0, geoValue);
        }
    }

    private GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, int xTile, int yTile, int precision) throws IOException {
        if (validTile(xTile, yTile, precision)) {
            final int tiles = 1 << precision;
            final int minX = GeoEncodingUtils.encodeLongitude(GeoTileUtils.tileToLon(xTile, tiles));
            final int maxX = GeoEncodingUtils.encodeLongitude(GeoTileUtils.tileToLon(xTile + 1, tiles));
            final int minY = GeoEncodingUtils.encodeLatitude(GeoTileUtils.tileToLat(yTile + 1, tiles));
            final int maxY = GeoEncodingUtils.encodeLatitude(GeoTileUtils.tileToLat(yTile, tiles));
            return geoValue.relate(
                minX,
                maxX == Integer.MAX_VALUE ? maxX : maxX - 1,
                minY == GeoTileUtils.ENCODED_NEGATIVE_LATITUDE_MASK ? minY : minY + 1,
                maxY
            );
        }
        return GeoRelation.QUERY_DISJOINT;
    }

    /**
     * Sets a singular doc-value with the provided x/y.
     */
    private int setValue(GeoShapeCellValues docValues, int xTile, int yTile) {
        if (validTile(xTile, yTile, precision)) {
            docValues.resizeCell(1);
            docValues.add(0, GeoTileUtils.longEncodeTiles(precision, xTile, yTile));
            return 1;
        }
        return 0;
    }

    /**
     * Checks all tiles between minXTile/maxXTile and minYTile/maxYTile.
     */
    // pack private for testing
    int setValuesByBruteForceScan(
        GeoShapeCellValues values,
        GeoShapeValues.GeoShapeValue geoValue,
        int minXTile,
        int minYTile,
        int maxXTile,
        int maxYTile
    ) throws IOException {
        int idx = 0;
        for (int i = minXTile; i <= maxXTile; i++) {
            for (int j = minYTile; j <= maxYTile; j++) {
                final GeoRelation relation = relateTile(geoValue, i, j, precision);
                if (relation != GeoRelation.QUERY_DISJOINT) {
                    values.resizeCell(idx + 1);
                    values.add(idx++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        }
        return idx;
    }

    /**
     * Recursively search the tile tree, only following branches that intersect the geometry.
     * Once at the required depth, then all cells that intersect are added to the collection.
     */
    // pkg protected for testing
    int setValuesByRasterization(
        int xTile,
        int yTile,
        int zTile,
        GeoShapeCellValues values,
        int valuesIndex,
        GeoShapeValues.GeoShapeValue geoValue
    ) throws IOException {
        zTile++;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                final int nextX = 2 * xTile + i;
                final int nextY = 2 * yTile + j;
                final GeoRelation relation = relateTile(geoValue, nextX, nextY, zTile);
                if (GeoRelation.QUERY_INSIDE == relation) {
                    if (zTile == precision) {
                        values.resizeCell(getNewSize(valuesIndex, 1));
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    } else {
                        final int numTilesAtPrecision = getNumTilesAtPrecision(precision, zTile);
                        values.resizeCell(getNewSize(valuesIndex, numTilesAtPrecision + 1));
                        valuesIndex = setValuesForFullyContainedTile(nextX, nextY, zTile, values, valuesIndex);
                    }
                } else if (GeoRelation.QUERY_DISJOINT != relation) {
                    if (zTile == precision) {
                        values.resizeCell(getNewSize(valuesIndex, 1));
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    } else {
                        valuesIndex = setValuesByRasterization(nextX, nextY, zTile, values, valuesIndex, geoValue);
                    }
                }
            }
        }
        return valuesIndex;
    }

    private int getNewSize(int valuesIndex, int increment) {
        final long newSize = (long) valuesIndex + increment;
        if (newSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) newSize;
    }

    private int getNumTilesAtPrecision(int finalPrecision, int currentPrecision) {
        final long numTilesAtPrecision = Math.min(1L << (2 * (finalPrecision - currentPrecision)), getMaxCells());
        if (numTilesAtPrecision > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) numTilesAtPrecision;
    }

    protected abstract int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex);

    protected int setValues(GeoShapeCellValues values, int valuesIndex, int minY, int maxY, int minX, int maxX) {
        for (int i = minX; i < maxX; i++) {
            for (int j = minY; j < maxY; j++) {
                assert validTile(i, j, precision);
                values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(precision, i, j));
            }
        }
        return valuesIndex;
    }

    /**
     * Bounded geotile aggregation. It accepts tiles that intersects the provided bounds.
     */
    private static class BoundedGeoTileGridTiler extends GeoTileGridTiler {

        private final GeoTileBoundedPredicate predicate;

        BoundedGeoTileGridTiler(int precision, GeoBoundingBox bbox) {
            super(precision);
            this.predicate = new GeoTileBoundedPredicate(precision, bbox);
        }

        @Override
        protected boolean validTile(int x, int y, int z) {
            return predicate.validTile(x, y, z);
        }

        @Override
        protected long getMaxCells() {
            return predicate.getMaxTiles();
        }

        @Override
        protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex) {
            // For every level we go down, we half each dimension. The total number of splits is equal to 1 << (levelEnd - levelStart)
            final int splits = 1 << precision - zTile;
            // The start value of a dimension is calculated by multiplying the value of that dimension at the start level
            // by the number of splits. Choose the max value with respect to the bounding box.
            final int minY = Math.max(predicate.minY(), yTile * splits);
            // The end value of a dimension is calculated by adding to the start value the number of splits.
            // Choose the min value with respect to the bounding box.
            final int maxY = Math.min(predicate.maxY(), yTile * splits + splits);
            // Do the same for the X dimension taking into account that the bounding box might cross the dateline.
            if (predicate.crossesDateline()) {
                final int westMinX = Math.max(predicate.leftX(), xTile * splits);
                final int westMaxX = xTile * splits + splits;
                valuesIndex = setValues(values, valuesIndex, minY, maxY, westMinX, westMaxX);
                // when the left and right box land in the same tile, we need to make sure we don't count then twice
                final int eastMaxX = Math.min(westMinX, Math.min(predicate.rightX(), xTile * splits + splits));
                final int eastMinX = xTile * splits;
                return setValues(values, valuesIndex, minY, maxY, eastMinX, eastMaxX);
            } else {
                final int minX = Math.max(predicate.leftX(), xTile * splits);
                final int maxX = Math.min(predicate.rightX(), xTile * splits + splits);
                return setValues(values, valuesIndex, minY, maxY, minX, maxX);
            }
        }
    }

    /**
     * Unbounded geotile aggregation. It accepts any tile.
     */
    private static class UnboundedGeoTileGridTiler extends GeoTileGridTiler {
        private final long maxTiles;

        UnboundedGeoTileGridTiler(int precision) {
            super(precision);
            maxTiles = (long) tiles * tiles;
        }

        @Override
        protected boolean validTile(int x, int y, int z) {
            return true;
        }

        @Override
        protected long getMaxCells() {
            return maxTiles;
        }

        @Override
        protected int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex) {
            // For every level we go down, we half each dimension. The total number of splits is equal to 1 << (levelEnd - levelStart)
            final int splits = 1 << precision - zTile;
            // The start value of a dimension is calculated by multiplying the value of that dimension at the start level
            // by the number of splits
            final int minX = xTile * splits;
            final int minY = yTile * splits;
            // The end value of a dimension is calculated by adding to the start value the number of splits
            final int maxX = minX + splits;
            final int maxY = minY + splits;
            return setValues(values, valuesIndex, minY, maxY, minX, maxX);
        }
    }
}
