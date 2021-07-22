/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;

import org.elasticsearch.search.aggregations.bucket.geogrid.GeoTileUtils;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoRelation;
import org.elasticsearch.xpack.spatial.index.fielddata.GeoShapeValues;

/**
 * Implements most of the logic for the GeoTile aggregation.
 */
abstract class AbstractGeoTileGridTiler extends GeoGridTiler {

    protected final long tiles;

    AbstractGeoTileGridTiler(int precision) {
        super(precision);
        tiles = 1L << precision;
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
    public int setValues(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue) {
        GeoShapeValues.BoundingBox bounds = geoValue.boundingBox();
        assert bounds.minX() <= bounds.maxX();

        // geo tiles are not defined at the extreme latitudes due to them
        // tiling the world as a square.
        if (bounds.bottom > GeoTileUtils.NORMALIZED_LATITUDE_MASK || bounds.top < GeoTileUtils.NORMALIZED_NEGATIVE_LATITUDE_MASK) {
            return 0;
        }

        if (precision == 0) {
            return validTile(0, 0, 0) ? 1 : 0;
        }

        final int minXTile = GeoTileUtils.getXTile(bounds.minX(), tiles);
        final int minYTile = GeoTileUtils.getYTile(bounds.maxY(), tiles);
        final int maxXTile = GeoTileUtils.getXTile(bounds.maxX(), tiles);
        final int maxYTile = GeoTileUtils.getYTile(bounds.minY(), tiles);
        final long count = (long) (maxXTile - minXTile + 1) * (maxYTile - minYTile + 1);
        if (count == 1) {
            return setValue(values, minXTile, minYTile);
        } else if (count <= precision) {
            return setValuesByBruteForceScan(values, geoValue, minXTile, minYTile, maxXTile, maxYTile);
        } else {
            return setValuesByRasterization(0, 0, 0, values, 0, geoValue);
        }
    }

    private GeoRelation relateTile(GeoShapeValues.GeoShapeValue geoValue, int xTile, int yTile, int precision) {
        return validTile(xTile, yTile, precision) ?
            geoValue.relate(GeoTileUtils.toBoundingBox(xTile, yTile, precision)) : GeoRelation.QUERY_DISJOINT;
    }

    /**
     * Sets a singular doc-value with the provided x/y.
     */
    protected int setValue(GeoShapeCellValues docValues, int xTile, int yTile) {
        if (validTile(xTile, yTile, precision)) {
            docValues.resizeCell(1);
            docValues.add(0, GeoTileUtils.longEncodeTiles(precision, xTile, yTile));
            return 1;
        }
        return 0;
    }

    /**
     *
     * @param values the bucket values as longs
     * @param geoValue the shape value
     * @return the number of buckets the geoValue is found in
     */
    protected int setValuesByBruteForceScan(GeoShapeCellValues values, GeoShapeValues.GeoShapeValue geoValue,
                                            int minXTile, int minYTile, int maxXTile, int maxYTile) {
        int idx = 0;
        for (int i = minXTile; i <= maxXTile; i++) {
            for (int j = minYTile; j <= maxYTile; j++) {
                GeoRelation relation = relateTile(geoValue, i, j, precision);
                if (relation != GeoRelation.QUERY_DISJOINT) {
                    values.resizeCell(idx + 1);
                    values.add(idx++, GeoTileUtils.longEncodeTiles(precision, i, j));
                }
            }
        }
        return idx;
    }

    protected int setValuesByRasterization(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex,
                                           GeoShapeValues.GeoShapeValue geoValue) {
        zTile++;
        for (int i = 0; i < 2; i++) {
            for (int j = 0; j < 2; j++) {
                int nextX = 2 * xTile + i;
                int nextY = 2 * yTile + j;
                GeoRelation relation = relateTile(geoValue, nextX, nextY, zTile);
                if (GeoRelation.QUERY_INSIDE == relation) {
                    if (zTile == precision) {
                        values.resizeCell(getNewSize(valuesIndex, 1));
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    } else {
                        int numTilesAtPrecision = getNumTilesAtPrecision(precision, zTile);
                        values.resizeCell(getNewSize(valuesIndex, numTilesAtPrecision + 1));
                        valuesIndex = setValuesForFullyContainedTile(nextX, nextY, zTile, values, valuesIndex);
                    }
                } else if (GeoRelation.QUERY_CROSSES == relation) {
                    if (zTile == precision) {
                        values.resizeCell(getNewSize(valuesIndex, 1));
                        values.add(valuesIndex++, GeoTileUtils.longEncodeTiles(zTile, nextX, nextY));
                    } else {
                        valuesIndex =
                            setValuesByRasterization(nextX, nextY, zTile, values, valuesIndex, geoValue);
                    }
                }
            }
        }
        return valuesIndex;
    }

    private int getNewSize(int valuesIndex, int increment) {
        long newSize  = (long) valuesIndex + increment;
        if (newSize > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) newSize;
    }

    private int getNumTilesAtPrecision(int finalPrecision, int currentPrecision) {
        final long numTilesAtPrecision  = Math.min(1L << (2 * (finalPrecision - currentPrecision)), getMaxCells());
        if (numTilesAtPrecision > Integer.MAX_VALUE) {
            throw new IllegalArgumentException("Tile aggregation array overflow");
        }
        return (int) numTilesAtPrecision;
    }

    protected abstract int setValuesForFullyContainedTile(int xTile, int yTile, int zTile, GeoShapeCellValues values, int valuesIndex);
}
