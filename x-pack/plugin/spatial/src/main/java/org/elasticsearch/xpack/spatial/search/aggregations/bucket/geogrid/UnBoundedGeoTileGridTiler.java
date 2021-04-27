/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.aggregations.bucket.geogrid;


/**
 * Unbounded geotile aggregation. It accepts any tile.
 */
public class UnBoundedGeoTileGridTiler extends AbstractGeoTileGridTiler {
    private final long maxTiles;

    public UnBoundedGeoTileGridTiler(int precision) {
        super(precision);
        maxTiles = tiles * tiles;
    }

    @Override
    protected boolean validTile(int x, int y, int z) {
       return true;
    }

    @Override
    protected long getMaxTiles() {
        return maxTiles;
    }
}
