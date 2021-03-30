/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import org.elasticsearch.geometry.Rectangle;
import org.locationtech.jts.geom.Envelope;

/**
 * Utility methods For vector tiles. Transforms WGS84 into spherical mercator.
 */
public class VectorTileUtils {

    public static VectorTile.Tile.Layer.Builder createLayerBuilder(String layerName, int extent) {
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setName(layerName);
        layerBuilder.setExtent(extent);
        return layerBuilder;
    }

    /**
     * Gets the JTS envelope for z/x/y/ tile in  spherical mercator projection.
     */
    public static Envelope getJTSTileBounds(int z, int x, int y) {
        return new Envelope(getLong(x, z), getLong(x + 1, z), getLat(y, z), getLat(y + 1, z));
    }

    /**
     * Gets the {@link org.elasticsearch.geometry.Geometry} envelope for z/x/y/ tile
     * in spherical mercator projection.
     */
    public static Rectangle getTileBounds(int z, int x, int y) {
        return new Rectangle(getLong(x, z), getLong(x + 1, z), getLat(y, z), getLat(y + 1, z));
    }

    private static double getLong(int x, int zoom)
    {
        return lonToSphericalMercator( x / Math.pow(2, zoom) * 360 - 180 );
    }

    private static double getLat(int y, int zoom) {
        double r2d = 180 / Math.PI;
        double n = Math.PI - 2 * Math.PI * y / Math.pow(2, zoom);
        return latToSphericalMercator(r2d * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n))));
    }

    private static double MERCATOR_FACTOR = 20037508.34 / 180.0;

    /**
     * Transforms WGS84 longitude to a Spherical mercator longitude
     */
    public static double lonToSphericalMercator(double lon) {
        return lon * MERCATOR_FACTOR;
    }

    /**
     * Transforms WGS84 latitude to a Spherical mercator latitude
     */
    public static double latToSphericalMercator(double lat) {
        double y = Math.log(Math.tan((90 + lat) * Math.PI / 360)) / (Math.PI / 180);
        return y * MERCATOR_FACTOR;
    }
}
