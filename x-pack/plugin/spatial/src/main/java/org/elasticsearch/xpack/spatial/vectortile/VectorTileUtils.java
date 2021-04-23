/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import com.wdtinc.mapbox_vector_tile.encoding.MvtValue;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.geometry.Rectangle;
import org.locationtech.jts.geom.Envelope;

import java.io.IOException;
import java.util.Map;

/**
 * Utility methods For vector tiles. Transforms WGS84 into spherical mercator.
 */
public class VectorTileUtils {

    /**
     * Creates a vector layer builder with the provided name and extent.
     */
    public static VectorTile.Tile.Layer.Builder createLayerBuilder(String layerName, int extent) {
        final VectorTile.Tile.Layer.Builder layerBuilder = VectorTile.Tile.Layer.newBuilder();
        layerBuilder.setVersion(2);
        layerBuilder.setName(layerName);
        layerBuilder.setExtent(extent);
        return layerBuilder;
    }

    /**
     * Adds the flatten elements of toXContent into the feature as tags.
     */
    public static void addToXContentToFeature(VectorTile.Tile.Feature.Builder feature, MvtLayerProps layerProps, ToXContent toXContent)
        throws IOException {
        final Map<String, Object> map = Maps.flatten(
            XContentHelper.convertToMap(XContentHelper.toXContent(toXContent, XContentType.CBOR, false), true, XContentType.CBOR).v2(),
            true,
            true
        );
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            if (entry.getValue() != null) {
                addPropertyToFeature(feature, layerProps, entry.getKey(), entry.getValue());
            }
        }
    }

    /**
     * Adds the provided key / value pair into the feature as tags.
     */
    public static void addPropertyToFeature(VectorTile.Tile.Feature.Builder feature, MvtLayerProps layerProps, String key, Object value) {
        feature.addTags(layerProps.addKey(key));
        feature.addTags(layerProps.addValue(value));
    }

    public static void addPropertiesToLayer(VectorTile.Tile.Layer.Builder layer, MvtLayerProps layerProps) {
        // Add keys
        layer.addAllKeys(layerProps.getKeys());
        // Add values
        final Iterable<Object> values = layerProps.getVals();
        for (Object value : values) {
            layer.addValues(MvtValue.toValue(value));
        }
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
