/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.rest;

import com.wdtinc.mapbox_vector_tile.VectorTile;
import com.wdtinc.mapbox_vector_tile.build.MvtLayerProps;
import com.wdtinc.mapbox_vector_tile.encoding.MvtValue;

import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.Map;

/**
 * Utility methods for vector tiles.
 */
class VectorTileUtils {

    private VectorTileUtils() {
        // no instances
    }

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
        if (value == null) {
            // guard for null values
            return;
        }
        if (value instanceof Byte || value instanceof Short) {
            // mvt does not support byte and short data types
            value = ((Number) value).intValue();
        }
        feature.addTags(layerProps.addKey(key));
        int valIndex = layerProps.addValue(value);
        if (valIndex < 0) {
            throw new IllegalArgumentException("Unsupported vector tile property type: " + value.getClass().getName());
        }
        feature.addTags(valIndex);
    }

    /**
     * Adds the given properties to the provided layer.
     */
    public static void addPropertiesToLayer(VectorTile.Tile.Layer.Builder layer, MvtLayerProps layerProps) {
        // Add keys
        layer.addAllKeys(layerProps.getKeys());
        // Add values
        final Iterable<Object> values = layerProps.getVals();
        for (Object value : values) {
            layer.addValues(MvtValue.toValue(value));
        }
    }
}
