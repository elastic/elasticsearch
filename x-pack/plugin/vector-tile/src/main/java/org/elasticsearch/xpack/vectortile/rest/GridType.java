/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile.rest;

import org.elasticsearch.common.geo.GeoPoint;
import org.elasticsearch.geometry.Rectangle;
import org.elasticsearch.search.aggregations.bucket.geogrid.InternalGeoGridBucket;
import org.elasticsearch.search.aggregations.metrics.InternalGeoCentroid;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

import java.io.IOException;
import java.util.Locale;

/**
 * Enum containing the basic geometry types for serializing {@link InternalGeoGridBucket}
 */
enum GridType {

    GRID {
        @Override
        public byte[] toFeature(GridAggregation gridAggregation, InternalGeoGridBucket bucket, String key, FeatureFactory featureFactory)
            throws IOException {
            return gridAggregation.toGrid(key, featureFactory);
        }
    },
    POINT {
        @Override
        public byte[] toFeature(GridAggregation gridAggregation, InternalGeoGridBucket bucket, String key, FeatureFactory featureFactory)
            throws IOException {
            final GeoPoint point = (GeoPoint) bucket.getKey();
            return featureFactory.point(point.lon(), point.lat());
        }
    },
    CENTROID {
        @Override
        public byte[] toFeature(GridAggregation gridAggregation, InternalGeoGridBucket bucket, String key, FeatureFactory featureFactory)
            throws IOException {
            final Rectangle r = gridAggregation.toRectangle(key);
            final InternalGeoCentroid centroid = bucket.getAggregations().get(RestVectorTileAction.CENTROID_AGG_NAME);
            final double featureLon = Math.min(Math.max(centroid.centroid().lon(), r.getMinLon()), r.getMaxLon());
            final double featureLat = Math.min(Math.max(centroid.centroid().lat(), r.getMinLat()), r.getMaxLat());
            return featureFactory.point(featureLon, featureLat);
        }
    };

    /** Builds the corresponding vector tile feature for the provided bucket */
    public abstract byte[] toFeature(
        GridAggregation gridAggregation,
        InternalGeoGridBucket bucket,
        String key,
        FeatureFactory featureFactory
    ) throws IOException;

    public static GridType fromString(String type) {
        return switch (type.toLowerCase(Locale.ROOT)) {
            case "grid" -> GRID;
            case "point" -> POINT;
            case "centroid" -> CENTROID;
            default -> throw new IllegalArgumentException("Invalid grid type [" + type + "]");
        };
    }
}
