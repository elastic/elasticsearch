/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.xpack.spatial.VectorTileExtension;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

/**
 * Unique implementation of VectorTileExtension so we can transform geometries
 * into its vector tile representation from the spatial module.
 */
public class SpatialVectorTileExtension implements VectorTileExtension {

    @Override
    public GeoFormatterFactory.VectorTileEngine<Geometry> getVectorTileEngine() {
        return (z, x, y, extent) -> {
            final FeatureFactory featureFactory = new FeatureFactory(z, x, y, extent);
            return geometries -> {
                final Geometry geometry = (geometries.size() == 1) ? geometries.get(0) : new GeometryCollection<>(geometries);
                return featureFactory.getFeatures(geometry);
            };
        };
    }
}
