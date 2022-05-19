/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.vectortile;

import org.elasticsearch.common.geo.GeoFormatterFactory;
import org.elasticsearch.common.geo.SimpleVectorTileFormatter;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.GeometryCollection;
import org.elasticsearch.xpack.spatial.GeometryFormatterExtension;
import org.elasticsearch.xpack.vectortile.feature.FeatureFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Unique implementation of VectorTileExtension so we can transform geometries
 * into its vector tile representation from the spatial module.
 */
public class SpatialGeometryFormatterExtension implements GeometryFormatterExtension {

    @Override
    public List<GeoFormatterFactory.FormatterFactory<Geometry>> getGeometryFormatterFactories() {
        return List.of(new GeoFormatterFactory.FormatterFactory<>() {
            @Override
            public String getName() {
                return SimpleVectorTileFormatter.MVT;
            }

            @Override
            public Function<String, Function<List<Geometry>, List<Object>>> getFormatterBuilder() {

                return (params) -> {
                    int[] parsed = SimpleVectorTileFormatter.parse(params);
                    final FeatureFactory featureFactory = new FeatureFactory(parsed[0], parsed[1], parsed[2], parsed[3], parsed[4]);
                    return geometries -> {
                        final Geometry geometry = (geometries.size() == 1) ? geometries.get(0) : new GeometryCollection<>(geometries);
                        return new ArrayList<>(featureFactory.getFeatures(geometry));
                    };
                };
            }
        });
    }
}
