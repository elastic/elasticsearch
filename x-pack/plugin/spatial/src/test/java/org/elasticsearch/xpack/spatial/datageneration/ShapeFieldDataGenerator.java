/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.datageneration;

import org.elasticsearch.common.geo.GeoJson;
import org.elasticsearch.datageneration.FieldDataGenerator;
import org.elasticsearch.datageneration.datasource.DataSource;
import org.elasticsearch.datageneration.datasource.DataSourceRequest;
import org.elasticsearch.datageneration.fields.leaf.Wrappers;
import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.util.Map;
import java.util.function.Supplier;

public class ShapeFieldDataGenerator implements FieldDataGenerator {
    private final Supplier<Object> formattedShapes;
    private final Supplier<Object> formattedShapesWithMalformed;

    public ShapeFieldDataGenerator(DataSource dataSource) {
        var shapes = dataSource.get(new DataSourceRequest.ShapeGenerator()).generator();
        var serializeToGeoJson = dataSource.get(new DataSourceRequest.TransformWrapper(0.5, g -> GeoJson.toMap((Geometry) g)));

        var formattedShapes = serializeToGeoJson.wrapper().andThen(values -> (Supplier<Object>) () -> {
            var value = values.get();
            if (value instanceof Geometry g) {
                // did not transform
                return WellKnownText.toWKT(g);
            }
            return value;
        }).apply(shapes::get);
        this.formattedShapes = Wrappers.defaults(formattedShapes, dataSource);

        var longs = dataSource.get(new DataSourceRequest.LongGenerator()).generator();
        this.formattedShapesWithMalformed = Wrappers.defaultsWithMalformed(formattedShapes, longs::get, dataSource);
    }

    @Override
    public Object generateValue(Map<String, Object> fieldMapping) {
        if (fieldMapping == null) {
            // dynamically mapped and dynamic mapping does not play well with this type (it gets mapped as an object)
            // return null to skip indexing this field
            return null;
        }

        if (fieldMapping != null && (Boolean) fieldMapping.getOrDefault("ignore_malformed", false)) {
            return formattedShapesWithMalformed.get();
        }

        return formattedShapes.get();
    }
}
