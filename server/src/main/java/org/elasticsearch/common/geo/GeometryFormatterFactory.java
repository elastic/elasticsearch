/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownBinary;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

/**
 * Output formatters supported by geometry fields.
 */
public class GeometryFormatterFactory {

    public static final String GEOJSON = "geojson";
    public static final String WKT = "wkt";
    public static final String WKB = "wkb";

    /**
     * Returns a formatter by name
     */
    public static <T> Function<List<T>, List<Object>> getFormatter(String name, Function<T, Geometry> toGeometry) {
        return switch (name) {
            case GEOJSON -> geometries -> {
                final List<Object> objects = new ArrayList<>(geometries.size());
                geometries.forEach((shape) -> objects.add(GeoJson.toMap(toGeometry.apply(shape))));
                return objects;
            };
            case WKT -> geometries -> {
                final List<Object> objects = new ArrayList<>(geometries.size());
                geometries.forEach((shape) -> objects.add(WellKnownText.toWKT(toGeometry.apply(shape))));
                return objects;
            };
            case WKB -> geometries -> {
                final List<Object> objects = new ArrayList<>(geometries.size());
                geometries.forEach((shape) -> objects.add(WellKnownBinary.toWKB(toGeometry.apply(shape), ByteOrder.LITTLE_ENDIAN)));
                return objects;
            };
            default -> throw new IllegalArgumentException("Unrecognized geometry format [" + name + "].");
        };
    }
}
