/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.utils.WellKnownText;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Output formatters supported by geo fields.
 */
public class GeoFormatterFactory {

    public static final String GEOJSON = "geojson";
    public static final String WKT = "wkt";

    private static final Map<String, Function<List<Geometry>, List<Object>>> FORMATTERS = new HashMap<>();
    static {
        FORMATTERS.put(GEOJSON, geometries -> {
            final List<Object> objects = new ArrayList<>(geometries.size());
            geometries.forEach((geometry) -> objects.add(GeoJson.toMap(geometry)));
            return objects;
        });
        FORMATTERS.put(WKT, geometries -> {
            final List<Object> objects = new ArrayList<>(geometries.size());
            geometries.forEach((geometry) -> objects.add(WellKnownText.toWKT(geometry)));
            return objects;
        });
    }

    /**
     * Returns a formatter by name
     */
    public static Function<List<Geometry>, List<Object>> getFormatter(String name) {
        Function<List<Geometry>, List<Object>> format = FORMATTERS.get(name);
        if (format == null) {
            throw new IllegalArgumentException("Unrecognized geometry format [" + name + "].");
        }
        return format;
    }
}
