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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

/**
 * Output formats supported by GeoFields.
 */
public class GeoFormatsFactory {

    public static final String GEOJSON = "geojson";
    public static final String WKT = "wkt";

    private static final Map<String, Function<Geometry, Object>> FORMATS = new HashMap<>();
    static {
        FORMATS.put(GEOJSON, GeoJson::toMap);
        FORMATS.put(WKT, WellKnownText.INSTANCE::toWKT);
    }

    /**
     * Returns a formatter by name
     */
    public static Function<Geometry, Object> getFormat(String name) {
        return FORMATS.get(name);
    }
}
