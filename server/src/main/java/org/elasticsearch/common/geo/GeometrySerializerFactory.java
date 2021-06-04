/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.common.geo;

import org.elasticsearch.geometry.utils.GeometryValidator;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;

/**
 * An utility class with a geometry parser methods supporting different shape representation formats
 */
public final class GeometrySerializerFactory {

    private final GeoJson geoJsonParser;
    private final WellKnownText wellKnownTextParser;

    public static final GeometrySerializerFactory INSTANCE = new GeometrySerializerFactory();

    private GeometrySerializerFactory() {
        GeometryValidator validator = new StandardValidator(true);
        geoJsonParser = new GeoJson(true, true, validator);
        wellKnownTextParser = new WellKnownText(true, validator);
    }

    /**
     * Returns a geometry format object that can parse and then serialize the object back to the same format.
     */
    public GeometrySerializer geometrySerializer(String format) {
        if (format.equals(GeoJsonGeometryFormat.NAME)) {
            return new GeoJsonGeometryFormat(geoJsonParser);
        } else if (format.equals(WKTGeometryFormat.NAME)) {
            return new WKTGeometryFormat(wellKnownTextParser);
        } else {
            throw new IllegalArgumentException("Unrecognized geometry format [" + format + "].");
        }
    }
}
