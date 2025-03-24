/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.geometry.Point;
import org.elasticsearch.geometry.ShapeType;
import org.elasticsearch.geometry.utils.StandardValidator;
import org.elasticsearch.geometry.utils.WellKnownText;
import org.elasticsearch.search.SearchHit;

import java.io.IOException;
import java.text.ParseException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

public class GeoShapeField extends SourceField {

    static final String TYPE = "geo_shape";

    private static final Set<String> TYPES = Collections.singleton(TYPE);

    public GeoShapeField(String name) {
        super(name, TYPES);
    }

    @Override
    public Object[] value(SearchHit hit, SourceSupplier source) {
        Object[] value = super.value(hit, source);
        if (value.length == 0) {
            return value;
        }
        if (value.length > 1) {
            throw new IllegalStateException("Unexpected values for a geo_shape field: " + Arrays.toString(value));
        }

        if (value[0] instanceof String stringValue) {
            value[0] = handleString(stringValue);
        } else if (value[0] instanceof Map<?, ?>) {
            @SuppressWarnings("unchecked")
            Map<String, Object> geoObject = (Map<String, Object>) value[0];
            value[0] = handleObject(geoObject);
        } else {
            throw new IllegalStateException("Unexpected value type for a geo_shape field: " + value[0].getClass());
        }
        return value;
    }

    private static String handleString(String geoString) {
        try {
            if (geoString.startsWith("POINT")) { // Entry is of the form "POINT (-77.03653 38.897676)"
                Geometry geometry = WellKnownText.fromWKT(StandardValidator.instance(true), true, geoString);
                if (geometry.type() != ShapeType.POINT) {
                    throw new IllegalArgumentException("Unexpected non-point geo_shape type: " + geometry.type().name());
                }
                Point pt = ((Point) geometry);
                return pt.getY() + "," + pt.getX();
            } else {
                throw new IllegalArgumentException("Unexpected value for a geo_shape field: " + geoString);
            }
        } catch (IOException | ParseException ex) {
            throw new IllegalArgumentException("Unexpected value for a geo_shape field: " + geoString);
        }
    }

    private static String handleObject(Map<String, Object> geoObject) {
        String geoType = (String) geoObject.get("type");
        if (geoType != null && "point".equals(geoType.toLowerCase(Locale.ROOT))) {
            @SuppressWarnings("unchecked")
            List<Double> coordinates = (List<Double>) geoObject.get("coordinates");
            if (coordinates == null || coordinates.size() != 2) {
                throw new IllegalArgumentException("Invalid coordinates for geo_shape point: " + geoObject);
            }
            return coordinates.get(1) + "," + coordinates.get(0);
        } else {
            throw new IllegalArgumentException("Unexpected value for a geo_shape field: " + geoObject);
        }
    }
}
