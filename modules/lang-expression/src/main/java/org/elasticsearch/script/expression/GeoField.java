/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.expression;

import org.apache.lucene.search.DoubleValuesSource;
import org.elasticsearch.index.fielddata.IndexFieldData;

/**
 * Expressions API for geo_point fields.
 */
final class GeoField {
    // no instance
    private GeoField() {}

    // supported variables
    static final String EMPTY_VARIABLE = "empty";
    static final String LAT_VARIABLE = "lat";
    static final String LON_VARIABLE = "lon";

    // supported methods
    static final String ISEMPTY_METHOD = "isEmpty";
    static final String GETLAT_METHOD = "getLat";
    static final String GETLON_METHOD = "getLon";

    static DoubleValuesSource getVariable(IndexFieldData<?> fieldData, String fieldName, String variable) {
        return switch (variable) {
            case EMPTY_VARIABLE -> new GeoEmptyValueSource(fieldData);
            case LAT_VARIABLE -> new GeoLatitudeValueSource(fieldData);
            case LON_VARIABLE -> new GeoLongitudeValueSource(fieldData);
            default -> throw new IllegalArgumentException(
                "Member variable [" + variable + "] does not exist for geo field [" + fieldName + "]."
            );
        };
    }

    static DoubleValuesSource getMethod(IndexFieldData<?> fieldData, String fieldName, String method) {
        return switch (method) {
            case ISEMPTY_METHOD -> new GeoEmptyValueSource(fieldData);
            case GETLAT_METHOD -> new GeoLatitudeValueSource(fieldData);
            case GETLON_METHOD -> new GeoLongitudeValueSource(fieldData);
            default -> throw new IllegalArgumentException(
                "Member method [" + method + "] does not exist for geo field [" + fieldName + "]."
            );
        };
    }
}
