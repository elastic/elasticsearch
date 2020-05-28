/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.extractor;

import org.elasticsearch.search.SearchHit;

import java.util.Arrays;
import java.util.Collections;
import java.util.Set;

public class GeoPointField extends DocValueField {

    static final String TYPE = "geo_point";

    private static final Set<String> TYPES = Collections.singleton(TYPE);

    public GeoPointField(String name) {
        super(name, TYPES);
    }

    @Override
    public Object[] value(SearchHit hit) {
        Object[] value = super.value(hit);
        if (value.length == 0) {
            return value;
        }
        if (value.length > 1) {
            throw new IllegalStateException("Unexpected values for a geo_point field: " + Arrays.toString(value));
        }

        if (value[0] instanceof String) {
            value[0] = handleString((String) value[0]);
        } else {
            throw new IllegalStateException("Unexpected value type for a geo_point field: " + value[0].getClass());
        }
        return value;
    }

    private String handleString(String geoString) {
        if (geoString.contains(",")) { // Entry is of the form "38.897676, -77.03653"
            return geoString.replace(" ", "");
        } else {
            throw new IllegalArgumentException("Unexpected value for a geo_point field: " + geoString);
        }
    }

    @Override
    public boolean supportsFromSource() {
        return false;
    }

    @Override
    public ExtractedField newFromSource() {
        throw new UnsupportedOperationException();
    }
}
