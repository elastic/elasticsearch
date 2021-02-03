/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.GeoPointFieldScript;

public class GeoPointScriptFieldExistsQuery extends AbstractGeoPointScriptFieldQuery {
    public GeoPointScriptFieldExistsQuery(Script script, GeoPointFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, leafFactory, fieldName);
    }

    @Override
    protected boolean matches(long[] values, int count) {
        return count > 0;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return getClass().getSimpleName();
        }
        return fieldName() + ":" + getClass().getSimpleName();
    }

    // Superclass's equals and hashCode are great for this class
}
