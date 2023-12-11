/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.search.runtime;

import org.elasticsearch.geometry.Geometry;
import org.elasticsearch.script.GeometryFieldScript;
import org.elasticsearch.script.Script;

public class GeoShapeScriptFieldExistsQuery extends AbstractGeoShapeScriptFieldQuery {
    public GeoShapeScriptFieldExistsQuery(Script script, GeometryFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, leafFactory, fieldName);
    }

    @Override
    protected boolean matches(Geometry geometry) {
        return geometry != null;
    }

    @Override
    public final String toString(String field) {
        if (fieldName().contentEquals(field)) {
            return getClass().getSimpleName();
        }
        return fieldName() + ":" + getClass().getSimpleName();
    }

    // Superclass's equals and hashCode are fine for this class
}
