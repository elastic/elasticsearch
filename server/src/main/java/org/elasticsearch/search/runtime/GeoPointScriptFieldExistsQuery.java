/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;

public class GeoPointScriptFieldExistsQuery extends AbstractGeoPointScriptFieldQuery {
    public GeoPointScriptFieldExistsQuery(Script script, GeoPointFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, leafFactory, fieldName);
    }

    @Override
    protected boolean matches(double[] lats, double[] lons, int count) {
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
