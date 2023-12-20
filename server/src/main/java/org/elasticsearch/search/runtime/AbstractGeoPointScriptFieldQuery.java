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

/**
 * Abstract base class for building queries based on {@link GeoPointFieldScript}.
 */
abstract class AbstractGeoPointScriptFieldQuery extends AbstractScriptFieldQuery<GeoPointFieldScript> {

    AbstractGeoPointScriptFieldQuery(Script script, GeoPointFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(GeoPointFieldScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return matches(scriptContext.lats(), scriptContext.lons(), scriptContext.count());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(double[] lats, double[] lons, int count);
}
