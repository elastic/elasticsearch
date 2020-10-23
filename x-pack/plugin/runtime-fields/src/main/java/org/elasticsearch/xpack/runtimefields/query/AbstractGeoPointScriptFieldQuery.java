/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.mapper.GeoPointFieldScript;

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
        return matches(scriptContext.values(), scriptContext.count());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(long[] values, int count);
}
