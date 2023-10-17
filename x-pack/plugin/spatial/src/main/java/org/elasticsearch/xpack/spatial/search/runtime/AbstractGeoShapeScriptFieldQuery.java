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
import org.elasticsearch.search.runtime.AbstractScriptFieldQuery;

/**
 * Abstract base class for building queries based on {@link GeometryFieldScript}.
 */
abstract class AbstractGeoShapeScriptFieldQuery extends AbstractScriptFieldQuery<GeometryFieldScript> {

    AbstractGeoShapeScriptFieldQuery(Script script, GeometryFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(GeometryFieldScript scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return matches(scriptContext.geometry());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(Geometry geometry);
}
