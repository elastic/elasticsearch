/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.AbstractPointFieldScript;
import org.elasticsearch.script.GeoPointFieldScript;
import org.elasticsearch.script.Script;
import org.elasticsearch.xcontent.ToXContentFragment;

/**
 * Abstract base class for building queries based on {@link GeoPointFieldScript}.
 */
abstract class AbstractGeoPointScriptFieldQuery<T extends ToXContentFragment> extends AbstractScriptFieldQuery<
    AbstractPointFieldScript<T>> {

    AbstractGeoPointScriptFieldQuery(Script script, GeoPointFieldScript.LeafFactory<T> leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected boolean matches(AbstractPointFieldScript<T> scriptContext, int docId) {
        scriptContext.runForDoc(docId);
        return matches(scriptContext.values(), scriptContext.count());
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(long[] values, int count);
}
