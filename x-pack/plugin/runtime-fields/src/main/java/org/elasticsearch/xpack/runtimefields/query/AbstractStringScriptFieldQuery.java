/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.runtimefields.query;

import org.elasticsearch.script.Script;
import org.elasticsearch.xpack.runtimefields.StringScriptFieldScript;

import java.util.List;

/**
 * Abstract base class for building queries based on {@link StringScriptFieldScript}.
 */
abstract class AbstractStringScriptFieldQuery extends AbstractScriptFieldQuery<StringScriptFieldScript> {

    AbstractStringScriptFieldQuery(Script script, StringScriptFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected final boolean matches(StringScriptFieldScript scriptContext, int docId) {
        return matches(scriptContext.resultsForDoc(docId));
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(List<String> values);
}
