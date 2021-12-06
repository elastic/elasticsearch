/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.search.runtime;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.StringFieldScript;

import java.util.List;

/**
 * Abstract base class for building queries based on {@link StringFieldScript}.
 */
abstract class AbstractStringScriptFieldQuery extends AbstractScriptFieldQuery<StringFieldScript> {

    AbstractStringScriptFieldQuery(Script script, StringFieldScript.LeafFactory leafFactory, String fieldName) {
        super(script, fieldName, leafFactory::newInstance);
    }

    @Override
    protected final boolean matches(StringFieldScript scriptContext, int docId) {
        return matches(scriptContext.resultsForDoc(docId));
    }

    /**
     * Does the value match this query?
     */
    protected abstract boolean matches(List<String> values);
}
