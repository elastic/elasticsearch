/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.mapper;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;

import java.util.Optional;

// Extracted from MapperTestCase, was marked as a protected class there.
// Will changing the access level cause any issues?
// Is it fine to keep this class in the index package, or should it be moved somewhere else?
protected abstract class IngestScriptSupport {
    // previously a private method
    <T> T compileScript(Script script, ScriptContext<T> context) {
        switch (script.getIdOrCode()) {
            case "empty":
                return context.factoryClazz.cast(emptyFieldScript());
            case "non-empty":
                return context.factoryClazz.cast(nonEmptyFieldScript());
            default:
                return compileOtherScript(script, context);
        }
    }

    protected <T> T compileOtherScript(Script script, ScriptContext<T> context) {
        throw new UnsupportedOperationException("Unknown script " + script.getIdOrCode());
    }

    /**
     * Create a script that can be run to produce no values for this
     * field or return {@link Optional#empty()} to signal that this
     * field doesn't support fields scripts.
     */
    abstract ScriptFactory emptyFieldScript();

    /**
     * Create a script that can be run to produce some value value for this
     * field or return {@link Optional#empty()} to signal that this
     * field doesn't support fields scripts.
     */
    abstract ScriptFactory nonEmptyFieldScript();
}
