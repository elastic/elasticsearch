/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

/**
 * Takes a Script definition and returns a compiled script factory
 */
public interface ScriptCompiler {

    /**
     * Takes a Script definition and returns a compiled script factory
     * @param script            the Script to compile
     * @param scriptContext     the ScriptContext defining how to compile the script
     * @param <T>               the class of the compiled Script factory
     * @return                  a Script factory
     */
    <T> T compile(Script script, ScriptContext<T> scriptContext);

    ScriptCompiler NONE = new ScriptCompiler() {
        @Override
        public <T> T compile(Script script, ScriptContext<T> scriptContext) {
            throw new UnsupportedOperationException();
        }
    };
}
