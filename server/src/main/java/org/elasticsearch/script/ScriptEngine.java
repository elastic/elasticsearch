/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;
import java.util.Set;

/**
 * A script language implementation.
 */
public interface ScriptEngine extends Closeable {

    /**
     * The language name used in the script APIs to refer to this scripting backend.
     */
    String getType();

    /**
     * Compiles a script.
     * @param name the name of the script. {@code null} if it is anonymous (inline). For a stored script, its the identifier.
     * @param code actual source of the script
     * @param context the context this script will be used for
     * @param params compile-time parameters (such as flags to the compiler)
     * @return A compiled script of the FactoryType from {@link ScriptContext}
     */
    <FactoryType> FactoryType compile(String name, String code, ScriptContext<FactoryType> context, Map<String, String> params);

    @Override
    default void close() throws IOException {}

    /**
     * Script contexts supported by this engine.
     */
    Set<ScriptContext<?>> getSupportedContexts();
}
