/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script;

import org.elasticsearch.script.Script.ScriptType;

/**
 * CompiledScript holds all the parameters necessary to execute a previously compiled script.
 */
public class CompiledScript {

    private final ScriptContext context;
    private final ScriptType type;
    private final String id;
    private final ScriptEngineService engine;
    private final Object compiled;

    /**
     * Constructor for CompiledScript.
     * @param type The type of script to be executed.
     * @param id The id of the script to be executed.
     * @param engine The {@link ScriptEngineService} used to compile this script.
     * @param compiled The compiled script Object that is executable.
     */
    public CompiledScript(ScriptContext context, ScriptType type, String id, ScriptEngineService engine, Object compiled) {
        this.context = context;
        this.type = type;
        this.id = id;
        this.engine = engine;
        this.compiled = compiled;
    }

    /**
     * Method to get the compilation context.
     * @return The context the script was compiled in.
     */
    public ScriptContext context() {
        return context;
    }

    /**
     * Method to get the type of language.
     * @return The type of language the script was compiled in.
     */
    public ScriptType type() {
        return type;
    }

    /**
     * Method to get the name of the script.
     * @return The name of the script to be executed.
     */
    public String id() {
        return id;
    }

    /**
     * Method to get the language.
     * @return The language of the script to be executed.
     */
    public String lang() {
        return engine.getType();
    }

    /**
     * Method to get the {@link ScriptEngineService}.
     * @return The {@link} ScriptEngineService used to compiled this script.
     */
    ScriptEngineService engine() {
        return engine;
    }

    /**
     * Method to get the compiled script object.
     * @return The compiled script Object that is executable.
     */
    public Object compiled() {
        return compiled;
    }

    /**
     * @return A string composed of type, lang, and name to describe the CompiledScript.
     */
    @Override
    public String toString() {
        return type + " script [" + id + "] using lang [" + lang() + "]";
    }
}
