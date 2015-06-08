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

/**
 * CompiledScript holds all the parameters necessary to execute a previously compiled script.
 */
public class CompiledScript {

    private final String lang;
    private final Object compiled;

    /**
     * Constructor for CompiledScript.
     * @param lang The language of the script to be executed.
     * @param compiled The compiled script Object that is executable.
     */
    public CompiledScript(String lang, Object compiled) {
        this.lang = lang;
        this.compiled = compiled;
    }

    /**
     * Method to get the language.
     * @return The language of the script to be executed.
     */
    public String lang() {
        return lang;
    }

    /**
     * Method to get the compiled script object.
     * @return The compiled script Object that is executable.
     */
    public Object compiled() {
        return compiled;
    }
}
