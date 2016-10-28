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

package org.elasticsearch.plugin.example;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.SearchScript;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.Map;

/**
 * Example of a simple script service that adds several pre-defined scripts
 */
public class ExampleScriptService implements ScriptEngineService {

    public static final String NAME = "example";

    public static final String NAME_EXT = "xmpl";

    public ExampleScriptService(Settings settings) {
        // TODO: Show how to access settings
    }

    /**
     * This name is used to identify the script engine in elasticsearch
     */
    @Override
    public String getType() {
        return NAME;
    }

    /**
     * We don't add a scripting language in this example storing these scripts on file system doesn't make
     * much sense. But we need to define a unique extension, so it wouldn't clash with other script engines.
     */
    @Override
    public String getExtension() {
        return NAME_EXT;
    }

    /**
     * This method is called every time elasticsearch encounters a new script. The returned object is cached and then
     * given back to use for execution either using {@link #executable(CompiledScript, Map)} or
     * {@link #search(CompiledScript, SearchLookup, Map)} methods depending on context.
     *
     * @param scriptName - the name of the file where script was stored, this parameter is ignored in most cases
     * @param scriptSource - the actual script
     * @param params - the script compilation parameters, in most cases this set of parameters is empty. This is NOT the user-specified
     *               list of parameters which will be provided in executable and search method.
     * @return the compiled version of the script suitable for caching
     */
    @Override
    public Object compile(String scriptName, String scriptSource, Map<String, String> params) {
        switch (scriptSource) {
            case "is_holiday":
                return new IsHolidayScriptFactory();
            default:
                throw new IllegalArgumentException("Script [" + scriptSource + "] not found");
        }
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        ExampleScriptFactory scriptFactory = (ExampleScriptFactory) compiledScript.compiled();
        return scriptFactory.executable(vars);
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        ExampleScriptFactory scriptFactory = (ExampleScriptFactory) compiledScript.compiled();
        return scriptFactory.search(lookup, vars);
    }

    @Override
    public void close() throws IOException {

    }

    /**
     * Indicates that the script can be called inline by default.
     *
     * This should be set to true only for script that are safe to execute in your environment.
     */
    @Override
    public boolean isInlineScriptEnabled() {
        return true;
    }
}
