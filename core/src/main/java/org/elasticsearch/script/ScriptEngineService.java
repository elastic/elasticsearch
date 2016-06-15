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

import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.lookup.SearchLookup;

import java.io.Closeable;
import java.util.List;
import java.util.Map;

/**
 *
 */
public interface ScriptEngineService extends Closeable {

    String getType();

    String getExtension();

    /**
     * Compiles a script.
     * @param scriptName name of the script. {@code null} if it is anonymous (inline).
     *                                        For a file script, its the file name (with extension).
     *                                        For a stored script, its the identifier.
     * @param scriptSource actual source of the script
     * @param params compile-time parameters (such as flags to the compiler)
     */
    Object compile(String scriptName, String scriptSource, Map<String, String> params);

    ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars);

    SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars);

    /**
     * Handler method called when a script is removed from the Guava cache.
     *
     * The passed script may be null if it has already been garbage collected.
     * */
    void scriptRemoved(@Nullable CompiledScript script);

    /**
     * Returns <code>true</code> if this scripting engine can safely accept inline scripts by default. The default is <code>false</code>
     */
    default boolean isInlineScriptEnabled() {
        return false;
    }
}
