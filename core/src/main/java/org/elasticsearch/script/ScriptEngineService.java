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
import java.util.Map;

/**
 *
 */
public interface ScriptEngineService extends Closeable {

    String getType();

    String getExtension();

    /**
     * Compiles a script.
     * @param context the context the script will be compiled under
     * @param id id of the script - inline will be {@code null},
     *                              stored will be the id,
     *                              file will be the file name
     * @param code actual source of the script
     * @param options compile-time parameters (such as flags to the compiler)
     */
    Object compile(ScriptContext context, String id, String code, Map<String, String> options);

    ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars);

    SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars);

    /**
     * Returns <code>true</code> if this scripting engine can safely accept inline scripts by default. The default is <code>false</code>
     */
    default boolean isInlineScriptEnabled() {
        return false;
    }
}
