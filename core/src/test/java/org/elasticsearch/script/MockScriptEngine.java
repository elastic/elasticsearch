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

import java.io.IOException;
import java.util.Map;

/**
 * A dummy script engine used for testing. Scripts must be a number. Running the script
 */
public class MockScriptEngine implements ScriptEngineService {
    public static final String NAME = "mockscript";

    @Override
    public String[] types() {
        return new String[]{ NAME };
    }

    @Override
    public String[] extensions() {
        return types();
    }

    @Override
    public boolean sandboxed() {
        return true;
    }

    @Override
    public Object compile(String script) {
        return Integer.parseInt(script);
    }

    @Override
    public ExecutableScript executable(CompiledScript compiledScript, @Nullable Map<String, Object> vars) {
        return null;
    }

    @Override
    public SearchScript search(CompiledScript compiledScript, SearchLookup lookup, @Nullable Map<String, Object> vars) {
        return null;
    }

    @Override
    public Object execute(CompiledScript compiledScript, Map<String, Object> vars) {
        return null;
    }

    @Override
    public Object unwrap(Object value) {
        return null;
    }

    @Override
    public void scriptRemoved(@Nullable CompiledScript script) {
    }

    @Override
    public void close() throws IOException {
    }
}
