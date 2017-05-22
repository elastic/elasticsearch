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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A holder for information about a context in which a script is compiled and run.
 */
public final class ScriptContext {

    public static final ScriptContext AGGS = new ScriptContext("aggs");
    public static final ScriptContext SEARCH = new ScriptContext("search");
    public static final ScriptContext UPDATE = new ScriptContext("update");
    public static final ScriptContext INGEST = new ScriptContext("ingest");

    public static final Map<String, ScriptContext> BUILTINS;
    static {
        Map<String, ScriptContext> builtins = new HashMap<>();
        builtins.put(AGGS.name, AGGS);
        builtins.put(SEARCH.name, SEARCH);
        builtins.put(UPDATE.name, UPDATE);
        builtins.put(INGEST.name, INGEST);
        BUILTINS = Collections.unmodifiableMap(builtins);
    }

    /** A unique identifier for this context. */
    public final String name;

    // pkg private ctor, only created by script module
    public ScriptContext(String name) {
        this.name = name;
    }
}
