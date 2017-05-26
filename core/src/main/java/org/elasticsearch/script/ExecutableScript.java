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

import java.util.Map;

/**
 * An executable script, can't be used concurrently.
 */
public interface ExecutableScript {

    /**
     * Sets a runtime script parameter.
     * <p>
     * Note that this method may be slow, involving put() and get() calls
     * to a hashmap or similar.
     * @param name parameter name
     * @param value parameter value
     */
    void setNextVar(String name, Object value);

    /**
     * Executes the script.
     */
    Object run();

    interface Factory {
        ExecutableScript newInstance(Map<String, Object> params);
    }

    ScriptContext<Factory> CONTEXT = new ScriptContext<>("executable", Factory.class);

    // TODO: remove these once each has its own script interface
    ScriptContext<Factory> AGGS_CONTEXT = new ScriptContext<>("aggs_executable", Factory.class);
    ScriptContext<Factory> UPDATE_CONTEXT = new ScriptContext<>("update", Factory.class);
    ScriptContext<Factory> INGEST_CONTEXT = new ScriptContext<>("ingest", Factory.class);
}
