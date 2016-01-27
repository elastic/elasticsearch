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

import java.util.HashMap;
import java.util.Map;

/**
 * Mode for a specific script, used for script settings.
 * Defines whether a certain script or category of scripts can be executed or not, or whether it can
 * only be executed by a sandboxed scripting language.
 */
enum ScriptMode {
    ON("true"),
    OFF("false"),
    SANDBOX("sandbox");

    private final String mode;

    ScriptMode(String mode) {
        this.mode = mode;
    }

    private static final Map<String, ScriptMode> SCRIPT_MODES;

    static {
        SCRIPT_MODES = new HashMap<>();
        for (ScriptMode scriptMode : ScriptMode.values()) {
            SCRIPT_MODES.put(scriptMode.mode, scriptMode);
        }
    }

    static ScriptMode parse(String input) {
        ScriptMode scriptMode = SCRIPT_MODES.get(input);
        if (scriptMode == null) {
            throw new IllegalArgumentException("script mode [" + input + "] not supported");
        }
        return scriptMode;
    }

    public String getMode() {
        return mode;
    }

    @Override
    public String toString() {
        return mode;
    }
}
