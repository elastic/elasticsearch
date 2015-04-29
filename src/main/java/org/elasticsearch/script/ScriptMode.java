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

import org.elasticsearch.common.Booleans;

import java.util.Locale;

/**
 * Mode for a specific script, used for script settings.
 * Defines whether a certain script or catefory of scripts can be executed or not, or whether it can
 * only be executed by a sandboxed scripting language.
 */
enum ScriptMode {
    ON,
    OFF,
    SANDBOX;

    static ScriptMode parse(String input) {
        input = input.toLowerCase(Locale.ROOT);
        if (Booleans.isExplicitTrue(input)) {
            return ON;
        }
        if (Booleans.isExplicitFalse(input)) {
            return OFF;
        }
        if (SANDBOX.toString().equals(input)) {
            return SANDBOX;
        }
        throw new IllegalArgumentException("script mode [" + input + "] not supported");
    }


    @Override
    public String toString() {
        return name().toLowerCase(Locale.ROOT);
    }
}
