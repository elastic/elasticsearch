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

package org.elasticsearch.search.aggregations.pipeline;

import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptFactory;

import java.util.Map;

/**
 * This class provides a custom script context for the Moving Function pipeline aggregation,
 * so that we can expose a number of pre-baked moving functions like min, max, movavg, etc
 */
public abstract class MovingFunctionScript {
    /**
     * @param params The user-provided parameters
     * @param values The values in the window that we are moving a function across
     * @return A double representing the value from this particular window
     */
    public abstract double execute(Map<String, Object> params, double[] values);

    public interface Factory extends ScriptFactory {
        MovingFunctionScript newInstance();
    }

    public static final String[] PARAMETERS = new String[] {"params", "values"};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("moving-function", Factory.class);
}
