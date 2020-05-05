
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

import org.apache.logging.log4j.LogManager;
import org.elasticsearch.common.logging.DeprecationLogger;

import java.util.Map;
import java.util.function.Function;

/**
 * An update script.
 */
public abstract class UpdateScript {

    private static final DeprecationLogger deprecationLogger =
            new DeprecationLogger(LogManager.getLogger(DynamicMap.class));
    private static final Map<String, Function<Object, Object>> PARAMS_FUNCTIONS = org.elasticsearch.common.collect.Map.of(
            "_type", value -> {
                deprecationLogger.deprecatedAndMaybeLog("update-script",
                        "[types removal] Looking up doc types [_type] in scripts is deprecated.");
                return value;
            });

    public static final String[] PARAMETERS = { };

    /** The context used to compile {@link UpdateScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("update", Factory.class);

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    /** The update context for the script. */
    private final Map<String, Object> ctx;

    public UpdateScript(Map<String, Object> params, Map<String, Object> ctx) {
        this.params = params;
        this.ctx = new DynamicMap(ctx, PARAMS_FUNCTIONS);
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    /** Return the update context for this script. */
    public Map<String, Object> getCtx() {
        return ctx;
    }

    public abstract void execute();

    public interface Factory {
        UpdateScript newInstance(Map<String, Object> params, Map<String, Object> ctx);
    }
}
