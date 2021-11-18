/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
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

    public static final String[] PARAMETERS = new String[] { "params", "values" };
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("moving-function", Factory.class);
}
