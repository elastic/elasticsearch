/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import java.util.Map;

/**
 * A script used in significant terms heuristic scoring.
 */
public abstract class SignificantTermsHeuristicScoreScript {

    public static final String[] PARAMETERS = { "params" };

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("script_heuristic", Factory.class);

    public abstract double execute(Map<String, Object> params);

    public interface Factory extends ScriptFactory {
        SignificantTermsHeuristicScoreScript newInstance();
    }
}
