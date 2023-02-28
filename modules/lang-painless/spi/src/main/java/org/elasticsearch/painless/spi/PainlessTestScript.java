/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.painless.spi;

import org.elasticsearch.script.ScriptContext;

import java.util.Map;

/**
 * Generic "test" context used by the painless execute REST API
 * for testing painless scripts.
 */
public abstract class PainlessTestScript {
    private final Map<String, Object> params;

    public PainlessTestScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract Object execute();

    public interface Factory {
        PainlessTestScript newInstance(Map<String, Object> params);
    }

    public static final String[] PARAMETERS = {};
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("painless_test", Factory.class);
}
