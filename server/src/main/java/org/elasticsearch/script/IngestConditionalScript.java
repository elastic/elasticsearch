/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.core.TimeValue;

import java.util.Map;

/**
 * A script used by {@link org.elasticsearch.ingest.ConditionalProcessor}.
 */
public abstract class IngestConditionalScript {

    public static final String[] PARAMETERS = { "ctx" };

    /** The context used to compile {@link IngestConditionalScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "processor_conditional",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public IngestConditionalScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract boolean execute(Map<String, Object> ctx);

    public interface Factory {
        IngestConditionalScript newInstance(Map<String, Object> params);
    }
}
