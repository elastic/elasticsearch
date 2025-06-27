/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.script;

import org.elasticsearch.core.TimeValue;

import java.util.Map;

/**
 * A script used by {@link org.elasticsearch.ingest.ConditionalProcessor}.
 * To properly expose the {@link SourceMapFieldScript#field(String)} API, make sure to provide a valid {@link CtxMap} before execution
 * through the {@link CtxMapWrapper} passed to the constructor and make sure to clear it after use to avoid leaks.
 */
public abstract class IngestConditionalScript extends SourceMapFieldScript {

    public static final String[] PARAMETERS = { "runtime-params" };

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

    public IngestConditionalScript(Map<String, Object> params, CtxMapWrapper ctxMapWrapper) {
        super(ctxMapWrapper);
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract boolean execute(Map<String, Object> params);

    public interface Factory {
        IngestConditionalScript newInstance(Map<String, Object> params, CtxMapWrapper ctxMapWrapper);
    }
}
