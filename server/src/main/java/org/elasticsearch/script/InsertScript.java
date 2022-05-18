
/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.core.TimeValue;
import org.elasticsearch.script.field.Op;

import java.util.Map;

/**
 * Insertions done via scripted upsert
 *
 * Metadata available: timestamp and the {@link Op}
 * Op must be "create" or "none"
 */
public abstract class InsertScript {

    public static final String[] PARAMETERS = { "ctx" };

    /** The context used to compile {@link InsertScript} factories. */
    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>(
        "insert",
        Factory.class,
        200,
        TimeValue.timeValueMillis(0),
        false,
        true
    );

    /** The generic runtime parameters for the script. */
    private final Map<String, Object> params;

    public InsertScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public abstract void execute(Map<String, Object> ctx);

    public interface Factory {
        InsertScript newInstance(Map<String, Object> params);
    }
}
