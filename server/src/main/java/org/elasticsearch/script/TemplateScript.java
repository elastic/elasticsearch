/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script;

import org.elasticsearch.common.util.CachedSupplier;
import org.elasticsearch.core.TimeValue;

import java.util.Collections;
import java.util.Map;
import java.util.function.Supplier;

/**
 * A string template rendered as a script.
 */
public abstract class TemplateScript {

    private final Supplier<Map<String, Object>> params;

    public TemplateScript() {
        this.params = Collections::emptyMap;
    }

    public TemplateScript(Map<String, Object> params) {
        this.params = () -> params;
    }

    public TemplateScript(Supplier<Map<String, Object>> params) {
        this.params = new CachedSupplier<>(params);
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params.get();
    }

    public static final String[] PARAMETERS = {};
    /** Run a template and return the resulting string, encoded in utf8 bytes. */
    public abstract String execute();

    public interface Factory {
        TemplateScript newInstance(Supplier<Map<String, Object>> params);
    }

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("template", Factory.class);

    // Remove compilation rate limit for ingest.  Ingest pipelines may use many mustache templates, triggering compilation
    // rate limiting.  MustacheScriptEngine explicitly checks for TemplateScript.  Rather than complicating the implementation there by
    // creating a new Script class (as would be customary), this context is used to avoid the default rate limit.
    public static final ScriptContext<Factory> INGEST_CONTEXT = new ScriptContext<>("ingest_template", Factory.class,
            200, TimeValue.timeValueMillis(0), ScriptCache.UNLIMITED_COMPILATION_RATE.asTuple(), true);
}
