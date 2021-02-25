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
 * A string template rendered as a script.
 */
public abstract class TemplateScript {

    private final Map<String, Object> params;

    public TemplateScript(Map<String, Object> params) {
        this.params = params;
    }

    /** Return the parameters for this script. */
    public Map<String, Object> getParams() {
        return params;
    }

    public static final String[] PARAMETERS = {};
    /** Run a template and return the resulting string, encoded in utf8 bytes. */
    public abstract String execute();

    public interface Factory {
        TemplateScript newInstance(Map<String, Object> params);
    }

    public static final ScriptContext<Factory> CONTEXT = new ScriptContext<>("template", Factory.class);
}
