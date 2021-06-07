/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.support;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Utility class for evaluating Mustache templates at runtime.
 */
public final class MustacheTemplateEvaluator {

    private MustacheTemplateEvaluator() {
        throw new UnsupportedOperationException("Cannot construct " + MustacheTemplateEvaluator.class);
    }

    public static Script parseForScript(XContentParser parser, Map<String, Object> extraParams) throws IOException {
        Script script = Script.parse(parser);
        // Add the user details to the params
        Map<String, Object> params = new HashMap<>();
        if (script.getParams() != null) {
            params.putAll(script.getParams());
        }
        extraParams.forEach(params::put);
        // Always enforce mustache script lang:
        script = new Script(script.getType(), script.getType() == ScriptType.STORED ? null : "mustache", script.getIdOrCode(),
                script.getOptions(), params);
        return script;
    }

    public static String evaluate(ScriptService scriptService, XContentParser parser, Map<String, Object> extraParams) throws IOException {
        Script script = parseForScript(parser, extraParams);
        TemplateScript compiledTemplate = scriptService.compile(script, TemplateScript.CONTEXT).newInstance(script.getParams());
        return compiledTemplate.execute();
    }
}
