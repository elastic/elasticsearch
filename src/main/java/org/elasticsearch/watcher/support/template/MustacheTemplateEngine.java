/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;

import java.util.HashMap;
import java.util.Map;

/**
 *
 */
public class MustacheTemplateEngine extends AbstractComponent implements TemplateEngine {

    private final ScriptServiceProxy service;

    @Inject
    public MustacheTemplateEngine(Settings settings, ScriptServiceProxy service) {
        super(settings);
        this.service = service;
    }

    @Override
    public String render(Template template, Map<String, Object> model) {
        Map<String, Object> mergedModel = new HashMap<>();
        mergedModel.putAll(template.getParams());
        mergedModel.putAll(model);
        ExecutableScript executable = service.executable(MustacheScriptEngineService.NAME, template.getTemplate(), template.getType(), mergedModel);
        Object result = executable.run();
        if (result instanceof BytesReference) {
            return ((BytesReference) result).toUtf8();
        }
        return result.toString();
    }
}
