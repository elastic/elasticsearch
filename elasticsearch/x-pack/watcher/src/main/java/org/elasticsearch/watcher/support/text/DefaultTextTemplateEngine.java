/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.text;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.Template;
import org.elasticsearch.watcher.support.ScriptServiceProxy;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class DefaultTextTemplateEngine extends AbstractComponent implements TextTemplateEngine {

    private final ScriptServiceProxy service;

    @Inject
    public DefaultTextTemplateEngine(Settings settings, ScriptServiceProxy service) {
        super(settings);
        this.service = service;
    }

    @Override
    public String render(TextTemplate template, Map<String, Object> model) {
        if (template == null) {
            return null;
        }

        XContentType contentType = detectContentType(template);
        Map<String, String> compileParams = compileParams(contentType);
        template = trimContentType(template);

        CompiledScript compiledScript = service.compile(convert(template, model), compileParams);
        ExecutableScript executable = service.executable(compiledScript, model);
        Object result = executable.run();
        if (result instanceof BytesReference) {
            return ((BytesReference) result).toUtf8();
        }
        return result.toString();
    }

    private TextTemplate trimContentType(TextTemplate textTemplate) {
        String template = textTemplate.getTemplate();
        if (!template.startsWith("__")){
            return textTemplate; //Doesn't even start with __ so can't have a content type
        }
        // There must be a __<content_type__:: prefix so the minimum length before detecting '__::' is 3
        int index = template.indexOf("__::", 3);
        // Assume that the content type name is less than 10 characters long otherwise we may falsely detect strings that start with '__
        // and have '__::' somewhere in the content
        if (index >= 0 && index < 12) {
            if (template.length() == 6) {
                template = "";
            } else {
                template = template.substring(index + 4);
            }
        }
        return new TextTemplate(template, textTemplate.getContentType(), textTemplate.getType(), textTemplate.getParams());
    }

    private XContentType detectContentType(TextTemplate textTemplate) {
        String template = textTemplate.getTemplate();
        if (template.startsWith("__")) {
            //There must be a __<content_type__:: prefix so the minimum length before detecting '__::' is 3
            int endOfContentName = template.indexOf("__::", 3);
            if (endOfContentName != -1) {
                return XContentType.fromMediaTypeOrFormat(template.substring(2, endOfContentName));
            }
        }
        return null;
    }

    private Template convert(TextTemplate textTemplate, Map<String, Object> model) {
        Map<String, Object> mergedModel = new HashMap<>();
        mergedModel.putAll(textTemplate.getParams());
        mergedModel.putAll(model);
        return new Template(textTemplate.getTemplate(), textTemplate.getType(), "mustache", textTemplate.getContentType(), mergedModel);
    }

    private Map<String, String> compileParams(XContentType contentType) {
        if (contentType == XContentType.JSON) {
            return Collections.singletonMap("content_type", "application/json");
        } else {
            return Collections.singletonMap("content_type", "text/plain");
        }
    }
}
