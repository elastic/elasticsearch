/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support;


import org.elasticsearch.ElasticsearchIllegalArgumentException;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 */
public class StringTemplateUtils extends AbstractComponent {

    private final ScriptServiceProxy scriptService;

    @Inject
    public StringTemplateUtils(Settings settings, ScriptServiceProxy scriptService) {
        super(settings);
        this.scriptService = scriptService;
    }

    public String executeTemplate(Template template) {
        return executeTemplate(template, Collections.<String, Object>emptyMap());
    }

    public String executeTemplate(Template template, Map<String, Object> additionalParams) {
        Map<String, Object> params = new HashMap<>();
        params.putAll(template.getParams());
        params.putAll(additionalParams);
        ExecutableScript script = scriptService.executable(template.getLanguage(), template.getTemplate(), template.getScriptType(), params);
        Object result = script.run();
        if (result instanceof String) {
            return (String) result;
        } else if (result instanceof BytesReference) {
            return ((BytesReference) script.run()).toUtf8();
        } else {
            return result.toString();
        }
    }
    public static Template readTemplate(XContentParser parser) throws IOException {
        assert parser.currentToken() == XContentParser.Token.START_OBJECT : "Expected START_OBJECT, but was " + parser.currentToken();
        Map<String, String> params = null;
        String script = null;
        ScriptService.ScriptType type = ScriptService.ScriptType.INLINE;
        String language = "mustache";
        String fieldName = parser.currentName();
        for (XContentParser.Token token = parser.nextToken(); token != XContentParser.Token.END_OBJECT; token = parser.nextToken()) {
            switch (token) {
                case FIELD_NAME:
                    fieldName = parser.currentName();
                    break;
                case START_OBJECT:
                    switch (fieldName) {
                        case "params":
                            params = (Map) parser.map();
                            break;
                        default:
                            throw new ElasticsearchIllegalArgumentException("Unexpected field [" + fieldName + "]");
                    }
                    break;
                case VALUE_STRING:
                    switch (fieldName) {
                        case "script":
                            script = parser.text();
                            break;
                        case "language":
                            language = parser.text();
                            break;
                        case "type":
                            type = AlertUtils.readScriptType(parser.text());
                            break;
                        default:
                            throw new ElasticsearchIllegalArgumentException("Unexpected field [" + fieldName + "]");
                    }
                    break;
                default:
                    throw new ElasticsearchIllegalArgumentException("Unexpected json token [" + token + "]");
            }
        }
        return new Template(script, params, language, type);
    }

    public static void writeTemplate(String objectName, Template template, XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject(objectName);
        builder.field("script", template.getTemplate());
        builder.field("type", AlertUtils.writeScriptType(template.getScriptType()));
        builder.field("language", template.getLanguage());
        if (template.getParams() != null && !template.getParams().isEmpty()) {
            builder.field("params", template.getParams());
        }
        builder.endObject();
    }

    public static class Template {
        private final String template;
        private final Map<String, String> params;
        private final String language;
        private final ScriptService.ScriptType scriptType;
        public Template(String template) {
            this.template = template;
            this.params = Collections.emptyMap();
            this.language = "mustache";
            this.scriptType = ScriptService.ScriptType.INLINE;
        }
        public Template(String template, Map<String, String> params, String language, ScriptService.ScriptType scriptType) {
            this.template = template;
            this.params = params;
            this.language = language;
            this.scriptType = scriptType;
        }
        public ScriptService.ScriptType getScriptType() {
            return scriptType;
        }
        public String getTemplate() {
            return template;
        }
        public String getLanguage() {
            return language;
        }
        public Map<String, String> getParams() {
            return params;
        }


        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Template template1 = (Template) o;

            if (language != null ? !language.equals(template1.language) : template1.language != null) return false;
            if (params != null ? !params.equals(template1.params) : template1.params != null) return false;
            if (scriptType != template1.scriptType) return false;
            if (template != null ? !template.equals(template1.template) : template1.template != null) return false;

            return true;
        }

        @Override
        public int hashCode() {
            int result = template != null ? template.hashCode() : 0;
            result = 31 * result + (params != null ? params.hashCode() : 0);
            result = 31 * result + (language != null ? language.hashCode() : 0);
            result = 31 * result + (scriptType != null ? scriptType.hashCode() : 0);
            return result;
        }
    }
}

