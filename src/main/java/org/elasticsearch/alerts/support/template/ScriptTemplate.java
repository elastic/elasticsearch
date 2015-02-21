/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.template;

import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.base.Function;
import org.elasticsearch.common.base.Joiner;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.Collections2;
import org.elasticsearch.common.collect.ImmutableList;
import org.elasticsearch.common.collect.ImmutableMap;
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
import java.util.Locale;
import java.util.Map;

/**
 *
 */
public class ScriptTemplate implements ToXContent, Template {

    static final ParseField TEXT_FIELD = new ParseField("script", "text");
    static final ParseField LANG_FIELD = new ParseField("lang", "language", "script_lang");
    static final ParseField TYPE_FIELD = new ParseField("type", "script_type");
    static final ParseField PARAMS_FIELD = new ParseField("model", "params");

    public static final String DEFAULT_LANG = "mustache";

    private final String text;
    private final String lang;
    private final ScriptService.ScriptType type;
    private final Map<String, Object> params;
    private final ScriptServiceProxy service;

    public ScriptTemplate(ScriptServiceProxy service, String text) {
        this(service, text, DEFAULT_LANG, ScriptService.ScriptType.INLINE, Collections.<String, Object>emptyMap());
    }

    public ScriptTemplate(ScriptServiceProxy service, String text, String lang, ScriptService.ScriptType type, Map<String, Object> params) {
        this.service = service;
        this.text = text;
        this.lang = lang;
        this.type = type;
        this.params = params;
    }

    public String text() {
        return text;
    }

    public ScriptService.ScriptType type() {
        return type;
    }

    public String lang() {
        return lang;
    }

    public Map<String, Object> params() {
        return params;
    }

    @Override
    public String render(Map<String, Object> model) {
        Map<String, Object> mergedModel = new HashMap<>();
        mergedModel.putAll(params);
        mergedModel.putAll(model);
        ExecutableScript script = service.executable(lang, text, type, mergedModel);
        Object result = script.run();
        if (result instanceof BytesReference) {
            return ((BytesReference) script.run()).toUtf8();
        }
        return result.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        ScriptTemplate template = (ScriptTemplate) o;

        if (!lang.equals(template.lang)) return false;
        if (!params.equals(template.params)) return false;
        if (!text.equals(template.text)) return false;
        if (type != template.type) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = text.hashCode();
        result = 31 * result + lang.hashCode();
        result = 31 * result + type.hashCode();
        result = 31 * result + params.hashCode();
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject()
                .field(TEXT_FIELD.getPreferredName(), text)
                .field(TYPE_FIELD.getPreferredName(), type.name().toLowerCase(Locale.ROOT))
                .field(LANG_FIELD.getPreferredName(), lang)
                .field(PARAMS_FIELD.getPreferredName(), this.params)
                .endObject();
    }

    /**
     */
    public static class Parser extends AbstractComponent implements Template.Parser<ScriptTemplate> {

        private final ScriptServiceProxy scriptService;

        @Inject
        public Parser(Settings settings, ScriptServiceProxy scriptService) {
            super(settings);
            this.scriptService = scriptService;
        }

        @Override
        public ScriptTemplate parse(XContentParser parser) throws IOException {
            assert parser.currentToken() == XContentParser.Token.START_OBJECT : "Expected START_OBJECT, but was " + parser.currentToken();

            String text = null;
            ScriptService.ScriptType type = ScriptService.ScriptType.INLINE;
            String lang = DEFAULT_LANG;
            ImmutableMap.Builder<String, Object> params = ImmutableMap.builder();

            String currentFieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    currentFieldName = parser.currentName();
                } else if (TEXT_FIELD.match(currentFieldName)) {
                    if (token.isValue()) {
                        text = parser.text();
                    } else {
                        throw new ParseException("expected a string value for [" + currentFieldName + "], but found [" + token + "] instead");
                    }
                } else if (LANG_FIELD.match(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        lang = parser.text();
                    } else {
                        throw new ParseException("expected a string value for [" + currentFieldName + "], but found [" + token + "] instead");
                    }
                } else if (TYPE_FIELD.match(currentFieldName)) {
                    if (token == XContentParser.Token.VALUE_STRING) {
                        String value = parser.text();
                        try {
                            type = ScriptService.ScriptType.valueOf(value.toUpperCase(Locale.ROOT));
                        } catch (IllegalArgumentException iae) {
                            String typeOptions = Joiner.on(",").join(Collections2.transform(ImmutableList.copyOf(ScriptService.ScriptType.values()), new Function<ScriptService.ScriptType, String>() {
                                @Override
                                public String apply(ScriptService.ScriptType scriptType) {
                                    return scriptType.name().toLowerCase(Locale.ROOT);
                                }
                            }));
                            throw new ParseException("unknown template script type [" + currentFieldName + "]. type can only be on of: [" + typeOptions + "]");
                        }
                    }
                } else if (PARAMS_FIELD.match(currentFieldName)) {
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new ParseException("expected an object for [" + currentFieldName + "], but found [" + token + "]");
                    }
                    params.putAll(parser.map());
                } else {
                    throw new ParseException("unexpected field [" + currentFieldName + "]");
                }
            }
            if (text == null) {
                throw new ParseException("missing required field [" + TEXT_FIELD.getPreferredName() + "]");
            }
            return new ScriptTemplate(scriptService, text, lang, type, params.build());
        }
    }

}
