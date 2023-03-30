/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.application.search;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.application.search.action.QuerySearchApplicationAction;
import org.elasticsearch.xpack.application.search.action.QuerySearchApplicationAction.Request;

import java.io.IOException;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Search template included in a {@link SearchApplication}. It will be used for searching using the
 * {@link QuerySearchApplicationAction}, overriding the parameters included on it via {@link Request}
 */
public class SearchApplicationTemplate implements ToXContentObject, Writeable {
    private final Script script;

    private static final ParseField TEMPLATE_SCRIPT_FIELD = new ParseField("script");
    public static final ParseField DICTIONARY_FIELD = new ParseField("dictionary");

    private static final ConstructingObjectParser<SearchApplicationTemplate, Void> PARSER = new ConstructingObjectParser<>(
        "search_template",
        p -> new SearchApplicationTemplate((Script) p[0], (TemplateParamValidator) p[1])
    );

    static {
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> Script.parse(p, Script.DEFAULT_TEMPLATE_LANG), TEMPLATE_SCRIPT_FIELD);
        PARSER.declareObject(optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = XContentFactory.jsonBuilder();
            return new TemplateParamValidator(builder.copyCurrentStructure(p));
        }, DICTIONARY_FIELD);
    }

    private final TemplateParamValidator templateParamValidator;

    public SearchApplicationTemplate(StreamInput in) throws IOException {
        this.script = in.readOptionalWriteable(Script::new);
        this.templateParamValidator = in.readOptionalWriteable(TemplateParamValidator::new);
    }

    public SearchApplicationTemplate(Script script, TemplateParamValidator templateParamValidator) {
        if (script != null && script.getLang() != null) {
            if (MustacheScriptEngine.NAME.equals(script.getLang()) == false) {
                throw new IllegalArgumentException("only [" + MustacheScriptEngine.NAME + "] scripting language is supported");
            }
        }
        this.script = script;

        this.templateParamValidator = templateParamValidator;
    }

    public static SearchApplicationTemplate parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (script != null) {
            builder.field(TEMPLATE_SCRIPT_FIELD.getPreferredName(), script);
        }
        if (templateParamValidator != null) {
            builder.field(DICTIONARY_FIELD.getPreferredName(), templateParamValidator);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(script);
        out.writeOptionalWriteable(templateParamValidator);
    }

    @Override
    public int hashCode() {
        return Objects.hash(script, templateParamValidator);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchApplicationTemplate template = (SearchApplicationTemplate) o;
        return Objects.equals(script, template.script) && Objects.equals(templateParamValidator, template.templateParamValidator);
    }

    public Script script() {
        return script;
    }
}
