/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.text;


import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.text.xmustache.XMustacheTextTemplateEngine;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static java.util.Collections.unmodifiableMap;
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.elasticsearch.watcher.support.Exceptions.illegalArgument;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class TextTemplateTests extends ESTestCase {
    private ScriptServiceProxy proxy;
    private TextTemplateEngine engine;
    private ExecutableScript script;
    private final String lang = "xmustache";

    @Before
    public void init() throws Exception {
        proxy = mock(ScriptServiceProxy.class);
        script = mock(ExecutableScript.class);
        engine = new XMustacheTextTemplateEngine(Settings.EMPTY, proxy);
    }

    public void testRender() throws Exception {
        String templateText = "_template";
        Map<String, Object> params = singletonMap("param_key", "param_val");
        Map<String, Object> model = singletonMap("model_key", "model_val");
        Map<String, Object> merged = new HashMap<>(params);
        merged.putAll(model);
        merged = unmodifiableMap(merged);
        ScriptType type = randomFrom(ScriptType.values());

        when(proxy.executable(new org.elasticsearch.script.Template(templateText, type, lang, null, merged))).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        TextTemplate template = templateBuilder(type, templateText).params(params).build();
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    public void testRenderOverridingModel() throws Exception {
        String templateText = "_template";
        Map<String, Object> params = singletonMap("key", "param_val");
        Map<String, Object> model = singletonMap("key", "model_val");
        ScriptType scriptType = randomFrom(ScriptType.values());

        when(proxy.executable(new org.elasticsearch.script.Template(templateText, scriptType, lang, null, model))).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        TextTemplate template = templateBuilder(scriptType, templateText).params(params).build();
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    public void testRenderDefaults() throws Exception {
        String templateText = "_template";
        Map<String, Object> model = singletonMap("key", "model_val");

        when(proxy.executable(new org.elasticsearch.script.Template(templateText, ScriptType.INLINE, lang, null, model))).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        TextTemplate template = new TextTemplate(templateText);
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    public void testParser() throws Exception {
        ScriptType type = randomScriptType();
        TextTemplate template = templateBuilder(type, "_template").params(singletonMap("param_key", "param_val")).build();
        XContentBuilder builder = jsonBuilder().startObject();
        switch (type) {
            case INLINE:
                builder.field("inline", template.getTemplate());
                break;
            case FILE:
                builder.field("file", template.getTemplate());
                break;
            case INDEXED:
                builder.field("id", template.getTemplate());
        }
        builder.field("params", template.getParams());
        builder.endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        TextTemplate parsed = TextTemplate.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    public void testParserParserSelfGenerated() throws Exception {
        TextTemplate template = templateBuilder(randomScriptType(), "_template").params(singletonMap("param_key", "param_val")).build();

        XContentBuilder builder = jsonBuilder().value(template);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        TextTemplate parsed = TextTemplate.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    public void testParserInvalidUnexpectedField() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("unknown_field", "value")
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        try {
            TextTemplate.parse(parser);
            fail("expected parse exception when encountering an unknown field");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("unexpected field [unknown_field]"));
        }
    }

    public void testParserInvalidUnknownScriptType() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("template", "_template")
                .field("type", "unknown_type")
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        try {
            TextTemplate.parse(parser);
            fail("expected parse exception when script type is unknown");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("unexpected field [template]"));
        }
    }

    public void testParserInvalidMissingText() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("type", ScriptType.INDEXED)
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        try {
            TextTemplate.parse(parser);
            fail("expected parse exception when template text is missing");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), is("unexpected field [type]"));
        }
    }

    private TextTemplate.Builder templateBuilder(ScriptType type, String text) {
        switch (type) {
            case INLINE:    return TextTemplate.inline(text);
            case FILE:      return TextTemplate.file(text);
            case INDEXED:   return TextTemplate.indexed(text);
            default:
                throw illegalArgument("unsupported script type [{}]", type);
        }
    }

    private static ScriptType randomScriptType() {
        return randomFrom(ScriptType.values());
    }
}
