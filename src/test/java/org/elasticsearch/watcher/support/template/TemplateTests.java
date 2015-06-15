/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.bytes.BytesReference;
import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService.ScriptType;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.elasticsearch.watcher.WatcherException;
import org.elasticsearch.watcher.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.watcher.support.template.xmustache.XMustacheTemplateEngine;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 *
 */
public class TemplateTests extends ElasticsearchTestCase {

    private ScriptServiceProxy proxy;
    private TemplateEngine engine;
    private ExecutableScript script;
    private final String lang = "xmustache";

    @Before
    public void init() throws Exception {
        proxy = mock(ScriptServiceProxy.class);
        script = mock(ExecutableScript.class);
        engine = new XMustacheTemplateEngine(Settings.EMPTY, proxy);
    }

    @Test
    public void testRender() throws Exception {
        String templateText = "_template";
        Map<String, Object> params = ImmutableMap.<String, Object>of("param_key", "param_val");
        Map<String, Object> model = ImmutableMap.<String, Object>of("model_key", "model_val");
        Map<String, Object> merged = ImmutableMap.<String, Object>builder().putAll(params).putAll(model).build();
        ScriptType type = randomFrom(ScriptType.values());

        when(proxy.executable(new org.elasticsearch.script.Template(templateText, type, lang, null, merged))).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        Template template = templateBuilder(type, templateText).params(params).build();
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    @Test
    public void testRender_OverridingModel() throws Exception {
        String templateText = "_template";
        Map<String, Object> params = ImmutableMap.<String, Object>of("key", "param_val");
        Map<String, Object> model = ImmutableMap.<String, Object>of("key", "model_val");
        ScriptType scriptType = randomFrom(ScriptType.values());

        when(proxy.executable(new org.elasticsearch.script.Template(templateText, scriptType, lang, null, model))).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        Template template = templateBuilder(scriptType, templateText).params(params).build();
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    @Test
    public void testRender_Defaults() throws Exception {
        String templateText = "_template";
        Map<String, Object> model = ImmutableMap.<String, Object>of("key", "model_val");

        when(proxy.executable(new org.elasticsearch.script.Template(templateText, ScriptType.INLINE, lang, null, model))).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        Template template = new Template(templateText);
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    @Test @Repeat(iterations = 5)
    public void testParser() throws Exception {
        ScriptType type = randomScriptType();
        Template template = templateBuilder(type, "_template").params(ImmutableMap.<String, Object>of("param_key", "param_val")).build();
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
        Template parsed = Template.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    @Test
    public void testParser_ParserSelfGenerated() throws Exception {
        Template template = templateBuilder(randomScriptType(), "_template").params(ImmutableMap.<String, Object>of("param_key", "param_val")).build();

        XContentBuilder builder = jsonBuilder().value(template);
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        Template parsed = Template.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    @Test(expected = Template.ParseException.class)
    public void testParser_Invalid_UnexpectedField() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("unknown_field", "value")
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        Template.parse(parser);
        fail("expected parse exception when encountering an unknown field");
    }

    @Test(expected = Template.ParseException.class)
    public void testParser_Invalid_UnknownScriptType() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("template", "_template")
                .field("type", "unknown_type")
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        Template.parse(parser);
        fail("expected parse exception when script type is unknown");
    }

    @Test(expected = Template.ParseException.class)
    public void testParser_Invalid_MissingText() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                .field("type", ScriptType.INDEXED)
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        Template.parse(parser);
        fail("expected parse exception when template text is missing");
    }

    private Template.Builder templateBuilder(ScriptType type, String text) {
        switch (type) {
            case INLINE:    return Template.inline(text);
            case FILE:      return Template.file(text);
            case INDEXED:   return Template.indexed(text);
            default:
                throw new WatcherException("unsupported script type [{}]", type);
        }
    }

    private static ScriptType randomScriptType() {
        return randomFrom(ScriptType.values());
    }
}
