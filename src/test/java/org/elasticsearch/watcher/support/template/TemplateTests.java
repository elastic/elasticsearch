/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchTestCase;
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
        engine = new XMustacheTemplateEngine(ImmutableSettings.EMPTY, proxy);
    }

    @Test
    public void testRender() throws Exception {
        String templateText = "_template";
        Map<String, Object> params = ImmutableMap.<String, Object>of("param_key", "param_val");
        Map<String, Object> model = ImmutableMap.<String, Object>of("model_key", "model_val");
        Map<String, Object> merged = ImmutableMap.<String, Object>builder().putAll(params).putAll(model).build();
        ScriptService.ScriptType scriptType = ScriptService.ScriptType.values()[randomIntBetween(0, ScriptService.ScriptType.values().length - 1)];

        when(proxy.executable(lang, templateText, scriptType, merged)).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        Template template = new Template(templateText, scriptType, params);
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    @Test
    public void testRender_OverridingModel() throws Exception {
        String templateText = "_template";
        Map<String, Object> params = ImmutableMap.<String, Object>of("key", "param_val");
        Map<String, Object> model = ImmutableMap.<String, Object>of("key", "model_val");
        ScriptService.ScriptType scriptType = randomScriptType();


        when(proxy.executable(lang, templateText, scriptType, model)).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        Template template = new Template(templateText, scriptType, params);
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    @Test
    public void testRender_Defaults() throws Exception {
        String templateText = "_template";
        Map<String, Object> model = ImmutableMap.<String, Object>of("key", "model_val");

        when(proxy.executable(lang, templateText, ScriptService.ScriptType.INLINE, model)).thenReturn(script);
        when(script.run()).thenReturn("rendered_text");

        Template template = new Template(templateText);
        assertThat(engine.render(template, model), is("rendered_text"));
    }

    @Test
    public void testParser() throws Exception {
        Template template = new Template("_template", randomScriptType(), ImmutableMap.<String, Object>of("param_key", "param_val"));

        XContentBuilder builder = jsonBuilder().startObject()
                .field(randomFrom("template"), template.getTemplate())
                .field(randomFrom("type"), template.getType().name())
                .field(randomFrom("params"), template.getParams())
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        Template parsed = Template.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    @Test
    public void testParser_ParserSelfGenerated() throws Exception {
        Template template = new Template("_template", randomScriptType(), ImmutableMap.<String, Object>of("param_key", "param_val"));

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
                .field("type", ScriptService.ScriptType.INDEXED)
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        Template.parse(parser);
        fail("expected parse exception when template text is missing");
    }

    private static ScriptService.ScriptType randomScriptType() {
        return randomFrom(ScriptService.ScriptType.values());
    }
}
