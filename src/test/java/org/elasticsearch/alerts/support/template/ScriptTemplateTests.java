/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.alerts.support.template;

import org.elasticsearch.alerts.support.Script;
import org.elasticsearch.alerts.support.init.proxy.ScriptServiceProxy;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ElasticsearchTestCase;
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
public class ScriptTemplateTests extends ElasticsearchTestCase {

    private ScriptServiceProxy proxy;
    private ExecutableScript script;

    @Before
    public void init() throws Exception {
        proxy = mock(ScriptServiceProxy.class);
        script = mock(ExecutableScript.class);
    }

    @Test
    public void testRender() throws Exception {
        String lang = "_lang";
        String templateText = "_template";
        Map<String, Object> params = ImmutableMap.<String, Object>of("param_key", "param_val");
        Map<String, Object> model = ImmutableMap.<String, Object>of("model_key", "model_val");
        Map<String, Object> merged = ImmutableMap.<String, Object>builder().putAll(params).putAll(model).build();
        ScriptService.ScriptType scriptType = ScriptService.ScriptType.values()[randomIntBetween(0, ScriptService.ScriptType.values().length - 1)];

        when(script.run()).thenReturn("rendered_text");
        when(proxy.executable(lang, templateText, scriptType, merged)).thenReturn(script);

        Script script = new Script(templateText, scriptType, lang, params);
        ScriptTemplate template = new ScriptTemplate(proxy, script);
        assertThat(template.render(model), is("rendered_text"));
    }

    @Test
    public void testRender_OverridingModel() throws Exception {
        String lang = "_lang";
        String templateText = "_template";
        Map<String, Object> params = ImmutableMap.<String, Object>of("key", "param_val");
        Map<String, Object> model = ImmutableMap.<String, Object>of("key", "model_val");
        ScriptService.ScriptType scriptType = randomScriptType();


        when(script.run()).thenReturn("rendered_text");
        when(proxy.executable(lang, templateText, scriptType, model)).thenReturn(script);

        Script script = new Script(templateText, scriptType, lang, params);
        ScriptTemplate template = new ScriptTemplate(proxy, script);
        assertThat(template.render(model), is("rendered_text"));
    }

    @Test
    public void testRender_Defaults() throws Exception {
        String templateText = "_template";
        Map<String, Object> model = ImmutableMap.<String, Object>of("key", "model_val");

        when(script.run()).thenReturn("rendered_text");
        when(proxy.executable(ScriptTemplate.DEFAULT_LANG, templateText, ScriptService.ScriptType.INLINE, model)).thenReturn(script);

        ScriptTemplate template = new ScriptTemplate(proxy, templateText);
        assertThat(template.render(model), is("rendered_text"));
    }

    @Test
    public void testParser() throws Exception {
        ScriptTemplate.Parser templateParser = new ScriptTemplate.Parser(ImmutableSettings.EMPTY, proxy);

        Script script = new Script("_template", randomScriptType(), "_lang", ImmutableMap.<String, Object>of("param_key", "param_val"));
        ScriptTemplate template = new ScriptTemplate(proxy, script);

        XContentBuilder builder = jsonBuilder().startObject()
                .field(randomFrom("lang"), template.script().lang())
                .field(randomFrom("script"), template.script().script())
                .field(randomFrom("type"), template.script().type().name())
                .field(randomFrom("params"), template.script().params())
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        ScriptTemplate parsed = templateParser.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    @Test
    public void testParser_ParserSelfGenerated() throws Exception {
        ScriptTemplate.Parser templateParser = new ScriptTemplate.Parser(ImmutableSettings.EMPTY, proxy);

        Script script = new Script("_template", randomScriptType(), "_lang", ImmutableMap.<String, Object>of("param_key", "param_val"));
        ScriptTemplate template = new ScriptTemplate(proxy, script);

        XContentBuilder builder = jsonBuilder().value(template);
        BytesReference bytes = builder.bytes();
        System.out.println(bytes.toUtf8());
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        ScriptTemplate parsed = templateParser.parse(parser);
        assertThat(parsed, notNullValue());
        assertThat(parsed, equalTo(template));
    }

    @Test(expected = Template.Parser.ParseException.class)
    public void testParser_Invalid_UnexpectedField() throws Exception {
        ScriptTemplate.Parser templateParser = new ScriptTemplate.Parser(ImmutableSettings.EMPTY, proxy);

        XContentBuilder builder = jsonBuilder().startObject()
                .field("unknown_field", "value")
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        templateParser.parse(parser);
        fail("expected parse exception when encountering an unknown field");
    }

    @Test(expected = Template.Parser.ParseException.class)
    public void testParser_Invalid_UnknownScriptType() throws Exception {
        ScriptTemplate.Parser templateParser = new ScriptTemplate.Parser(ImmutableSettings.EMPTY, proxy);

        XContentBuilder builder = jsonBuilder().startObject()
                .field("lang", ScriptTemplate.DEFAULT_LANG)
                .field("script", "_template")
                .field("type", "unknown_type")
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        templateParser.parse(parser);
        fail("expected parse exception when script type is unknown");
    }

    @Test(expected = Template.Parser.ParseException.class)
    public void testParser_Invalid_MissingScript() throws Exception {
        ScriptTemplate.Parser templateParser = new ScriptTemplate.Parser(ImmutableSettings.EMPTY, proxy);

        XContentBuilder builder = jsonBuilder().startObject()
                .field("lang", ScriptTemplate.DEFAULT_LANG)
                .field("type", ScriptService.ScriptType.INDEXED)
                .startObject("params").endObject()
                .endObject();
        BytesReference bytes = builder.bytes();
        XContentParser parser = JsonXContent.jsonXContent.createParser(bytes);
        parser.nextToken();
        templateParser.parse(parser);
        fail("expected parse exception when template text is missing");
    }

    private static ScriptService.ScriptType randomScriptType() {
        return randomFrom(ScriptService.ScriptType.values());
    }
}
