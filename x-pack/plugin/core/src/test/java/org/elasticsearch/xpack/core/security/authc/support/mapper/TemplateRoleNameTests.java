/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authc.support.mapper;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.ByteBufferStreamInput;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.ScriptMetadata;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.StoredScriptSource;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.EqualsHashCodeTestUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xpack.core.security.authc.support.mapper.TemplateRoleName.Format;
import org.elasticsearch.xpack.core.security.authc.support.mapper.expressiondsl.ExpressionModel;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TemplateRoleNameTests extends ESTestCase {

    public void testParseRoles() throws Exception {
        final TemplateRoleName role1 = parse("""
            { "template": { "source": "_user_{{username}}" } }""");
        assertThat(role1, Matchers.instanceOf(TemplateRoleName.class));
        assertThat(role1.getTemplate().utf8ToString(), equalTo("""
            {"source":"_user_{{username}}"}"""));
        assertThat(role1.getFormat(), equalTo(Format.STRING));

        final TemplateRoleName role2 = parse("""
            { "template": "{\\"source\\":\\"{{#tojson}}groups{{/tojson}}\\"}", "format":"json" }""");
        assertThat(role2, Matchers.instanceOf(TemplateRoleName.class));
        assertThat(role2.getTemplate().utf8ToString(), equalTo("""
            {"source":"{{#tojson}}groups{{/tojson}}"}"""));
        assertThat(role2.getFormat(), equalTo(Format.JSON));
    }

    public void testToXContent() throws Exception {
        final String json = """
            {"template":"{\\"source\\":\\"%s\\"}","format":"%s"}\
            """.formatted(randomAlphaOfLengthBetween(8, 24), randomFrom(Format.values()).formatName());
        assertThat(Strings.toString(parse(json)), equalTo(json));
    }

    public void testSerializeTemplate() throws Exception {
        trySerialize(new TemplateRoleName(new BytesArray(randomAlphaOfLengthBetween(12, 60)), randomFrom(Format.values())));
    }

    public void testEqualsAndHashCode() throws Exception {
        tryEquals(new TemplateRoleName(new BytesArray(randomAlphaOfLengthBetween(12, 60)), randomFrom(Format.values())));
    }

    public void testEvaluateRoles() throws Exception {
        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        final ExpressionModel model = new ExpressionModel();
        model.defineField("username", "hulk");
        model.defineField("groups", Arrays.asList("avengers", "defenders", "panthenon"));

        final TemplateRoleName plainString = new TemplateRoleName(new BytesArray("""
            { "source":"heroes" }"""), Format.STRING);
        assertThat(plainString.getRoleNames(scriptService, model), contains("heroes"));

        final TemplateRoleName user = new TemplateRoleName(new BytesArray("""
            { "source":"_user_{{username}}" }"""), Format.STRING);
        assertThat(user.getRoleNames(scriptService, model), contains("_user_hulk"));

        final TemplateRoleName groups = new TemplateRoleName(new BytesArray("""
            { "source":"{{#tojson}}groups{{/tojson}}" }"""), Format.JSON);
        assertThat(groups.getRoleNames(scriptService, model), contains("avengers", "defenders", "panthenon"));
    }

    private TemplateRoleName parse(String json) throws IOException {
        final XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, json);
        final TemplateRoleName role = TemplateRoleName.parse(parser);
        assertThat(role, notNullValue());
        return role;
    }

    public void trySerialize(TemplateRoleName original) throws Exception {
        BytesStreamOutput output = new BytesStreamOutput();
        original.writeTo(output);

        final StreamInput rawInput = ByteBufferStreamInput.wrap(BytesReference.toBytes(output.bytes()));
        final TemplateRoleName serialized = new TemplateRoleName(rawInput);
        assertEquals(original, serialized);
    }

    public void tryEquals(TemplateRoleName original) {
        final EqualsHashCodeTestUtils.CopyFunction<TemplateRoleName> copy = rmt -> new TemplateRoleName(rmt.getTemplate(), rmt.getFormat());
        final EqualsHashCodeTestUtils.MutateFunction<TemplateRoleName> mutate = rmt -> {
            if (randomBoolean()) {
                return new TemplateRoleName(rmt.getTemplate(), randomValueOtherThan(rmt.getFormat(), () -> randomFrom(Format.values())));
            } else {
                final String templateStr = rmt.getTemplate().utf8ToString();
                return new TemplateRoleName(
                    new BytesArray(templateStr.substring(randomIntBetween(1, templateStr.length() / 2))),
                    rmt.getFormat()
                );
            }
        };
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(original, copy, mutate);
    }

    public void testValidate() {
        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );

        final TemplateRoleName plainString = new TemplateRoleName(new BytesArray("""
            { "source":"heroes" }"""), Format.STRING);
        plainString.validate(scriptService);

        final TemplateRoleName user = new TemplateRoleName(new BytesArray("""
            { "source":"_user_{{username}}" }"""), Format.STRING);
        user.validate(scriptService);

        final TemplateRoleName groups = new TemplateRoleName(new BytesArray("""
            { "source":"{{#tojson}}groups{{/tojson}}" }"""), Format.JSON);
        groups.validate(scriptService);

        final TemplateRoleName notObject = new TemplateRoleName(new BytesArray("heroes"), Format.STRING);
        expectThrows(IllegalArgumentException.class, () -> notObject.validate(scriptService));

        final TemplateRoleName invalidField = new TemplateRoleName(new BytesArray("""
            { "foo":"heroes" }"""), Format.STRING);
        expectThrows(IllegalArgumentException.class, () -> invalidField.validate(scriptService));
    }

    public void testValidateWillPassWithEmptyContext() {
        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );

        final BytesReference template = new BytesArray("""
            { "source":"\
              {{username}}/{{dn}}/{{realm}}/{{metadata}}\
              {{#realm}}\
                {{name}}/{{type}}\
              {{/realm}}\
              {{#toJson}}groups{{/toJson}}\
              {{^groups}}{{.}}{{/groups}}\
              {{#metadata}}\
                {{#first}}\
                  <li><strong>{{name}}</strong></li>\
                {{/first}}\
                {{#link}}\
                  <li><a href=\\"{{url}}\\">{{name}}</a></li>\
                {{/link}}\
                {{#toJson}}subgroups{{/toJson}}\
                {{something-else}}\
              {{/metadata}}" }
            """);
        final TemplateRoleName templateRoleName = new TemplateRoleName(template, Format.STRING);
        templateRoleName.validate(scriptService);
    }

    public void testValidateWillFailForSyntaxError() {
        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );

        final BytesReference template = new BytesArray("""
            { "source":" {{#not-closed}} {{other-variable}} " }""");

        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TemplateRoleName(template, Format.STRING).validate(scriptService)
        );
        assertTrue(e.getCause() instanceof ScriptException);
    }

    public void testValidateWillCompileButNotExecutePainlessScript() {
        final TemplateScript compiledScript = mock(TemplateScript.class);
        doThrow(new IllegalStateException("Validate should not execute painless script")).when(compiledScript).execute();
        final TemplateScript.Factory scriptFactory = mock(TemplateScript.Factory.class);
        when(scriptFactory.newInstance(any())).thenReturn(compiledScript);

        final ScriptEngine scriptEngine = mock(ScriptEngine.class);
        when(scriptEngine.getType()).thenReturn("painless");
        when(scriptEngine.compile(eq("valid"), eq("params.metedata.group"), any(), eq(Map.of()))).thenReturn(scriptFactory);
        final ScriptException scriptException = new ScriptException(
            "exception",
            new IllegalStateException(),
            List.of(),
            "bad syntax",
            "painless"
        );
        doThrow(scriptException).when(scriptEngine).compile(eq("invalid"), eq("bad syntax"), any(), eq(Map.of()));

        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Map.of("painless", scriptEngine),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        ) {
            @Override
            protected StoredScriptSource getScriptFromClusterState(String id) {
                if ("valid".equals(id)) {
                    return new StoredScriptSource("painless", "params.metedata.group", Map.of());
                } else {
                    return new StoredScriptSource("painless", "bad syntax", Map.of());
                }
            }
        };
        // Validation succeeds if compilation is successful
        new TemplateRoleName(new BytesArray("""
            { "id":"valid" }"""), Format.STRING).validate(scriptService);
        verify(scriptEngine, times(1)).compile(eq("valid"), eq("params.metedata.group"), any(), eq(Map.of()));
        verify(compiledScript, never()).execute();

        // Validation fails if compilation fails
        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new TemplateRoleName(new BytesArray("""
            { "id":"invalid" }"""), Format.STRING).validate(scriptService));
        assertSame(scriptException, e.getCause());
    }

    public void testValidationWillFailWhenInlineScriptIsNotEnabled() {
        final Settings settings = Settings.builder().put("script.allowed_types", ScriptService.ALLOW_NONE).build();
        final ScriptService scriptService = new ScriptService(
            settings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        final BytesReference inlineScript = new BytesArray("""
            { "source":"" }""");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TemplateRoleName(inlineScript, Format.STRING).validate(scriptService)
        );
        assertThat(e.getMessage(), containsString("[inline]"));
    }

    public void testValidateWillFailWhenStoredScriptIsNotEnabled() {
        final Settings settings = Settings.builder().put("script.allowed_types", ScriptService.ALLOW_NONE).build();
        final ScriptService scriptService = new ScriptService(
            settings,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final StoredScriptSource storedScriptSource = mock(StoredScriptSource.class);
        final ScriptMetadata scriptMetadata = new ScriptMetadata.Builder(null).storeScript("foo", storedScriptSource).build();
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.custom(ScriptMetadata.TYPE)).thenReturn(scriptMetadata);
        when(storedScriptSource.getLang()).thenReturn("mustache");
        when(storedScriptSource.getSource()).thenReturn("");
        when(storedScriptSource.getOptions()).thenReturn(Collections.emptyMap());
        scriptService.applyClusterState(clusterChangedEvent);

        final BytesReference storedScript = new BytesArray("""
            { "id":"foo" }""");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TemplateRoleName(storedScript, Format.STRING).validate(scriptService)
        );
        assertThat(e.getMessage(), containsString("[stored]"));
    }

    public void testValidateWillFailWhenStoredScriptIsNotFound() {
        final ScriptService scriptService = new ScriptService(
            Settings.EMPTY,
            Collections.singletonMap(MustacheScriptEngine.NAME, new MustacheScriptEngine()),
            ScriptModule.CORE_CONTEXTS,
            () -> 1L
        );
        final ClusterChangedEvent clusterChangedEvent = mock(ClusterChangedEvent.class);
        final ClusterState clusterState = mock(ClusterState.class);
        final Metadata metadata = mock(Metadata.class);
        final ScriptMetadata scriptMetadata = new ScriptMetadata.Builder(null).build();
        when(clusterChangedEvent.state()).thenReturn(clusterState);
        when(clusterState.metadata()).thenReturn(metadata);
        when(metadata.custom(ScriptMetadata.TYPE)).thenReturn(scriptMetadata);
        scriptService.applyClusterState(clusterChangedEvent);

        final BytesReference storedScript = new BytesArray("""
            { "id":"foo" }""");
        final IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new TemplateRoleName(storedScript, Format.STRING).validate(scriptService)
        );
        assertThat(e.getMessage(), containsString("unable to find script"));
    }
}
