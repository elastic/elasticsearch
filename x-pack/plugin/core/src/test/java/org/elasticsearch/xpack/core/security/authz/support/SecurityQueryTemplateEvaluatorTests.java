/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class SecurityQueryTemplateEvaluatorTests extends ESTestCase {
    private ScriptService scriptService;

    @Before
    public void setup() throws Exception {
        scriptService = mock(ScriptService.class);
    }

    public void testTemplating() throws Exception {
        User user = new User("_username", new String[] { "role1", "role2" }, "_full_name", "_email",
                Map.of("key", "value"), true);

        TemplateScript.Factory compiledTemplate = templateParams -> new TemplateScript() {
            @Override
            public String execute() {
                return "rendered_text";
            }
        };

        when(scriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenReturn(compiledTemplate);

        XContentBuilder builder = jsonBuilder();
        String query = Strings.toString(new TermQueryBuilder("field", "{{_user.username}}").toXContent(builder, ToXContent.EMPTY_PARAMS));
        Script script = new Script(ScriptType.INLINE, "mustache", query, Collections.singletonMap("custom", "value"));
        builder = jsonBuilder().startObject().field("template");
        script.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String querySource = Strings.toString(builder.endObject());

        SecurityQueryTemplateEvaluator.evaluateTemplate(querySource, scriptService, user);
        ArgumentCaptor<Script> argument = ArgumentCaptor.forClass(Script.class);
        verify(scriptService).compile(argument.capture(), eq(TemplateScript.CONTEXT));
        Script usedScript = argument.getValue();
        assertThat(usedScript.getIdOrCode(), equalTo(script.getIdOrCode()));
        assertThat(usedScript.getType(), equalTo(script.getType()));
        assertThat(usedScript.getLang(), equalTo("mustache"));
        assertThat(usedScript.getOptions(), equalTo(script.getOptions()));
        assertThat(usedScript.getParams().size(), equalTo(2));
        assertThat(usedScript.getParams().get("custom"), equalTo("value"));

        Map<String, Object> userModel = new HashMap<>();
        userModel.put("username", user.principal());
        userModel.put("full_name", user.fullName());
        userModel.put("email", user.email());
        userModel.put("roles", Arrays.asList(user.roles()));
        userModel.put("metadata", user.metadata());
        assertThat(usedScript.getParams().get("_user"), equalTo(userModel));
    }

    public void testDocLevelSecurityTemplateWithOpenIdConnectStyleMetadata() throws Exception {
        User user = new User(randomAlphaOfLength(8), generateRandomStringArray(5, 5, false), randomAlphaOfLength(9), "sample@example.com",
            Map.of("oidc(email)", "sample@example.com"), true);

        final MustacheScriptEngine mustache = new MustacheScriptEngine();

        when(scriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenAnswer(inv -> {
            assertThat(inv.getArguments(), arrayWithSize(2));
            Script script = (Script) inv.getArguments()[0];
            TemplateScript.Factory factory = mustache.compile(
                script.getIdOrCode(), script.getIdOrCode(), TemplateScript.CONTEXT, script.getOptions());
            return factory;
        });

        String template = "{ \"template\" : { \"source\" : {\"term\":{\"field\":\"{{_user.metadata.oidc(email)}}\"}} } }";

        String evaluated = SecurityQueryTemplateEvaluator.evaluateTemplate(template, scriptService, user);
        assertThat(evaluated, equalTo("{\"term\":{\"field\":\"sample@example.com\"}}"));
    }

    public void testSkipTemplating() throws Exception {
        XContentBuilder builder = jsonBuilder();
        String querySource = Strings.toString(new TermQueryBuilder("field", "value").toXContent(builder, ToXContent.EMPTY_PARAMS));
        String result = SecurityQueryTemplateEvaluator.evaluateTemplate(querySource, scriptService, null);
        assertThat(result, sameInstance(querySource));
        verifyZeroInteractions(scriptService);
    }

}
