/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.security.authz.support;

import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.query.TermQueryBuilder;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xpack.core.security.user.User;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class SecurityQueryTemplateEvaluatorTests extends ESTestCase {
    private ScriptService scriptService;

    @Before
    public void setup() throws Exception {
        scriptService = mock(ScriptService.class);
    }

    public void testTemplating() throws Exception {
        User user = new User("_username", new String[] { "role1", "role2" }, "_full_name", "_email", Map.of("key", "value"), true);

        TemplateScript.Factory compiledTemplate = templateParams -> new TemplateScript(templateParams) {
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
        User user = new User(
            randomAlphaOfLength(8),
            generateRandomStringArray(5, 5, false),
            randomAlphaOfLength(9),
            "sample@example.com",
            Map.of("oidc(email)", "sample@example.com"),
            true
        );

        final MustacheScriptEngine mustache = new MustacheScriptEngine(Settings.EMPTY);

        when(scriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenAnswer(inv -> {
            assertThat(inv.getArguments(), arrayWithSize(2));
            Script script = (Script) inv.getArguments()[0];
            TemplateScript.Factory factory = mustache.compile(
                script.getIdOrCode(),
                script.getIdOrCode(),
                TemplateScript.CONTEXT,
                script.getOptions()
            );
            return factory;
        });

        String template = """
            {
              "template": {
                "source": {
                  "term": {
                    "field": "{{_user.metadata.oidc(email)}}"
                  }
                }
              }
            }""";

        String evaluated = SecurityQueryTemplateEvaluator.evaluateTemplate(template, scriptService, user);
        assertThat(evaluated, equalTo("""
            {"term":{"field":"sample@example.com"}}"""));
    }

    public void testSkipTemplating() throws Exception {
        XContentBuilder builder = jsonBuilder();
        String querySource = Strings.toString(new TermQueryBuilder("field", "value").toXContent(builder, ToXContent.EMPTY_PARAMS));
        String result = SecurityQueryTemplateEvaluator.evaluateTemplate(querySource, scriptService, null);
        assertThat(result, sameInstance(querySource));
        verifyNoMoreInteractions(scriptService);
    }

    public void testLookupsAreExposedUnderLookupNamespace() {
        useRealMustacheEngine();
        final User user = new User("alice", new String[] { "analyst" }, null, null, Map.of(), true);
        final String template = """
            {
              "template": {
                "source": "{\\"bool\\":{\\"filter\\":[{\\"terms\\":{\\"ml_job_id\\":{{#toJson}}_lookup.ml_jobs{{/toJson}}}},\
            {\\"term\\":{\\"owner\\":\\"{{_lookup.owner}}\\"}},{\\"term\\":{\\"user\\":\\"{{_user.username}}\\"}}]}}"
              },
              "lookups": {
                "ml_jobs": { "type": "ml_job_ids", "params": { "spaces": ["marketing"] } },
                "owner": { "type": "profile_uid" }
              }
            }""";
        // Values are keyed by type and params, not by the name the template binds them to
        final DlsLookup jobs = new DlsLookup(randomAlphaOfLength(5), "ml_job_ids", Map.of("spaces", List.of("marketing")));
        final DlsLookup owner = new DlsLookup(randomAlphaOfLength(6), "profile_uid", Map.of());
        final ResolvedDlsLookups resolved = new ResolvedDlsLookups(Map.of(jobs.key(), List.of("j1", "j2"), owner.key(), "u_alice"));

        final String evaluated = SecurityQueryTemplateEvaluator.evaluateTemplate(template, scriptService, user, resolved);
        assertThat(evaluated, equalTo("""
            {"bool":{"filter":[{"terms":{"ml_job_id":["j1","j2"]}},{"term":{"owner":"u_alice"}},{"term":{"user":"alice"}}]}}"""));
    }

    public void testUnresolvedLookupFailsEvaluation() {
        useRealMustacheEngine();
        final User user = new User("alice");
        final String template = """
            {
              "template": { "source": "{\\"terms\\":{\\"ml_job_id\\":{{#toJson}}_lookup.ml_jobs{{/toJson}}}}" },
              "lookups": { "ml_jobs": { "type": "ml_job_ids", "params": { "spaces": ["marketing"] } } }
            }""";
        // Resolved for different params, so the declared lookup is still missing
        final ResolvedDlsLookups resolved = randomBoolean()
            ? ResolvedDlsLookups.EMPTY
            : new ResolvedDlsLookups(
                Map.of(new DlsLookup("ml_jobs", "ml_job_ids", Map.of("spaces", List.of("sales"))).key(), List.of("j9"))
            );
        final IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> SecurityQueryTemplateEvaluator.evaluateTemplate(template, scriptService, user, resolved)
        );
        assertThat(e.getMessage(), equalTo("DLS lookup [ml_jobs] of type [ml_job_ids] has not been resolved for this request"));
        verifyNoMoreInteractions(scriptService);
    }

    public void testTemplateWithoutLookupsSeesEmptyLookupNamespace() {
        useRealMustacheEngine();
        final User user = new User("alice");
        final String template = """
            {
              "template": { "source": "{\\"term\\":{\\"user\\":\\"{{_user.username}}{{_lookup.missing}}\\"}}" },
              "trailing": { "ignored": true }
            }""";
        final String evaluated = SecurityQueryTemplateEvaluator.evaluateTemplate(template, scriptService, user, ResolvedDlsLookups.EMPTY);
        assertThat(evaluated, equalTo("""
            {"term":{"user":"alice"}}"""));
    }

    public void testMalformedLookupsFailEvaluation() {
        final User user = new User("alice");
        final String template = """
            {
              "template": { "source": "{}" },
              "lookups": { "bad": { "params": {} } }
            }""";
        final Exception e = expectThrows(
            Exception.class,
            () -> SecurityQueryTemplateEvaluator.evaluateTemplate(template, scriptService, user, ResolvedDlsLookups.EMPTY)
        );
        assertThat(e.getMessage(), containsString("lookup [bad] is missing required field [type]"));
        verifyNoMoreInteractions(scriptService);
    }

    private void useRealMustacheEngine() {
        final MustacheScriptEngine mustache = new MustacheScriptEngine(Settings.EMPTY);
        when(scriptService.compile(any(Script.class), eq(TemplateScript.CONTEXT))).thenAnswer(inv -> {
            final Script script = (Script) inv.getArguments()[0];
            return mustache.compile(script.getIdOrCode(), script.getIdOrCode(), TemplateScript.CONTEXT, script.getOptions());
        });
    }

}
