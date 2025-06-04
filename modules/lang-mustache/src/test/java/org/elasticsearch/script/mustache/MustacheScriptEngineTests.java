/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */
package org.elasticsearch.script.mustache;

import com.github.mustachejava.MustacheException;
import com.github.mustachejava.MustacheFactory;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.text.SizeLimitingStringWriter;
import org.elasticsearch.script.GeneralScriptException;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.json.JsonXContent;
import org.junit.Before;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;

/**
 * Mustache based templating test
 */
public class MustacheScriptEngineTests extends ESTestCase {
    private MustacheScriptEngine qe;
    private MustacheFactory factory;

    @Before
    public void setup() {
        qe = new MustacheScriptEngine(Settings.builder().put(MustacheScriptEngine.MUSTACHE_RESULT_SIZE_LIMIT.getKey(), "1kb").build());
        factory = CustomMustacheFactory.builder().build();
    }

    public void testSimpleParameterReplace() {
        Map<String, String> compileParams = Map.of("content_type", "application/json");
        {
            String template = """
                GET _search
                {
                  "query": {
                    "boosting": {
                      "positive": {
                        "match": {
                          "body": "gift"
                        }
                      },
                      "negative": {
                        "term": {
                          "body": {
                            "value": "solr"
                          }
                        }
                      },
                      "negative_boost": {{boost_val}}
                    }
                  }
                }""";
            Map<String, Object> vars = Map.of("boost_val", "0.3");
            String o = qe.compile(null, template, TemplateScript.CONTEXT, compileParams).newInstance(vars).execute();
            assertEquals("""
                GET _search
                {
                  "query": {
                    "boosting": {
                      "positive": {
                        "match": {
                          "body": "gift"
                        }
                      },
                      "negative": {
                        "term": {
                          "body": {
                            "value": "solr"
                          }
                        }
                      },
                      "negative_boost": 0.3
                    }
                  }
                }""", o);
        }
        {
            String template = """
                GET _search
                {
                  "query": {
                    "boosting": {
                      "positive": {
                        "match": {
                          "body": "gift"
                        }
                      },
                      "negative": {
                        "term": {
                          "body": {
                            "value": "{{body_val}}"
                          }
                        }
                      },
                      "negative_boost": {{boost_val}}
                    }
                  }
                }""";
            Map<String, Object> vars = Map.of("boost_val", "0.3", "body_val", "\"quick brown\"");
            String o = qe.compile(null, template, TemplateScript.CONTEXT, compileParams).newInstance(vars).execute();
            assertEquals("""
                GET _search
                {
                  "query": {
                    "boosting": {
                      "positive": {
                        "match": {
                          "body": "gift"
                        }
                      },
                      "negative": {
                        "term": {
                          "body": {
                            "value": "\\"quick brown\\""
                          }
                        }
                      },
                      "negative_boost": 0.3
                    }
                  }
                }""", o);
        }
    }

    public void testChangingDelimiters() {
        Map<String, String> compileParams = Map.of("content_type", "application/json");
        {
            String template = """
                GET _search
                {
                  "query": {
                    "match": {
                      "content": "{{query_string}}"
                    }
                  },
                  "highlight": {
                    {{=<% %>=}}
                    "pre_tags": [
                      "{{{{"
                    ],
                    "post_tags": [
                      "}}}}"
                    ],
                    <%={{ }}=%>
                    "fields": {
                      "content": {},
                      "title": {}
                    }
                  }
                }""";
            Map<String, Object> vars = Map.of("query_string", "test");
            String o = qe.compile(null, template, TemplateScript.CONTEXT, compileParams).newInstance(vars).execute();
            assertEquals("""
                GET _search
                {
                  "query": {
                    "match": {
                      "content": "test"
                    }
                  },
                  "highlight": {
                   \s
                    "pre_tags": [
                      "{{{{"
                    ],
                    "post_tags": [
                      "}}}}"
                    ],
                   \s
                    "fields": {
                      "content": {},
                      "title": {}
                    }
                  }
                }""", o);
        }
    }

    public void testSimple() throws IOException {
        String templateString = """
            {"source":{"match_{{template}}": {}},"params":{"template":"all"}}""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, templateString);
        Script script = Script.parse(parser);
        TemplateScript.Factory compiled = qe.compile(null, script.getIdOrCode(), TemplateScript.CONTEXT, Map.of());
        TemplateScript TemplateScript = compiled.newInstance(script.getParams());
        assertThat(TemplateScript.execute(), equalTo("{\"match_all\":{}}"));
    }

    @SuppressWarnings("deprecation") // GeneralScriptException
    public void testDetectMissingParam() {
        Map<String, String> scriptOptions = Map.ofEntries(Map.entry(MustacheScriptEngine.DETECT_MISSING_PARAMS_OPTION, "true"));

        // fails when a param is missing and the DETECT_MISSING_PARAMS_OPTION option is set to true.
        {
            String source = "{\"match\": { \"field\": \"{{query_string}}\" }";
            TemplateScript.Factory compiled = qe.compile(null, source, TemplateScript.CONTEXT, scriptOptions);
            Map<String, Object> params = Collections.emptyMap();
            GeneralScriptException e = expectThrows(GeneralScriptException.class, () -> compiled.newInstance(params).execute());
            assertThat(e.getRootCause(), instanceOf(MustacheInvalidParameterException.class));
            assertThat(e.getRootCause().getMessage(), startsWith("Parameter [query_string] is missing"));
        }

        // fails when params is null and the DETECT_MISSING_PARAMS_OPTION option is set to true.
        {
            String source = "{\"match\": { \"field\": \"{{query_string}}\" }";
            TemplateScript.Factory compiled = qe.compile(null, source, TemplateScript.CONTEXT, scriptOptions);
            GeneralScriptException e = expectThrows(GeneralScriptException.class, () -> compiled.newInstance(null).execute());
            assertThat(e.getRootCause(), instanceOf(MustacheInvalidParameterException.class));
            assertThat(e.getRootCause().getMessage(), startsWith("Parameter [query_string] is missing"));
        }

        // works as expected when params are specified and the DETECT_MISSING_PARAMS_OPTION option is set to true
        {
            String source = "{\"match\": { \"field\": \"{{query_string}}\" }";
            TemplateScript.Factory compiled = qe.compile(null, source, TemplateScript.CONTEXT, scriptOptions);
            Map<String, Object> params = Map.ofEntries(Map.entry("query_string", "foo"));
            assertThat(compiled.newInstance(params).execute(), equalTo("{\"match\": { \"field\": \"foo\" }"));
        }

        // do not throw when using a missing param in the conditional when DETECT_MISSING_PARAMS_OPTION option is set to true
        {
            String source = "{\"match\": { \"field\": \"{{#query_string}}{{.}}{{/query_string}}\" }";
            TemplateScript.Factory compiled = qe.compile(null, source, TemplateScript.CONTEXT, scriptOptions);
            Map<String, Object> params = Map.of();
            assertThat(compiled.newInstance(params).execute(), equalTo("{\"match\": { \"field\": \"\" }"));
        }
    }

    public void testMissingParam() {
        Map<String, String> scriptOptions = Collections.emptyMap();
        String source = "{\"match\": { \"field\": \"{{query_string}}\" }";
        TemplateScript.Factory compiled = qe.compile(null, source, TemplateScript.CONTEXT, scriptOptions);

        // When the DETECT_MISSING_PARAMS_OPTION is not specified, missing variable is replaced with an empty string.
        assertThat(compiled.newInstance(Collections.emptyMap()).execute(), equalTo("{\"match\": { \"field\": \"\" }"));
        assertThat(compiled.newInstance(null).execute(), equalTo("{\"match\": { \"field\": \"\" }"));
    }

    public void testParseTemplateAsSingleStringWithConditionalClause() throws IOException {
        String templateString = """
            {
              "source": "{ \\"match_{{#use_it}}{{template}}{{/use_it}}\\":{} }",
              "params": {
                "template": "all",
                "use_it": true
              }
            }""";
        XContentParser parser = createParser(JsonXContent.jsonXContent, templateString);
        Script script = Script.parse(parser);
        TemplateScript.Factory compiled = qe.compile(null, script.getIdOrCode(), TemplateScript.CONTEXT, Map.of());
        TemplateScript TemplateScript = compiled.newInstance(script.getParams());
        assertThat(TemplateScript.execute(), equalTo("{ \"match_all\":{} }"));
    }

    private static class TestReflection {

        private final int privateField = 1;

        public final int publicField = 2;

        private int getPrivateMethod() {
            return 3;
        }

        public int getPublicMethod() {
            return 4;
        }

        @Override
        public String toString() {
            return List.of(privateField, publicField, getPrivateMethod(), getPublicMethod()).toString();
        }
    }

    /**
     * BWC test for some odd reflection edge-cases. It's not really expected that customer code would be exercising this,
     * but maybe it's out there! Who knows!?
     *
     * If we change this, we should *know* that we're changing it.
     */
    public void testReflection() {
        Map<String, Object> vars = Map.of("obj", new TestReflection());

        {
            // non-reflective access calls toString
            String templateString = "{{obj}}";
            String o = qe.compile(null, templateString, TemplateScript.CONTEXT, Map.of()).newInstance(vars).execute();
            assertThat(o, equalTo("[1, 2, 3, 4]"));
        }
        {
            // accessing a field/method that *doesn't* exist will give an empty result
            String templateString = "{{obj.missing}}";
            String o = qe.compile(null, templateString, TemplateScript.CONTEXT, Map.of()).newInstance(vars).execute();
            assertThat(o, equalTo(""));
        }
        {
            // accessing a private field that does exist will give an empty result
            String templateString = "{{obj.privateField}}";
            String o = qe.compile(null, templateString, TemplateScript.CONTEXT, Map.of()).newInstance(vars).execute();
            assertThat(o, equalTo(""));
        }
        {
            // accessing a private method that does exist will give an empty result
            String templateString = "{{obj.privateMethod}}";
            String o = qe.compile(null, templateString, TemplateScript.CONTEXT, Map.of()).newInstance(vars).execute();
            assertThat(o, equalTo(""));
        }
        {
            // accessing a public field that does exist will give an empty result
            String templateString = "{{obj.publicField}}";
            String o = qe.compile(null, templateString, TemplateScript.CONTEXT, Map.of()).newInstance(vars).execute();
            assertThat(o, equalTo(""));
        }
        {
            // accessing a public method that does exist will give an empty result
            String templateString = "{{obj.publicMethod}}";
            String o = qe.compile(null, templateString, TemplateScript.CONTEXT, Map.of()).newInstance(vars).execute();
            assertThat(o, equalTo(""));
        }
    }

    public void testEscapeJson() throws IOException {
        {
            StringWriter writer = new StringWriter();
            factory.encode("hello \n world", writer);
            assertThat(writer.toString(), equalTo("hello \\n world"));
        }
        {
            StringWriter writer = new StringWriter();
            factory.encode("\n", writer);
            assertThat(writer.toString(), equalTo("\\n"));
        }

        Character[] specialChars = new Character[] {
            '\"',
            '\\',
            '\u0000',
            '\u0001',
            '\u0002',
            '\u0003',
            '\u0004',
            '\u0005',
            '\u0006',
            '\u0007',
            '\u0008',
            '\u0009',
            '\u000B',
            '\u000C',
            '\u000E',
            '\u000F',
            '\u001F' };
        String[] escapedChars = new String[] {
            "\\\"",
            "\\\\",
            "\\u0000",
            "\\u0001",
            "\\u0002",
            "\\u0003",
            "\\u0004",
            "\\u0005",
            "\\u0006",
            "\\u0007",
            "\\u0008",
            "\\u0009",
            "\\u000B",
            "\\u000C",
            "\\u000E",
            "\\u000F",
            "\\u001F" };
        int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            int rounds = scaledRandomIntBetween(1, 20);
            StringWriter expect = new StringWriter();
            StringWriter writer = new StringWriter();
            for (int j = 0; j < rounds; j++) {
                String s = getChars();
                writer.write(s);
                expect.write(s);

                int charIndex = randomInt(7);
                writer.append(specialChars[charIndex]);
                expect.append(escapedChars[charIndex]);
            }
            StringWriter target = new StringWriter();
            factory.encode(writer.toString(), target);
            assertThat(expect.toString(), equalTo(target.toString()));
        }
    }

    public void testResultSizeLimit() throws IOException {
        String vals = "\"" + "{{val}}".repeat(200) + "\"";
        String params = "\"val\":\"aaaaaaaaaa\"";
        XContentParser parser = createParser(JsonXContent.jsonXContent, Strings.format("{\"source\":%s,\"params\":{%s}}", vals, params));
        Script script = Script.parse(parser);
        var compiled = qe.compile(null, script.getIdOrCode(), TemplateScript.CONTEXT, Map.of());
        TemplateScript templateScript = compiled.newInstance(script.getParams());
        var ex = expectThrows(ElasticsearchParseException.class, templateScript::execute);
        assertThat(ex.getCause(), instanceOf(MustacheException.class));
        assertThat(
            ex.getCause().getCause(),
            allOf(
                instanceOf(SizeLimitingStringWriter.SizeLimitExceededException.class),
                transformedMatch(Throwable::getMessage, endsWith("has size [1030] which exceeds the size limit [1024]"))
            )
        );
    }

    private String getChars() {
        String string = randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
        for (int i = 0; i < string.length(); i++) {
            if (isEscapeChar(string.charAt(i))) {
                return string.substring(0, i);
            }
        }
        return string;
    }

    /**
     * From https://www.ietf.org/rfc/rfc4627.txt:
     *
     * All Unicode characters may be placed within the
     * quotation marks except for the characters that must be escaped:
     * quotation mark, reverse solidus, and the control characters (U+0000
     * through U+001F).
     * */
    private static boolean isEscapeChar(char c) {
        switch (c) {
            case '"':
            case '\\':
                return true;
        }

        if (c < '\u002F') return true;
        return false;
    }
}
