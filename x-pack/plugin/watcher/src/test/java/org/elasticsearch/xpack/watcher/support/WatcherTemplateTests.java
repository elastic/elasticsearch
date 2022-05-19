/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.watcher.support;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.mustache.MustacheScriptEngine;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonStringEncoder;
import org.elasticsearch.xpack.watcher.Watcher;
import org.elasticsearch.xpack.watcher.common.text.TextTemplate;
import org.elasticsearch.xpack.watcher.common.text.TextTemplateEngine;
import org.junit.Before;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class WatcherTemplateTests extends ESTestCase {

    private TextTemplateEngine textTemplateEngine;

    @Before
    public void init() throws Exception {
        MustacheScriptEngine engine = new MustacheScriptEngine();
        Map<String, ScriptEngine> engines = Collections.singletonMap(engine.getType(), engine);
        Map<String, ScriptContext<?>> contexts = Collections.singletonMap(
            Watcher.SCRIPT_TEMPLATE_CONTEXT.name,
            Watcher.SCRIPT_TEMPLATE_CONTEXT
        );
        ScriptService scriptService = new ScriptService(Settings.EMPTY, engines, contexts, () -> 1L);
        textTemplateEngine = new TextTemplateEngine(scriptService);
    }

    public void testEscaping() throws Exception {
        XContentType contentType = randomFrom(XContentType.values()).canonical();
        if (rarely()) {
            contentType = null;
        }
        Character[] specialChars = new Character[] { '\f', '\n', '\r', '"', '\\', (char) 11, '\t', '\b' };
        int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            int rounds = scaledRandomIntBetween(1, 20);
            StringWriter escaped = new StringWriter(); // This will be escaped as it is constructed
            StringWriter unescaped = new StringWriter(); // This will be escaped at the end

            for (int j = 0; j < rounds; j++) {
                String s = getChars();
                unescaped.write(s);
                if (contentType == XContentType.JSON) {
                    escaped.write(JsonStringEncoder.getInstance().quoteAsString(s));
                } else {
                    escaped.write(s);
                }

                char c = randomFrom(specialChars);
                unescaped.append(c);

                if (contentType == XContentType.JSON) {
                    escaped.write(JsonStringEncoder.getInstance().quoteAsString("" + c));
                } else {
                    escaped.append(c);
                }
            }

            if (contentType == XContentType.JSON) {
                assertThat(escaped.toString(), equalTo(new String(JsonStringEncoder.getInstance().quoteAsString(unescaped.toString()))));
            } else {
                assertThat(escaped.toString(), equalTo(unescaped.toString()));
            }

            String template = prepareTemplate("{{data}}", contentType);

            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("data", unescaped.toString());
            String renderedTemplate = textTemplateEngine.render(new TextTemplate(template), dataMap);
            assertThat(renderedTemplate, notNullValue());

            if (contentType == XContentType.JSON) {
                if (escaped.toString().equals(renderedTemplate) == false) {
                    String escapedString = escaped.toString();
                    for (int l = 0; l < renderedTemplate.length() && l < escapedString.length(); ++l) {
                        if (renderedTemplate.charAt(l) != escapedString.charAt(l)) {
                            logger.error("at [{}] expected [{}] but got [{}]", l, renderedTemplate.charAt(l), escapedString.charAt(l));
                        }
                    }
                }
                assertThat(escaped.toString(), equalTo(renderedTemplate));
            } else {
                assertThat(unescaped.toString(), equalTo(renderedTemplate));
            }
        }
    }

    public void testSimpleParameterReplace() {
        {
            String template = """
                __json__::GET _search {"query": {"boosting": {"positive": {"match": {"body": "gift"}},"negative": \
                {"term": {"body": {"value": "solr"}}}, "negative_boost": {{boost_val}} } }}""";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            String result = textTemplateEngine.render(new TextTemplate(template), vars);
            assertEquals("""
                GET _search {"query": {"boosting": {"positive": {"match": {"body": "gift"}},"negative": \
                {"term": {"body": {"value": "solr"}}}, "negative_boost": 0.3 } }}""", result);
        }
        {
            String template = """
                __json__::GET _search {"query": {"boosting": {"positive": {"match": {"body": "gift"}},"negative": \
                {"term": {"body": {"value": "{{body_val}}"}}}, "negative_boost": {{boost_val}} } }}""";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            vars.put("body_val", "\"quick brown\"");
            String result = textTemplateEngine.render(new TextTemplate(template), vars);
            assertEquals("""
                GET _search {"query": {"boosting": {"positive": {"match": {"body": "gift"}},"negative": \
                {"term": {"body": {"value": "\\"quick brown\\""}}}, "negative_boost": 0.3 } }}""", result);
        }
    }

    public void testInvalidPrefixes() throws Exception {
        String[] specialStrings = new String[] { "\f", "\n", "\r", "\"", "\\", "\t", "\b", "__::", "__" };
        String prefix = randomFrom("", "__", "____::", "___::", "____", "::", "++json__::", "__json__", "+_json__::", "__json__:");
        String template = prefix + " {{test_var1}} {{test_var2}}";
        Map<String, Object> vars = new HashMap<>();
        Writer var1Writer = new StringWriter();
        Writer var2Writer = new StringWriter();

        for (int i = 0; i < scaledRandomIntBetween(10, 1000); ++i) {
            var1Writer.write(randomRealisticUnicodeOfCodepointLengthBetween(0, 10));
            var2Writer.write(randomRealisticUnicodeOfCodepointLengthBetween(0, 10));
            var1Writer.append(randomFrom(specialStrings));
            var2Writer.append(randomFrom(specialStrings));
        }

        vars.put("test_var1", var1Writer.toString());
        vars.put("test_var2", var2Writer.toString());
        String s1 = textTemplateEngine.render(new TextTemplate(template), vars);
        String s2 = prefix + " " + var1Writer.toString() + " " + var2Writer.toString();
        assertThat(s1, equalTo(s2));
    }

    static String getChars() throws IOException {
        return randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
    }

    static String prepareTemplate(String template, @Nullable XContentType contentType) {
        if (contentType == null) {
            return template;
        }
        return new StringBuilder("__").append(contentType.queryParameter().toLowerCase(Locale.ROOT))
            .append("__::")
            .append(template)
            .toString();
    }

}
