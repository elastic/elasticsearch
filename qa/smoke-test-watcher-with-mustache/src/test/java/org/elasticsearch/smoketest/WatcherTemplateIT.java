/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.smoketest;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.env.Environment;
import org.elasticsearch.script.ScriptContext;
import org.elasticsearch.script.ScriptContextRegistry;
import org.elasticsearch.script.ScriptEngineRegistry;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptSettings;
import org.elasticsearch.script.mustache.MustacheScriptEngineService;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xpack.common.text.TextTemplate;
import org.elasticsearch.xpack.common.text.TextTemplateEngine;
import org.junit.Before;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class WatcherTemplateIT extends ESTestCase {

    private TextTemplateEngine engine;

    @Before
    public void init() throws Exception {
        Settings setting = Settings.builder().put(ScriptService.SCRIPT_AUTO_RELOAD_ENABLED_SETTING, true).build();
        Environment environment = Mockito.mock(Environment.class);
        ResourceWatcherService resourceWatcherService = Mockito.mock(ResourceWatcherService.class);
        ScriptContextRegistry registry = new ScriptContextRegistry(Collections.singletonList(new ScriptContext.Plugin("xpack", "watch")));

        ScriptEngineRegistry scriptEngineRegistry = new ScriptEngineRegistry(
                Collections.singleton(new MustacheScriptEngineService())
        );
        ScriptSettings scriptSettings = new ScriptSettings(scriptEngineRegistry, registry);
        ScriptService scriptService = new ScriptService(setting, environment, resourceWatcherService, scriptEngineRegistry,
                registry, scriptSettings);
        engine = new TextTemplateEngine(Settings.EMPTY, scriptService);
    }

    public void testEscaping() throws Exception {
        XContentType contentType = randomFrom(XContentType.values());
        if (rarely()) {
            contentType = null;
        }
        Character[] specialChars = new Character[]{'\f', '\n', '\r', '"', '\\', (char) 11, '\t', '\b' };
        int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            int rounds = scaledRandomIntBetween(1, 20);
            StringWriter escaped = new StringWriter(); //This will be escaped as it is constructed
            StringWriter unescaped = new StringWriter(); //This will be escaped at the end

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
            }
            else {
                assertThat(escaped.toString(), equalTo(unescaped.toString()));
            }

            String template = prepareTemplate("{{data}}", contentType);

            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("data", unescaped.toString());
            String renderedTemplate = engine.render(new TextTemplate(template), dataMap);
            assertThat(renderedTemplate, notNullValue());

            if (contentType == XContentType.JSON) {
                if (!escaped.toString().equals(renderedTemplate)) {
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
            String template = "__json__::GET _search {\"query\": " + "{\"boosting\": {" + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
                    + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}" + "}}, \"negative_boost\": {{boost_val}} } }}";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            String result = engine.render(new TextTemplate(template), vars);
            assertEquals("GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                            + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}}}, \"negative_boost\": 0.3 } }}",
                        result);
        }
        {
            String template = "__json__::GET _search {\"query\": " + "{\"boosting\": {" + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
                    + "\"negative\": {\"term\": {\"body\": {\"value\": \"{{body_val}}\"}" + "}}, \"negative_boost\": {{boost_val}} } }}";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            vars.put("body_val", "\"quick brown\"");
            String result = engine.render(new TextTemplate(template), vars);
            assertEquals("GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                            + "\"negative\": {\"term\": {\"body\": {\"value\": \"\\\"quick brown\\\"\"}}}, \"negative_boost\": 0.3 } }}",
                    result);
        }
    }

    public void testInvalidPrefixes() throws Exception {
        String[] specialStrings = new String[]{"\f", "\n", "\r", "\"", "\\", "\t", "\b", "__::", "__" };
        String prefix = randomFrom("", "__", "____::", "___::", "____", "::", "++json__::", "__json__", "+_json__::", "__json__:");
        String template = prefix + " {{test_var1}} {{test_var2}}";
        Map<String, Object> vars = new HashMap<>();
        Writer var1Writer = new StringWriter();
        Writer var2Writer = new StringWriter();

        for(int i = 0; i < scaledRandomIntBetween(10,1000); ++i) {
            var1Writer.write(randomRealisticUnicodeOfCodepointLengthBetween(0, 10));
            var2Writer.write(randomRealisticUnicodeOfCodepointLengthBetween(0, 10));
            var1Writer.append(randomFrom(specialStrings));
            var2Writer.append(randomFrom(specialStrings));
        }

        vars.put("test_var1", var1Writer.toString());
        vars.put("test_var2", var2Writer.toString());
        String s1 = engine.render(new TextTemplate(template), vars);
        String s2 =  prefix + " " + var1Writer.toString() + " " + var2Writer.toString();
        assertThat(s1, equalTo(s2));
    }

    static String getChars() throws IOException {
        return randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
    }

    static String prepareTemplate(String template, @Nullable XContentType contentType) {
        if (contentType == null) {
            return template;
        }
        return new StringBuilder("__")
                .append(contentType.shortName().toLowerCase(Locale.ROOT))
                .append("__::")
                .append(template)
                .toString();
    }

}
