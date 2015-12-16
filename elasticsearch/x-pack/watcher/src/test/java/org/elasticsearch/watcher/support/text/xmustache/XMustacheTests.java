/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.text.xmustache;

import com.fasterxml.jackson.core.io.JsonStringEncoder;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.StringWriter;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

/**
 *
 */
public class XMustacheTests extends ESTestCase {
    private ScriptEngineService engine;

    @Before
    public void init() throws Exception {
        engine = new XMustacheScriptEngineService(Settings.EMPTY);
    }

    public void testArrayAccess() throws Exception {
        String template = "{{data.0}} {{data.1}}";
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(template, Collections.emptyMap()));
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
                new String[] { "foo", "bar" },
                Arrays.asList("foo", "bar"));
        vars.put("data", data);
        Object output = engine.executable(mustache, vars).run();
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        BytesReference bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), equalTo("foo bar"));

        // Sets can come out in any order
        Set<String> setData = new HashSet<>();
        setData.add("foo");
        setData.add("bar");
        vars.put("data", setData);
        output = engine.executable(mustache, vars).run();
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), both(containsString("foo")).and(containsString("bar")));
    }

    public void testArrayInArrayAccess() throws Exception {
        String template = "{{data.0.0}} {{data.0.1}}";
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(template, Collections.emptyMap()));
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
                new String[][] { new String[] { "foo", "bar" }},
                Collections.singletonList(new String[] { "foo", "bar" }),
                singleton(new String[] { "foo", "bar" })
        );
        vars.put("data", data);
        Object output = engine.executable(mustache, vars).run();
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        BytesReference bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), equalTo("foo bar"));
    }

    public void testMapInArrayAccess() throws Exception {
        String template = "{{data.0.key}} {{data.1.key}}";
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(template, Collections.emptyMap()));
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
                new Map[] { singletonMap("key", "foo"), singletonMap("key", "bar") },
                Arrays.asList(singletonMap("key", "foo"), singletonMap("key", "bar")));
        vars.put("data", data);
        Object output = engine.executable(mustache, vars).run();
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        BytesReference bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), equalTo("foo bar"));

        // HashSet iteration order isn't fixed
        Set<Object> setData = new HashSet<>();
        setData.add(singletonMap("key", "foo"));
        setData.add(singletonMap("key", "bar"));
        vars.put("data", setData);
        output = engine.executable(mustache, vars).run();
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), both(containsString("foo")).and(containsString("bar")));

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

            String template = XMustacheScriptEngineService.prepareTemplate("{{data}}", contentType);

            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("data", unescaped.toString());
            CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(template, Collections.emptyMap()));
            Object output = engine.executable(mustache, dataMap).run();

            assertThat(output, notNullValue());
            assertThat(output, instanceOf(BytesReference.class));
            BytesReference bytes = (BytesReference) output;
            String renderedTemplate = bytes.toUtf8();

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

    private String getChars() throws IOException {
        return randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
    }
}
