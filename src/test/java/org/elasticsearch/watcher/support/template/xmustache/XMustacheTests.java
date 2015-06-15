/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template.xmustache;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableList;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import com.fasterxml.jackson.core.io.JsonStringEncoder;
import org.elasticsearch.common.bytes.BytesReference;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 *
 */
public class XMustacheTests extends ElasticsearchTestCase {

    private ScriptEngineService engine;

    @Before
    public void init() throws Exception {
        engine = new XMustacheScriptEngineService(Settings.EMPTY);
    }

    @Test @Repeat(iterations = 10)
    public void testArrayAccess() throws Exception {
        String template = "{{data.0}} {{data.1}}";
        Object mustache = engine.compile(template);
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
                new String[] { "foo", "bar" },
                ImmutableList.of("foo", "bar"),
                ImmutableSet.of("foo", "bar"));
        vars.put("data", data);
        Object output = engine.execute(mustache, vars);
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        BytesReference bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), equalTo("foo bar"));
    }

    @Test @Repeat(iterations = 10)
    public void testArrayInArrayAccess() throws Exception {
        String template = "{{data.0.0}} {{data.0.1}}";
        Object mustache = engine.compile(template);
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
                new String[][] { new String[] {"foo", "bar" }},
                ImmutableList.of(new String[] {"foo", "bar" }),
                ImmutableSet.of(new String[] {"foo", "bar" })
        );
        vars.put("data", data);
        Object output = engine.execute(mustache, vars);
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        BytesReference bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), equalTo("foo bar"));
    }

    @Test @Repeat(iterations = 10)
    public void testMapInArrayAccess() throws Exception {
        String template = "{{data.0.key}} {{data.1.key}}";
        Object mustache = engine.compile(template);
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
                new Map[] { ImmutableMap.<String, Object>of("key", "foo"), ImmutableMap.<String, Object>of("key", "bar") },
                ImmutableList.of(ImmutableMap.<String, Object>of("key", "foo"), ImmutableMap.<String, Object>of("key", "bar")),
                ImmutableSet.of(ImmutableMap.<String, Object>of("key", "foo"), ImmutableMap.<String, Object>of("key", "bar")));
        vars.put("data", data);
        Object output = engine.execute(mustache, vars);
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));
        BytesReference bytes = (BytesReference) output;
        assertThat(bytes.toUtf8(), equalTo("foo bar"));
    }

    @Test @Repeat(iterations = 10)
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
            Object mustache = engine.compile(template);
            Object output = engine.execute(mustache, dataMap);

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
