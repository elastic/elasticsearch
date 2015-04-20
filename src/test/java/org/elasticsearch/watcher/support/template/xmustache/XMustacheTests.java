/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.watcher.support.template.xmustache;

import com.carrotsearch.ant.tasks.junit4.dependencies.com.google.common.collect.ImmutableList;
import com.carrotsearch.randomizedtesting.annotations.Repeat;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.ImmutableSet;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

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
        engine = new XMustacheScriptEngineService(ImmutableSettings.EMPTY);
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
}
