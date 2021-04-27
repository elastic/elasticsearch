/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.script.mustache.CustomMustacheFactory.JSON_MEDIA_TYPE;
import static org.elasticsearch.script.mustache.CustomMustacheFactory.PLAIN_TEXT_MEDIA_TYPE;
import static org.elasticsearch.script.mustache.CustomMustacheFactory.X_WWW_FORM_URLENCODED_MEDIA_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CustomMustacheFactoryTests extends ESTestCase {

    public void testCreateEncoder() {
        {
            final IllegalArgumentException e =
                    expectThrows(IllegalArgumentException.class, () -> CustomMustacheFactory.createEncoder("non-existent"));
            assertThat(e.getMessage(), equalTo("No encoder found for media type [non-existent]"));
        }

        {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CustomMustacheFactory.createEncoder(""));
            assertThat(e.getMessage(), equalTo("No encoder found for media type []"));
        }

        {
            final IllegalArgumentException e =
                    expectThrows(IllegalArgumentException.class, () -> CustomMustacheFactory.createEncoder("test"));
            assertThat(e.getMessage(), equalTo("No encoder found for media type [test]"));
        }

        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.JSON_MEDIA_TYPE_WITH_CHARSET),
            instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.JSON_MEDIA_TYPE),
                instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.PLAIN_TEXT_MEDIA_TYPE),
                instanceOf(CustomMustacheFactory.DefaultEncoder.class));
        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.X_WWW_FORM_URLENCODED_MEDIA_TYPE),
                instanceOf(CustomMustacheFactory.UrlEncoder.class));
    }

    public void testJsonEscapeEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = randomBoolean() ? singletonMap(Script.CONTENT_TYPE_OPTION, JSON_MEDIA_TYPE) : emptyMap();

        TemplateScript.Factory compiled = engine.compile(null, "{\"field\": \"{{value}}\"}", TemplateScript.CONTEXT, params);

        TemplateScript executable = compiled.newInstance(singletonMap("value", "a \"value\""));
        assertThat(executable.execute(), equalTo("{\"field\": \"a \\\"value\\\"\"}"));
    }

    public void testDefaultEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = singletonMap(Script.CONTENT_TYPE_OPTION, PLAIN_TEXT_MEDIA_TYPE);

        TemplateScript.Factory compiled = engine.compile(null, "{\"field\": \"{{value}}\"}", TemplateScript.CONTEXT, params);

        TemplateScript executable = compiled.newInstance(singletonMap("value", "a \"value\""));
        assertThat(executable.execute(), equalTo("{\"field\": \"a \"value\"\"}"));
    }

    public void testUrlEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = singletonMap(Script.CONTENT_TYPE_OPTION, X_WWW_FORM_URLENCODED_MEDIA_TYPE);

        TemplateScript.Factory compiled = engine.compile(null, "{\"field\": \"{{value}}\"}", TemplateScript.CONTEXT, params);

        TemplateScript executable = compiled.newInstance(singletonMap("value", "tilde~ AND date:[2016 FROM*]"));
        assertThat(executable.execute(), equalTo("{\"field\": \"tilde%7E+AND+date%3A%5B2016+FROM*%5D\"}"));
    }
}
