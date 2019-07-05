/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.script.mustache;

import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.Map;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.script.mustache.CustomMustacheFactory.JSON_MIME_TYPE;
import static org.elasticsearch.script.mustache.CustomMustacheFactory.PLAIN_TEXT_MIME_TYPE;
import static org.elasticsearch.script.mustache.CustomMustacheFactory.X_WWW_FORM_URLENCODED_MIME_TYPE;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CustomMustacheFactoryTests extends ESTestCase {

    public void testCreateEncoder() {
        {
            final IllegalArgumentException e =
                    expectThrows(IllegalArgumentException.class, () -> CustomMustacheFactory.createEncoder("non-existent"));
            assertThat(e.getMessage(), equalTo("No encoder found for MIME type [non-existent]"));
        }

        {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> CustomMustacheFactory.createEncoder(""));
            assertThat(e.getMessage(), equalTo("No encoder found for MIME type []"));
        }

        {
            final IllegalArgumentException e =
                    expectThrows(IllegalArgumentException.class, () -> CustomMustacheFactory.createEncoder("test"));
            assertThat(e.getMessage(), equalTo("No encoder found for MIME type [test]"));
        }

        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.JSON_MIME_TYPE_WITH_CHARSET),
            instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.JSON_MIME_TYPE),
                instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.PLAIN_TEXT_MIME_TYPE),
                instanceOf(CustomMustacheFactory.DefaultEncoder.class));
        assertThat(CustomMustacheFactory.createEncoder(CustomMustacheFactory.X_WWW_FORM_URLENCODED_MIME_TYPE),
                instanceOf(CustomMustacheFactory.UrlEncoder.class));
    }

    public void testJsonEscapeEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = randomBoolean() ? singletonMap(Script.CONTENT_TYPE_OPTION, JSON_MIME_TYPE) : emptyMap();

        TemplateScript.Factory compiled = engine.compile(null, "{\"field\": \"{{value}}\"}", TemplateScript.CONTEXT, params);

        TemplateScript executable = compiled.newInstance(singletonMap("value", "a \"value\""));
        assertThat(executable.execute(), equalTo("{\"field\": \"a \\\"value\\\"\"}"));
    }

    public void testDefaultEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = singletonMap(Script.CONTENT_TYPE_OPTION, PLAIN_TEXT_MIME_TYPE);

        TemplateScript.Factory compiled = engine.compile(null, "{\"field\": \"{{value}}\"}", TemplateScript.CONTEXT, params);

        TemplateScript executable = compiled.newInstance(singletonMap("value", "a \"value\""));
        assertThat(executable.execute(), equalTo("{\"field\": \"a \"value\"\"}"));
    }

    public void testUrlEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = singletonMap(Script.CONTENT_TYPE_OPTION, X_WWW_FORM_URLENCODED_MIME_TYPE);

        TemplateScript.Factory compiled = engine.compile(null, "{\"field\": \"{{value}}\"}", TemplateScript.CONTEXT, params);

        TemplateScript executable = compiled.newInstance(singletonMap("value", "tilde~ AND date:[2016 FROM*]"));
        assertThat(executable.execute(), equalTo("{\"field\": \"tilde%7E+AND+date%3A%5B2016+FROM*%5D\"}"));
    }
}
