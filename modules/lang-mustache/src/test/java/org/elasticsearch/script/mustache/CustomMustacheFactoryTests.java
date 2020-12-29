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

import org.elasticsearch.common.xcontent.ParsedMediaType;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.script.MustacheMediaType;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.Map;

import static java.util.Collections.singletonMap;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class CustomMustacheFactoryTests extends ESTestCase {
    String JSON_MIME_TYPE_WITH_CHARSET = ParsedMediaType.parseMediaType(XContentType.JSON, Map.of("charset", "utf-8"))
        .responseContentTypeHeader();
    String JSON_MIME_TYPE = ParsedMediaType.parseMediaType(XContentType.JSON, Collections.emptyMap())
        .responseContentTypeHeader();
    String PLAIN_TEXT_MIME_TYPE = ParsedMediaType.parseMediaType(MustacheMediaType.PLAIN_TEXT, Collections.emptyMap())
        .responseContentTypeHeader();
    String X_WWW_FORM_URLENCODED_MIME_TYPE = ParsedMediaType.parseMediaType(MustacheMediaType.X_WWW_FORM_URLENCODED, Collections.emptyMap())
        .responseContentTypeHeader();

    public void testCreateEncoder() {
        {
            final IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new CustomMustacheFactory("non-existent"));
            assertThat(e.getMessage(), equalTo("invalid media-type [non-existent]"));
        }

        {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new CustomMustacheFactory(""));
            assertThat(e.getMessage(), equalTo("invalid media-type []"));
        }

        {
            final IllegalArgumentException e =
                expectThrows(IllegalArgumentException.class, () -> new CustomMustacheFactory("test"));
            assertThat(e.getMessage(), equalTo("invalid media-type [test]"));
        }

        String contentType = ParsedMediaType.parseMediaType("application/json ; charset=UTF-8")
            .responseContentTypeHeader();
        assertThat(new CustomMustacheFactory(contentType).getEncoder(),
            instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
        assertThat(new CustomMustacheFactory(JSON_MIME_TYPE_WITH_CHARSET).getEncoder(),
            instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
        assertThat(new CustomMustacheFactory(JSON_MIME_TYPE).getEncoder(),
            instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));

        assertThat(new CustomMustacheFactory(PLAIN_TEXT_MIME_TYPE).getEncoder(),
            instanceOf(CustomMustacheFactory.DefaultEncoder.class));
        assertThat(new CustomMustacheFactory(X_WWW_FORM_URLENCODED_MIME_TYPE).getEncoder(),
            instanceOf(CustomMustacheFactory.UrlEncoder.class));

        //TODO PG should this be rejected?
        contentType = ParsedMediaType.parseMediaType("application/json ; unknown=UTF-8")
            .responseContentTypeHeader();
        assertThat(new CustomMustacheFactory(contentType).getEncoder(),
            instanceOf(CustomMustacheFactory.JsonEscapeEncoder.class));
    }

    public void testJsonEscapeEncoder() {
        final ScriptEngine engine = new MustacheScriptEngine();
        final Map<String, String> params = randomBoolean() ? singletonMap(Script.CONTENT_TYPE_OPTION, JSON_MIME_TYPE) :
            Collections.emptyMap();

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
