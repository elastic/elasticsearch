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

import com.github.mustachejava.Mustache;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ExecutableScript;
import org.elasticsearch.script.ScriptEngineService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static java.util.Collections.singletonMap;
import static org.elasticsearch.script.mustache.MustacheScriptEngineService.CONTENT_TYPE_PARAM;
import static org.elasticsearch.script.mustache.MustacheScriptEngineService.JSON_CONTENT_TYPE;
import static org.elasticsearch.script.mustache.MustacheScriptEngineService.PLAIN_TEXT_CONTENT_TYPE;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.notNullValue;

public class MustacheTests extends ESTestCase {

    private ScriptEngineService engine = new MustacheScriptEngineService(Settings.EMPTY);

    public void testBasics() {
        String template = "GET _search {\"query\": " + "{\"boosting\": {"
            + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
            + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}"
            + "}}, \"negative_boost\": {{boost_val}} } }}";
        Map<String, Object> params = Collections.singletonMap("boost_val", "0.2");

        Mustache mustache = (Mustache) engine.compile(null, template, Collections.emptyMap());
        CompiledScript compiledScript = new CompiledScript(ScriptService.ScriptType.INLINE, "my-name", "mustache", mustache);
        ExecutableScript result = engine.executable(compiledScript, params);
        assertEquals(
                "Mustache templating broken",
                "GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                        + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}}}, \"negative_boost\": 0.2 } }}",
                ((BytesReference) result.run()).toUtf8()
        );
    }

    public void testArrayAccess() throws Exception {
        String template = "{{data.0}} {{data.1}}";
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(null, template, Collections.emptyMap()));
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
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(null, template, Collections.emptyMap()));
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
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(null, template, Collections.emptyMap()));
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
            new Object[] { singletonMap("key", "foo"), singletonMap("key", "bar") },
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

    public void testEscaping() {
        // json string escaping enabled:
        Map<String, String> params = randomBoolean() ? Collections.emptyMap() : Collections.singletonMap(CONTENT_TYPE_PARAM, JSON_CONTENT_TYPE);
        Mustache mustache = (Mustache) engine.compile(null, "{ \"field1\": \"{{value}}\"}", Collections.emptyMap());
        CompiledScript compiledScript = new CompiledScript(ScriptService.ScriptType.INLINE, "name", "mustache", mustache);
        ExecutableScript executableScript = engine.executable(compiledScript, Collections.singletonMap("value", "a \"value\""));
        BytesReference rawResult = (BytesReference) executableScript.run();
        String result = rawResult.toUtf8();
        assertThat(result, equalTo("{ \"field1\": \"a \\\"value\\\"\"}"));

        // json string escaping disabled:
        mustache = (Mustache) engine.compile(null, "{ \"field1\": \"{{value}}\"}", Collections.singletonMap(CONTENT_TYPE_PARAM, PLAIN_TEXT_CONTENT_TYPE));
        compiledScript = new CompiledScript(ScriptService.ScriptType.INLINE, "name", "mustache", mustache);
        executableScript = engine.executable(compiledScript, Collections.singletonMap("value", "a \"value\""));
        rawResult = (BytesReference) executableScript.run();
        result = rawResult.toUtf8();
        assertThat(result, equalTo("{ \"field1\": \"a \"value\"\"}"));
    }

    public void testSizeAccessForCollectionsAndArrays() throws Exception {
        String[] randomArrayValues = generateRandomStringArray(10, 20, false);
        List<String> randomList = Arrays.asList(generateRandomStringArray(10, 20, false));

        String template = "{{data.array.size}} {{data.list.size}}";
        CompiledScript mustache = new CompiledScript(ScriptService.ScriptType.INLINE, "inline", "mustache", engine.compile(null, template, Collections.emptyMap()));
        Map<String, Object> data = new HashMap<>();
        data.put("array", randomArrayValues);
        data.put("list", randomList);
        Map<String, Object> vars = new HashMap<>();
        vars.put("data", data);

        Object output = engine.executable(mustache, vars).run();
        assertThat(output, notNullValue());
        assertThat(output, instanceOf(BytesReference.class));

        BytesReference bytes = (BytesReference) output;
        String expectedString = String.format(Locale.ROOT, "%s %s", randomArrayValues.length, randomList.size());
        assertThat(bytes.toUtf8(), equalTo(expectedString));
    }
}
