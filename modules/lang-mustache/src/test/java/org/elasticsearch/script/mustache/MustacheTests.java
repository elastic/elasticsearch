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

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.script.ScriptEngine;
import org.elasticsearch.script.ScriptException;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matcher;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;

public class MustacheTests extends ESTestCase {

    private ScriptEngine engine = new MustacheScriptEngine();

    public void testBasics() {
        String template = "GET _search {\"query\": " + "{\"boosting\": {"
            + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
            + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}"
            + "}}, \"negative_boost\": {{boost_val}} } }}";
        Map<String, Object> params = singletonMap("boost_val", "0.2");

        TemplateScript.Factory factory = engine.compile(null, template, TemplateScript.CONTEXT, Collections.emptyMap());
        TemplateScript result = factory.newInstance(params);
        assertEquals(
                "Mustache templating broken",
                "GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                        + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}}}, \"negative_boost\": 0.2 } }}",
                result.execute()
        );
    }

    public void testArrayAccess() throws Exception {
        String template = "{{data.0}} {{data.1}}";
        TemplateScript.Factory factory  = engine.compile(null, template, TemplateScript.CONTEXT, Collections.emptyMap());
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
            new String[] { "foo", "bar" },
            Arrays.asList("foo", "bar"));
        vars.put("data", data);
        assertThat(factory.newInstance(vars).execute(), equalTo("foo bar"));

        // Sets can come out in any order
        Set<String> setData = new HashSet<>();
        setData.add("foo");
        setData.add("bar");
        vars.put("data", setData);
        String output = factory.newInstance(vars).execute();
        assertThat(output, both(containsString("foo")).and(containsString("bar")));
    }

    public void testArrayInArrayAccess() throws Exception {
        String template = "{{data.0.0}} {{data.0.1}}";
        TemplateScript.Factory factory = engine.compile(null, template, TemplateScript.CONTEXT, Collections.emptyMap());
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
            new String[][] { new String[] { "foo", "bar" }},
            Collections.singletonList(new String[] { "foo", "bar" }),
            singleton(new String[] { "foo", "bar" })
        );
        vars.put("data", data);
        assertThat(factory.newInstance(vars).execute(), equalTo("foo bar"));
    }

    public void testMapInArrayAccess() throws Exception {
        String template = "{{data.0.key}} {{data.1.key}}";
        TemplateScript.Factory factory = engine.compile(null, template, TemplateScript.CONTEXT, Collections.emptyMap());
        Map<String, Object> vars = new HashMap<>();
        Object data = randomFrom(
            new Object[] { singletonMap("key", "foo"), singletonMap("key", "bar") },
            Arrays.asList(singletonMap("key", "foo"), singletonMap("key", "bar")));
        vars.put("data", data);
        assertThat(factory.newInstance(vars).execute(), equalTo("foo bar"));

        // HashSet iteration order isn't fixed
        Set<Object> setData = new HashSet<>();
        setData.add(singletonMap("key", "foo"));
        setData.add(singletonMap("key", "bar"));
        vars.put("data", setData);
        String output = factory.newInstance(vars).execute();
        assertThat(output, both(containsString("foo")).and(containsString("bar")));
    }


    public void testSizeAccessForCollectionsAndArrays() throws Exception {
        String[] randomArrayValues = generateRandomStringArray(10, 20, false);
        List<String> randomList = Arrays.asList(generateRandomStringArray(10, 20, false));

        String template = "{{data.array.size}} {{data.list.size}}";
        TemplateScript.Factory factory = engine.compile(null, template, TemplateScript.CONTEXT, Collections.emptyMap());
        Map<String, Object> data = new HashMap<>();
        data.put("array", randomArrayValues);
        data.put("list", randomList);
        Map<String, Object> vars = new HashMap<>();
        vars.put("data", data);
        String expectedString = String.format(Locale.ROOT, "%s %s", randomArrayValues.length, randomList.size());
        assertThat(factory.newInstance(vars).execute(), equalTo(expectedString));
    }

    public void testPrimitiveToJSON() throws Exception {
        String template = "{{#toJson}}ctx{{/toJson}}";
        assertScript(template, singletonMap("ctx", "value"), equalTo("value"));
        assertScript(template, singletonMap("ctx", ""), equalTo(""));
        assertScript(template, singletonMap("ctx", true), equalTo("true"));
        assertScript(template, singletonMap("ctx", 42), equalTo("42"));
        assertScript(template, singletonMap("ctx", 42L), equalTo("42"));
        assertScript(template, singletonMap("ctx", 42.5f), equalTo("42.5"));
        assertScript(template, singletonMap("ctx", null), equalTo(""));

        template = "{{#toJson}}.{{/toJson}}";
        assertScript(template, singletonMap("ctx", "value"), equalTo("{\"ctx\":\"value\"}"));
        assertScript(template, singletonMap("ctx", ""), equalTo("{\"ctx\":\"\"}"));
        assertScript(template, singletonMap("ctx", true), equalTo("{\"ctx\":true}"));
        assertScript(template, singletonMap("ctx", 42), equalTo("{\"ctx\":42}"));
        assertScript(template, singletonMap("ctx", 42L), equalTo("{\"ctx\":42}"));
        assertScript(template, singletonMap("ctx", 42.5f), equalTo("{\"ctx\":42.5}"));
        assertScript(template, singletonMap("ctx", null), equalTo("{\"ctx\":null}"));
    }

    public void testSimpleMapToJSON() throws Exception {
        Map<String, Object> human0 = new HashMap<>();
        human0.put("age", 42);
        human0.put("name", "John Smith");
        human0.put("height", 1.84);

        Map<String, Object> ctx = singletonMap("ctx", human0);

        assertScript("{{#toJson}}.{{/toJson}}", ctx, equalTo("{\"ctx\":{\"name\":\"John Smith\",\"age\":42,\"height\":1.84}}"));
        assertScript("{{#toJson}}ctx{{/toJson}}", ctx, equalTo("{\"name\":\"John Smith\",\"age\":42,\"height\":1.84}"));
        assertScript("{{#toJson}}ctx.name{{/toJson}}", ctx, equalTo("John Smith"));
    }

    public void testMultipleMapsToJSON() throws Exception {
        Map<String, Object> human0 = new HashMap<>();
        human0.put("age", 42);
        human0.put("name", "John Smith");
        human0.put("height", 1.84);

        Map<String, Object> human1 = new HashMap<>();
        human1.put("age", 27);
        human1.put("name", "Dave Smith");
        human1.put("height", 1.71);

        Map<String, Object> humans = new HashMap<>();
        humans.put("first", human0);
        humans.put("second", human1);

        Map<String, Object> ctx = singletonMap("ctx", humans);

        assertScript("{{#toJson}}.{{/toJson}}", ctx,
                equalTo("{\"ctx\":{\"first\":{\"name\":\"John Smith\",\"age\":42,\"height\":1.84},\"second\":" +
                        "{\"name\":\"Dave Smith\",\"age\":27,\"height\":1.71}}}"));

        assertScript("{{#toJson}}ctx{{/toJson}}", ctx,
                equalTo("{\"first\":{\"name\":\"John Smith\",\"age\":42,\"height\":1.84},\"second\":" +
                        "{\"name\":\"Dave Smith\",\"age\":27,\"height\":1.71}}"));

        assertScript("{{#toJson}}ctx.first{{/toJson}}", ctx,
                equalTo("{\"name\":\"John Smith\",\"age\":42,\"height\":1.84}"));

        assertScript("{{#toJson}}ctx.second{{/toJson}}", ctx,
                equalTo("{\"name\":\"Dave Smith\",\"age\":27,\"height\":1.71}"));
    }

    public void testSimpleArrayToJSON() throws Exception {
        String[] array = new String[]{"one", "two", "three"};
        Map<String, Object> ctx = singletonMap("array", array);

        assertScript("{{#toJson}}.{{/toJson}}", ctx, equalTo("{\"array\":[\"one\",\"two\",\"three\"]}"));
        assertScript("{{#toJson}}array{{/toJson}}", ctx, equalTo("[\"one\",\"two\",\"three\"]"));
        assertScript("{{#toJson}}array.0{{/toJson}}", ctx, equalTo("one"));
        assertScript("{{#toJson}}array.1{{/toJson}}", ctx, equalTo("two"));
        assertScript("{{#toJson}}array.2{{/toJson}}", ctx, equalTo("three"));
        assertScript("{{#toJson}}array.size{{/toJson}}", ctx, equalTo("3"));
    }

    public void testSimpleListToJSON() throws Exception {
        List<String> list = Arrays.asList("one", "two", "three");
        Map<String, Object> ctx = singletonMap("ctx", list);

        assertScript("{{#toJson}}.{{/toJson}}", ctx, equalTo("{\"ctx\":[\"one\",\"two\",\"three\"]}"));
        assertScript("{{#toJson}}ctx{{/toJson}}", ctx, equalTo("[\"one\",\"two\",\"three\"]"));
        assertScript("{{#toJson}}ctx.0{{/toJson}}", ctx, equalTo("one"));
        assertScript("{{#toJson}}ctx.1{{/toJson}}", ctx, equalTo("two"));
        assertScript("{{#toJson}}ctx.2{{/toJson}}", ctx, equalTo("three"));
        assertScript("{{#toJson}}ctx.size{{/toJson}}", ctx, equalTo("3"));
    }

    public void testsUnsupportedTagsToJson() {
        final String script = "{{#toJson}}{{foo}}{{bar}}{{/toJson}}";
        ScriptException e = expectThrows(ScriptException.class, () -> compile(script));
        assertThat(e.getMessage(), containsString("Mustache function [toJson] must contain one and only one identifier"));
        assertEquals(MustacheScriptEngine.NAME, e.getLang());
        assertEquals(script, e.getScript());

        final String script2 = "{{#toJson}}{{/toJson}}";
        e = expectThrows(ScriptException.class, () -> compile(script2));
        assertThat(e.getMessage(), containsString("Mustache function [toJson] must contain one and only one identifier"));
        assertEquals(MustacheScriptEngine.NAME, e.getLang());
        assertEquals(script2, e.getScript());
    }

    public void testEmbeddedToJSON() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                        .startArray("bulks")
                            .startObject()
                                .field("index", "index-1")
                                .field("type", "type-1")
                                .field("id", 1)
                            .endObject()
                            .startObject()
                                .field("index", "index-2")
                                .field("type", "type-2")
                                .field("id", 2)
                            .endObject()
                        .endArray()
                    .endObject();

        Map<String, Object> ctx =
            singletonMap("ctx", XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2());

        assertScript("{{#ctx.bulks}}{{#toJson}}.{{/toJson}}{{/ctx.bulks}}", ctx,
                equalTo("{\"index\":\"index-1\",\"id\":1,\"type\":\"type-1\"}{\"index\":\"index-2\",\"id\":2,\"type\":\"type-2\"}"));

        assertScript("{{#ctx.bulks}}<{{#toJson}}id{{/toJson}}>{{/ctx.bulks}}", ctx,
                equalTo("<1><2>"));
    }

    public void testSimpleArrayJoin() throws Exception {
        String template = "{{#join}}array{{/join}}";
        assertScript(template, singletonMap("array", new String[]{"one", "two", "three"}), equalTo("one,two,three"));
        assertScript(template, singletonMap("array", new int[]{1, 2, 3}), equalTo("1,2,3"));
        assertScript(template, singletonMap("array", new long[]{1L, 2L, 3L}), equalTo("1,2,3"));
        assertScript(template, singletonMap("array", new double[]{1.5, 2.5, 3.5}), equalTo("1.5,2.5,3.5"));
        assertScript(template, singletonMap("array", new boolean[]{true, false, true}), equalTo("true,false,true"));
        assertScript(template, singletonMap("array", new boolean[]{true, false, true}), equalTo("true,false,true"));
    }

    public void testEmbeddedArrayJoin() throws Exception {
        XContentBuilder builder = jsonBuilder().startObject()
                                                    .startArray("people")
                                                        .startObject()
                                                            .field("name", "John Smith")
                                                            .startArray("emails")
                                                                .value("john@smith.com")
                                                                .value("john.smith@email.com")
                                                                .value("jsmith@email.com")
                                                            .endArray()
                                                        .endObject()
                                                        .startObject()
                                                            .field("name", "John Doe")
                                                            .startArray("emails")
                                                                .value("john@doe.com")
                                                                .value("john.doe@email.com")
                                                                .value("jdoe@email.com")
                                                            .endArray()
                                                        .endObject()
                                                    .endArray()
                                                .endObject();

        Map<String, Object> ctx =
            singletonMap("ctx", XContentHelper.convertToMap(BytesReference.bytes(builder), false, builder.contentType()).v2());

        assertScript("{{#join}}ctx.people.0.emails{{/join}}", ctx,
                equalTo("john@smith.com,john.smith@email.com,jsmith@email.com"));

        assertScript("{{#join}}ctx.people.1.emails{{/join}}", ctx,
                equalTo("john@doe.com,john.doe@email.com,jdoe@email.com"));

        assertScript("{{#ctx.people}}to: {{#join}}emails{{/join}};{{/ctx.people}}", ctx,
                equalTo("to: john@smith.com,john.smith@email.com,jsmith@email.com;to: john@doe.com,john.doe@email.com,jdoe@email.com;"));
    }

    public void testJoinWithToJson() {
        Map<String, Object> params = singletonMap("terms",
                Arrays.asList(singletonMap("term", "foo"), singletonMap("term", "bar")));

        assertScript("{{#join}}{{#toJson}}terms{{/toJson}}{{/join}}", params,
                equalTo("[{\"term\":\"foo\"},{\"term\":\"bar\"}]"));
    }

    public void testsUnsupportedTagsJoin() {
        final String script = "{{#join}}{{/join}}";
        ScriptException e = expectThrows(ScriptException.class, () -> compile(script));
        assertThat(e.getMessage(), containsString("Mustache function [join] must contain one and only one identifier"));
        assertEquals(MustacheScriptEngine.NAME, e.getLang());
        assertEquals(script, e.getScript());

        final String script2 = "{{#join delimiter='a'}}{{/join delimiter='b'}}";
        e = expectThrows(ScriptException.class, () -> compile(script2));
        assertThat(e.getMessage(), containsString("Mismatched start/end tags"));
        assertEquals(MustacheScriptEngine.NAME, e.getLang());
        assertEquals(script2, e.getScript());
    }

    public void testJoinWithCustomDelimiter() {
        Map<String, Object> params = singletonMap("params", Arrays.asList(1, 2, 3, 4));

        assertScript("{{#join delimiter=''}}params{{/join delimiter=''}}", params, equalTo("1234"));
        assertScript("{{#join delimiter=','}}params{{/join delimiter=','}}", params, equalTo("1,2,3,4"));
        assertScript("{{#join delimiter='/'}}params{{/join delimiter='/'}}", params, equalTo("1/2/3/4"));
        assertScript("{{#join delimiter=' and '}}params{{/join delimiter=' and '}}", params, equalTo("1 and 2 and 3 and 4"));
    }

    public void testUrlEncoder() {
        Map<String, String> urls = new HashMap<>();
        urls.put("https://www.elastic.co",
                "https%3A%2F%2Fwww.elastic.co");
        urls.put("<logstash-{now/d}>",
                "%3Clogstash-%7Bnow%2Fd%7D%3E");
        urls.put("?query=(foo:A OR baz:B) AND title:/joh?n(ath[oa]n)/ AND date:{* TO 2012-01}",
                "%3Fquery%3D%28foo%3AA+OR+baz%3AB%29+AND+title%3A%2Fjoh%3Fn%28ath%5Boa%5Dn%29%2F+AND+date%3A%7B*+TO+2012-01%7D");

        for (Map.Entry<String, String> url : urls.entrySet()) {
            assertScript("{{#url}}{{params}}{{/url}}", singletonMap("params", url.getKey()), equalTo(url.getValue()));
        }
    }

    public void testUrlEncoderWithParam() throws Exception {
        assertScript("{{#url}}{{index}}{{/url}}", singletonMap("index", "<logstash-{now/d{YYYY.MM.dd|+12:00}}>"),
                equalTo("%3Clogstash-%7Bnow%2Fd%7BYYYY.MM.dd%7C%2B12%3A00%7D%7D%3E"));

        final String random = randomAlphaOfLength(10);
        assertScript("{{#url}}prefix_{{s}}{{/url}}", singletonMap("s", random),
                equalTo("prefix_" + URLEncoder.encode(random, StandardCharsets.UTF_8.name())));
    }

    public void testUrlEncoderWithJoin() {
        Map<String, Object> params = singletonMap("emails", Arrays.asList("john@smith.com", "john.smith@email.com", "jsmith@email.com"));
        assertScript("?query={{#url}}{{#join}}emails{{/join}}{{/url}}", params,
                equalTo("?query=john%40smith.com%2Cjohn.smith%40email.com%2Cjsmith%40email.com"));

        params = singletonMap("indices", new String[]{"<logstash-{now/d-2d}>", "<logstash-{now/d-1d}>", "<logstash-{now/d}>"});
        assertScript("{{#url}}https://localhost:9200/{{#join}}indices{{/join}}/_stats{{/url}}", params,
                equalTo("https%3A%2F%2Flocalhost%3A9200%2F%3Clogstash-%7Bnow%2Fd-2d%7D" +
                        "%3E%2C%3Clogstash-%7Bnow%2Fd-1d%7D%3E%2C%3Clogstash-%7Bnow%2Fd%7D%3E%2F_stats"));

        params = singletonMap("fibonacci", new int[]{1, 1, 2, 3, 5, 8, 13, 21, 34, 55});
        assertScript("{{#url}}{{#join delimiter='+'}}fibonacci{{/join delimiter='+'}}{{/url}}", params,
                equalTo("1%2B1%2B2%2B3%2B5%2B8%2B13%2B21%2B34%2B55"));
    }

    private void assertScript(String script, Map<String, Object> vars, Matcher<String> matcher) {
        String result = compile(script).newInstance(vars).execute();
        assertThat(result, matcher);
    }

    private TemplateScript.Factory compile(String script) {
        assertThat("cannot compile null or empty script", script, not(isEmptyOrNullString()));
        return engine.compile(null, script, TemplateScript.CONTEXT, Collections.emptyMap());
    }
}
