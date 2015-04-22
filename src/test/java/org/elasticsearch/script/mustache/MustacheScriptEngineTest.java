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

import com.carrotsearch.randomizedtesting.generators.RandomPicks;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.test.ElasticsearchTestCase;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.*;

/**
 * Mustache based templating test
 */
public class MustacheScriptEngineTest extends ElasticsearchTestCase {

    private MustacheScriptEngineService engine;

    @Before
    public void setup() {
        engine = new MustacheScriptEngineService(ImmutableSettings.Builder.EMPTY_SETTINGS);
    }

    @Test
    public void testSimpleParameterReplace() {
        {
            String template = "GET _search {\"query\": " + "{\"boosting\": {" + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
                    + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}" + "}}, \"negative_boost\": {{boost_val}} } }}";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            BytesReference o = (BytesReference) engine.execute(engine.compile(template), vars);
            assertEquals("GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                            + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}}}, \"negative_boost\": 0.3 } }}",
                    new String(o.toBytes(), Charset.forName("UTF-8")));
        }
        {
            String template = "GET _search {\"query\": " + "{\"boosting\": {" + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
                    + "\"negative\": {\"term\": {\"body\": {\"value\": \"{{body_val}}\"}" + "}}, \"negative_boost\": {{boost_val}} } }}";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            vars.put("body_val", "\"quick brown\"");
            BytesReference o = (BytesReference) engine.execute(engine.compile(template), vars);
            assertEquals("GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                            + "\"negative\": {\"term\": {\"body\": {\"value\": \"\\\"quick brown\\\"\"}}}, \"negative_boost\": 0.3 } }}",
                    new String(o.toBytes(), Charset.forName("UTF-8")));
        }
    }

    @Test
    public void testEscapeJson() throws IOException {
        {
            StringWriter writer = new StringWriter();
            InternalMustacheFactory.escape("hello \n world", writer);
            assertThat(writer.toString(), equalTo("hello \\\n world"));
        }
        {
            StringWriter writer = new StringWriter();
            InternalMustacheFactory.escape("\n", writer);
            assertThat(writer.toString(), equalTo("\\\n"));
        }

        Character[] specialChars = new Character[]{'\f', '\n', '\r', '"', '\\', (char) 11, '\t', '\b' };
        int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            int rounds = scaledRandomIntBetween(1, 20);
            StringWriter escaped = new StringWriter();
            StringWriter writer = new StringWriter();
            for (int j = 0; j < rounds; j++) {
                String s = getChars();
                writer.write(s);
                escaped.write(s);
                char c = RandomPicks.randomFrom(getRandom(), specialChars);
                writer.append(c);
                escaped.append('\\');
                escaped.append(c);
            }
            StringWriter target = new StringWriter();
            assertThat(escaped.toString(), equalTo(InternalMustacheFactory.escape(writer.toString(), target).toString()));
        }
    }

    @Test
    public void testArrayAccess() throws Exception {
        Object[] datas = new Object[] {
                new String[] { "bar", "foo" },
                ImmutableList.of("bar", "foo"),
                ImmutableSortedSet.of("bar", "foo")
        };
        for (Object data : datas) {
            Map<String, Object> vars = new HashMap<>();
            vars.put("data", data);
            Object output = engine.execute(engine.compile("{{data.0}} {{data.1}}"), vars);
            assertThat(output, notNullValue());
            assertThat(output, instanceOf(BytesReference.class));
            BytesReference bytes = (BytesReference) output;
            assertThat(bytes.toUtf8(), equalTo("bar foo"));
        }
    }

    @Test
    public void testArrayInArrayAccess() throws Exception {
        Object[] datas = new Object[] {
                new String[] { "bar", "foo" },
                ImmutableList.of("bar", "foo"),
                ImmutableSortedSet.of("bar", "foo")
        };
        for (Object data : datas) {
            Map<String, Object> vars = new HashMap<>();
            vars.put("data", new Object[] { data });
            Object output = engine.execute(engine.compile("{{data.0.0}} {{data.0.1}}"), vars);
            assertThat(output, notNullValue());
            assertThat(output, instanceOf(BytesReference.class));
            BytesReference bytes = (BytesReference) output;
            assertThat(bytes.toUtf8(), equalTo("bar foo"));
        }
    }

    @Test
    public void testMapInArrayAccess() throws Exception {
        Map<String, Object> vars = new HashMap<>();
        Object[] datas = new Object[] {
                new Map[] { ImmutableMap.<String, Object>of("key", "bar"), ImmutableMap.<String, Object>of("key", "foo") },
                ImmutableList.of(ImmutableMap.<String, Object>of("key", "bar"), ImmutableMap.<String, Object>of("key", "foo")),
                ImmutableSortedSet.orderedBy(new MapKeyValueComparator("key"))
                        .add(ImmutableMap.<String, Object>of("key", "bar"))
                        .add(ImmutableMap.<String, Object>of("key", "foo"))
                        .build()
        };
        for (Object data : datas) {
            vars.put("data", data);
            Object output = engine.execute(engine.compile("{{data.0.key}} {{data.1.key}}"), vars);
            assertThat(output, notNullValue());
            assertThat(output, instanceOf(BytesReference.class));
            BytesReference bytes = (BytesReference) output;
            assertThat(bytes.toUtf8(), equalTo("bar foo"));
        }
    }

    private String getChars() {
        String string = randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
        for (int i = 0; i < string.length(); i++) {
            if (InternalMustacheFactory.isEscapeChar(string.charAt(i))) {
                return string.substring(0, i);
            }
        }
        return string;
    }

    private static class MapKeyValueComparator implements Comparator<Map<String, Object>> {

        private final String key;

        public MapKeyValueComparator(String key) {
            this.key = key;
        }

        @Override
        public int compare(Map<String, Object> m1, Map<String, Object> m2) {
            return ((String) m1.get(key)).compareTo((String) m2.get(key));
        }
    }

}
