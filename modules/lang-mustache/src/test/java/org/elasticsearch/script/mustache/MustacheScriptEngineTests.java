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
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.script.CompiledScript;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.io.StringWriter;
import java.nio.charset.Charset;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

/**
 * Mustache based templating test
 */
public class MustacheScriptEngineTests extends ESTestCase {
    private MustacheScriptEngineService qe;
    private JsonEscapingMustacheFactory escaper;

    @Before
    public void setup() {
        qe = new MustacheScriptEngineService(Settings.Builder.EMPTY_SETTINGS);
        escaper = new JsonEscapingMustacheFactory();
    }

    public void testSimpleParameterReplace() {
        {
            String template = "GET _search {\"query\": " + "{\"boosting\": {" + "\"positive\": {\"match\": {\"body\": \"gift\"}},"
                    + "\"negative\": {\"term\": {\"body\": {\"value\": \"solr\"}" + "}}, \"negative_boost\": {{boost_val}} } }}";
            Map<String, Object> vars = new HashMap<>();
            vars.put("boost_val", "0.3");
            BytesReference o = (BytesReference) qe.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "", "mustache", qe.compile(template)), vars).run();
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
            BytesReference o = (BytesReference) qe.executable(new CompiledScript(ScriptService.ScriptType.INLINE, "", "mustache", qe.compile(template)), vars).run();
            assertEquals("GET _search {\"query\": {\"boosting\": {\"positive\": {\"match\": {\"body\": \"gift\"}},"
                    + "\"negative\": {\"term\": {\"body\": {\"value\": \"\\\"quick brown\\\"\"}}}, \"negative_boost\": 0.3 } }}",
                    new String(o.toBytes(), Charset.forName("UTF-8")));
        }
    }

    public void testEscapeJson() throws IOException {
        {
            StringWriter writer = new StringWriter();
            escaper.encode("hello \n world", writer);
            assertThat(writer.toString(), equalTo("hello \\n world"));
        }
        {
            StringWriter writer = new StringWriter();
            escaper.encode("\n", writer);
            assertThat(writer.toString(), equalTo("\\n"));
        }

        Character[] specialChars = new Character[]{
                '\"',
                '\\',
                '\u0000',
                '\u0001',
                '\u0002',
                '\u0003',
                '\u0004',
                '\u0005',
                '\u0006',
                '\u0007',
                '\u0008',
                '\u0009',
                '\u000B',
                '\u000C',
                '\u000E',
                '\u000F',
                '\u001F'};
        String[] escapedChars = new String[]{
                "\\\"",
                "\\\\",
                "\\u0000",
                "\\u0001",
                "\\u0002",
                "\\u0003",
                "\\u0004",
                "\\u0005",
                "\\u0006",
                "\\u0007",
                "\\u0008",
                "\\u0009",
                "\\u000B",
                "\\u000C",
                "\\u000E",
                "\\u000F",
                "\\u001F"};
        int iters = scaledRandomIntBetween(100, 1000);
        for (int i = 0; i < iters; i++) {
            int rounds = scaledRandomIntBetween(1, 20);
            StringWriter expect = new StringWriter();
            StringWriter writer = new StringWriter();
            for (int j = 0; j < rounds; j++) {
                String s = getChars();
                writer.write(s);
                expect.write(s);

                int charIndex = randomInt(7);
                writer.append(specialChars[charIndex]);
                expect.append(escapedChars[charIndex]);
            }
            StringWriter target = new StringWriter();
            escaper.encode(writer.toString(), target);
            assertThat(expect.toString(), equalTo(target.toString()));
        }
    }

    private String getChars() {
        String string = randomRealisticUnicodeOfCodepointLengthBetween(0, 10);
        for (int i = 0; i < string.length(); i++) {
            if (isEscapeChar(string.charAt(i))) {
                return string.substring(0, i);
            }
        }
        return string;
    }

    /**
     * From https://www.ietf.org/rfc/rfc4627.txt:
     *
     * All Unicode characters may be placed within the
     * quotation marks except for the characters that must be escaped:
     * quotation mark, reverse solidus, and the control characters (U+0000
     * through U+001F).
     * */
    private static boolean isEscapeChar(char c) {
        switch (c) {
        case '"':
        case '\\':
            return true;
        }

        if (c < '\u002F')
            return true;
        return false;
    }
}
