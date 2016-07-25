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

import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.nullValue;

public class SearchTemplateRequestTests extends ESTestCase {

    public void testParseInlineTemplate() throws Exception {
        String source = "{" +
                "    'inline' : {\n" +
                "    'query': {\n" +
                "      'terms': {\n" +
                "        'status': [\n" +
                "          '{{#status}}',\n" +
                "          '{{.}}',\n" +
                "          '{{/status}}'\n" +
                "        ]\n" +
                "      }\n" +
                "    }\n" +
                "  }" +
                "}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("{\"query\":{\"terms\":{\"status\":[\"{{#status}}\",\"{{.}}\",\"{{/status}}\"]}}}"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.INLINE));
        assertThat(request.getScriptParams(), nullValue());
    }

    public void testParseInlineTemplateWithParams() throws Exception {
        String source = "{" +
                "    'inline' : {" +
                "      'query': { 'match' : { '{{my_field}}' : '{{my_value}}' } }," +
                "      'size' : '{{my_size}}'" +
                "    }," +
                "    'params' : {" +
                "        'my_field' : 'foo'," +
                "        'my_value' : 'bar'," +
                "        'my_size' : 5" +
                "    }" +
                "}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("{\"query\":{\"match\":{\"{{my_field}}\":\"{{my_value}}\"}},\"size\":\"{{my_size}}\"}"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.INLINE));
        assertThat(request.getScriptParams().size(), equalTo(3));
        assertThat(request.getScriptParams(), hasEntry("my_field", "foo"));
        assertThat(request.getScriptParams(), hasEntry("my_value", "bar"));
        assertThat(request.getScriptParams(), hasEntry("my_size", 5));
    }

    public void testParseInlineTemplateAsString() throws Exception {
        String source = "{'inline' : '{\\\"query\\\":{\\\"bool\\\":{\\\"must\\\":{\\\"match\\\":{\\\"foo\\\":\\\"{{text}}\\\"}}}}}'}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("{\"query\":{\"bool\":{\"must\":{\"match\":{\"foo\":\"{{text}}\"}}}}}"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.INLINE));
        assertThat(request.getScriptParams(), nullValue());
    }

    @SuppressWarnings("unchecked")
    public void testParseInlineTemplateAsStringWithParams() throws Exception {
        String source = "{'inline' : '{\\\"query\\\":{\\\"match\\\":{\\\"{{field}}\\\":\\\"{{value}}\\\"}}}', " +
                "'params': {'status': ['pending', 'published']}}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("{\"query\":{\"match\":{\"{{field}}\":\"{{value}}\"}}}"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.INLINE));
        assertThat(request.getScriptParams().size(), equalTo(1));
        assertThat(request.getScriptParams(), hasKey("status"));
        assertThat((List<String>) request.getScriptParams().get("status"), hasItems("pending", "published"));
    }

    public void testParseFileTemplate() throws Exception {
        String source = "{'file' : 'fileTemplate'}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("fileTemplate"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.FILE));
        assertThat(request.getScriptParams(), nullValue());
    }

    public void testParseFileTemplateWithParams() throws Exception {
        String source = "{'file' : 'template_foo', 'params' : {'foo': 'bar', 'size': 500}}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("template_foo"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.FILE));
        assertThat(request.getScriptParams().size(), equalTo(2));
        assertThat(request.getScriptParams(), hasEntry("foo", "bar"));
        assertThat(request.getScriptParams(), hasEntry("size", 500));
    }

    public void testParseStoredTemplate() throws Exception {
        String source = "{'id' : 'storedTemplate'}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("storedTemplate"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.STORED));
        assertThat(request.getScriptParams(), nullValue());
    }

    public void testParseStoredTemplateWithParams() throws Exception {
        String source = "{'id' : 'another_template', 'params' : {'bar': 'foo'}}";

        SearchTemplateRequest request = RestSearchTemplateAction.parse(newBytesReference(source));
        assertThat(request.getScript(), equalTo("another_template"));
        assertThat(request.getScriptType(), equalTo(ScriptService.ScriptType.STORED));
        assertThat(request.getScriptParams().size(), equalTo(1));
        assertThat(request.getScriptParams(), hasEntry("bar", "foo"));
    }

    public void testParseWrongTemplate() {
        // Unclosed template id
        expectThrows(ParsingException.class, () -> RestSearchTemplateAction.parse(newBytesReference("{'id' : 'another_temp }")));
    }

    /**
     * Creates a {@link BytesReference} with the given string while replacing single quote to double quotes.
     */
    private static BytesReference newBytesReference(String s) {
        assertNotNull(s);
        return new BytesArray(s.replace("'", "\""));
    }
}
