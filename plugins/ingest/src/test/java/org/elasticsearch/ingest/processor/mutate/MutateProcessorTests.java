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

package org.elasticsearch.ingest.processor.mutate;

import org.elasticsearch.ingest.Data;
import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.*;
import java.util.regex.Pattern;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class MutateProcessorTests extends ESTestCase {
    private Data data;

    @Before
    public void setData() {
        Map<String, Object> document = new HashMap<>();
        document.put("foo", "bar");
        document.put("alpha", "aBcD");
        document.put("num", "64");
        document.put("to_strip", " clean    ");
        document.put("arr", Arrays.asList("1", "2", "3"));
        document.put("ip", "127.0.0.1");
        Map<String, Object> fizz = new HashMap<>();
        fizz.put("buzz", "hello world");
        document.put("fizz", fizz);

        data = new Data("index", "type", "id", document);
    }

    public void testUpdate() throws IOException {
        Map<String, Object> update = new HashMap<>();
        update.put("foo", 123);
        Processor processor = new MutateProcessor(update, null, null, null, null, null, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("foo", Integer.class), equalTo(123));
    }

    public void testRename() throws IOException {
        Map<String, String> rename = new HashMap<>();
        rename.put("foo", "bar");
        Processor processor = new MutateProcessor(null, rename, null, null, null, null, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("bar", String.class), equalTo("bar"));
        assertThat(data.hasPropertyValue("foo"), is(false));
    }

    public void testConvert() throws IOException {
        Map<String, String> convert = new HashMap<>();
        convert.put("num", "integer");
        Processor processor = new MutateProcessor(null, null, convert, null, null, null, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("num", Integer.class), equalTo(64));
    }

    public void testConvertNullField() throws IOException {
        Map<String, String> convert = new HashMap<>();
        convert.put("null", "integer");
        Processor processor = new MutateProcessor(null, null, convert, null, null, null, null, null, null, null);
        try {
            processor.execute(data);
            fail("processor execute should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field \"null\" is null, cannot be converted to a/an integer"));
        }
    }

    public void testConvertList() throws IOException {
        Map<String, String> convert = new HashMap<>();
        convert.put("arr", "integer");
        Processor processor = new MutateProcessor(null, null, convert, null, null, null, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("arr", List.class), equalTo(Arrays.asList(1, 2, 3)));
    }

    public void testSplit() throws IOException {
        Map<String, String> split = new HashMap<>();
        split.put("ip", "\\.");
        Processor processor = new MutateProcessor(null, null, null, split, null, null, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("ip", List.class), equalTo(Arrays.asList("127", "0", "0", "1")));
    }

    public void testSplitNullValue() throws IOException {
        Map<String, String> split = new HashMap<>();
        split.put("not.found", "\\.");
        Processor processor = new MutateProcessor(null, null, null, split, null, null, null, null, null, null);
        try {
            processor.execute(data);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Cannot split field. [not.found] is null."));
        }
    }

    public void testGsub() throws IOException {
        List<GsubExpression> gsubExpressions = Collections.singletonList(new GsubExpression("ip", Pattern.compile("\\."), "-"));
        Processor processor = new MutateProcessor(null, null, null, null, gsubExpressions, null, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("ip", String.class), equalTo("127-0-0-1"));
    }

    public void testGsub_NullValue() throws IOException {
        List<GsubExpression> gsubExpressions = Collections.singletonList(new GsubExpression("null_field", Pattern.compile("\\."), "-"));
        Processor processor = new MutateProcessor(null, null, null, null, gsubExpressions, null, null, null, null, null);
        try {
            processor.execute(data);
            fail("processor execution should have failed");
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field \"null_field\" is null, cannot match pattern."));
        }
    }

    public void testJoin() throws IOException {
        HashMap<String, String> join = new HashMap<>();
        join.put("arr", "-");
        Processor processor = new MutateProcessor(null, null, null, null, null, join, null, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("arr", String.class), equalTo("1-2-3"));
    }

    public void testRemove() throws IOException {
        List<String> remove = Arrays.asList("foo", "ip");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, remove, null, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(5));
        assertThat(data.getPropertyValue("foo", Object.class), nullValue());
        assertThat(data.getPropertyValue("ip", Object.class), nullValue());
    }

    public void testTrim() throws IOException {
        List<String> trim = Arrays.asList("to_strip", "foo");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, null, trim, null, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("foo", String.class), equalTo("bar"));
        assertThat(data.getPropertyValue("to_strip", String.class), equalTo("clean"));
    }

    public void testTrimNullValue() throws IOException {
        List<String> trim = Collections.singletonList("not.found");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, null, trim, null, null);
        try {
            processor.execute(data);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Cannot trim field. [not.found] is null."));
        }
    }

    public void testUppercase() throws IOException {
        List<String> uppercase = Collections.singletonList("foo");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, null, null, uppercase, null);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("foo", String.class), equalTo("BAR"));
    }

    public void testUppercaseNullValue() throws IOException {
        List<String> uppercase = Collections.singletonList("not.found");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, null, null, uppercase, null);
        try {
            processor.execute(data);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Cannot uppercase field. [not.found] is null."));
        }
    }

    public void testLowercase() throws IOException {
        List<String> lowercase = Collections.singletonList("alpha");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, null, null, null, lowercase);
        processor.execute(data);
        assertThat(data.getDocument().size(), equalTo(7));
        assertThat(data.getPropertyValue("alpha", String.class), equalTo("abcd"));
    }

    public void testLowercaseNullValue() throws IOException {
        List<String> lowercase = Collections.singletonList("not.found");
        Processor processor = new MutateProcessor(null, null, null, null, null, null, null, null, null, lowercase);
        try {
            processor.execute(data);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Cannot lowercase field. [not.found] is null."));
        }
    }
}
