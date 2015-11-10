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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;


public class MutateProcessorTests extends ESTestCase {
    private static final MutateProcessor.Factory FACTORY = new MutateProcessor.Factory();
    private Data data;
    private Map<String, Object> config;

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
        config = new HashMap<>();
    }

    public void testUpdate() throws IOException {
        Map<String, Object> update = new HashMap<>();
        update.put("foo", 123);
        config.put("update", update);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("foo"), equalTo(123));
    }

    public void testRename() throws IOException {
        Map<String, String> rename = new HashMap<>();
        rename.put("foo", "bar");
        config.put("rename", rename);
        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("bar"), equalTo("bar"));
        assertThat(data.containsProperty("foo"), is(false));
    }

    public void testConvert() throws IOException {
        Map<String, String> convert = new HashMap<>();
        convert.put("num", "integer");
        config.put("convert", convert);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("num"), equalTo(64));
    }

    public void testConvert_NullField() throws IOException {
        Map<String, String> convert = new HashMap<>();
        convert.put("null", "integer");
        config.put("convert", convert);

        Processor processor = FACTORY.create(config);
        try {
            processor.execute(data);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field \"null\" is null, cannot be converted to a/an integer"));
        }
    }

    public void testConvert_List() throws IOException {
        Map<String, String> convert = new HashMap<>();
        convert.put("arr", "integer");
        config.put("convert", convert);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("arr"), equalTo(Arrays.asList(1, 2, 3)));
    }

    public void testSplit() throws IOException {
        HashMap<String, String> split = new HashMap<>();
        split.put("ip", "\\.");
        config.put("split", split);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("ip"), equalTo(Arrays.asList("127", "0", "0", "1")));
    }

    public void testGsub() throws IOException {
        HashMap<String, List<String>> gsub = new HashMap<>();
        gsub.put("ip", Arrays.asList("\\.", "-"));
        config.put("gsub", gsub);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("ip"), equalTo("127-0-0-1"));
    }

    public void testGsub_NullValue() throws IOException {
        HashMap<String, List<String>> gsub = new HashMap<>();
        gsub.put("null_field", Arrays.asList("\\.", "-"));
        config.put("gsub", gsub);

        Processor processor = FACTORY.create(config);
        try {
            processor.execute(data);
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("Field \"null_field\" is null, cannot match pattern."));
        }
    }

    public void testJoin() throws IOException {
        HashMap<String, String> join = new HashMap<>();
        join.put("arr", "-");
        config.put("join", join);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("arr"), equalTo("1-2-3"));
    }

    public void testRemove() throws IOException {
        List<String> remove = Arrays.asList("foo", "ip");
        config.put("remove", remove);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("foo"), nullValue());
        assertThat(data.getProperty("ip"), nullValue());
    }

    public void testTrim() throws IOException {
        List<String> trim = Arrays.asList("to_strip", "foo");
        config.put("trim", trim);

        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("foo"), equalTo("bar"));
        assertThat(data.getProperty("to_strip"), equalTo("clean"));
    }

    public void testUppercase() throws IOException {
        List<String> uppercase = Arrays.asList("foo");
        config.put("uppercase", uppercase);
        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("foo"), equalTo("BAR"));
    }

    public void testLowercase() throws IOException {
        List<String> lowercase = Arrays.asList("alpha");
        config.put("lowercase", lowercase);
        Processor processor = FACTORY.create(config);
        processor.execute(data);
        assertThat(data.getProperty("alpha"), equalTo("abcd"));
    }
}
