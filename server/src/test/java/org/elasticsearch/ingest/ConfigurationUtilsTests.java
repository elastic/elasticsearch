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

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ConfigurationUtilsTests extends ESTestCase {
    private Map<String, Object> config;

    @Before
    public void setConfig() {
        config = new HashMap<>();
        config.put("foo", "bar");
        config.put("boolVal", true);
        config.put("null", null);
        config.put("arr", Arrays.asList("1", "2", "3"));
        List<Integer> list = new ArrayList<>();
        list.add(2);
        config.put("int", list);
        config.put("ip", "127.0.0.1");
        Map<String, Object> fizz = new HashMap<>();
        fizz.put("buzz", "hello world");
        config.put("fizz", fizz);
        config.put("num", 1);
    }

    public void testReadStringProperty() {
        String val = ConfigurationUtils.readStringProperty(null, null, config, "foo");
        assertThat(val, equalTo("bar"));
    }

    public void testReadStringPropertyInvalidType() {
        try {
            ConfigurationUtils.readStringProperty(null, null, config, "arr");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a string, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadBooleanProperty() {
        Boolean val = ConfigurationUtils.readBooleanProperty(null, null, config, "boolVal", false);
        assertThat(val, equalTo(true));
    }

    public void testReadNullBooleanProperty() {
        Boolean val = ConfigurationUtils.readBooleanProperty(null, null, config, "null", false);
        assertThat(val, equalTo(false));
    }

    public void testReadBooleanPropertyInvalidType() {
        try {
            ConfigurationUtils.readBooleanProperty(null, null, config, "arr", true);
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[arr] property isn't a boolean, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    // TODO(talevy): Issue with generics. This test should fail, "int" is of type List<Integer>
    public void testOptional_InvalidType() {
        List<String> val = ConfigurationUtils.readList(null, null, config, "int");
        assertThat(val, equalTo(Collections.singletonList(2)));
    }

    public void testReadStringOrIntProperty() {
        String val1 = ConfigurationUtils.readStringOrIntProperty(null, null, config, "foo", null);
        String val2 = ConfigurationUtils.readStringOrIntProperty(null, null, config, "num", null);
        assertThat(val1, equalTo("bar"));
        assertThat(val2, equalTo("1"));
    }

    public void testReadStringOrIntPropertyInvalidType() {
        try {
            ConfigurationUtils.readStringOrIntProperty(null, null, config, "arr", null);
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo(
                "[arr] property isn't a string or int, but of type [java.util.Arrays$ArrayList]"));
        }
    }

    public void testReadProcessors() throws Exception {
        Processor processor = mock(Processor.class);
        Map<String, Processor.Factory> registry =
            Collections.singletonMap("test_processor", (factories, tag, config) -> processor);

        List<Map<String, Map<String, Object>>> config = new ArrayList<>();
        Map<String, Object> emptyConfig = Collections.emptyMap();
        config.add(Collections.singletonMap("test_processor", emptyConfig));
        config.add(Collections.singletonMap("test_processor", emptyConfig));

        List<Processor> result = ConfigurationUtils.readProcessorConfigs(config, registry);
        assertThat(result.size(), equalTo(2));
        assertThat(result.get(0), sameInstance(processor));
        assertThat(result.get(1), sameInstance(processor));

        Map<String, Object> unknownTaggedConfig = new HashMap<>();
        unknownTaggedConfig.put("tag", "my_unknown");
        config.add(Collections.singletonMap("unknown_processor", unknownTaggedConfig));
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class,
            () -> ConfigurationUtils.readProcessorConfigs(config, registry));
        assertThat(e.getMessage(), equalTo("No processor type exists with name [unknown_processor]"));
        assertThat(e.getHeader("processor_tag"), equalTo(Collections.singletonList("my_unknown")));
        assertThat(e.getHeader("processor_type"), equalTo(Collections.singletonList("unknown_processor")));
        assertThat(e.getHeader("property_name"), is(nullValue()));

        List<Map<String, Map<String, Object>>> config2 = new ArrayList<>();
        unknownTaggedConfig = new HashMap<>();
        unknownTaggedConfig.put("tag", "my_unknown");
        config2.add(Collections.singletonMap("unknown_processor", unknownTaggedConfig));
        Map<String, Object> secondUnknonwTaggedConfig = new HashMap<>();
        secondUnknonwTaggedConfig.put("tag", "my_second_unknown");
        config2.add(Collections.singletonMap("second_unknown_processor", secondUnknonwTaggedConfig));
        e = expectThrows(ElasticsearchParseException.class, () -> ConfigurationUtils.readProcessorConfigs(config2, registry));
        assertThat(e.getMessage(), equalTo("No processor type exists with name [unknown_processor]"));
        assertThat(e.getHeader("processor_tag"), equalTo(Collections.singletonList("my_unknown")));
        assertThat(e.getHeader("processor_type"), equalTo(Collections.singletonList("unknown_processor")));
        assertThat(e.getHeader("property_name"), is(nullValue()));

        assertThat(e.getSuppressed().length, equalTo(1));
        assertThat(e.getSuppressed()[0], instanceOf(ElasticsearchParseException.class));
        ElasticsearchParseException e2 = (ElasticsearchParseException) e.getSuppressed()[0];
        assertThat(e2.getMessage(), equalTo("No processor type exists with name [second_unknown_processor]"));
        assertThat(e2.getHeader("processor_tag"), equalTo(Collections.singletonList("my_second_unknown")));
        assertThat(e2.getHeader("processor_type"), equalTo(Collections.singletonList("second_unknown_processor")));
        assertThat(e2.getHeader("property_name"), is(nullValue()));
    }

}
