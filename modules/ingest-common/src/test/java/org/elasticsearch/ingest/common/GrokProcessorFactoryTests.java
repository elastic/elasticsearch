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

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GrokProcessorFactoryTests extends ESTestCase {

    public void testBuild() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isIgnoreMissing(), is(false));
    }

    public void testBuildWithIgnoreMissing() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, config);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isIgnoreMissing(), is(true));
    }

    public void testBuildMissingField() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());
        Map<String, Object> config = new HashMap<>();
        config.put("patterns", Collections.singletonList("(?<foo>\\w+)"));
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testBuildMissingPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(), equalTo("[patterns] required property is missing"));
    }

    public void testBuildEmptyPatternsList() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        config.put("patterns", Collections.emptyList());
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(), equalTo("[patterns] List of patterns must not be empty"));
    }

    public void testCreateWithCustomPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Collections.singletonMap("MY_PATTERN", "foo"));
        GrokProcessor processor = factory.create(null, null, config);
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.getGrok().match("foo!"), equalTo(true));
    }

    public void testCreateWithInvalidPattern() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("["));
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(), equalTo("[patterns] Invalid regex pattern found in: [[]. premature end of char-class"));
    }

    public void testCreateWithInvalidPatternDefinition() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(Collections.emptyMap());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", Collections.singletonList("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Collections.singletonMap("MY_PATTERN", "["));
        ElasticsearchParseException e = expectThrows(ElasticsearchParseException.class, () -> factory.create(null, null, config));
        assertThat(e.getMessage(),
            equalTo("[patterns] Invalid regex pattern found in: [%{MY_PATTERN:name}!]. premature end of char-class"));
    }
}
