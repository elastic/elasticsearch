/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

public class GrokProcessorFactoryTests extends ESTestCase {

    public void testBuild() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("(?<foo>\\w+)"));
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isIgnoreMissing(), is(false));
    }

    public void testBuildWithIgnoreMissing() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("(?<foo>\\w+)"));
        config.put("ignore_missing", true);
        String processorTag = randomAlphaOfLength(10);
        GrokProcessor processor = factory.create(null, processorTag, null, config, null);
        assertThat(processor.getTag(), equalTo(processorTag));
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.isIgnoreMissing(), is(true));
    }

    public void testBuildMissingField() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("patterns", List.of("(?<foo>\\w+)"));
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testBuildMissingPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[patterns] required property is missing"));
    }

    public void testBuildEmptyPatternsList() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "foo");
        config.put("patterns", List.of());
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[patterns] List of patterns must not be empty"));
    }

    public void testCreateWithCustomPatterns() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));
        GrokProcessor processor = factory.create(null, null, null, config, null);
        assertThat(processor.getMatchField(), equalTo("_field"));
        assertThat(processor.getGrok(), notNullValue());
        assertThat(processor.getGrok().match("foo!"), equalTo(true));
    }

    public void testCreateWithInvalidPattern() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("["));
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[patterns] Invalid regex pattern found in: [[]. premature end of char-class"));
    }

    public void testCreateWithInvalidPatternDefinition() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "["));
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(
            e.getMessage(),
            equalTo("[patterns] Invalid regex pattern found in: [%{MY_PATTERN:name}!]. premature end of char-class")
        );
    }

    public void testCreateWithInvalidEcsCompatibilityMode() throws Exception {
        GrokProcessor.Factory factory = new GrokProcessor.Factory(MatcherWatchdog.noop());
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("(?<foo>\\w+)"));
        String invalidEcsMode = randomAlphaOfLength(3);
        config.put("ecs_compatibility", invalidEcsMode);
        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> factory.create(null, null, null, config, null)
        );
        assertThat(e.getMessage(), equalTo("[ecs_compatibility] unsupported mode '" + invalidEcsMode + "'"));
    }
}
