/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.redact;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.grok.MatcherWatchdog;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.not;

public class RedactProcessorFactoryTests extends ESTestCase {

    public void testPatternNotSet() {
        RedactProcessor.Factory factory = new RedactProcessor.Factory(MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of());
        ElasticsearchException e = expectThrows(ElasticsearchException.class, () -> factory.create(null, null, null, config));
        assertThat(e.getMessage(), containsString("List of patterns must not be empty"));
    }

    public void testCreateWithCustomPatterns() throws Exception {
        RedactProcessor.Factory factory = new RedactProcessor.Factory(MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));
        RedactProcessor processor = factory.create(null, null, null, config);
        assertThat(processor.getGroks(), not(empty()));
        assertThat(processor.getGroks().get(0).match("foo!"), equalTo(true));
    }

    public void testConfigKeysRemoved() throws Exception {
        RedactProcessor.Factory factory = new RedactProcessor.Factory(MatcherWatchdog.noop());

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("patterns", List.of("%{MY_PATTERN:name}!"));
        config.put("pattern_definitions", Map.of("MY_PATTERN", "foo"));
        config.put("ignore_missing", true);
        config.put("extra", "unused");

        factory.create(null, null, null, config);
        assertThat(config.entrySet(), hasSize(1));
        assertEquals("unused", config.get("extra"));
    }
}
