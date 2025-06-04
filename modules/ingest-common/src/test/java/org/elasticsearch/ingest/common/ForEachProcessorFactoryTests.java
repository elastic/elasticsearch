/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest.common;

import org.elasticsearch.exception.ElasticsearchParseException;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.mock;

public class ForEachProcessorFactoryTests extends ESTestCase {

    private final ScriptService scriptService = mock(ScriptService.class);

    public void testCreate() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> {});
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, description, c, projectId) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Map.of("_name", Collections.emptyMap()));
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, null, null, config, null);
        assertThat(forEachProcessor, notNullValue());
        assertThat(forEachProcessor.getField(), equalTo("_field"));
        assertThat(forEachProcessor.getInnerProcessor(), sameInstance(processor));
        assertFalse(forEachProcessor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> {});
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, description, c, projectId) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Map.of("_name", Collections.emptyMap()));
        config.put("ignore_missing", true);
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, null, null, config, null);
        assertThat(forEachProcessor, notNullValue());
        assertThat(forEachProcessor.getField(), equalTo("_field"));
        assertThat(forEachProcessor.getInnerProcessor(), sameInstance(processor));
        assertTrue(forEachProcessor.isIgnoreMissing());
    }

    public void testCreateWithTooManyProcessorTypes() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> {});
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_first", (r, t, description, c, projectId) -> processor);
        registry.put("_second", (r, t, description, c, projectId) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        Map<String, Object> processorTypes = new HashMap<>();
        processorTypes.put("_first", Map.of());
        processorTypes.put("_second", Map.of());
        config.put("processor", processorTypes);
        Exception exception = expectThrows(
            ElasticsearchParseException.class,
            () -> forEachFactory.create(registry, null, null, config, null)
        );
        assertThat(exception.getMessage(), equalTo("[processor] Must specify exactly one processor type"));
    }

    public void testCreateWithNonExistingProcessorType() throws Exception {
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Map.of("_name", Collections.emptyMap()));
        Exception expectedException = expectThrows(
            ElasticsearchParseException.class,
            () -> forEachFactory.create(Map.of(), null, null, config, null)
        );
        assertThat(expectedException.getMessage(), equalTo("No processor type exists with name [_name]"));
    }

    public void testCreateWithMissingField() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> {});
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, description, c, projectId) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);
        Map<String, Object> config = new HashMap<>();
        config.put("processor", List.of(Map.of("_name", Map.of())));
        Exception exception = expectThrows(Exception.class, () -> forEachFactory.create(registry, null, null, config, null));
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithMissingProcessor() {
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        Exception exception = expectThrows(Exception.class, () -> forEachFactory.create(Map.of(), null, null, config, null));
        assertThat(exception.getMessage(), equalTo("[processor] required property is missing"));
    }

}
