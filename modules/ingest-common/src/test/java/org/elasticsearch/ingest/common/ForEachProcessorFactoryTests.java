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
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.mock;

public class ForEachProcessorFactoryTests extends ESTestCase {

    private final ScriptService scriptService = mock(ScriptService.class);
    private final Consumer<Runnable> genericExecutor = Runnable::run;

    public void testCreate() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService, genericExecutor);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Collections.singletonMap("_name", Collections.emptyMap()));
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, null, config);
        assertThat(forEachProcessor, Matchers.notNullValue());
        assertThat(forEachProcessor.getField(), equalTo("_field"));
        assertThat(forEachProcessor.getInnerProcessor(), Matchers.sameInstance(processor));
        assertFalse(forEachProcessor.isIgnoreMissing());
    }

    public void testSetIgnoreMissing() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService, genericExecutor);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Collections.singletonMap("_name", Collections.emptyMap()));
        config.put("ignore_missing", true);
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, null, config);
        assertThat(forEachProcessor, Matchers.notNullValue());
        assertThat(forEachProcessor.getField(), equalTo("_field"));
        assertThat(forEachProcessor.getInnerProcessor(), Matchers.sameInstance(processor));
        assertTrue(forEachProcessor.isIgnoreMissing());
    }

    public void testCreateWithTooManyProcessorTypes() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_first", (r, t, c) -> processor);
        registry.put("_second", (r, t, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService, genericExecutor);

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        Map<String, Object> processorTypes = new HashMap<>();
        processorTypes.put("_first", Collections.emptyMap());
        processorTypes.put("_second", Collections.emptyMap());
        config.put("processor", processorTypes);
        Exception exception = expectThrows(ElasticsearchParseException.class, () -> forEachFactory.create(registry, null, config));
        assertThat(exception.getMessage(), equalTo("[processor] Must specify exactly one processor type"));
    }

    public void testCreateWithNonExistingProcessorType() throws Exception {
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService, genericExecutor);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processor", Collections.singletonMap("_name", Collections.emptyMap()));
        Exception expectedException = expectThrows(ElasticsearchParseException.class,
            () -> forEachFactory.create(Collections.emptyMap(), null, config));
        assertThat(expectedException.getMessage(), equalTo("No processor type exists with name [_name]"));
    }

    public void testCreateWithMissingField() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> { });
        Map<String, Processor.Factory> registry = new HashMap<>();
        registry.put("_name", (r, t, c) -> processor);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService, genericExecutor);
        Map<String, Object> config = new HashMap<>();
        config.put("processor", Collections.singletonList(Collections.singletonMap("_name", Collections.emptyMap())));
        Exception exception = expectThrows(Exception.class, () -> forEachFactory.create(registry, null, config));
        assertThat(exception.getMessage(), equalTo("[field] required property is missing"));
    }

    public void testCreateWithMissingProcessor() {
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory(scriptService, genericExecutor);
        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        Exception exception = expectThrows(Exception.class, () -> forEachFactory.create(Collections.emptyMap(), null, config));
        assertThat(exception.getMessage(), equalTo("[processor] required property is missing"));
    }

}
