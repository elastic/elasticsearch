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

import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.ingest.ProcessorsRegistry;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;
import org.hamcrest.Matchers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;

public class ForEachProcessorFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        Processor processor = new TestProcessor(ingestDocument -> {});
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("_name", (r, c) -> processor);
        ProcessorsRegistry registry = new ProcessorsRegistry(processors);
        ForEachProcessor.Factory forEachFactory = new ForEachProcessor.Factory();

        Map<String, Object> config = new HashMap<>();
        config.put("field", "_field");
        config.put("processors", Collections.singletonList(Collections.singletonMap("_name", Collections.emptyMap())));
        ForEachProcessor forEachProcessor = forEachFactory.create(registry, config);
        assertThat(forEachProcessor, Matchers.notNullValue());
        assertThat(forEachProcessor.getField(), Matchers.equalTo("_field"));
        assertThat(forEachProcessor.getProcessors().size(), Matchers.equalTo(1));
        assertThat(forEachProcessor.getProcessors().get(0), Matchers.sameInstance(processor));

        config = new HashMap<>();
        config.put("processors", Collections.singletonList(Collections.singletonMap("_name", Collections.emptyMap())));
        try {
            forEachFactory.create(registry, config);
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), Matchers.equalTo("[field] required property is missing"));
        }

        config = new HashMap<>();
        config.put("field", "_field");
        try {
            forEachFactory.create(registry, config);
            fail("exception expected");
        } catch (Exception e) {
            assertThat(e.getMessage(), Matchers.equalTo("[processors] required property is missing"));
        }
    }

}
