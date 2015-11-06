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

import org.elasticsearch.ingest.processor.Processor;
import org.elasticsearch.ingest.processor.simple.SimpleProcessor;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class PipelineFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        processorRegistry.put("simple", new SimpleProcessor.Factory());

        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("path", "_path");
        processorConfig.put("expected_value", "_expected_value");
        processorConfig.put("add_field", "_add_field");
        processorConfig.put("add_field_value", "_add_field_value");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("description", "_description");
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("simple", processorConfig)));
        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);

        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0), instanceOf(SimpleProcessor.class));
    }

    public void testCreate_unusedProcessorOptions() throws Exception {
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        processorRegistry.put("simple", new SimpleProcessor.Factory());

        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("path", "_path");
        processorConfig.put("expected_value", "_expected_value");
        processorConfig.put("add_field", "_add_field");
        processorConfig.put("add_field_value", "_add_field_value");
        processorConfig.put("foo", "bar");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("description", "_description");
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("simple", processorConfig)));

        try {
            factory.create("_id", pipelineConfig, processorRegistry);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("processor [simple] doesn't support one or more provided configuration parameters [[foo]]"));
        }
    }

}
