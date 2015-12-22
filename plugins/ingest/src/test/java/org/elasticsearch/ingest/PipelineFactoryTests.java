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
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class PipelineFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("description", "_description");
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("test-processor");
        Processor.Factory processorFactory = mock(Processor.Factory.class);
        when(processorFactory.create(processorConfig)).thenReturn(processor);
        processorRegistry.put("test", processorFactory);

        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
    }

    public void testCreateWithPipelineOnFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("description", "_description");
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("test-processor", processorConfig)));
        pipelineConfig.put("on_failure", Collections.singletonList(Collections.singletonMap("test-processor", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("test-processor");
        Processor.Factory processorFactory = mock(Processor.Factory.class);
        when(processorFactory.create(processorConfig)).thenReturn(processor);
        processorRegistry.put("test-processor", processorFactory);

        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
        assertThat(pipeline.getOnFailureProcessors().size(), equalTo(1));
        assertThat(pipeline.getOnFailureProcessors().get(0).getType(), equalTo("test-processor"));
    }

    public void testCreateUnusedProcessorOptions() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("unused", "value");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("description", "_description");
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = new HashMap<>();
        Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("test-processor");
        Processor.Factory processorFactory = mock(Processor.Factory.class);
        when(processorFactory.create(processorConfig)).thenReturn(processor);
        processorRegistry.put("test", processorFactory);
        try {
            factory.create("_id", pipelineConfig, processorRegistry);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));
        }
    }

    public void testCreateProcessorsWithOnFailureProperties() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("on_failure", Collections.singletonList(Collections.singletonMap("test", new HashMap<>())));

        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put("description", "_description");
        pipelineConfig.put("processors", Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorFactoryStore = new HashMap<>();
        Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("test-processor");
        Processor.Factory processorFactory = mock(Processor.Factory.class);
        when(processorFactory.create(processorConfig)).thenReturn(processor);
        processorFactoryStore.put("test", processorFactory);

        Pipeline pipeline = factory.create("_id", pipelineConfig, processorFactoryStore);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("compound[test-processor]"));
    }
}
