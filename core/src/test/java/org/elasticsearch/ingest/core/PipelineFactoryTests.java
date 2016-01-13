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

package org.elasticsearch.ingest.core;

import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.core.Pipeline;
import org.elasticsearch.ingest.core.Processor;
import org.elasticsearch.test.ESTestCase;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;

public class PipelineFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
    }

    public void testCreateWithPipelineOnFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        pipelineConfig.put(Pipeline.ON_FAILURE_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
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
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        try {
            factory.create("_id", pipelineConfig, processorRegistry);
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));
        }
    }

    public void testCreateProcessorsWithOnFailureProperties() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(Pipeline.ON_FAILURE_KEY, Collections.singletonList(Collections.singletonMap("test", new HashMap<>())));

        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("compound[test-processor]"));
    }
}
