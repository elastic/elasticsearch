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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.ingest.ProcessorsRegistry;
import org.elasticsearch.ingest.TestProcessor;
import org.elasticsearch.ingest.TestTemplateService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PipelineFactoryTests extends ESTestCase {

    public void testCreate() throws Exception {
        Map<String, Object> processorConfig0 = new HashMap<>();
        Map<String, Object> processorConfig1 = new HashMap<>();
        processorConfig0.put(AbstractProcessorFactory.TAG_KEY, "first-processor");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY,
                Arrays.asList(Collections.singletonMap("test", processorConfig0), Collections.singletonMap("test", processorConfig1)));
        Pipeline.Factory factory = new Pipeline.Factory();
        ProcessorsRegistry processorRegistry = createProcessorRegistry(Collections.singletonMap("test", new TestProcessor.Factory()));
        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(2));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
        assertThat(pipeline.getProcessors().get(0).getTag(), equalTo("first-processor"));
        assertThat(pipeline.getProcessors().get(1).getType(), equalTo("test-processor"));
        assertThat(pipeline.getProcessors().get(1).getTag(), nullValue());
    }

    public void testCreateWithNoProcessorsField() throws Exception {
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        Pipeline.Factory factory = new Pipeline.Factory();
        try {
            factory.create("_id", pipelineConfig, createProcessorRegistry(Collections.emptyMap()));
            fail("should fail, missing required [processors] field");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
        }
    }

    public void testCreateWithPipelineOnFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        pipelineConfig.put(Pipeline.ON_FAILURE_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        ProcessorsRegistry processorRegistry = createProcessorRegistry(Collections.singletonMap("test", new TestProcessor.Factory()));
        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
        assertThat(pipeline.getOnFailureProcessors().size(), equalTo(1));
        assertThat(pipeline.getOnFailureProcessors().get(0).getType(), equalTo("test-processor"));
    }

    public void testCreateWithPipelineIgnoreFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("ignore_failure", true);

        ProcessorsRegistry processorRegistry = createProcessorRegistry(Collections.singletonMap("test", new TestProcessor.Factory()));
        Pipeline.Factory factory = new Pipeline.Factory();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY,
                Collections.singletonList(Collections.singletonMap("test", processorConfig)));

        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getOnFailureProcessors().size(), equalTo(0));

        CompoundProcessor processor = (CompoundProcessor) pipeline.getProcessors().get(0);
        assertThat(processor.isIgnoreFailure(), is(true));
        assertThat(processor.getProcessors().get(0).getType(), equalTo("test-processor"));
    }

    public void testCreateUnusedProcessorOptions() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("unused", "value");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Pipeline.Factory factory = new Pipeline.Factory();
        ProcessorsRegistry processorRegistry = createProcessorRegistry(Collections.singletonMap("test", new TestProcessor.Factory()));
        try {
            factory.create("_id", pipelineConfig, processorRegistry);
        } catch (ElasticsearchParseException e) {
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
        ProcessorsRegistry processorRegistry = createProcessorRegistry(Collections.singletonMap("test", new TestProcessor.Factory()));
        Pipeline pipeline = factory.create("_id", pipelineConfig, processorRegistry);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("compound"));
    }

    public void testFlattenProcessors() throws Exception {
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor processor1 = new CompoundProcessor(testProcessor, testProcessor);
        CompoundProcessor processor2 =
                new CompoundProcessor(false, Collections.singletonList(testProcessor), Collections.singletonList(testProcessor));
        Pipeline pipeline = new Pipeline("_id", "_description", new CompoundProcessor(processor1, processor2));
        List<Processor> flattened = pipeline.flattenAllProcessors();
        assertThat(flattened.size(), equalTo(4));
    }

    private ProcessorsRegistry createProcessorRegistry(Map<String, Processor.Factory> processorRegistry) {
        ProcessorsRegistry.Builder builder = new ProcessorsRegistry.Builder();
        for (Map.Entry<String, Processor.Factory> entry : processorRegistry.entrySet()) {
            builder.registerProcessor(entry.getKey(), ((registry) -> entry.getValue()));
        }
        return builder.build(mock(ScriptService.class), mock(ClusterService.class));
    }
}
