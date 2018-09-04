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

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PipelineFactoryTests extends ESTestCase {

    private final Integer version = randomBoolean() ? randomInt() : null;
    private final String versionString = version != null ? Integer.toString(version) : null;
    private final ScriptService scriptService = mock(ScriptService.class);

    public void testCreate() throws Exception {
        Map<String, Object> processorConfig0 = new HashMap<>();
        Map<String, Object> processorConfig1 = new HashMap<>();
        processorConfig0.put(ConfigurationUtils.TAG_KEY, "first-processor");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY,
                Arrays.asList(Collections.singletonMap("test", processorConfig0), Collections.singletonMap("test", processorConfig1)));
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
        assertThat(pipeline.getProcessors().size(), equalTo(2));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
        assertThat(pipeline.getProcessors().get(0).getTag(), equalTo("first-processor"));
        assertThat(pipeline.getProcessors().get(1).getType(), equalTo("test-processor"));
        assertThat(pipeline.getProcessors().get(1).getTag(), nullValue());
    }

    public void testCreateWithNoProcessorsField() throws Exception {
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        try {
            Pipeline.create("_id", pipelineConfig, Collections.emptyMap(), scriptService);
            fail("should fail, missing required [processors] field");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
        }
    }

    public void testCreateWithEmptyProcessorsField() throws Exception {
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.emptyList());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, null, scriptService);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
        assertThat(pipeline.getProcessors(), is(empty()));
    }

    public void testCreateWithPipelineOnFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        pipelineConfig.put(Pipeline.ON_FAILURE_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("test-processor"));
        assertThat(pipeline.getOnFailureProcessors().size(), equalTo(1));
        assertThat(pipeline.getOnFailureProcessors().get(0).getType(), equalTo("test-processor"));
    }

    public void testCreateWithPipelineEmptyOnFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        pipelineConfig.put(Pipeline.ON_FAILURE_KEY, Collections.emptyList());
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService)
        );
        assertThat(e.getMessage(), equalTo("pipeline [_id] cannot have an empty on_failure option defined"));
    }

    public void testCreateWithPipelineEmptyOnFailureInProcessor() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(Pipeline.ON_FAILURE_KEY, Collections.emptyList());
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService)
        );
        assertThat(e.getMessage(), equalTo("[on_failure] processors list cannot be empty"));
    }

    public void testCreateWithPipelineIgnoreFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("ignore_failure", true);

        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY,
                Collections.singletonList(Collections.singletonMap("test", processorConfig)));

        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
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
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService)
        );
        assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));
    }

    public void testCreateProcessorsWithOnFailureProperties() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(Pipeline.ON_FAILURE_KEY, Collections.singletonList(Collections.singletonMap("test", new HashMap<>())));

        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, Collections.singletonList(Collections.singletonMap("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Collections.singletonMap("test", new TestProcessor.Factory());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("compound"));
    }

    public void testFlattenProcessors() throws Exception {
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor processor1 = new CompoundProcessor(testProcessor, testProcessor);
        CompoundProcessor processor2 =
                new CompoundProcessor(false, Collections.singletonList(testProcessor), Collections.singletonList(testProcessor));
        Pipeline pipeline = new Pipeline("_id", "_description", version, new CompoundProcessor(processor1, processor2));
        List<Processor> flattened = pipeline.flattenAllProcessors();
        assertThat(flattened.size(), equalTo(4));
    }
}
