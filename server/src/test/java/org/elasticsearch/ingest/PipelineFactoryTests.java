/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.ingest;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;

public class PipelineFactoryTests extends ESTestCase {

    private final ProjectId projectId = randomProjectIdOrDefault();
    private final Integer version = randomBoolean() ? randomInt() : null;
    private final String versionString = version != null ? Integer.toString(version) : null;
    private final ScriptService scriptService = mock(ScriptService.class);
    private final Map<String, Object> metadata = randomMapOfMaps();
    private final Boolean deprecated = randomOptionalBoolean();

    public void testCreate() throws Exception {
        Map<String, Object> processorConfig0 = new HashMap<>();
        Map<String, Object> processorConfig1 = new HashMap<>();
        processorConfig0.put(ConfigurationUtils.TAG_KEY, "first-processor");
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.DEPRECATED_KEY, deprecated);
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig0), Map.of("test", processorConfig1)));
        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
        assertThat(pipeline.getDeprecated(), equalTo(deprecated));
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
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        try {
            Pipeline.create("_id", pipelineConfig, Map.of(), scriptService, null);
            fail("should fail, missing required [processors] field");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
        }
    }

    public void testCreateWithEmptyProcessorsField() throws Exception {
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, null, scriptService, null);
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
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig)));
        pipelineConfig.put(Pipeline.ON_FAILURE_KEY, List.of(Map.of("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null);
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
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig)));
        pipelineConfig.put(Pipeline.ON_FAILURE_KEY, List.of());
        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null)
        );
        assertThat(e.getMessage(), equalTo("pipeline [_id] cannot have an empty on_failure option defined"));
    }

    public void testCreateWithPipelineEmptyOnFailureInProcessor() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(Pipeline.ON_FAILURE_KEY, List.of());
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null)
        );
        assertThat(e.getMessage(), equalTo("[on_failure] processors list cannot be empty"));
    }

    public void testCreateWithPipelineIgnoreFailure() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put("ignore_failure", true);

        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig)));

        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null);
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
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Exception e = expectThrows(
            ElasticsearchParseException.class,
            () -> Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null)
        );
        assertThat(e.getMessage(), equalTo("processor [test] doesn't support one or more provided configuration parameters [unused]"));
    }

    public void testCreateProcessorsWithOnFailureProperties() throws Exception {
        Map<String, Object> processorConfig = new HashMap<>();
        processorConfig.put(Pipeline.ON_FAILURE_KEY, List.of(Map.of("test", new HashMap<>())));

        Map<String, Object> pipelineConfig = new HashMap<>();
        pipelineConfig.put(Pipeline.DESCRIPTION_KEY, "_description");
        pipelineConfig.put(Pipeline.VERSION_KEY, versionString);
        if (metadata != null) {
            pipelineConfig.put(Pipeline.META_KEY, metadata);
        }
        pipelineConfig.put(Pipeline.PROCESSORS_KEY, List.of(Map.of("test", processorConfig)));
        Map<String, Processor.Factory> processorRegistry = Map.of("test", new TestProcessor.Factory());
        Pipeline pipeline = Pipeline.create("_id", pipelineConfig, processorRegistry, scriptService, null);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getVersion(), equalTo(version));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("on_failure"));
    }

    public void testFlattenProcessors() throws Exception {
        TestProcessor testProcessor = new TestProcessor(ingestDocument -> {});
        CompoundProcessor processor1 = new CompoundProcessor(testProcessor, testProcessor);
        CompoundProcessor processor2 = new CompoundProcessor(false, List.of(testProcessor), List.of(testProcessor));
        Pipeline pipeline = new Pipeline("_id", "_description", version, null, new CompoundProcessor(processor1, processor2));
        List<Processor> flattened = pipeline.flattenAllProcessors();
        assertThat(flattened.size(), equalTo(4));
    }

    private Map<String, Object> randomMapOfMaps() {
        if (randomBoolean()) {
            return randomNonNullMapOfMaps(10);
        } else {
            return null;
        }
    }

    private Map<String, Object> randomNonNullMapOfMaps(int maxDepth) {
        return randomMap(0, randomIntBetween(1, 5), () -> randomNonNullMapOfMapsSupplier(maxDepth));
    }

    private Tuple<String, Object> randomNonNullMapOfMapsSupplier(int maxDepth) {
        String key = randomAlphaOfLength(randomIntBetween(2, 15));
        Object value;
        if (maxDepth == 0 || randomBoolean()) {
            value = randomAlphaOfLength(randomIntBetween(1, 10));
        } else {
            value = randomNonNullMapOfMaps(maxDepth - 1);
        }
        return Tuple.tuple(key, value);
    }
}
