/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.elasticsearch.action.bulk.FailureStoreMetrics;
import org.elasticsearch.action.bulk.SimulateBulkRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.LambdaMatchers.transformedMatch;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimulateIngestServiceTests extends ESTestCase {

    private static <K, V> Map<K, V> newHashMap(K key, V value) {
        Map<K, V> map = new HashMap<>();
        map.put(key, value);
        return map;
    }

    public void testGetPipeline() {
        PipelineConfiguration pipelineConfiguration = new PipelineConfiguration("pipeline1", new BytesArray("""
            {"processors": [{"processor1" : {}}]}"""), XContentType.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(Map.of("pipeline1", pipelineConfiguration));
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put(
            "processor1",
            (factories, tag, description, config) -> new FakeProcessor("processor1", tag, description, ingestDocument -> {}) {
            }
        );
        processors.put(
            "processor2",
            (factories, tag, description, config) -> new FakeProcessor("processor2", tag, description, ingestDocument -> {}) {
            }
        );
        processors.put(
            "processor3",
            (factories, tag, description, config) -> new FakeProcessor("processor3", tag, description, ingestDocument -> {}) {
            }
        );
        IngestService ingestService = createWithProcessors(processors);
        ingestService.innerUpdatePipelines(ingestMetadata);
        {
            // First we make sure that if there are no substitutions that we get our original pipeline back:
            SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest((Map<String, Map<String, Object>>) null);
            SimulateIngestService simulateIngestService = new SimulateIngestService(ingestService, simulateBulkRequest);
            Pipeline pipeline = simulateIngestService.getPipeline("pipeline1");
            assertThat(pipeline.getProcessors(), contains(transformedMatch(Processor::getType, equalTo("processor1"))));
            assertNull(simulateIngestService.getPipeline("pipeline2"));
        }
        {
            // Here we make sure that if we have a substitution with the same name as the original pipeline that we get the new one back
            Map<String, Map<String, Object>> pipelineSubstitutions = new HashMap<>();
            pipelineSubstitutions.put(
                "pipeline1",
                newHashMap(
                    "processors",
                    List.of(newHashMap("processor2", Collections.emptyMap()), newHashMap("processor3", Collections.emptyMap()))
                )
            );
            pipelineSubstitutions.put("pipeline2", newHashMap("processors", List.of(newHashMap("processor3", Collections.emptyMap()))));

            SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(pipelineSubstitutions);
            SimulateIngestService simulateIngestService = new SimulateIngestService(ingestService, simulateBulkRequest);
            Pipeline pipeline1 = simulateIngestService.getPipeline("pipeline1");
            assertThat(
                pipeline1.getProcessors(),
                contains(
                    transformedMatch(Processor::getType, equalTo("processor2")),
                    transformedMatch(Processor::getType, equalTo("processor3"))
                )
            );
            Pipeline pipeline2 = simulateIngestService.getPipeline("pipeline2");
            assertThat(pipeline2.getProcessors(), contains(transformedMatch(Processor::getType, equalTo("processor3"))));
        }
        {
            /*
             * Here we make sure that if we have a substitution for a new pipeline we still get the original one back (as well as the new
             * one).
             */
            Map<String, Map<String, Object>> pipelineSubstitutions = new HashMap<>();
            pipelineSubstitutions.put("pipeline2", newHashMap("processors", List.of(newHashMap("processor3", Collections.emptyMap()))));
            SimulateBulkRequest simulateBulkRequest = new SimulateBulkRequest(pipelineSubstitutions);
            SimulateIngestService simulateIngestService = new SimulateIngestService(ingestService, simulateBulkRequest);
            Pipeline pipeline1 = simulateIngestService.getPipeline("pipeline1");
            assertThat(pipeline1.getProcessors(), contains(transformedMatch(Processor::getType, equalTo("processor1"))));
            Pipeline pipeline2 = simulateIngestService.getPipeline("pipeline2");
            assertThat(pipeline2.getProcessors(), contains(transformedMatch(Processor::getType, equalTo("processor3"))));
        }
    }

    private static IngestService createWithProcessors(Map<String, Processor.Factory> processors) {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        var ingestPlugin = new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
                return processors;
            }
        };
        return new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            List.of(ingestPlugin),
            client,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE,
            FailureStoreMetrics.NOOP
        );
    }
}
