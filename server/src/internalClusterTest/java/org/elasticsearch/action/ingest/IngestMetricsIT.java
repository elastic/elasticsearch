/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.action.ingest;

import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.elasticsearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.routing.allocation.decider.AllocationDeciders;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.LoggingDeprecationHandler;
import org.elasticsearch.env.Environment;
import org.elasticsearch.env.NodeEnvironment;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.ingest.AbstractProcessor;
import org.elasticsearch.ingest.IngestDocument;
import org.elasticsearch.ingest.IngestStats;
import org.elasticsearch.ingest.PipelineProcessor;
import org.elasticsearch.ingest.Processor;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.repositories.RepositoriesService;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.test.ESSingleNodeTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.tracing.Tracer;
import org.elasticsearch.watcher.ResourceWatcherService;
import org.elasticsearch.xcontent.NamedXContentRegistry;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentFactory;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

/**
 * The purpose of this test is to make sure that ingestion counters are correct.
 */
public class IngestMetricsIT extends ESSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return List.of(TestPlugin.class);
    }

    @SuppressWarnings("unchecked")
    public void testNestedMetrics() throws IOException {
        String pipeline3 = """
            {
                "processors": [
                    {
                        "test-async-processor": {
                            "description": "test-async-processor-in-pipeline3"
                        }
                    }
                ]
            }
            """;
        BytesReference pipeline3Reference = new BytesArray(pipeline3);
        client().admin().cluster().putPipeline(new PutPipelineRequest("pipeline3", pipeline3Reference, XContentType.JSON)).actionGet();
        String pipeline2 = """
            {
                "processors": [
                    {
                        "test-async-processor": {
                            "description": "test-async-processor-in-pipeline2"
                        },
                        "pipeline": {
                            "name": "pipeline3",
                            "description": "pipeline3-in-pipeline2"
                        }
                    }
                ]
            }
            """;
        BytesReference pipeline2Reference = new BytesArray(pipeline2);
        client().admin().cluster().putPipeline(new PutPipelineRequest("pipeline2", pipeline2Reference, XContentType.JSON)).actionGet();
        String pipeline1 = """
            {
                "processors": [
                    {
                        "pipeline": {
                            "name": "pipeline2",
                            "description": "pipeline2-in-pipeline1"
                        }
                    },
                    {
                        "test-async-processor": {
                            "description": "test-async-processor-in-pipeline1"
                        },
                        "pipeline": {
                            "name": "pipeline3",
                            "description": "pipeline3-in-pipeline1"
                        }
                    }
                ]
            }
            """;
        BytesReference pipeline1Reference = new BytesArray(pipeline1);
        client().admin().cluster().putPipeline(new PutPipelineRequest("pipeline1", pipeline1Reference, XContentType.JSON)).actionGet();

        client().admin()
            .indices()
            .preparePutTemplate("final_pipeline_template")
            .setPatterns(Collections.singletonList("*"))
            .setOrder(0)
            .setSettings(Settings.builder().put(IndexSettings.FINAL_PIPELINE.getKey(), "pipeline3"))
            .get();

        BulkRequest bulkRequest = new BulkRequest();
        int numDocsToPipeline1 = randomIntBetween(1, 200);
        for (int i = 0; i < numDocsToPipeline1; i++) {
            bulkRequest.add(new IndexRequest("index1").id(Integer.toString(i)).source("{}", XContentType.JSON).setPipeline("pipeline1"));
        }
        BulkResponse bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocsToPipeline1));
        for (int i = 0; i < numDocsToPipeline1; i++) {
            String id = Integer.toString(i);
            assertThat(bulkResponse.getItems()[i].getId(), equalTo(id));
        }
        bulkRequest = new BulkRequest();
        int numDocsToPipeline2 = randomIntBetween(1, 200);
        for (int i = 0; i < numDocsToPipeline2; i++) {
            bulkRequest.add(new IndexRequest("index2").id(Integer.toString(i)).source("{}", XContentType.JSON).setPipeline("pipeline2"));
        }
        bulkResponse = client().bulk(bulkRequest).actionGet();
        assertThat(bulkResponse.getItems().length, equalTo(numDocsToPipeline2));
        for (int i = 0; i < numDocsToPipeline2; i++) {
            String id = Integer.toString(i);
            assertThat(bulkResponse.getItems()[i].getId(), equalTo(id));
        }
        int totalDocs = numDocsToPipeline1 + numDocsToPipeline2;
        NodesStatsResponse nodesStatsResponse = client().admin()
            .cluster()
            .nodesStats(new NodesStatsRequest().addMetric("ingest"))
            .actionGet();
        IngestStats ingestStats = nodesStatsResponse.getNodes().get(0).getIngestStats();
        Map<String, Object> ingestStatsMap = xContentToMap(ingestStats);
        Map<String, Object> ingest = (Map<String, Object>) ingestStatsMap.get("ingest");
        Map<String, Object> total = (Map<String, Object>) ingest.get("total");
        int totalCount = (int) total.get("count");
        assertThat(totalCount, equalTo(totalDocs * 2)); // because we have a final pipeline
        int totalTime = (int) total.get("time_in_millis");
        assertThat(totalTime, greaterThan(0));
        Map<String, Object> pipelines = (Map<String, Object>) ingest.get("pipelines");
        Map<String, Object> pipeline1Map = (Map<String, Object>) pipelines.get("pipeline1");
        assertThat(pipeline1Map.get("count"), equalTo(numDocsToPipeline1));
        int pipeline1Time = (int) pipeline1Map.get("time_in_millis");
        AtomicInteger pipelinesChecked = new AtomicInteger(0);
        checkPipeline(pipeline1Map, numDocsToPipeline1, totalTime, pipelinesChecked);
        assertThat(pipelinesChecked.get(), equalTo(4));

        Map<String, Object> pipeline2Map = (Map<String, Object>) pipelines.get("pipeline2");
        assertThat(pipeline2Map.get("count"), equalTo(numDocsToPipeline2));
        int pipeline2Time = (int) pipeline2Map.get("time_in_millis");
        Map<String, Object> pipeline3Map = (Map<String, Object>) pipelines.get("pipeline3");
        assertThat(pipeline3Map.get("count"), equalTo(numDocsToPipeline1 + numDocsToPipeline2)); // because it's the final pipeline
        int pipeline3Time = (int) pipeline3Map.get("time_in_millis");
        assertThat(pipeline1Time + pipeline2Time + pipeline3Time, lessThanOrEqualTo(totalTime));
    }

    /*
     * Recursively check that the count in each processor is the same, and that the time spent in all processors as a given level is less
     * than or equal to the time at the previous level.
     */
    @SuppressWarnings("unchecked")
    private void checkPipeline(
        Map<String, Object> pipelineMap,
        int previousLevelCount,
        int previousLevelTime,
        AtomicInteger pipelinesCheckedAccumulator
    ) {
        pipelinesCheckedAccumulator.incrementAndGet();
        assertThat(pipelineMap.get("count"), equalTo(previousLevelCount)); // We don't have conditionals
        int time = (int) pipelineMap.get("time_in_millis");
        assertThat(time, greaterThan(0));
        assertThat(time, lessThanOrEqualTo(previousLevelTime));
        List<Map<String, Object>> pipelineProcessors = (List<Map<String, Object>>) pipelineMap.get("processors");
        int allProcessorsTime = 0;
        for (Map<String, Object> processorMap : pipelineProcessors) {
            assertThat(processorMap.size(), equalTo(1));
            Map<String, Object> innerProcessorMap = (Map<String, Object>) processorMap.values().iterator().next();
            String type = (String) innerProcessorMap.get("type");
            Map<String, Object> stats = (Map<String, Object>) innerProcessorMap.get("stats");
            int processorTime = (int) stats.get("time_in_millis");
            assertThat(processorTime, greaterThanOrEqualTo(0));
            allProcessorsTime += processorTime;
            int count = (int) stats.get("count");
            assertThat(count, equalTo(previousLevelCount));
            if (PipelineProcessor.TYPE.equals(type)) {
                checkPipeline(stats, count, time, pipelinesCheckedAccumulator);
            }
        }
        assertThat(allProcessorsTime, lessThanOrEqualTo(previousLevelTime));
    }

    private Map<String, Object> xContentToMap(ToXContent xcontent) throws IOException {
        XContentBuilder builder = XContentFactory.yamlBuilder().prettyPrint();
        builder.startObject();
        xcontent.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        XContentParser parser = XContentType.YAML.xContent()
            .createParser(xContentRegistry(), LoggingDeprecationHandler.INSTANCE, BytesReference.bytes(builder).streamInput());
        logger.info(((ByteArrayOutputStream) builder.getOutputStream()).toString(StandardCharsets.UTF_8));
        return parser.map();
    }

    public static class TestPlugin extends Plugin implements IngestPlugin {

        private ThreadPool threadPool;

        @Override
        public Collection<Object> createComponents(
            Client client,
            ClusterService clusterService,
            ThreadPool threadPool,
            ResourceWatcherService resourceWatcherService,
            ScriptService scriptService,
            NamedXContentRegistry xContentRegistry,
            Environment environment,
            NodeEnvironment nodeEnvironment,
            NamedWriteableRegistry namedWriteableRegistry,
            IndexNameExpressionResolver expressionResolver,
            Supplier<RepositoriesService> repositoriesServiceSupplier,
            Tracer tracer,
            AllocationDeciders allocationDeciders
        ) {
            this.threadPool = threadPool;
            return List.of();
        }

        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            Map<String, Processor.Factory> procMap = new HashMap<>();
            procMap.put("test-async-processor", (factories, tag, description, config) -> new AbstractProcessor(tag, description) {
                @Override
                public void execute(IngestDocument ingestDocument, String context, BiConsumer<IngestDocument, Exception> handler) {
                    threadPool.generic().execute(() -> {
                        String id = (String) ingestDocument.getSourceAndMetadata().get("_id");
                        if (usually()) {
                            try {
                                Thread.sleep(10);
                            } catch (InterruptedException e) {
                                // ignore
                            }
                        }
                        ingestDocument.setFieldValue(randomAlphaOfLength(5), "bar-" + id);
                        handler.accept(ingestDocument, null);
                    });
                }

                @Override
                public String getType() {
                    return "test-async-processor";
                }

                @Override
                public boolean isAsync() {
                    return true;
                }

            });
            Processor.Factory pipelineFactory1 = new PipelineProcessor.Factory(parameters.ingestService);
            procMap.put("pipeline", pipelineFactory1);
            return procMap;
        }
    }
}
