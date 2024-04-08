/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.ingest;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.client.internal.Requests;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils;
import org.elasticsearch.common.TriConsumer;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.util.Maps;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.core.Strings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.plugins.internal.DocumentParsingProvider;
import org.elasticsearch.plugins.internal.DocumentSizeObserver;
import org.elasticsearch.plugins.internal.DocumentSizeReporter;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.script.TemplateScript;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.cbor.CborXContent;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.service.ClusterStateTaskExecutorUtils.executeAndAssertSuccessful;
import static org.elasticsearch.core.Tuple.tuple;
import static org.elasticsearch.ingest.ConfigurationUtils.newConfigurationException;
import static org.elasticsearch.ingest.IngestService.NOOP_PIPELINE_NAME;
import static org.elasticsearch.ingest.IngestService.hasPipeline;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class IngestServiceTests extends ESTestCase {

    private static final IngestPlugin DUMMY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Map.of("foo", (factories, tag, description, config) -> null);
        }
    };

    private ThreadPool threadPool;

    @Before
    public void setup() {
        threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
    }

    public void testIngestPlugin() {
        Client client = mock(Client.class);
        IngestService ingestService = new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            List.of(DUMMY_PLUGIN),
            client,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE
        );
        Map<String, Processor.Factory> factories = ingestService.getProcessorFactories();
        assertTrue(factories.containsKey("foo"));
        assertEquals(1, factories.size());
    }

    public void testIngestPluginDuplicate() {
        Client client = mock(Client.class);
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> new IngestService(
                mock(ClusterService.class),
                threadPool,
                null,
                null,
                null,
                List.of(DUMMY_PLUGIN, DUMMY_PLUGIN),
                client,
                null,
                DocumentParsingProvider.EMPTY_INSTANCE
            )
        );
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }

    public void testExecuteIndexPipelineDoesNotExist() {
        Client client = mock(Client.class);
        IngestService ingestService = new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            List.of(DUMMY_PLUGIN),
            client,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE
        );
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");

        final SetOnce<Boolean> failure = new SetOnce<>();
        final BiConsumer<Integer, Exception> failureHandler = (slot, e) -> {
            failure.set(true);
            assertThat(slot, equalTo(0));
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        };

        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);

        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline = new PipelineConfiguration("_id", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(1));

        Pipeline p = ingestService.getPipeline("_id");
        assertThat(p.getId(), equalTo("_id"));
        assertThat(p.getDescription(), nullValue());
        assertThat(p.getProcessors().size(), equalTo(1));
        assertThat(p.getProcessors().get(0).getType(), equalTo("set"));
    }

    public void testInnerUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline1 = new PipelineConfiguration("_id1", new BytesArray("{\"processors\": []}"), XContentType.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(1));
        {
            Pipeline p1 = ingestService.getPipeline("_id1");
            assertThat(p1.getId(), equalTo("_id1"));
            assertThat(p1.getProcessors().size(), equalTo(0));
        }

        PipelineConfiguration pipeline2 = new PipelineConfiguration("_id2", new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id2", pipeline2));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        {
            Pipeline p1 = ingestService.getPipeline("_id1");
            assertThat(p1.getId(), equalTo("_id1"));
            assertThat(p1.getProcessors().size(), equalTo(0));
            Pipeline p2 = ingestService.getPipeline("_id2");
            assertThat(p2.getId(), equalTo("_id2"));
            assertThat(p2.getProcessors().size(), equalTo(0));
        }

        PipelineConfiguration pipeline3 = new PipelineConfiguration("_id3", new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id2", pipeline2, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(3));
        {
            Pipeline p1 = ingestService.getPipeline("_id1");
            assertThat(p1.getId(), equalTo("_id1"));
            assertThat(p1.getProcessors().size(), equalTo(0));
            Pipeline p2 = ingestService.getPipeline("_id2");
            assertThat(p2.getId(), equalTo("_id2"));
            assertThat(p2.getProcessors().size(), equalTo(0));
            Pipeline p3 = ingestService.getPipeline("_id3");
            assertThat(p3.getId(), equalTo("_id3"));
            assertThat(p3.getProcessors().size(), equalTo(0));
        }

        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        {
            Pipeline p1 = ingestService.getPipeline("_id1");
            assertThat(p1.getId(), equalTo("_id1"));
            assertThat(p1.getProcessors().size(), equalTo(0));
            Pipeline p3 = ingestService.getPipeline("_id3");
            assertThat(p3.getId(), equalTo("_id3"));
            assertThat(p3.getProcessors().size(), equalTo(0));
        }

        pipeline3 = new PipelineConfiguration("_id3", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        {
            Pipeline p1 = ingestService.getPipeline("_id1");
            assertThat(p1.getId(), equalTo("_id1"));
            assertThat(p1.getProcessors().size(), equalTo(0));
            Pipeline p3 = ingestService.getPipeline("_id3");
            assertThat(p3.getId(), equalTo("_id3"));
            assertThat(p3.getProcessors().size(), equalTo(1));
            assertThat(p3.getProcessors().get(0).getType(), equalTo("set"));
        }

        // Perform an update with no changes:
        Map<String, IngestService.PipelineHolder> pipelines = ingestService.pipelines();
        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines(), sameInstance(pipelines));
    }

    public void testInnerUpdatePipelinesValidation() {
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("fail_validation", (factories, tag, description, config) -> {
            // ordinary validation issues happen at processor construction time
            throw newConfigurationException("fail_validation", tag, "no_property_name", "validation failure reason");
        });
        processors.put("fail_extra_validation", (factories, tag, description, config) -> {
            // 'extra validation' issues happen post- processor construction time
            return new FakeProcessor("fail_extra_validation", tag, description, ingestDocument -> {}) {
                @Override
                public void extraValidation() throws Exception {
                    throw newConfigurationException("fail_extra_validation", tag, "no_property_name", "extra validation failure reason");
                }
            };
        });

        {
            // a processor that fails ordinary validation (i.e. the processor factory throws an exception while constructing it)
            // will result in a placeholder pipeline being substituted

            IngestService ingestService = createWithProcessors(processors);
            PipelineConfiguration config = new PipelineConfiguration("_id", new BytesArray("""
                {"processors": [{"fail_validation" : {}}]}"""), XContentType.JSON);
            IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id", config));
            ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
            ClusterState previousClusterState = clusterState;
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
                .build();
            ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

            Pipeline pipeline = ingestService.getPipeline("_id");
            assertThat(
                pipeline.getDescription(),
                equalTo("this is a place holder pipeline, because pipeline with id [_id] could not be loaded")
            );
        }

        {
            // a processor that fails extra validation (i.e. an exception is throw from `extraValidation`)
            // will be processed just fine -- extraValidation is for rest/transport validation, not for when
            // a processor is being created from a processor factory

            IngestService ingestService = createWithProcessors(processors);
            PipelineConfiguration config = new PipelineConfiguration("_id", new BytesArray("""
                {"processors": [{"fail_extra_validation" : {}}]}"""), XContentType.JSON);
            IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id", config));
            ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
            ClusterState previousClusterState = clusterState;
            clusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
                .build();
            ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

            Pipeline pipeline = ingestService.getPipeline("_id");
            assertThat(pipeline.getDescription(), nullValue());
            assertThat(pipeline.getProcessors().size(), equalTo(1));
            Processor processor = pipeline.getProcessors().get(0);
            assertThat(processor.getType(), equalTo("fail_extra_validation"));
        }
    }

    public void testDelete() {
        IngestService ingestService = createWithProcessors();
        PipelineConfiguration config = new PipelineConfiguration("_id", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        // Delete pipeline:
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");
        previousClusterState = clusterState;
        clusterState = executeDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), nullValue());

        // Delete not existing pipeline:
        ClusterState finalClusterState = clusterState;
        assertThat(
            expectThrows(ResourceNotFoundException.class, () -> executeFailingDelete(deleteRequest, finalClusterState)).getMessage(),
            equalTo("pipeline [_id] is missing")
        );
    }

    public void testValidateNoIngestInfo() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);

        var pipelineConfig = XContentHelper.convertToMap(putRequest.getSource(), false, putRequest.getXContentType()).v2();
        Exception e = expectThrows(
            IllegalStateException.class,
            () -> ingestService.validatePipeline(Map.of(), putRequest.getId(), pipelineConfig)
        );
        assertEquals("Ingest info is empty", e.getMessage());

        DiscoveryNode discoveryNode = DiscoveryNodeUtils.create("_node_id", buildNewFakeTransportAddress(), Map.of(), Set.of());
        IngestInfo ingestInfo = new IngestInfo(List.of(new ProcessorInfo("set")));
        ingestService.validatePipeline(Map.of(discoveryNode, ingestInfo), putRequest.getId(), pipelineConfig);
    }

    public void testValidateNotInUse() {
        String pipeline = "pipeline";
        Map<String, IndexMetadata> indices = new HashMap<>();
        int defaultIndicesCount = randomIntBetween(0, 4);
        List<String> defaultIndices = new ArrayList<>();
        for (int i = 0; i < defaultIndicesCount; i++) {
            String indexName = "index" + i;
            defaultIndices.add(indexName);
            IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
            Settings.Builder settingsBuilder = settings(IndexVersion.current());
            settingsBuilder.put(IndexSettings.DEFAULT_PIPELINE.getKey(), pipeline);
            builder.settings(settingsBuilder);
            IndexMetadata indexMetadata = builder.settings(settingsBuilder).numberOfShards(1).numberOfReplicas(1).build();
            indices.put(indexName, indexMetadata);
        }

        int finalIndicesCount = randomIntBetween(defaultIndicesCount > 0 ? 0 : 1, 4);
        List<String> finalIndices = new ArrayList<>();
        for (int i = defaultIndicesCount; i < (finalIndicesCount + defaultIndicesCount); i++) {
            String indexName = "index" + i;
            finalIndices.add(indexName);
            IndexMetadata.Builder builder = IndexMetadata.builder(indexName);
            Settings.Builder settingsBuilder = settings(IndexVersion.current());
            settingsBuilder.put(IndexSettings.FINAL_PIPELINE.getKey(), pipeline);
            builder.settings(settingsBuilder);
            IndexMetadata indexMetadata = builder.settings(settingsBuilder).numberOfShards(1).numberOfReplicas(1).build();
            indices.put(indexName, indexMetadata);
        }

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> IngestService.validateNotInUse(pipeline, indices.values())
        );

        if (defaultIndices.size() > 0) {
            assertThat(
                e.getMessage(),
                containsString(
                    String.format(
                        Locale.ROOT,
                        "default pipeline for %s index(es) including [%s]",
                        defaultIndices.size(),
                        defaultIndices.stream().sorted().limit(3).collect(Collectors.joining(","))
                    )
                )
            );
        }

        if (defaultIndices.size() > 0 && finalIndices.size() > 0) {
            assertThat(e.getMessage(), containsString(" and "));
        }

        if (finalIndices.size() > 0) {
            assertThat(
                e.getMessage(),
                containsString(
                    String.format(
                        Locale.ROOT,
                        "final pipeline for %s index(es) including [%s]",
                        finalIndices.size(),
                        finalIndices.stream().sorted().limit(3).collect(Collectors.joining(","))
                    )
                )
            );
        }
    }

    public void testGetProcessorsInPipeline() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {
              "processors": [
                {
                  "set": {
                    "field": "_field",
                    "value": "_value",
                    "tag": "tag1"
                  }
                },
                {
                  "remove": {
                    "field": "_field",
                    "tag": "tag2"
                  }
                }
              ]
            }"""), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());

        assertThat(ingestService.getProcessorsInPipeline(id, Processor.class).size(), equalTo(3));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessorImpl.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessor.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, FakeProcessor.class).size(), equalTo(2));

        assertThat(ingestService.getProcessorsInPipeline(id, ConditionalProcessor.class).size(), equalTo(0));

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ingestService.getProcessorsInPipeline("fakeID", Processor.class)
        );
        assertThat("pipeline with id [fakeID] does not exist", equalTo(e.getMessage()));
    }

    public void testGetPipelineWithProcessorType() throws Exception {
        IngestService ingestService = createWithProcessors();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;

        PutPipelineRequest putRequest1 = new PutPipelineRequest("_id1", new BytesArray("""
            {
              "processors": [
                {
                  "set": {
                    "field": "_field",
                    "value": "_value",
                    "tag": "tag1"
                  }
                },
                {
                  "remove": {
                    "field": "_field",
                    "tag": "tag2"
                  }
                }
              ]
            }"""), XContentType.JSON);
        clusterState = executePut(putRequest1, clusterState);
        PutPipelineRequest putRequest2 = new PutPipelineRequest("_id2", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value", "tag": "tag2"}}]}"""), XContentType.JSON);
        clusterState = executePut(putRequest2, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        assertThat(ingestService.getPipelineWithProcessorType(FakeProcessor.class, processor -> true), containsInAnyOrder("_id1", "_id2"));
        assertThat(ingestService.getPipelineWithProcessorType(FakeProcessor.class, processor -> false), emptyIterable());
        assertThat(ingestService.getPipelineWithProcessorType(WrappingProcessorImpl.class, processor -> true), containsInAnyOrder("_id1"));
    }

    public void testReloadPipeline() throws Exception {
        boolean[] externalProperty = new boolean[] { false };

        Map<String, Processor.Factory> processorFactories = new HashMap<>();
        processorFactories.put("set", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");
            if (externalProperty[0]) {
                return new FakeProcessor("set", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
            } else {
                return new AbstractProcessor(tag, description) {
                    @Override
                    public IngestDocument execute(IngestDocument ingestDocument) throws Exception {
                        throw new RuntimeException("reload me");
                    }

                    @Override
                    public String getType() {
                        return "set";
                    }
                };
            }
        });

        IngestService ingestService = createWithProcessors(processorFactories);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;

        PutPipelineRequest putRequest1 = new PutPipelineRequest("_id1", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value", "tag": "tag1"}}]}"""), XContentType.JSON);
        clusterState = executePut(putRequest1, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        {
            Exception[] exceptionHolder = new Exception[1];
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
            ingestService.getPipeline("_id1").execute(ingestDocument, (ingestDocument1, e) -> exceptionHolder[0] = e);
            assertThat(exceptionHolder[0], notNullValue());
            assertThat(exceptionHolder[0].getMessage(), containsString("reload me"));
            assertThat(ingestDocument.getSourceAndMetadata().get("_field"), nullValue());
        }

        externalProperty[0] = true;
        ingestService.reloadPipeline("_id1");

        {
            Exception[] holder = new Exception[1];
            IngestDocument ingestDocument = RandomDocumentPicks.randomIngestDocument(random(), new HashMap<>());
            ingestService.getPipeline("_id1").execute(ingestDocument, (ingestDocument1, e) -> holder[0] = e);
            assertThat(holder[0], nullValue());
            assertThat(ingestDocument.getSourceAndMetadata().get("_field"), equalTo("_value"));
        }
    }

    public void testGetProcessorsInPipelineComplexConditional() throws Exception {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        String scriptName = "conditionalScript";
        ScriptService scriptService = new ScriptService(
            Settings.builder().build(),
            Map.of(Script.DEFAULT_SCRIPT_LANG, new MockScriptEngine(Script.DEFAULT_SCRIPT_LANG, Map.of(scriptName, ctx -> {
                ctx.get("_type");
                return true;
            }), Map.of())),
            new HashMap<>(ScriptModule.CORE_CONTEXTS),
            () -> 1L
        );

        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("complexSet", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");

            return new ConditionalProcessor(
                randomAlphaOfLength(10),
                null,
                new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of()),
                scriptService,
                new ConditionalProcessor(
                    randomAlphaOfLength(10) + "-nested",
                    null,
                    new Script(ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG, scriptName, Map.of()),
                    scriptService,
                    new FakeProcessor("complexSet", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value))
                )
            );
        });

        IngestService ingestService = createWithProcessors(processors);
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("""
            {"processors": [{"complexSet" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());

        assertThat(ingestService.getProcessorsInPipeline(id, Processor.class).size(), equalTo(3));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessor.class).size(), equalTo(2));
        assertThat(ingestService.getProcessorsInPipeline(id, FakeProcessor.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, ConditionalProcessor.class).size(), equalTo(2));

        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessorImpl.class).size(), equalTo(0));
    }

    public void testCrud() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest(id);
        previousClusterState = clusterState;
        clusterState = executeDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
    }

    public void testPut() {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        // add a new pipeline:
        PutPipelineRequest putRequest = new PutPipelineRequest(id, new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest = new PutPipelineRequest(id, new BytesArray("""
            {"processors": [], "description": "_description"}"""), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(0));
    }

    public void testPutWithErrorResponse() throws IllegalAccessException {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        PutPipelineRequest putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"description\": \"empty processors\"}"),
            XContentType.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1",
                IngestService.class.getCanonicalName(),
                Level.WARN,
                "failed to update ingest pipelines"
            )
        );
        Logger ingestLogger = LogManager.getLogger(IngestService.class);
        Loggers.addAppender(ingestLogger, mockAppender);
        try {
            ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
            mockAppender.assertAllExpectationsMatched();
        } finally {
            Loggers.removeAppender(ingestLogger, mockAppender);
            mockAppender.stop();
        }
        pipeline = ingestService.getPipeline(id);
        assertNotNull(pipeline);
        assertThat(pipeline.getId(), equalTo("_id"));
        assertThat(
            pipeline.getDescription(),
            equalTo("this is a place holder pipeline, because pipeline with" + " id [_id] could not be loaded")
        );
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertNull(pipeline.getProcessors().get(0).getTag());
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("unknown"));
    }

    public void testDeleteUsingWildcard() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}""");
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        pipelines.put("p2", new PipelineConfiguration("p2", definition, XContentType.JSON));
        pipelines.put("q1", new PipelineConfiguration("q1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());
        assertThat(ingestService.getPipeline("p2"), notNullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Delete pipeline matching wildcard
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("p*");
        previousClusterState = clusterState;
        clusterState = executeDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Exception if we used name which does not exist
        ClusterState finalClusterState = clusterState;
        assertThat(
            expectThrows(
                ResourceNotFoundException.class,
                () -> executeFailingDelete(new DeletePipelineRequest("unknown"), finalClusterState)
            ).getMessage(),
            equalTo("pipeline [unknown] is missing")
        );

        // match all wildcard works on last remaining pipeline
        DeletePipelineRequest matchAllDeleteRequest = new DeletePipelineRequest("*");
        previousClusterState = clusterState;
        clusterState = executeDelete(matchAllDeleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), nullValue());

        // match all wildcard does not throw exception if none match
        executeDelete(matchAllDeleteRequest, clusterState);
    }

    public void testDeleteWithExistingUnmatchedPipelines() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}""");
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState)
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());

        ClusterState finalClusterState = clusterState;
        assertThat(
            expectThrows(ResourceNotFoundException.class, () -> executeFailingDelete(new DeletePipelineRequest("z*"), finalClusterState))
                .getMessage(),
            equalTo("pipeline [z*] is missing")
        );
    }

    public void testDeleteWithIndexUsePipeline() {
        IngestService ingestService = createWithProcessors();
        PipelineConfiguration config = new PipelineConfiguration("_id", new BytesArray("""
            {"processors": [{"set" : {"field": "_field", "value": "_value"}}]}"""), XContentType.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id", config));
        Metadata.Builder builder = Metadata.builder();
        for (int i = 0; i < randomIntBetween(2, 10); i++) {
            builder.put(
                IndexMetadata.builder("test" + i).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1).build(),
                true
            );
        }
        builder.putCustom(IngestMetadata.TYPE, ingestMetadata);
        Metadata metadata = builder.build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metadata(metadata).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");

        {
            // delete pipeline which is in used of default_pipeline
            IndexMetadata indexMetadata = IndexMetadata.builder("pipeline-index")
                .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "_id"))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            ClusterState finalClusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(metadata).put(indexMetadata, true))
                .build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> executeFailingDelete(deleteRequest, finalClusterState)
            );
            assertThat(e.getMessage(), containsString("default pipeline for 1 index(es) including [pipeline-index]"));
        }

        {
            // delete pipeline which is in used of final_pipeline
            IndexMetadata indexMetadata = IndexMetadata.builder("pipeline-index")
                .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), "_id"))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .build();
            ClusterState finalClusterState = ClusterState.builder(clusterState)
                .metadata(Metadata.builder(metadata).put(indexMetadata, true))
                .build();
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> executeFailingDelete(deleteRequest, finalClusterState)
            );
            assertThat(e.getMessage(), containsString("final pipeline for 1 index(es) including [pipeline-index]"));
        }

        // Delete pipeline:
        previousClusterState = clusterState;
        clusterState = executeDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), nullValue());
    }

    public void testGetPipelines() {
        Map<String, PipelineConfiguration> configs = new HashMap<>();
        configs.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{\"processors\": []}"), XContentType.JSON));
        configs.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{\"processors\": []}"), XContentType.JSON));

        assertThat(IngestService.innerGetPipelines(null, "_id1").isEmpty(), is(true));

        IngestMetadata ingestMetadata = new IngestMetadata(configs);
        List<PipelineConfiguration> pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id1");
        assertThat(pipelines.size(), equalTo(1));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id1", "_id2");
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "_id*");
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        // get all variants: (no IDs or '*')
        pipelines = IngestService.innerGetPipelines(ingestMetadata);
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));

        pipelines = IngestService.innerGetPipelines(ingestMetadata, "*");
        pipelines.sort(Comparator.comparing(PipelineConfiguration::getId));
        assertThat(pipelines.size(), equalTo(2));
        assertThat(pipelines.get(0).getId(), equalTo("_id1"));
        assertThat(pipelines.get(1).getId(), equalTo("_id2"));
    }

    public void testValidateProcessorTypeOnAllNodes() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {
              "processors": [
                {
                  "set": {
                    "field": "_field",
                    "value": "_value",
                    "tag": "tag1"
                  }
                },
                {
                  "remove": {
                    "field": "_field",
                    "tag": "tag2"
                  }
                }
              ]
            }"""), XContentType.JSON);
        var pipelineConfig = XContentHelper.convertToMap(putRequest.getSource(), false, putRequest.getXContentType()).v2();

        DiscoveryNode node1 = DiscoveryNodeUtils.create("_node_id1", buildNewFakeTransportAddress(), Map.of(), Set.of());
        DiscoveryNode node2 = DiscoveryNodeUtils.create("_node_id2", buildNewFakeTransportAddress(), Map.of(), Set.of());
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(List.of(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(List.of(new ProcessorInfo("set"))));

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> ingestService.validatePipeline(ingestInfos, putRequest.getId(), pipelineConfig)
        );
        assertEquals("Processor type [remove] is not installed on node [" + node2 + "]", e.getMessage());
        assertEquals("remove", e.getMetadata("es.processor_type").get(0));
        assertEquals("tag2", e.getMetadata("es.processor_tag").get(0));

        var pipelineConfig2 = XContentHelper.convertToMap(putRequest.getSource(), false, putRequest.getXContentType()).v2();
        ingestInfos.put(node2, new IngestInfo(List.of(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestService.validatePipeline(ingestInfos, putRequest.getId(), pipelineConfig2);
    }

    public void testValidateConfigurationExceptions() {
        IngestService ingestService = createWithProcessors(Map.of("fail_validation", (factories, tag, description, config) -> {
            // ordinary validation issues happen at processor construction time
            throw newConfigurationException("fail_validation", tag, "no_property_name", "validation failure reason");
        }));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {
              "processors": [
                {
                  "fail_validation": {
                  }
                }
              ]
            }"""), XContentType.JSON);
        var pipelineConfig = XContentHelper.convertToMap(putRequest.getSource(), false, putRequest.getXContentType()).v2();

        // other validation actually consults this map, but this validation does not. however, it must not be empty.
        DiscoveryNode node1 = DiscoveryNodeUtils.create("_node_id1", buildNewFakeTransportAddress(), Map.of(), Set.of());
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(List.of()));

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> ingestService.validatePipeline(ingestInfos, putRequest.getId(), pipelineConfig)
        );
        assertEquals("[no_property_name] validation failure reason", e.getMessage());
        assertEquals("fail_validation", e.getMetadata("es.processor_type").get(0));
    }

    public void testValidateExtraValidationConfigurationExceptions() {
        IngestService ingestService = createWithProcessors(Map.of("fail_extra_validation", (factories, tag, description, config) -> {
            // 'extra validation' issues happen post- processor construction time
            return new FakeProcessor("fail_extra_validation", tag, description, ingestDocument -> {}) {
                @Override
                public void extraValidation() throws Exception {
                    throw newConfigurationException("fail_extra_validation", tag, "no_property_name", "extra validation failure reason");
                }
            };
        }));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {
              "processors": [
                {
                  "fail_extra_validation": {
                  }
                }
              ]
            }"""), XContentType.JSON);
        var pipelineConfig = XContentHelper.convertToMap(putRequest.getSource(), false, putRequest.getXContentType()).v2();

        // other validation actually consults this map, but this validation does not. however, it must not be empty.
        DiscoveryNode node1 = DiscoveryNodeUtils.create("_node_id1", buildNewFakeTransportAddress(), Map.of(), Set.of());
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(List.of()));

        ElasticsearchParseException e = expectThrows(
            ElasticsearchParseException.class,
            () -> ingestService.validatePipeline(ingestInfos, putRequest.getId(), pipelineConfig)
        );
        assertEquals("[no_property_name] extra validation failure reason", e.getMessage());
        assertEquals("fail_extra_validation", e.getMetadata("es.processor_type").get(0));
    }

    public void testExecuteIndexPipelineExistsButFailedParsing() {
        IngestService ingestService = createWithProcessors(
            Map.of("mock", (factories, tag, description, config) -> new AbstractProcessor("mock", "description") {
                @Override
                public IngestDocument execute(IngestDocument ingestDocument) {
                    throw new IllegalStateException("error");
                }

                @Override
                public String getType() {
                    return null;
                }
            })
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        String id = "_id";
        PutPipelineRequest putRequest = new PutPipelineRequest(
            id,
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final SetOnce<Boolean> failure = new SetOnce<>();

        BulkRequest bulkRequest = new BulkRequest();
        final IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1")
            .source(Map.of())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(Map.of()).setPipeline(id).setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);

        final BiConsumer<Integer, Exception> failureHandler = (slot, e) -> {
            assertThat(e.getCause(), instanceOf(IllegalStateException.class));
            assertThat(e.getCause().getMessage(), equalTo("error"));
            failure.set(true);
            assertThat(slot, equalTo(1));
        };

        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);

        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteBulkPipelineDoesNotExist() {
        IngestService ingestService = createWithProcessors(
            Map.of("mock", (factories, tag, description, config) -> mockCompoundProcessor())
        );

        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1").source(Map.of()).setPipeline("_none").setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(Map.of()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);
        IndexRequest indexRequest3 = new IndexRequest("_index").id("_id3")
            .source(Map.of())
            .setPipeline("does_not_exist")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest3);
        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(failureHandler, times(1)).accept(
            argThat(item -> item == 2),
            argThat(iae -> "pipeline with id [does_not_exist] does not exist".equals(iae.getMessage()))
        );
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteBulkRequestCallsDocumentSizeObserver() {
        /*
         * This test makes sure that for both insert and upsert requests, when we call executeBulkRequest DocumentSizeObserver is
         * called using a non-null index name.
         */
        AtomicInteger wrappedObserverWasUsed = new AtomicInteger(0);
        AtomicInteger parsedValueWasUsed = new AtomicInteger(0);
        DocumentParsingProvider documentParsingProvider = new DocumentParsingProvider() {
            @Override
            public DocumentSizeObserver newDocumentSizeObserver() {
                return new DocumentSizeObserver() {
                    @Override
                    public XContentParser wrapParser(XContentParser xContentParser) {
                        wrappedObserverWasUsed.incrementAndGet();
                        return xContentParser;
                    }

                    @Override
                    public long normalisedBytesParsed() {
                        parsedValueWasUsed.incrementAndGet();
                        return 0;
                    }
                };
            }

            @Override
            public DocumentSizeReporter getDocumentParsingReporter() {
                return null;
            }

            @Override
            public DocumentSizeObserver newFixedSizeDocumentObserver(long normalisedBytesParsed) {
                return null;
            }
        };
        IngestService ingestService = createWithProcessors(
            Map.of("mock", (factories, tag, description, config) -> mockCompoundProcessor()),
            documentParsingProvider
        );

        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        BulkRequest bulkRequest = new BulkRequest();
        UpdateRequest updateRequest = new UpdateRequest("_index", "_id1").upsert("{}", "{}");
        updateRequest.upsertRequest().setPipeline("_id");
        bulkRequest.add(updateRequest);
        IndexRequest indexRequest = new IndexRequest("_index").id("_id1").source(Map.of()).setPipeline("_id1");
        bulkRequest.add(indexRequest);
        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        assertThat(wrappedObserverWasUsed.get(), equalTo(2));
        assertThat(parsedValueWasUsed.get(), equalTo(2));
    }

    public void testExecuteSuccess() {
        IngestService ingestService = createWithProcessors(
            Map.of("mock", (factories, tag, description, config) -> mockCompoundProcessor())
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testDynamicTemplates() throws Exception {
        IngestService ingestService = createWithProcessors(
            Map.of(
                "set",
                (factories, tag, description, config) -> new FakeProcessor(
                    "set",
                    "",
                    "",
                    (ingestDocument) -> ingestDocument.setFieldValue("_dynamic_templates", Map.of("foo", "bar", "foo.bar", "baz"))
                )
            )
        );
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"set\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        CountDownLatch latch = new CountDownLatch(1);
        final BiConsumer<Integer, Exception> failureHandler = (v, e) -> { throw new AssertionError("must never fail", e); };
        final BiConsumer<Thread, Exception> completionHandler = (t, e) -> latch.countDown();
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        latch.await();
        assertThat(indexRequest.getDynamicTemplates(), equalTo(Map.of("foo", "bar", "foo.bar", "baz")));
    }

    public void testExecuteEmptyPipeline() throws Exception {
        IngestService ingestService = createWithProcessors(Map.of());
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {"processors": [], "description": "_description"}"""), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecutePropagateAllMetadataUpdates() throws Exception {
        final CompoundProcessor processor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(Map.of("mock", (factories, tag, description, config) -> processor));
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final long newVersion = randomLong();
        final String versionType = randomFrom("internal", "external", "external_gt", "external_gte");
        final long ifSeqNo = randomNonNegativeLong();
        final long ifPrimaryTerm = randomNonNegativeLong();
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.Metadata metadata : IngestDocument.Metadata.values()) {
                if (metadata == IngestDocument.Metadata.VERSION) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), newVersion);
                } else if (metadata == IngestDocument.Metadata.VERSION_TYPE) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), versionType);
                } else if (metadata == IngestDocument.Metadata.IF_SEQ_NO) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), ifSeqNo);
                } else if (metadata == IngestDocument.Metadata.IF_PRIMARY_TERM) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), ifPrimaryTerm);
                } else if (metadata == IngestDocument.Metadata.DYNAMIC_TEMPLATES) {
                    ingestDocument.setFieldValue(metadata.getFieldName(), Map.of("foo", "bar"));
                } else if (metadata == IngestDocument.Metadata.TYPE) {
                    // can't update _type
                } else {
                    ingestDocument.setFieldValue(metadata.getFieldName(), "update" + metadata.getFieldName());
                }
            }

            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer<IngestDocument, Exception>) invocationOnMock.getArguments()[1];
            handler.accept(ingestDocument, null);
            return null;
        }).when(processor).execute(any(), any());
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(processor).execute(any(), any());
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.version(), equalTo(newVersion));
        assertThat(indexRequest.versionType(), equalTo(VersionType.fromString(versionType)));
        assertThat(indexRequest.ifSeqNo(), equalTo(ifSeqNo));
        assertThat(indexRequest.ifPrimaryTerm(), equalTo(ifPrimaryTerm));
    }

    public void testExecuteFailure() throws Exception {
        final CompoundProcessor processor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Map.of(
                "mock",
                (factories, tag, description, config) -> processor,
                "set",
                (factories, tag, description, config) -> new FakeProcessor("set", "", "", (ingestDocument) -> fail())
            )
        );
        PutPipelineRequest putRequest1 = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        // given that set -> fail() above, it's a failure if a document executes against this pipeline
        PutPipelineRequest putRequest2 = new PutPipelineRequest(
            "_id2",
            new BytesArray("{\"processors\": [{\"set\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest1, clusterState);
        clusterState = executePut(putRequest2, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id1")
            .setFinalPipeline("_id2");
        doThrow(new RuntimeException()).when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        verify(failureHandler, times(1)).accept(eq(0), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteSuccessWithOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock_processor_type");
        when(processor.getTag()).thenReturn("mock_processor_tag");
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(null, new RuntimeException());
            return null;
        }).when(processor).execute(eqIndexTypeId(Map.of()), any());

        final Processor onFailureProcessor = mock(Processor.class);
        doAnswer(args -> {
            IngestDocument ingestDocument = (IngestDocument) args.getArguments()[0];
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(ingestDocument, null);
            return null;
        }).when(onFailureProcessor).execute(eqIndexTypeId(Map.of()), any());

        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor),
            List.of(new CompoundProcessor(onFailureProcessor))
        );
        IngestService ingestService = createWithProcessors(Map.of("mock", (factories, tag, description, config) -> compoundProcessor));
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(failureHandler, never()).accept(eq(0), any(IngestProcessorException.class));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteFailureWithNestedOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.isAsync()).thenReturn(true);
        final Processor onFailureProcessor = mock(Processor.class);
        when(onFailureProcessor.isAsync()).thenReturn(true);
        final Processor onFailureOnFailureProcessor = mock(Processor.class);
        when(onFailureOnFailureProcessor.isAsync()).thenReturn(true);
        final List<Processor> processors = List.of(onFailureProcessor);
        final List<Processor> onFailureProcessors = List.of(onFailureOnFailureProcessor);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor),
            List.of(new CompoundProcessor(false, processors, onFailureProcessors))
        );
        IngestService ingestService = createWithProcessors(Map.of("mock", (factories, tag, description, config) -> compoundProcessor));
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        doThrow(new RuntimeException()).when(onFailureOnFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        doThrow(new RuntimeException()).when(onFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        doThrow(new RuntimeException()).when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        verify(failureHandler, times(1)).accept(eq(0), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testBulkRequestExecutionWithFailures() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numRequest = scaledRandomIntBetween(8, 64);
        int numIndexRequests = 0;
        for (int i = 0; i < numRequest; i++) {
            DocWriteRequest<?> request;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_id");
                } else {
                    request = new UpdateRequest("_index", "_id");
                }
            } else {
                IndexRequest indexRequest = new IndexRequest("_index").id("_id").setPipeline(pipelineId).setFinalPipeline("_none");
                indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
                request = indexRequest;
                numIndexRequests++;
            }
            bulkRequest.add(request);
        }

        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.isAsync()).thenReturn(true);
        when(processor.getProcessors()).thenReturn(List.of(mock(Processor.class)));
        Exception error = new RuntimeException();
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(null, error);
            return null;
        }).when(processor).execute(any(), any());
        IngestService ingestService = createWithProcessors(Map.of("mock", (factories, tag, description, config) -> processor));
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            numRequest,
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            requestItemErrorHandler,
            completionHandler,
            Names.WRITE
        );

        verify(requestItemErrorHandler, times(numIndexRequests)).accept(anyInt(), argThat(e -> e.getCause().equals(error)));
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteFailureRedirection() throws Exception {
        final CompoundProcessor processor = mockCompoundProcessor();
        IngestService ingestService = createWithProcessors(
            Map.of(
                "mock",
                (factories, tag, description, config) -> processor,
                "set",
                (factories, tag, description, config) -> new FakeProcessor("set", "", "", (ingestDocument) -> fail())
            )
        );
        PutPipelineRequest putRequest1 = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        // given that set -> fail() above, it's a failure if a document executes against this pipeline
        PutPipelineRequest putRequest2 = new PutPipelineRequest(
            "_id2",
            new BytesArray("{\"processors\": [{\"set\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest1, clusterState);
        clusterState = executePut(putRequest2, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id1")
            .setFinalPipeline("_id2");
        doThrow(new RuntimeException()).when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        final Predicate<String> redirectCheck = (idx) -> indexRequest.index().equals(idx);
        @SuppressWarnings("unchecked")
        final TriConsumer<Integer, String, Exception> redirectHandler = mock(TriConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            redirectCheck,
            redirectHandler,
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        verify(redirectHandler, times(1)).apply(eq(0), eq(indexRequest.index()), any(RuntimeException.class));
        verifyNoInteractions(failureHandler);
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testExecuteFailureRedirectionWithNestedOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.isAsync()).thenReturn(true);
        final Processor onFailureProcessor = mock(Processor.class);
        when(onFailureProcessor.isAsync()).thenReturn(true);
        final Processor onFailureOnFailureProcessor = mock(Processor.class);
        when(onFailureOnFailureProcessor.isAsync()).thenReturn(true);
        final List<Processor> processors = List.of(onFailureProcessor);
        final List<Processor> onFailureProcessors = List.of(onFailureOnFailureProcessor);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            List.of(processor),
            List.of(new CompoundProcessor(false, processors, onFailureProcessors))
        );
        IngestService ingestService = createWithProcessors(Map.of("mock", (factories, tag, description, config) -> compoundProcessor));
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index").id("_id")
            .source(Map.of())
            .setPipeline("_id")
            .setFinalPipeline("_none");
        doThrow(new RuntimeException()).when(onFailureOnFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        doThrow(new RuntimeException()).when(onFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        doThrow(new RuntimeException()).when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        final Predicate<String> redirectPredicate = (idx) -> indexRequest.index().equals(idx);
        @SuppressWarnings("unchecked")
        final TriConsumer<Integer, String, Exception> redirectHandler = mock(TriConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            redirectPredicate,
            redirectHandler,
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), Map.of()), any());
        verify(redirectHandler, times(1)).apply(eq(0), eq(indexRequest.index()), any(RuntimeException.class));
        verifyNoInteractions(failureHandler);
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testBulkRequestExecutionWithRedirectedFailures() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numRequest = scaledRandomIntBetween(8, 64);
        int numIndexRequests = 0;
        for (int i = 0; i < numRequest; i++) {
            DocWriteRequest<?> request;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_id");
                } else {
                    request = new UpdateRequest("_index", "_id");
                }
            } else {
                IndexRequest indexRequest = new IndexRequest("_index").id("_id").setPipeline(pipelineId).setFinalPipeline("_none");
                indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
                request = indexRequest;
                numIndexRequests++;
            }
            bulkRequest.add(request);
        }

        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.isAsync()).thenReturn(true);
        when(processor.getProcessors()).thenReturn(List.of(mock(Processor.class)));
        Exception error = new RuntimeException();
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(null, error);
            return null;
        }).when(processor).execute(any(), any());
        IngestService ingestService = createWithProcessors(Map.of("mock", (factories, tag, description, config) -> processor));
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        TriConsumer<Integer, String, Exception> requestItemRedirectHandler = mock(TriConsumer.class);
        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            numRequest,
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> true,
            requestItemRedirectHandler,
            requestItemErrorHandler,
            completionHandler,
            Names.WRITE
        );

        verify(requestItemRedirectHandler, times(numIndexRequests)).apply(anyInt(), anyString(), argThat(e -> e.getCause().equals(error)));
        verifyNoInteractions(requestItemErrorHandler);
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
    }

    public void testBulkRequestExecution() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        // Test to make sure that ingest respects content types other than the default index content type
        XContentType xContentType = randomFrom(
            Arrays.stream(XContentType.values()).filter(t -> Requests.INDEX_CONTENT_TYPE.equals(t) == false).toList()
        );

        logger.info("Using [{}], not randomly determined default [{}]", xContentType, Requests.INDEX_CONTENT_TYPE);
        int numRequest = scaledRandomIntBetween(8, 64);
        List<Boolean> executedPipelinesExpected = new ArrayList<>();
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index").id("_id").setPipeline(pipelineId).setFinalPipeline("_none");
            indexRequest.source(xContentType, "field1", "value1");
            boolean shouldListExecutedPiplines = randomBoolean();
            executedPipelinesExpected.add(shouldListExecutedPiplines);
            indexRequest.setListExecutedPipelines(shouldListExecutedPiplines);
            bulkRequest.add(indexRequest);
        }

        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(RandomDocumentPicks.randomIngestDocument(random()), null);
            return null;
        }).when(processor).execute(any(), any());
        Map<String, Processor.Factory> map = Maps.newMapWithExpectedSize(2);
        map.put("mock", (factories, tag, description, config) -> processor);

        IngestService ingestService = createWithProcessors(map);
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray("""
            {"processors": [{"mock": {}}], "description": "_description"}"""), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<Integer, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        ingestService.executeBulkRequest(
            numRequest,
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            requestItemErrorHandler,
            completionHandler,
            Names.WRITE
        );

        verify(requestItemErrorHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        for (int i = 0; i < bulkRequest.requests().size(); i++) {
            DocWriteRequest<?> docWriteRequest = bulkRequest.requests().get(i);
            IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(docWriteRequest);
            assertThat(indexRequest, notNullValue());
            assertThat(indexRequest.getContentType(), equalTo(xContentType.canonical()));
            if (executedPipelinesExpected.get(i)) {
                assertThat(indexRequest.getExecutedPipelines(), equalTo(List.of(pipelineId)));
            } else if (indexRequest.getListExecutedPipelines()) {
                assertThat(indexRequest.getExecutedPipelines(), equalTo(List.of()));
            } else {
                assertThat(indexRequest.getExecutedPipelines(), nullValue());
            }
        }
    }

    public void testIngestAndPipelineStats() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        when(processor.isAsync()).thenReturn(true);

        // avoid returning null and dropping the document
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(RandomDocumentPicks.randomIngestDocument(random()), null);
            return null;
        }).when(processor).execute(any(IngestDocument.class), any());

        // mock up an ingest service for returning a pipeline, this is used by the pipeline processor
        final Pipeline[] pipelineToReturn = new Pipeline[1];
        final IngestService pipelineIngestService = mock(IngestService.class);
        when(pipelineIngestService.getPipeline(anyString())).thenAnswer(inv -> pipelineToReturn[0]);

        IngestService ingestService = createWithProcessors(
            Map.of(
                "pipeline",
                (factories, tag, description, config) -> new PipelineProcessor(tag, description, (params) -> new TemplateScript(params) {
                    @Override
                    public String execute() {
                        return "_id3";
                    } // this pipeline processor will always execute the '_id3' processor
                }, false, pipelineIngestService),
                "mock",
                (factories, tag, description, config) -> processor
            )
        );

        {
            // all zeroes since nothing has executed
            final IngestStats ingestStats = ingestService.stats();
            assertThat(ingestStats.pipelineStats().size(), equalTo(0));
            assertStats(ingestStats.totalStats(), 0, 0, 0);
        }

        // put some pipelines, and now there are pipeline and processor stats, too
        PutPipelineRequest putRequest1 = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        // n.b. this 'pipeline' processor will always run the '_id3' pipeline, see the mocking/plumbing above and below
        PutPipelineRequest putRequest2 = new PutPipelineRequest(
            "_id2",
            new BytesArray("{\"processors\": [{\"pipeline\" : {}}]}"),
            XContentType.JSON
        );
        PutPipelineRequest putRequest3 = new PutPipelineRequest(
            "_id3",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest1, clusterState);
        clusterState = executePut(putRequest2, clusterState);
        clusterState = executePut(putRequest3, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        // hook up the mock ingest service to return pipeline3 when asked by the pipeline processor
        pipelineToReturn[0] = ingestService.getPipeline("_id3");

        {
            final IngestStats ingestStats = ingestService.stats();
            assertThat(ingestStats.pipelineStats().size(), equalTo(3));

            // total
            assertStats(ingestStats.totalStats(), 0, 0, 0);
            // pipeline
            assertPipelineStats(ingestStats.pipelineStats(), "_id1", 0, 0, 0);
            assertPipelineStats(ingestStats.pipelineStats(), "_id2", 0, 0, 0);
            assertPipelineStats(ingestStats.pipelineStats(), "_id3", 0, 0, 0);
            // processor
            assertProcessorStats(0, ingestStats, "_id1", 0, 0, 0);
            assertProcessorStats(0, ingestStats, "_id2", 0, 0, 0);
            assertProcessorStats(0, ingestStats, "_id3", 0, 0, 0);
        }

        // put a single document through ingest processing
        final IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1").setFinalPipeline("_id2");
        indexRequest.source(randomAlphaOfLength(10), randomAlphaOfLength(10));
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            (integer, e) -> {},
            (thread, e) -> {},
            Names.WRITE
        );

        {
            final IngestStats ingestStats = ingestService.stats();
            assertThat(ingestStats.pipelineStats().size(), equalTo(3));

            // total
            assertStats(ingestStats.totalStats(), 1, 0, 0);
            // pipeline
            assertPipelineStats(ingestStats.pipelineStats(), "_id1", 1, 0, 0);
            assertPipelineStats(ingestStats.pipelineStats(), "_id2", 1, 0, 0);
            assertPipelineStats(ingestStats.pipelineStats(), "_id3", 1, 0, 0);
            // processor
            assertProcessorStats(0, ingestStats, "_id1", 1, 0, 0);
            assertProcessorStats(0, ingestStats, "_id2", 1, 0, 0);
            assertProcessorStats(0, ingestStats, "_id3", 1, 0, 0);
        }
    }

    public void testStats() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor processorFailure = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        when(processor.isAsync()).thenReturn(true);
        when(processorFailure.isAsync()).thenReturn(true);
        when(processorFailure.getType()).thenReturn("failure-mock");
        // avoid returning null and dropping the document
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(RandomDocumentPicks.randomIngestDocument(random()), null);
            return null;
        }).when(processor).execute(any(IngestDocument.class), any());
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept(null, new RuntimeException("error"));
            return null;
        }).when(processorFailure).execute(any(IngestDocument.class), any());
        Map<String, Processor.Factory> map = Maps.newMapWithExpectedSize(2);
        map.put("mock", (factories, tag, description, config) -> processor);
        map.put("failure-mock", (factories, tag, description, config) -> processorFailure);
        IngestService ingestService = createWithProcessors(map);

        final IngestStats initialStats = ingestService.stats();
        assertThat(initialStats.pipelineStats().size(), equalTo(0));
        assertStats(initialStats.totalStats(), 0, 0, 0);

        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        putRequest = new PutPipelineRequest("_id2", new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);

        final IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1").setFinalPipeline("_none");
        indexRequest.source(randomAlphaOfLength(10), randomAlphaOfLength(10));
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        final IngestStats afterFirstRequestStats = ingestService.stats();
        assertThat(afterFirstRequestStats.pipelineStats().size(), equalTo(2));

        afterFirstRequestStats.processorStats().get("_id1").forEach(p -> assertEquals(p.name(), "mock:mockTag"));
        afterFirstRequestStats.processorStats().get("_id2").forEach(p -> assertEquals(p.name(), "mock:mockTag"));

        // total
        assertStats(afterFirstRequestStats.totalStats(), 1, 0, 0);
        // pipeline
        assertPipelineStats(afterFirstRequestStats.pipelineStats(), "_id1", 1, 0, 0);
        assertPipelineStats(afterFirstRequestStats.pipelineStats(), "_id2", 0, 0, 0);
        // processor
        assertProcessorStats(0, afterFirstRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterFirstRequestStats, "_id2", 0, 0, 0);

        indexRequest.setPipeline("_id2");
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        final IngestStats afterSecondRequestStats = ingestService.stats();
        assertThat(afterSecondRequestStats.pipelineStats().size(), equalTo(2));
        // total
        assertStats(afterSecondRequestStats.totalStats(), 2, 0, 0);
        // pipeline
        assertPipelineStats(afterSecondRequestStats.pipelineStats(), "_id1", 1, 0, 0);
        assertPipelineStats(afterSecondRequestStats.pipelineStats(), "_id2", 1, 0, 0);
        // processor
        assertProcessorStats(0, afterSecondRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterSecondRequestStats, "_id2", 1, 0, 0);

        // update cluster state and ensure that new stats are added to old stats
        putRequest = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}, {\"mock\" : {}}]}"),
            XContentType.JSON
        );
        previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        final IngestStats afterThirdRequestStats = ingestService.stats();
        assertThat(afterThirdRequestStats.pipelineStats().size(), equalTo(2));
        // total
        assertStats(afterThirdRequestStats.totalStats(), 3, 0, 0);
        // pipeline
        assertPipelineStats(afterThirdRequestStats.pipelineStats(), "_id1", 2, 0, 0);
        assertPipelineStats(afterThirdRequestStats.pipelineStats(), "_id2", 1, 0, 0);
        // The number of processors for the "id1" pipeline changed, so the per-processor metrics are not carried forward. This is
        // due to the parallel array's used to identify which metrics to carry forward. Without unique ids or semantic equals for each
        // processor, parallel arrays are the best option for of carrying forward metrics between pipeline changes. However, in some cases,
        // like this one it may not be readily obvious why the metrics were not carried forward.
        assertProcessorStats(0, afterThirdRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(1, afterThirdRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterThirdRequestStats, "_id2", 1, 0, 0);

        // test a failure, and that the processor stats are added from the old stats
        putRequest = new PutPipelineRequest("_id1", new BytesArray("""
            {"processors": [{"failure-mock" : { "on_failure": [{"mock" : {}}]}}, {"mock" : {}}]}"""), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(
            1,
            List.of(indexRequest),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        final IngestStats afterForthRequestStats = ingestService.stats();
        assertThat(afterForthRequestStats.pipelineStats().size(), equalTo(2));
        // total
        assertStats(afterForthRequestStats.totalStats(), 4, 0, 0);
        // pipeline
        assertPipelineStats(afterForthRequestStats.pipelineStats(), "_id1", 3, 0, 0);
        assertPipelineStats(afterForthRequestStats.pipelineStats(), "_id2", 1, 0, 0);
        // processor
        assertProcessorStats(0, afterForthRequestStats, "_id1", 1, 1, 0); // not carried forward since type changed
        assertProcessorStats(1, afterForthRequestStats, "_id1", 2, 0, 0); // carried forward and added from old stats
        assertProcessorStats(0, afterForthRequestStats, "_id2", 1, 0, 0);
    }

    public void testStatName() {
        Processor processor = mock(Processor.class);
        String name = randomAlphaOfLength(10);
        when(processor.getType()).thenReturn(name);
        assertThat(IngestService.getProcessorName(processor), equalTo(name));
        String tag = randomAlphaOfLength(10);
        when(processor.getTag()).thenReturn(tag);
        assertThat(IngestService.getProcessorName(processor), equalTo(name + ":" + tag));

        ConditionalProcessor conditionalProcessor = mock(ConditionalProcessor.class);
        when(conditionalProcessor.getInnerProcessor()).thenReturn(processor);
        assertThat(IngestService.getProcessorName(conditionalProcessor), equalTo(name + ":" + tag));

        PipelineProcessor pipelineProcessor = mock(PipelineProcessor.class);
        String pipelineName = randomAlphaOfLength(10);
        when(pipelineProcessor.getPipelineTemplate()).thenReturn(new TestTemplateService.MockTemplateScript.Factory(pipelineName));
        name = PipelineProcessor.TYPE;
        when(pipelineProcessor.getType()).thenReturn(name);
        assertThat(IngestService.getProcessorName(pipelineProcessor), equalTo(name + ":" + pipelineName));
        when(pipelineProcessor.getTag()).thenReturn(tag);
        assertThat(IngestService.getProcessorName(pipelineProcessor), equalTo(name + ":" + pipelineName + ":" + tag));
    }

    public void testExecuteWithDrop() {
        Map<String, Processor.Factory> factories = new HashMap<>();
        factories.put("drop", new DropProcessor.Factory());
        factories.put("mock", (processorFactories, tag, description, config) -> new Processor() {
            @Override
            public IngestDocument execute(final IngestDocument ingestDocument) {
                throw new AssertionError("Document should have been dropped but reached this processor");
            }

            @Override
            public String getType() {
                return null;
            }

            @Override
            public String getTag() {
                return null;
            }

            @Override
            public String getDescription() {
                return null;
            }
        });
        IngestService ingestService = createWithProcessors(factories);
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"drop\" : {}}, {\"mock\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        BulkRequest bulkRequest = new BulkRequest();
        final IndexRequest indexRequest1 = new IndexRequest("_index").id("_id1")
            .source(Map.of())
            .setPipeline("_none")
            .setFinalPipeline("_none");
        bulkRequest.add(indexRequest1);

        IndexRequest indexRequest2 = new IndexRequest("_index").id("_id2").source(Map.of()).setPipeline("_id").setFinalPipeline("_none");
        bulkRequest.add(indexRequest2);

        @SuppressWarnings("unchecked")
        final BiConsumer<Integer, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final BiConsumer<Thread, Exception> completionHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final IntConsumer dropHandler = mock(IntConsumer.class);
        ingestService.executeBulkRequest(
            bulkRequest.numberOfActions(),
            bulkRequest.requests(),
            dropHandler,
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            failureHandler,
            completionHandler,
            Names.WRITE
        );
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(Thread.currentThread(), null);
        verify(dropHandler, times(1)).accept(1);
    }

    public void testIngestClusterStateListeners_orderOfExecution() {
        final AtomicInteger counter = new AtomicInteger(0);

        // Ingest cluster state listener state should be invoked first:
        Consumer<ClusterState> ingestClusterStateListener = clusterState -> { assertThat(counter.compareAndSet(0, 1), is(true)); };

        // Processor factory should be invoked secondly after ingest cluster state listener:
        IngestPlugin testPlugin = new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
                return Map.of("test", (factories, tag, description, config) -> {
                    assertThat(counter.compareAndSet(1, 2), is(true));
                    return new FakeProcessor("test", tag, description, ingestDocument -> {});
                });
            }
        };

        // Create ingest service:
        Client client = mock(Client.class);
        IngestService ingestService = new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            List.of(testPlugin),
            client,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE
        );
        ingestService.addIngestClusterStateListener(ingestClusterStateListener);

        // Create pipeline and apply the resulting cluster state, which should update the counter in the right order:
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"test\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        // Sanity check that counter has been updated twice:
        assertThat(counter.get(), equalTo(2));
    }

    public void testCBORParsing() throws Exception {
        AtomicReference<Object> reference = new AtomicReference<>();
        Consumer<IngestDocument> executor = doc -> reference.set(doc.getFieldValueAsBytes("data"));
        final IngestService ingestService = createWithProcessors(
            Map.of("foo", (factories, tag, description, config) -> new FakeProcessor("foo", tag, description, executor))
        );

        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        PutPipelineRequest putRequest = new PutPipelineRequest(
            "_id",
            new BytesArray("{\"processors\": [{\"foo\" : {}}]}"),
            XContentType.JSON
        );
        clusterState = executePut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        try (XContentBuilder builder = CborXContent.contentBuilder()) {
            builder.startObject();
            builder.field("data", "This is my data".getBytes(StandardCharsets.UTF_8));
            builder.endObject();

            IndexRequest indexRequest = new IndexRequest("_index").id("_doc-id")
                .source(builder)
                .setPipeline("_id")
                .setFinalPipeline("_none");

            ingestService.executeBulkRequest(
                1,
                List.of(indexRequest),
                indexReq -> {},
                (s) -> false,
                (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
                (integer, e) -> {},
                (thread, e) -> {},
                Names.WRITE
            );
        }

        assertThat(reference.get(), is(instanceOf(byte[].class)));
    }

    public void testSetsRawTimestamp() {
        IngestService ingestService = createWithProcessors(
            Map.of(
                "mock",
                (factories, tag, description, config) -> mockCompoundProcessor(),
                "set",
                (factories, tag, description, config) -> new FakeProcessor(
                    "set",
                    tag,
                    description,
                    // just always set the timestamp field to 100
                    (ingestDocument) -> ingestDocument.setFieldValue(DataStream.TIMESTAMP_FIELD_NAME, 100)
                )
            )
        );

        PutPipelineRequest putRequest1 = new PutPipelineRequest(
            "_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"),
            XContentType.JSON
        );
        PutPipelineRequest putRequest2 = new PutPipelineRequest(
            "_id2",
            new BytesArray("{\"processors\": [{\"set\" : {}}]}"),
            XContentType.JSON
        );
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = executePut(putRequest1, clusterState);
        clusterState = executePut(putRequest2, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        // feed a document with no timestamp through four scenarios
        Map<String, Object> doc1 = Map.of("foo", "bar");

        // neither a request nor a final pipeline
        IndexRequest indexRequest1 = new IndexRequest("idx").setPipeline("_none").setFinalPipeline("_none").source(doc1);
        // just a request pipeline
        IndexRequest indexRequest2 = new IndexRequest("idx").setPipeline("_id1").setFinalPipeline("_none").source(doc1);
        // just a final pipeline
        IndexRequest indexRequest3 = new IndexRequest("idx").setPipeline("_none").setFinalPipeline("_id2").source(doc1);
        // both a request and a final pipeline
        IndexRequest indexRequest4 = new IndexRequest("idx").setPipeline("_id1").setFinalPipeline("_id2").source(doc1);

        // feed a document with a timestamp through four scenarios
        Map<String, Object> doc2 = Map.of(DataStream.TIMESTAMP_FIELD_NAME, 10);

        // neither a request nor a final pipeline
        IndexRequest indexRequest5 = new IndexRequest("idx").setPipeline("_none").setFinalPipeline("_none").source(doc2);
        // just a request pipeline
        IndexRequest indexRequest6 = new IndexRequest("idx").setPipeline("_id1").setFinalPipeline("_none").source(doc2);
        // just a final pipeline
        IndexRequest indexRequest7 = new IndexRequest("idx").setPipeline("_none").setFinalPipeline("_id2").source(doc2);
        // both a request and a final pipeline
        IndexRequest indexRequest8 = new IndexRequest("idx").setPipeline("_id1").setFinalPipeline("_id2").source(doc2);

        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(indexRequest1);
        bulkRequest.add(indexRequest2);
        bulkRequest.add(indexRequest3);
        bulkRequest.add(indexRequest4);
        bulkRequest.add(indexRequest5);
        bulkRequest.add(indexRequest6);
        bulkRequest.add(indexRequest7);
        bulkRequest.add(indexRequest8);
        ingestService.executeBulkRequest(
            8,
            bulkRequest.requests(),
            indexReq -> {},
            (s) -> false,
            (slot, targetIndex, e) -> fail("Should not be redirecting failures"),
            (integer, e) -> {},
            (thread, e) -> {},
            Names.WRITE
        );

        assertThat(indexRequest1.getRawTimestamp(), nullValue());
        assertThat(indexRequest2.getRawTimestamp(), nullValue());
        assertThat(indexRequest3.getRawTimestamp(), equalTo(100));
        assertThat(indexRequest4.getRawTimestamp(), equalTo(100));
        assertThat(indexRequest5.getRawTimestamp(), nullValue());
        assertThat(indexRequest6.getRawTimestamp(), equalTo(10));
        assertThat(indexRequest7.getRawTimestamp(), equalTo(100));
        assertThat(indexRequest8.getRawTimestamp(), equalTo(100));
    }

    public void testUpdateIndexRequestWithPipelinesShouldShortCircuitExecution() {
        var mockedRequest = mock(DocWriteRequest.class);
        var mockedIndexRequest = mock(IndexRequest.class);
        var mockedMetadata = mock(Metadata.class);

        when(mockedIndexRequest.isPipelineResolved()).thenReturn(true);

        IngestService.resolvePipelinesAndUpdateIndexRequest(mockedRequest, mockedIndexRequest, mockedMetadata);

        verify(mockedIndexRequest, times(1)).isPipelineResolved();
        verifyNoMoreInteractions(mockedIndexRequest);
        verifyNoInteractions(mockedRequest, mockedMetadata);
    }

    public void testHasPipeline() {
        var indexRequest = new IndexRequest("idx").isPipelineResolved(true);

        indexRequest.setPipeline(NOOP_PIPELINE_NAME).setFinalPipeline(NOOP_PIPELINE_NAME);
        assertFalse(hasPipeline(indexRequest));

        indexRequest.setPipeline("some-pipeline").setFinalPipeline(NOOP_PIPELINE_NAME);
        assertTrue(hasPipeline(indexRequest));

        indexRequest.setPipeline(NOOP_PIPELINE_NAME).setFinalPipeline("some-final-pipeline");
        assertTrue(hasPipeline(indexRequest));

        indexRequest.setPipeline("some-pipeline").setFinalPipeline("some-final-pipeline");
        assertTrue(hasPipeline(indexRequest));
    }

    public void testResolveRequiredOrDefaultPipelineDefaultPipeline() {
        IndexMetadata.Builder builder = IndexMetadata.builder("idx")
            .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias").writeIndex(true).build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // index name matches with IDM:
        IndexRequest indexRequest = new IndexRequest("idx");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("default-pipeline"));

        // alias name matches with IDM:
        indexRequest = new IndexRequest("alias");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("default-pipeline"));

        // index name matches with ITMD:
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("name1")
            .patterns(List.of("id*"))
            .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"));
        metadata = Metadata.builder().put(templateBuilder).build();
        indexRequest = new IndexRequest("idx");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("default-pipeline"));
    }

    public void testResolveFinalPipeline() {
        IndexMetadata.Builder builder = IndexMetadata.builder("idx")
            .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putAlias(AliasMetadata.builder("alias").writeIndex(true).build());
        Metadata metadata = Metadata.builder().put(builder).build();

        // index name matches with IDM:
        IndexRequest indexRequest = new IndexRequest("idx");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));

        // alias name matches with IDM:
        indexRequest = new IndexRequest("alias");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));

        // index name matches with ITMD:
        IndexTemplateMetadata.Builder templateBuilder = IndexTemplateMetadata.builder("name1")
            .patterns(List.of("id*"))
            .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"));
        metadata = Metadata.builder().put(templateBuilder).build();
        indexRequest = new IndexRequest("idx");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));
    }

    public void testResolveFinalPipelineWithDateMathExpression() {
        final long epochMillis = randomLongBetween(1, System.currentTimeMillis());
        final DateFormatter dateFormatter = DateFormatter.forPattern("uuuu.MM.dd");
        IndexMetadata.Builder builder = IndexMetadata.builder("idx-" + dateFormatter.formatMillis(epochMillis))
            .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"))
            .numberOfShards(1)
            .numberOfReplicas(0);
        Metadata metadata = Metadata.builder().put(builder).build();

        // index name matches with IDM:
        IndexRequest indexRequest = new IndexRequest("<idx-{now/d}>");
        IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata, epochMillis);
        assertTrue(hasPipeline(indexRequest));
        assertTrue(indexRequest.isPipelineResolved());
        assertThat(indexRequest.getPipeline(), equalTo("_none"));
        assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));
    }

    public void testResolveRequestOrDefaultPipelineAndFinalPipeline() {
        // no pipeline:
        {
            Metadata metadata = Metadata.builder().build();
            IndexRequest indexRequest = new IndexRequest("idx");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertFalse(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo(NOOP_PIPELINE_NAME));
        }

        // request pipeline:
        {
            Metadata metadata = Metadata.builder().build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("request-pipeline");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertTrue(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo("request-pipeline"));
        }

        // request pipeline with default pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("request-pipeline");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertTrue(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo("request-pipeline"));
        }

        // request pipeline with final pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("request-pipeline");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertTrue(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo("request-pipeline"));
            assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));
        }
    }

    public void testUpdatingRandomPipelineWithoutChangesIsNoOp() throws Exception {
        var randomMap = randomMap(10, 50, IngestServiceTests::randomMapEntry);

        XContentBuilder x = XContentBuilder.builder(XContentType.JSON.xContent()).startObject().field("processors", randomMap).endObject();

        OutputStream os = x.getOutputStream();
        x.generator().close();
        testUpdatingPipeline(os.toString());
    }

    public void testUpdatingPipelineWithoutChangesIsNoOp() throws Exception {
        var value = randomAlphaOfLength(5);
        var pipelineString = Strings.format("""
            {"processors": [{"set" : {"field": "_field", "value": "%s"}}]}
            """, value);
        testUpdatingPipeline(pipelineString);
    }

    private void testUpdatingPipeline(String pipelineString) throws Exception {
        var pipelineId = randomAlphaOfLength(5);
        var existingPipeline = new PipelineConfiguration(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, new IngestMetadata(Map.of(pipelineId, existingPipeline))).build())
            .build();

        Client client = mock(Client.class);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(clusterState);
        IngestService ingestService = new IngestService(
            clusterService,
            threadPool,
            null,
            null,
            null,
            List.of(DUMMY_PLUGIN),
            client,
            null,
            DocumentParsingProvider.EMPTY_INSTANCE
        );
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, clusterState));

        CountDownLatch latch = new CountDownLatch(1);
        var listener = new ActionListener<AcknowledgedResponse>() {
            final AtomicLong successCount = new AtomicLong(0);
            final AtomicLong failureCount = new AtomicLong(0);

            @Override
            public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                successCount.incrementAndGet();
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                failureCount.incrementAndGet();
                latch.countDown();
            }

            public long getSuccessCount() {
                return successCount.get();
            }

            public long getFailureCount() {
                return failureCount.get();
            }
        };

        var consumer = new Consumer<ActionListener<NodesInfoResponse>>() {
            final AtomicLong executionCount = new AtomicLong(0);

            @Override
            public void accept(ActionListener<NodesInfoResponse> nodesInfoResponseActionListener) {
                executionCount.incrementAndGet();
            }

            public long getExecutionCount() {
                return executionCount.get();
            }
        };

        var request = new PutPipelineRequest(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        ingestService.putPipeline(request, listener, consumer);
        latch.await();

        assertThat(consumer.getExecutionCount(), equalTo(0L));
        assertThat(listener.getSuccessCount(), equalTo(1L));
        assertThat(listener.getFailureCount(), equalTo(0L));
    }

    public void testPutPipelineWithVersionedUpdateWithoutExistingPipeline() {
        var pipelineId = randomAlphaOfLength(5);
        var clusterState = ClusterState.EMPTY_STATE;

        final Integer version = randomInt();
        var pipelineString = "{\"version\": " + version + ", \"processors\": []}";
        var request = new PutPipelineRequest(pipelineId, new BytesArray(pipelineString), XContentType.JSON, version);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> executeFailingPut(request, clusterState));
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "version conflict, required version [%s] for pipeline [%s] but no pipeline was found",
                    version,
                    pipelineId
                )
            )
        );
    }

    public void testPutPipelineWithVersionedUpdateDoesNotMatchExistingPipeline() {
        var pipelineId = randomAlphaOfLength(5);
        final Integer version = randomInt();
        var pipelineString = "{\"version\": " + version + ", \"processors\": []}";
        var existingPipeline = new PipelineConfiguration(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, new IngestMetadata(Map.of(pipelineId, existingPipeline))).build())
            .build();

        final Integer requestedVersion = randomValueOtherThan(version, ESTestCase::randomInt);
        var request = new PutPipelineRequest(pipelineId, new BytesArray(pipelineString), XContentType.JSON, requestedVersion);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> executeFailingPut(request, clusterState));
        assertThat(
            e.getMessage(),
            equalTo(
                String.format(
                    Locale.ROOT,
                    "version conflict, required version [%s] for pipeline [%s] but current version is [%s]",
                    requestedVersion,
                    pipelineId,
                    version
                )
            )
        );
    }

    public void testPutPipelineWithVersionedUpdateSpecifiesSameVersion() throws Exception {
        var pipelineId = randomAlphaOfLength(5);
        final Integer version = randomInt();
        var pipelineString = "{\"version\": " + version + ", \"processors\": []}";
        var existingPipeline = new PipelineConfiguration(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, new IngestMetadata(Map.of(pipelineId, existingPipeline))).build())
            .build();

        var request = new PutPipelineRequest(pipelineId, new BytesArray(pipelineString), XContentType.JSON, version);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> executeFailingPut(request, clusterState));
        assertThat(e.getMessage(), equalTo(Strings.format("cannot update pipeline [%s] with the same version [%s]", pipelineId, version)));
    }

    public void testPutPipelineWithVersionedUpdateSpecifiesValidVersion() throws Exception {
        var pipelineId = randomAlphaOfLength(5);
        final Integer existingVersion = randomInt();
        var pipelineString = "{\"version\": " + existingVersion + ", \"processors\": []}";
        var existingPipeline = new PipelineConfiguration(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, new IngestMetadata(Map.of(pipelineId, existingPipeline))).build())
            .build();

        final int specifiedVersion = randomValueOtherThan(existingVersion, ESTestCase::randomInt);
        var updatedPipelineString = "{\"version\": " + specifiedVersion + ", \"processors\": []}";
        var request = new PutPipelineRequest(pipelineId, new BytesArray(updatedPipelineString), XContentType.JSON, existingVersion);
        var updatedState = executePut(request, clusterState);

        var updatedConfig = ((IngestMetadata) updatedState.metadata().custom(IngestMetadata.TYPE)).getPipelines().get(pipelineId);
        assertThat(updatedConfig, notNullValue());
        assertThat(updatedConfig.getVersion(), equalTo(specifiedVersion));
    }

    public void testPutPipelineWithVersionedUpdateIncrementsVersion() throws Exception {
        var pipelineId = randomAlphaOfLength(5);
        final Integer existingVersion = randomInt();
        var pipelineString = "{\"version\": " + existingVersion + ", \"processors\": []}";
        var existingPipeline = new PipelineConfiguration(pipelineId, new BytesArray(pipelineString), XContentType.JSON);
        var clusterState = ClusterState.builder(new ClusterName("test"))
            .metadata(Metadata.builder().putCustom(IngestMetadata.TYPE, new IngestMetadata(Map.of(pipelineId, existingPipeline))).build())
            .build();

        var updatedPipelineString = "{\"processors\": []}";
        var request = new PutPipelineRequest(pipelineId, new BytesArray(updatedPipelineString), XContentType.JSON, existingVersion);
        var updatedState = executePut(request, clusterState);

        var updatedConfig = ((IngestMetadata) updatedState.metadata().custom(IngestMetadata.TYPE)).getPipelines().get(pipelineId);
        assertThat(updatedConfig, notNullValue());
        assertThat(updatedConfig.getVersion(), equalTo(existingVersion + 1));
    }

    public void testResolvePipelinesWithNonePipeline() {
        // _none request pipeline:
        {
            Metadata metadata = Metadata.builder().build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline(NOOP_PIPELINE_NAME);
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertFalse(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo(NOOP_PIPELINE_NAME));
        }

        // _none default pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), NOOP_PIPELINE_NAME))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertFalse(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo(NOOP_PIPELINE_NAME));
        }

        // _none default pipeline with request pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), NOOP_PIPELINE_NAME))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("pipeline1");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertTrue(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo("pipeline1"));
        }

        // _none request pipeline with default pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default-pipeline"))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline(NOOP_PIPELINE_NAME);
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertFalse(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo(NOOP_PIPELINE_NAME));
        }

        // _none request pipeline with final pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), "final-pipeline"))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline(NOOP_PIPELINE_NAME);
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertTrue(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo(NOOP_PIPELINE_NAME));
            assertThat(indexRequest.getFinalPipeline(), equalTo("final-pipeline"));
        }

        // _none final pipeline:
        {
            IndexMetadata.Builder builder = IndexMetadata.builder("idx")
                .settings(settings(IndexVersion.current()).put(IndexSettings.FINAL_PIPELINE.getKey(), NOOP_PIPELINE_NAME))
                .numberOfShards(1)
                .numberOfReplicas(0);
            Metadata metadata = Metadata.builder().put(builder).build();
            IndexRequest indexRequest = new IndexRequest("idx").setPipeline("pipeline1");
            IngestService.resolvePipelinesAndUpdateIndexRequest(indexRequest, indexRequest, metadata);
            assertTrue(hasPipeline(indexRequest));
            assertTrue(indexRequest.isPipelineResolved());
            assertThat(indexRequest.getPipeline(), equalTo("pipeline1"));
            assertThat(indexRequest.getFinalPipeline(), equalTo(NOOP_PIPELINE_NAME));
        }
    }

    private static Tuple<String, Object> randomMapEntry() {
        return tuple(randomAlphaOfLength(5), randomObject());
    }

    private static Object randomObject() {
        return randomFrom(
            random(),
            ESTestCase::randomLong,
            () -> generateRandomStringArray(10, 5, true),
            () -> randomMap(3, 5, IngestServiceTests::randomMapEntry),
            () -> randomAlphaOfLength(5),
            ESTestCase::randomTimeValue,
            ESTestCase::randomDouble
        );
    }

    private IngestDocument eqIndexTypeId(final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", -3L, VersionType.INTERNAL, source));
    }

    private IngestDocument eqIndexTypeId(final long version, final VersionType versionType, final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", version, versionType, source));
    }

    private static IngestService createWithProcessors() {
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("set", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");
            return new FakeProcessor("set", tag, description, (ingestDocument) -> ingestDocument.setFieldValue(field, value));
        });
        processors.put("remove", (factories, tag, description, config) -> {
            String field = (String) config.remove("field");
            return new WrappingProcessorImpl("remove", tag, description, (ingestDocument -> ingestDocument.removeField(field))) {
            };
        });
        return createWithProcessors(processors);
    }

    private static IngestService createWithProcessors(Map<String, Processor.Factory> processors) {
        return createWithProcessors(processors, DocumentParsingProvider.EMPTY_INSTANCE);
    }

    private static IngestService createWithProcessors(
        Map<String, Processor.Factory> processors,
        DocumentParsingProvider documentParsingProvider
    ) {
        Client client = mock(Client.class);
        ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.generic()).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        IngestService ingestService = new IngestService(
            mock(ClusterService.class),
            threadPool,
            null,
            null,
            null,
            List.of(new IngestPlugin() {
                @Override
                public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
                    return processors;
                }
            }),
            client,
            null,
            documentParsingProvider
        );
        if (randomBoolean()) {
            /*
             * Testing the copy constructor directly is difficult because there is no equals() method in IngestService, but there is a lot
             * of private internal state. Here we use the copy constructor half the time in all of the unit tests, with the assumption that
             * if none of our tests observe any difference then the copy constructor is working as expected.
             */
            return new IngestService(ingestService);
        } else {
            return ingestService;
        }
    }

    private CompoundProcessor mockCompoundProcessor() {
        CompoundProcessor processor = mock(CompoundProcessor.class);
        doAnswer(args -> true).when(processor).isAsync();
        doAnswer(args -> {
            @SuppressWarnings("unchecked")
            BiConsumer<IngestDocument, Exception> handler = (BiConsumer) args.getArguments()[1];
            handler.accept((IngestDocument) args.getArguments()[0], null);
            return null;
        }).when(processor).execute(any(), any());
        return processor;
    }

    private class IngestDocumentMatcher implements ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        IngestDocumentMatcher(String index, String type, String id, long version, VersionType versionType, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, id, version, null, versionType, source);
        }

        @Override
        public boolean matches(IngestDocument other) {
            // ingest metadata and IngestCtxMap will not be the same (timestamp differs every time)
            return Objects.equals(ingestDocument.getSource(), other.getSource())
                && Objects.equals(ingestDocument.getMetadata().getMap(), other.getMetadata().getMap());
        }
    }

    private void assertProcessorStats(int processor, IngestStats stats, String pipelineId, long count, long failed, long time) {
        assertStats(stats.processorStats().get(pipelineId).get(processor).stats(), count, failed, time);
    }

    private void assertPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String pipelineId, long count, long failed, long time) {
        assertStats(getPipelineStats(pipelineStats, pipelineId), count, failed, time);
    }

    private void assertStats(IngestStats.Stats stats, long count, long failed, long time) {
        assertThat(stats.ingestCount(), equalTo(count));
        assertThat(stats.ingestCurrent(), equalTo(0L));
        assertThat(stats.ingestFailedCount(), equalTo(failed));
        assertThat(stats.ingestTimeInMillis(), greaterThanOrEqualTo(time));
    }

    private IngestStats.Stats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream().filter(p1 -> p1.pipelineId().equals(id)).findFirst().map(p2 -> p2.stats()).orElse(null);
    }

    private static List<IngestService.PipelineClusterStateUpdateTask> oneTask(DeletePipelineRequest request) {
        return List.of(new IngestService.DeletePipelineClusterStateUpdateTask(ActionTestUtils.assertNoFailureListener(t -> {}), request));
    }

    private static ClusterState executeDelete(DeletePipelineRequest request, ClusterState clusterState) {
        try {
            return executeAndAssertSuccessful(clusterState, IngestService.PIPELINE_TASK_EXECUTOR, oneTask(request));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void executeFailingDelete(DeletePipelineRequest request, ClusterState clusterState) throws Exception {
        ClusterStateTaskExecutorUtils.executeAndThrowFirstFailure(clusterState, IngestService.PIPELINE_TASK_EXECUTOR, oneTask(request));
    }

    private static List<IngestService.PipelineClusterStateUpdateTask> oneTask(PutPipelineRequest request) {
        return List.of(new IngestService.PutPipelineClusterStateUpdateTask(ActionTestUtils.assertNoFailureListener(t -> {}), request));
    }

    private static ClusterState executePut(PutPipelineRequest request, ClusterState clusterState) {
        try {
            return executeAndAssertSuccessful(clusterState, IngestService.PIPELINE_TASK_EXECUTOR, oneTask(request));
        } catch (Exception e) {
            throw new AssertionError(e);
        }
    }

    private static void executeFailingPut(PutPipelineRequest request, ClusterState clusterState) throws Exception {
        ClusterStateTaskExecutorUtils.executeAndThrowFirstFailure(clusterState, IngestService.PIPELINE_TASK_EXECUTOR, oneTask(request));
    }
}
