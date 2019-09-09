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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.TransportBulkAction;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.logging.Loggers;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.script.MockScriptEngine;
import org.elasticsearch.script.Script;
import org.elasticsearch.script.ScriptModule;
import org.elasticsearch.script.ScriptService;
import org.elasticsearch.script.ScriptType;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockLogAppender;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.CustomTypeSafeMatcher;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyString;
import static org.mockito.Matchers.argThat;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class IngestServiceTests extends ESTestCase {

    private static final IngestPlugin DUMMY_PLUGIN = new IngestPlugin() {
        @Override
        public Map<String, Processor.Factory> getProcessors(Processor.Parameters parameters) {
            return Collections.singletonMap("foo", (factories, tag, config) -> null);
        }
    };

    public void testIngestPlugin() {
        ThreadPool tp = mock(ThreadPool.class);
        Client client = mock(Client.class);
        IngestService ingestService = new IngestService(mock(ClusterService.class), tp, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN), client);
        Map<String, Processor.Factory> factories = ingestService.getProcessorFactories();
        assertTrue(factories.containsKey("foo"));
        assertEquals(1, factories.size());
    }

    public void testIngestPluginDuplicate() {
        ThreadPool tp = mock(ThreadPool.class);
        Client client = mock(Client.class);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new IngestService(mock(ClusterService.class), tp, null, null,
            null, Arrays.asList(DUMMY_PLUGIN, DUMMY_PLUGIN), client));
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }

    public void testExecuteIndexPipelineDoesNotExist() {
        ThreadPool threadPool = mock(ThreadPool.class);
        Client client = mock(Client.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        IngestService ingestService = new IngestService(mock(ClusterService.class), threadPool, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN), client);
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");

        final SetOnce<Boolean> failure = new SetOnce<>();
        final BiConsumer<IndexRequest, Exception> failureHandler = (request, e) -> {
            failure.set(true);
            assertThat(request, sameInstance(indexRequest));
            assertThat(e, instanceOf(IllegalArgumentException.class));
            assertThat(e.getMessage(), equalTo("pipeline with id [_id] does not exist"));
        };

        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);

        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", pipeline));
        clusterState = ClusterState.builder(clusterState)
            .metaData(MetaData.builder().putCustom(IngestMetadata.TYPE, ingestMetadata))
            .build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.pipelines().size(), is(1));
        assertThat(ingestService.pipelines().get("_id").pipeline.getId(), equalTo("_id"));
        assertThat(ingestService.pipelines().get("_id").pipeline.getDescription(), nullValue());
        assertThat(ingestService.pipelines().get("_id").pipeline.getProcessors().size(), equalTo(1));
        assertThat(ingestService.pipelines().get("_id").pipeline.getProcessors().get(0).getType(), equalTo("set"));
    }

    public void testInnerUpdatePipelines() {
        IngestService ingestService = createWithProcessors();
        assertThat(ingestService.pipelines().size(), is(0));

        PipelineConfiguration pipeline1 = new PipelineConfiguration("_id1", new BytesArray("{\"processors\": []}"), XContentType.JSON);
        IngestMetadata ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(1));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));

        PipelineConfiguration pipeline2 = new PipelineConfiguration("_id2", new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id2", pipeline2));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getId(), equalTo("_id2"));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getProcessors().size(), equalTo(0));

        PipelineConfiguration pipeline3 = new PipelineConfiguration("_id3", new BytesArray("{\"processors\": []}"), XContentType.JSON);
        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id2", pipeline2, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(3));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getId(), equalTo("_id2"));
        assertThat(ingestService.pipelines().get("_id2").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getId(), equalTo("_id3"));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().size(), equalTo(0));

        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getId(), equalTo("_id3"));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().size(), equalTo(0));

        pipeline3 = new PipelineConfiguration(
            "_id3",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        ingestMetadata = new IngestMetadata(Map.of("_id1", pipeline1, "_id3", pipeline3));

        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines().size(), is(2));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getId(), equalTo("_id1"));
        assertThat(ingestService.pipelines().get("_id1").pipeline.getProcessors().size(), equalTo(0));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getId(), equalTo("_id3"));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().size(), equalTo(1));
        assertThat(ingestService.pipelines().get("_id3").pipeline.getProcessors().get(0).getType(), equalTo("set"));

        // Perform an update with no changes:
        Map<String, IngestService.PipelineHolder> pipelines = ingestService.pipelines();
        ingestService.innerUpdatePipelines(ingestMetadata);
        assertThat(ingestService.pipelines(), sameInstance(pipelines));
    }

    public void testDelete() {
        IngestService ingestService = createWithProcessors();
        PipelineConfiguration config = new PipelineConfiguration(
            "_id",new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON
        );
        IngestMetadata ingestMetadata = new IngestMetadata(Collections.singletonMap("_id", config));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), notNullValue());

        // Delete pipeline:
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("_id");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("_id"), nullValue());

        // Delete existing pipeline:
        try {
            IngestService.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [_id] is missing"));
        }
    }

    public void testValidateNoIngestInfo() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        Exception e = expectThrows(IllegalStateException.class, () -> ingestService.validatePipeline(emptyMap(), putRequest));
        assertEquals("Ingest info is empty", e.getMessage());

        DiscoveryNode discoveryNode = new DiscoveryNode("_node_id", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        IngestInfo ingestInfo = new IngestInfo(Collections.singletonList(new ProcessorInfo("set")));
        ingestService.validatePipeline(Collections.singletonMap(discoveryNode, ingestInfo), putRequest);
    }

    public void testGetProcessorsInPipeline() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}}," +
                "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"),
            XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());

        assertThat(ingestService.getProcessorsInPipeline(id, Processor.class).size(), equalTo(3));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessorImpl.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, WrappingProcessor.class).size(), equalTo(1));
        assertThat(ingestService.getProcessorsInPipeline(id, FakeProcessor.class).size(), equalTo(2));

        assertThat(ingestService.getProcessorsInPipeline(id, ConditionalProcessor.class).size(), equalTo(0));

        IllegalArgumentException e = expectThrows(IllegalArgumentException.class,
            () -> ingestService.getProcessorsInPipeline("fakeID", Processor.class));
        assertThat("pipeline with id [fakeID] does not exist", equalTo(e.getMessage()));
    }

    public void testGetProcessorsInPipelineComplexConditional() throws Exception {
        LongSupplier relativeTimeProvider = mock(LongSupplier.class);
        String scriptName = "conditionalScript";
        ScriptService scriptService = new ScriptService(Settings.builder().build(),
            Collections.singletonMap(
                Script.DEFAULT_SCRIPT_LANG,
                new MockScriptEngine(
                    Script.DEFAULT_SCRIPT_LANG,
                    Collections.singletonMap(
                        scriptName, ctx -> {
                            ctx.get("_type");
                            return true;
                        }
                    ),
                    Collections.emptyMap()
                )
            ),
            new HashMap<>(ScriptModule.CORE_CONTEXTS)
        );

        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("complexSet", (factories, tag, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");

            return new ConditionalProcessor(randomAlphaOfLength(10),
                new Script(
                    ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                    scriptName, Collections.emptyMap()), scriptService,
                new ConditionalProcessor(randomAlphaOfLength(10) + "-nested",
                    new Script(
                        ScriptType.INLINE, Script.DEFAULT_SCRIPT_LANG,
                        scriptName, Collections.emptyMap()), scriptService,
                    new FakeProcessor("complexSet", tag, (ingestDocument) -> ingestDocument.setFieldValue(field, value))));
        });

        IngestService ingestService = createWithProcessors(processors);
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(id,
            new BytesArray("{\"processors\": [{\"complexSet\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
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

        PutPipelineRequest putRequest = new PutPipelineRequest(id,
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("set"));

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest(id);
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
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
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), nullValue());
        assertThat(pipeline.getProcessors().size(), equalTo(0));

        // overwrite existing pipeline:
        putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"processors\": [], \"description\": \"_description\"}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
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

        PutPipelineRequest putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"description\": \"empty processors\"}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        MockLogAppender mockAppender = new MockLogAppender();
        mockAppender.start();
        mockAppender.addExpectation(
            new MockLogAppender.SeenEventExpectation(
                "test1",
                IngestService.class.getCanonicalName(),
                Level.WARN,
                "failed to update ingest pipelines"));
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
        assertThat(pipeline.getDescription(), equalTo("this is a place holder pipeline, because pipeline with" +
            " id [_id] could not be loaded"));
        assertThat(pipeline.getProcessors().size(), equalTo(1));
        assertNull(pipeline.getProcessors().get(0).getTag());
        assertThat(pipeline.getProcessors().get(0).getType(), equalTo("unknown"));
    }

    public void testDeleteUsingWildcard() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"
        );
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        pipelines.put("p2", new PipelineConfiguration("p2", definition, XContentType.JSON));
        pipelines.put("q1", new PipelineConfiguration("q1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());
        assertThat(ingestService.getPipeline("p2"), notNullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Delete pipeline matching wildcard
        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("p*");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(deleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), notNullValue());

        // Exception if we used name which does not exist
        try {
            IngestService.innerDelete(new DeletePipelineRequest("unknown"), clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [unknown] is missing"));
        }

        // match all wildcard works on last remaining pipeline
        DeletePipelineRequest matchAllDeleteRequest = new DeletePipelineRequest("*");
        previousClusterState = clusterState;
        clusterState = IngestService.innerDelete(matchAllDeleteRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), nullValue());
        assertThat(ingestService.getPipeline("p2"), nullValue());
        assertThat(ingestService.getPipeline("q1"), nullValue());

        // match all wildcard does not throw exception if none match
        IngestService.innerDelete(matchAllDeleteRequest, clusterState);
    }

    public void testDeleteWithExistingUnmatchedPipelines() {
        IngestService ingestService = createWithProcessors();
        HashMap<String, PipelineConfiguration> pipelines = new HashMap<>();
        BytesArray definition = new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"
        );
        pipelines.put("p1", new PipelineConfiguration("p1", definition, XContentType.JSON));
        IngestMetadata ingestMetadata = new IngestMetadata(pipelines);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = ClusterState.builder(clusterState).metaData(MetaData.builder()
            .putCustom(IngestMetadata.TYPE, ingestMetadata)).build();
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        assertThat(ingestService.getPipeline("p1"), notNullValue());

        DeletePipelineRequest deleteRequest = new DeletePipelineRequest("z*");
        try {
            IngestService.innerDelete(deleteRequest, clusterState);
            fail("exception expected");
        } catch (ResourceNotFoundException e) {
            assertThat(e.getMessage(), equalTo("pipeline [z*] is missing"));
        }
    }

    public void testGetPipelines() {
        Map<String, PipelineConfiguration> configs = new HashMap<>();
        configs.put("_id1", new PipelineConfiguration(
            "_id1", new BytesArray("{\"processors\": []}"), XContentType.JSON
        ));
        configs.put("_id2", new PipelineConfiguration(
            "_id2", new BytesArray("{\"processors\": []}"), XContentType.JSON
        ));

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

    public void testValidate() throws Exception {
        IngestService ingestService = createWithProcessors();
        PutPipelineRequest putRequest = new PutPipelineRequest("_id", new BytesArray(
            "{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\", \"tag\": \"tag1\"}}," +
                "{\"remove\" : {\"field\": \"_field\", \"tag\": \"tag2\"}}]}"),
            XContentType.JSON);

        DiscoveryNode node1 = new DiscoveryNode("_node_id1", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        DiscoveryNode node2 = new DiscoveryNode("_node_id2", buildNewFakeTransportAddress(),
            emptyMap(), emptySet(), Version.CURRENT);
        Map<DiscoveryNode, IngestInfo> ingestInfos = new HashMap<>();
        ingestInfos.put(node1, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"))));

        ElasticsearchParseException e =
            expectThrows(ElasticsearchParseException.class, () -> ingestService.validatePipeline(ingestInfos, putRequest));
        assertEquals("Processor type [remove] is not installed on node [" + node2 + "]", e.getMessage());
        assertEquals("remove", e.getMetadata("es.processor_type").get(0));
        assertEquals("tag2", e.getMetadata("es.processor_tag").get(0));

        ingestInfos.put(node2, new IngestInfo(Arrays.asList(new ProcessorInfo("set"), new ProcessorInfo("remove"))));
        ingestService.validatePipeline(ingestInfos, putRequest);
    }

    public void testExecuteIndexPipelineExistsButFailedParsing() {
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> new AbstractProcessor("mock") {
                @Override
                public IngestDocument execute(IngestDocument ingestDocument) {
                    throw new IllegalStateException("error");
                }

                @Override
                public String getType() {
                    return null;
                }
            }
        ));
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        String id = "_id";
        PutPipelineRequest putRequest = new PutPipelineRequest(id,
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final SetOnce<Boolean> failure = new SetOnce<>();
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline(id);
        final BiConsumer<IndexRequest, Exception> failureHandler = (request, e) -> {
            assertThat(e.getCause(), instanceOf(IllegalArgumentException.class));
            assertThat(e.getCause().getCause(), instanceOf(IllegalStateException.class));
            assertThat(e.getCause().getCause().getMessage(), equalTo("error"));
            failure.set(true);
        };

        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);

        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});

        assertTrue(failure.get());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteBulkPipelineDoesNotExist() {
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> mock(CompoundProcessor.class)));

        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        BulkRequest bulkRequest = new BulkRequest();

        IndexRequest indexRequest1 = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        bulkRequest.add(indexRequest1);
        IndexRequest indexRequest2 =
            new IndexRequest("_index", "_type", "_id").source(Collections.emptyMap()).setPipeline("does_not_exist");
        bulkRequest.add(indexRequest2);
        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(bulkRequest.requests(), failureHandler, completionHandler, indexReq -> {});
        verify(failureHandler, times(1)).accept(
            argThat(new CustomTypeSafeMatcher<IndexRequest>("failure handler was not called with the expected arguments") {
                @Override
                protected boolean matchesSafely(IndexRequest item) {
                    return item == indexRequest2;
                }

            }),
            argThat(new CustomTypeSafeMatcher<IllegalArgumentException>("failure handler was not called with the expected arguments") {
                @Override
                protected boolean matchesSafely(IllegalArgumentException iae) {
                    return "pipeline with id [does_not_exist] does not exist".equals(iae.getMessage());
                }
            })
        );
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteSuccess() {
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> mock(CompoundProcessor.class)));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteEmptyPipeline() throws Exception {
        IngestService ingestService = createWithProcessors(emptyMap());
        PutPipelineRequest putRequest =
            new PutPipelineRequest("_id", new BytesArray("{\"processors\": [], \"description\": \"_description\"}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecutePropagateAllMetaDataUpdates() throws Exception {
        final CompoundProcessor processor = mock(CompoundProcessor.class);
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> processor));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final long newVersion = randomLong();
        final String versionType = randomFrom("internal", "external", "external_gt", "external_gte");
        doAnswer((InvocationOnMock invocationOnMock) -> {
            IngestDocument ingestDocument = (IngestDocument) invocationOnMock.getArguments()[0];
            for (IngestDocument.MetaData metaData : IngestDocument.MetaData.values()) {
                if (metaData == IngestDocument.MetaData.VERSION) {
                    ingestDocument.setFieldValue(metaData.getFieldName(), newVersion);
                } else if (metaData == IngestDocument.MetaData.VERSION_TYPE) {
                    ingestDocument.setFieldValue(metaData.getFieldName(), versionType);
                } else {
                    ingestDocument.setFieldValue(metaData.getFieldName(), "update" + metaData.getFieldName());
                }
            }
            return ingestDocument;
        }).when(processor).execute(any());
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        verify(processor).execute(any());
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
        assertThat(indexRequest.index(), equalTo("update_index"));
        assertThat(indexRequest.type(), equalTo("update_type"));
        assertThat(indexRequest.id(), equalTo("update_id"));
        assertThat(indexRequest.routing(), equalTo("update_routing"));
        assertThat(indexRequest.version(), equalTo(newVersion));
        assertThat(indexRequest.versionType(), equalTo(VersionType.fromString(versionType)));
    }

    public void testExecuteFailure() throws Exception {
        final CompoundProcessor processor = mock(CompoundProcessor.class);
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> processor));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException())
            .when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        verify(failureHandler, times(1)).accept(eq(indexRequest), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteSuccessWithOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock_processor_type");
        when(processor.getTag()).thenReturn("mock_processor_tag");
        final Processor onFailureProcessor = mock(Processor.class);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false, Collections.singletonList(processor), Collections.singletonList(new CompoundProcessor(onFailureProcessor)));
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> compoundProcessor));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(processor).execute(eqIndexTypeId(emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        verify(failureHandler, never()).accept(eq(indexRequest), any(ElasticsearchException.class));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteFailureWithNestedOnFailure() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor onFailureProcessor = mock(Processor.class);
        final Processor onFailureOnFailureProcessor = mock(Processor.class);
        final List<Processor> processors = Collections.singletonList(onFailureProcessor);
        final List<Processor> onFailureProcessors = Collections.singletonList(onFailureOnFailureProcessor);
        final CompoundProcessor compoundProcessor = new CompoundProcessor(
            false,
            Collections.singletonList(processor),
            Collections.singletonList(new CompoundProcessor(false, processors, onFailureProcessors)));
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> compoundProcessor));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException())
            .when(onFailureOnFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        doThrow(new RuntimeException())
            .when(onFailureProcessor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        doThrow(new RuntimeException())
            .when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        verify(processor).execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        verify(failureHandler, times(1)).accept(eq(indexRequest), any(RuntimeException.class));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testBulkRequestExecutionWithFailures() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numRequest = scaledRandomIntBetween(8, 64);
        int numIndexRequests = 0;
        for (int i = 0; i < numRequest; i++) {
            DocWriteRequest request;
            if (randomBoolean()) {
                if (randomBoolean()) {
                    request = new DeleteRequest("_index", "_type", "_id");
                } else {
                    request = new UpdateRequest("_index", "_type", "_id");
                }
            } else {
                IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline(pipelineId);
                indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
                request = indexRequest;
                numIndexRequests++;
            }
            bulkRequest.add(request);
        }

        CompoundProcessor processor = mock(CompoundProcessor.class);
        when(processor.getProcessors()).thenReturn(Collections.singletonList(mock(Processor.class)));
        Exception error = new RuntimeException();
        doThrow(error).when(processor).execute(any());
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> processor));
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(bulkRequest.requests(), requestItemErrorHandler, completionHandler, indexReq -> {});

        verify(requestItemErrorHandler, times(numIndexRequests)).accept(any(IndexRequest.class), argThat(new ArgumentMatcher<Exception>() {
            @Override
            public boolean matches(final Object o) {
                return ((Exception)o).getCause().getCause().equals(error);
            }
        }));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testBulkRequestExecution() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        // Test to make sure that ingest respects content types other than the default index content type
        XContentType xContentType = randomFrom(Arrays.stream(XContentType.values())
                .filter(t -> Requests.INDEX_CONTENT_TYPE.equals(t) == false)
                .collect(Collectors.toList()));

        logger.info("Using [{}], not randomly determined default [{}]", xContentType, Requests.INDEX_CONTENT_TYPE);
        int numRequest = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline(pipelineId);
            indexRequest.source(xContentType, "field1", "value1");
            bulkRequest.add(indexRequest);
        }

        final Processor processor = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        when(processor.execute(any(IngestDocument.class))).thenReturn( RandomDocumentPicks.randomIngestDocument(random()));
        Map<String, Processor.Factory> map = new HashMap<>(2);
        map.put("mock", (factories, tag, config) -> processor);

        IngestService ingestService = createWithProcessors(map);
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"mock\": {}}], \"description\": \"_description\"}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(bulkRequest.requests(), requestItemErrorHandler, completionHandler, indexReq -> {});

        verify(requestItemErrorHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
        for (DocWriteRequest<?> docWriteRequest : bulkRequest.requests()) {
            IndexRequest indexRequest = TransportBulkAction.getIndexWriteRequest(docWriteRequest);
            assertThat(indexRequest, notNullValue());
            assertThat(indexRequest.getContentType(), equalTo(xContentType));
        }
    }

    public void testStats() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor processorFailure = mock(Processor.class);
        when(processor.getType()).thenReturn("mock");
        when(processor.getTag()).thenReturn("mockTag");
        when(processorFailure.getType()).thenReturn("failure-mock");
        //avoid returning null and dropping the document
        when(processor.execute(any(IngestDocument.class))).thenReturn( RandomDocumentPicks.randomIngestDocument(random()));
        when(processorFailure.execute(any(IngestDocument.class))).thenThrow(new RuntimeException("error"));
        Map<String, Processor.Factory> map = new HashMap<>(2);
        map.put("mock", (factories, tag, config) -> processor);
        map.put("failure-mock", (factories, tag, config) -> processorFailure);
        IngestService ingestService = createWithProcessors(map);

        final IngestStats initialStats = ingestService.stats();
        assertThat(initialStats.getPipelineStats().size(), equalTo(0));
        assertStats(initialStats.getTotalStats(), 0, 0, 0);

        PutPipelineRequest putRequest = new PutPipelineRequest("_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        putRequest = new PutPipelineRequest("_id2",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked") final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked") final Consumer<Exception> completionHandler = mock(Consumer.class);

        final IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1");
        indexRequest.source(randomAlphaOfLength(10), randomAlphaOfLength(10));
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        final IngestStats afterFirstRequestStats = ingestService.stats();
        assertThat(afterFirstRequestStats.getPipelineStats().size(), equalTo(2));

        afterFirstRequestStats.getProcessorStats().get("_id1").forEach(p -> assertEquals(p.getName(), "mock:mockTag"));
        afterFirstRequestStats.getProcessorStats().get("_id2").forEach(p -> assertEquals(p.getName(), "mock:mockTag"));

        //total
        assertStats(afterFirstRequestStats.getTotalStats(), 1, 0 ,0);
        //pipeline
        assertPipelineStats(afterFirstRequestStats.getPipelineStats(), "_id1", 1, 0, 0);
        assertPipelineStats(afterFirstRequestStats.getPipelineStats(), "_id2", 0, 0, 0);
        //processor
        assertProcessorStats(0, afterFirstRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterFirstRequestStats, "_id2", 0, 0, 0);


        indexRequest.setPipeline("_id2");
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        final IngestStats afterSecondRequestStats = ingestService.stats();
        assertThat(afterSecondRequestStats.getPipelineStats().size(), equalTo(2));
        //total
        assertStats(afterSecondRequestStats.getTotalStats(), 2, 0 ,0);
        //pipeline
        assertPipelineStats(afterSecondRequestStats.getPipelineStats(), "_id1", 1, 0, 0);
        assertPipelineStats(afterSecondRequestStats.getPipelineStats(), "_id2", 1, 0, 0);
        //processor
        assertProcessorStats(0, afterSecondRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterSecondRequestStats, "_id2", 1, 0, 0);

        //update cluster state and ensure that new stats are added to old stats
        putRequest = new PutPipelineRequest("_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}, {\"mock\" : {}}]}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        final IngestStats afterThirdRequestStats = ingestService.stats();
        assertThat(afterThirdRequestStats.getPipelineStats().size(), equalTo(2));
        //total
        assertStats(afterThirdRequestStats.getTotalStats(), 3, 0 ,0);
        //pipeline
        assertPipelineStats(afterThirdRequestStats.getPipelineStats(), "_id1", 2, 0, 0);
        assertPipelineStats(afterThirdRequestStats.getPipelineStats(), "_id2", 1, 0, 0);
        //The number of processors for the "id1" pipeline changed, so the per-processor metrics are not carried forward. This is
        //due to the parallel array's used to identify which metrics to carry forward. With out unique ids or semantic equals for each
        //processor, parallel arrays are the best option for of carrying forward metrics between pipeline changes. However, in some cases,
        //like this one it may not readily obvious why the metrics were not carried forward.
        assertProcessorStats(0, afterThirdRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(1, afterThirdRequestStats, "_id1", 1, 0, 0);
        assertProcessorStats(0, afterThirdRequestStats, "_id2", 1, 0, 0);

        //test a failure, and that the processor stats are added from the old stats
        putRequest = new PutPipelineRequest("_id1",
            new BytesArray("{\"processors\": [{\"failure-mock\" : { \"on_failure\": [{\"mock\" : {}}]}}, {\"mock\" : {}}]}"),
            XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, indexReq -> {});
        final IngestStats afterForthRequestStats = ingestService.stats();
        assertThat(afterForthRequestStats.getPipelineStats().size(), equalTo(2));
        //total
        assertStats(afterForthRequestStats.getTotalStats(), 4, 0 ,0);
        //pipeline
        assertPipelineStats(afterForthRequestStats.getPipelineStats(), "_id1", 3, 0, 0);
        assertPipelineStats(afterForthRequestStats.getPipelineStats(), "_id2", 1, 0, 0);
        //processor
        assertProcessorStats(0, afterForthRequestStats, "_id1", 1, 1, 0); //not carried forward since type changed
        assertProcessorStats(1, afterForthRequestStats, "_id1", 2, 0, 0); //carried forward and added from old stats
        assertProcessorStats(0, afterForthRequestStats, "_id2", 1, 0, 0);
    }

    public void testStatName(){
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
        when(pipelineProcessor.getPipelineName()).thenReturn(pipelineName);
        name = PipelineProcessor.TYPE;
        when(pipelineProcessor.getType()).thenReturn(name);
        assertThat(IngestService.getProcessorName(pipelineProcessor), equalTo(name + ":" + pipelineName));
        when(pipelineProcessor.getTag()).thenReturn(tag);
        assertThat(IngestService.getProcessorName(pipelineProcessor), equalTo(name + ":" + pipelineName + ":" + tag));
    }


    public void testExecuteWithDrop() {
        Map<String, Processor.Factory> factories = new HashMap<>();
        factories.put("drop", new DropProcessor.Factory());
        factories.put("mock", (processorFactories, tag, config) -> new Processor() {
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
        });
        IngestService ingestService = createWithProcessors(factories);
        PutPipelineRequest putRequest = new PutPipelineRequest("_id",
            new BytesArray("{\"processors\": [{\"drop\" : {}}, {\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<IndexRequest> dropHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler, dropHandler);
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
        verify(dropHandler, times(1)).accept(indexRequest);
    }

    private IngestDocument eqIndexTypeId(final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", source));
    }

    private IngestDocument eqIndexTypeId(final Long version, final VersionType versionType, final Map<String, Object> source) {
        return argThat(new IngestDocumentMatcher("_index", "_type", "_id", version, versionType, source));
    }

    private static IngestService createWithProcessors() {
        Map<String, Processor.Factory> processors = new HashMap<>();
        processors.put("set", (factories, tag, config) -> {
            String field = (String) config.remove("field");
            String value = (String) config.remove("value");
            return new FakeProcessor("set", tag, (ingestDocument) ->ingestDocument.setFieldValue(field, value));
        });
        processors.put("remove", (factories, tag, config) -> {
            String field = (String) config.remove("field");
            return new WrappingProcessorImpl("remove", tag, (ingestDocument -> ingestDocument.removeField(field))) {
            };
        });
        return createWithProcessors(processors);
    }

    private static IngestService createWithProcessors(Map<String, Processor.Factory> processors) {
        ThreadPool threadPool = mock(ThreadPool.class);
        Client client = mock(Client.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        return new IngestService(mock(ClusterService.class), threadPool, null, null,
            null, Collections.singletonList(new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
                return processors;
            }
        }), client);
    }

    private class IngestDocumentMatcher extends ArgumentMatcher<IngestDocument> {

        private final IngestDocument ingestDocument;

        IngestDocumentMatcher(String index, String type, String id, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, null, null, null, source);
        }

        IngestDocumentMatcher(String index, String type, String id, Long version, VersionType versionType, Map<String, Object> source) {
            this.ingestDocument = new IngestDocument(index, type, id, null, version, versionType, source);
        }

        @Override
        public boolean matches(Object o) {
            if (o.getClass() == IngestDocument.class) {
                IngestDocument otherIngestDocument = (IngestDocument) o;
                //ingest metadata will not be the same (timestamp differs every time)
                return Objects.equals(ingestDocument.getSourceAndMetadata(), otherIngestDocument.getSourceAndMetadata());
            }
            return false;
        }
    }

    private void assertProcessorStats(int processor, IngestStats stats, String pipelineId, long count, long failed, long time) {
        assertStats(stats.getProcessorStats().get(pipelineId).get(processor).getStats(), count, failed, time);
    }

    private void assertPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String pipelineId, long count, long failed, long time) {
        assertStats(getPipelineStats(pipelineStats, pipelineId), count, failed, time);
    }

    private void assertStats(IngestStats.Stats stats, long count, long failed, long time) {
        assertThat(stats.getIngestCount(), equalTo(count));
        assertThat(stats.getIngestCurrent(), equalTo(0L));
        assertThat(stats.getIngestFailedCount(), equalTo(failed));
        assertThat(stats.getIngestTimeInMillis(), greaterThanOrEqualTo(time));
    }

    private IngestStats.Stats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream().filter(p1 -> p1.getPipelineId().equals(id)).findFirst().map(p2 -> p2.getStats()).orElse(null);
    }
}
