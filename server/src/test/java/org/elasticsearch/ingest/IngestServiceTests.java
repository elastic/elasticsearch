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
import org.apache.lucene.util.SetOnce;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.ResourceNotFoundException;
import org.elasticsearch.Version;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.DeletePipelineRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.plugins.IngestPlugin;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.hamcrest.CustomTypeSafeMatcher;
import org.mockito.ArgumentMatcher;
import org.mockito.invocation.InvocationOnMock;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
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
        IngestService ingestService = new IngestService(mock(ClusterService.class), tp, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN));
        Map<String, Processor.Factory> factories = ingestService.getProcessorFactories();
        assertTrue(factories.containsKey("foo"));
        assertEquals(1, factories.size());
    }

    public void testIngestPluginDuplicate() {
        ThreadPool tp = mock(ThreadPool.class);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () ->
            new IngestService(mock(ClusterService.class), tp, null, null,
            null, Arrays.asList(DUMMY_PLUGIN, DUMMY_PLUGIN)));
        assertTrue(e.getMessage(), e.getMessage().contains("already registered"));
    }

    public void testExecuteIndexPipelineDoesNotExist() {
        ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        IngestService ingestService = new IngestService(mock(ClusterService.class), threadPool, null, null,
            null, Collections.singletonList(DUMMY_PLUGIN));
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

        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);

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
        assertThat(ingestService.pipelines().get("_id").getId(), equalTo("_id"));
        assertThat(ingestService.pipelines().get("_id").getDescription(), nullValue());
        assertThat(ingestService.pipelines().get("_id").getProcessors().size(), equalTo(1));
        assertThat(ingestService.pipelines().get("_id").getProcessors().get(0).getType(), equalTo("set"));
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

    public void testCrud() throws Exception {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty

        PutPipelineRequest putRequest = new PutPipelineRequest(id,
            new BytesArray("{\"processors\": [{\"set\" : {\"field\": \"_field\", \"value\": \"_value\"}}]}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, notNullValue());
        assertThat(pipeline.getId(), equalTo(id));
        assertThat(pipeline.getDescription(), equalTo("_description"));
        assertThat(pipeline.getProcessors().size(), equalTo(0));
    }

    public void testPutWithErrorResponse() {
        IngestService ingestService = createWithProcessors();
        String id = "_id";
        Pipeline pipeline = ingestService.getPipeline(id);
        assertThat(pipeline, nullValue());
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();

        PutPipelineRequest putRequest =
            new PutPipelineRequest(id, new BytesArray("{\"description\": \"empty processors\"}"), XContentType.JSON);
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        try {
            ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
            fail("should fail");
        } catch (ElasticsearchParseException e) {
            assertThat(e.getMessage(), equalTo("[processors] required property is missing"));
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
                public void execute(IngestDocument ingestDocument) {
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
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

        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);

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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
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
        ingestService.executeBulkRequest(bulkRequest.requests(), failureHandler, completionHandler);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        verify(failureHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testExecuteEmptyPipeline() throws Exception {
        IngestService ingestService = createWithProcessors(emptyMap());
        PutPipelineRequest putRequest =
            new PutPipelineRequest("_id", new BytesArray("{\"processors\": [], \"description\": \"_description\"}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
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
            return null;
        }).when(processor).execute(any());
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException())
            .when(processor)
            .execute(eqIndexTypeId(indexRequest.version(), indexRequest.versionType(), emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").source(emptyMap()).setPipeline("_id");
        doThrow(new RuntimeException()).when(processor).execute(eqIndexTypeId(emptyMap()));
        @SuppressWarnings("unchecked")
        final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        final Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
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
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
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
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(bulkRequest.requests(), requestItemErrorHandler, completionHandler);

        verify(requestItemErrorHandler, times(numIndexRequests)).accept(any(IndexRequest.class), argThat(new ArgumentMatcher<Exception>() {
            @Override
            public boolean matches(final Object o) {
                return ((Exception)o).getCause().getCause().equals(error);
            }
        }));
        verify(completionHandler, times(1)).accept(null);
    }

    public void testBulkRequestExecution() {
        BulkRequest bulkRequest = new BulkRequest();
        String pipelineId = "_id";

        int numRequest = scaledRandomIntBetween(8, 64);
        for (int i = 0; i < numRequest; i++) {
            IndexRequest indexRequest = new IndexRequest("_index", "_type", "_id").setPipeline(pipelineId);
            indexRequest.source(Requests.INDEX_CONTENT_TYPE, "field1", "value1");
            bulkRequest.add(indexRequest);
        }

        IngestService ingestService = createWithProcessors(emptyMap());
        PutPipelineRequest putRequest =
            new PutPipelineRequest("_id", new BytesArray("{\"processors\": [], \"description\": \"_description\"}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build();
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));

        @SuppressWarnings("unchecked")
        BiConsumer<IndexRequest, Exception> requestItemErrorHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked")
        Consumer<Exception> completionHandler = mock(Consumer.class);
        ingestService.executeBulkRequest(bulkRequest.requests(), requestItemErrorHandler, completionHandler);

        verify(requestItemErrorHandler, never()).accept(any(), any());
        verify(completionHandler, times(1)).accept(null);
    }

    public void testStats() {
        final Processor processor = mock(Processor.class);
        IngestService ingestService = createWithProcessors(Collections.singletonMap(
            "mock", (factories, tag, config) -> processor));
        final IngestStats initialStats = ingestService.stats();
        assertThat(initialStats.getStatsPerPipeline().size(), equalTo(0));
        assertThat(initialStats.getTotalStats().getIngestCount(), equalTo(0L));
        assertThat(initialStats.getTotalStats().getIngestCurrent(), equalTo(0L));
        assertThat(initialStats.getTotalStats().getIngestFailedCount(), equalTo(0L));
        assertThat(initialStats.getTotalStats().getIngestTimeInMillis(), equalTo(0L));

        PutPipelineRequest putRequest = new PutPipelineRequest("_id1",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        ClusterState clusterState = ClusterState.builder(new ClusterName("_name")).build(); // Start empty
        ClusterState previousClusterState = clusterState;
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        putRequest = new PutPipelineRequest("_id2",
            new BytesArray("{\"processors\": [{\"mock\" : {}}]}"), XContentType.JSON);
        previousClusterState = clusterState;
        clusterState = IngestService.PipelineStore.innerPut(putRequest, clusterState);
        ingestService.applyClusterState(new ClusterChangedEvent("", clusterState, previousClusterState));
        final Map<String, PipelineConfiguration> configurationMap = new HashMap<>();
        configurationMap.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{}"), XContentType.JSON));
        configurationMap.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{}"), XContentType.JSON));
        ingestService.updatePipelineStats(new IngestMetadata(configurationMap));

        @SuppressWarnings("unchecked") final BiConsumer<IndexRequest, Exception> failureHandler = mock(BiConsumer.class);
        @SuppressWarnings("unchecked") final Consumer<Exception> completionHandler = mock(Consumer.class);

        final IndexRequest indexRequest = new IndexRequest("_index");
        indexRequest.setPipeline("_id1");
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        final IngestStats afterFirstRequestStats = ingestService.stats();
        assertThat(afterFirstRequestStats.getStatsPerPipeline().size(), equalTo(2));
        assertThat(afterFirstRequestStats.getStatsPerPipeline().get("_id1").getIngestCount(), equalTo(1L));
        assertThat(afterFirstRequestStats.getStatsPerPipeline().get("_id2").getIngestCount(), equalTo(0L));
        assertThat(afterFirstRequestStats.getTotalStats().getIngestCount(), equalTo(1L));

        indexRequest.setPipeline("_id2");
        ingestService.executeBulkRequest(Collections.singletonList(indexRequest), failureHandler, completionHandler);
        final IngestStats afterSecondRequestStats = ingestService.stats();
        assertThat(afterSecondRequestStats.getStatsPerPipeline().size(), equalTo(2));
        assertThat(afterSecondRequestStats.getStatsPerPipeline().get("_id1").getIngestCount(), equalTo(1L));
        assertThat(afterSecondRequestStats.getStatsPerPipeline().get("_id2").getIngestCount(), equalTo(1L));
        assertThat(afterSecondRequestStats.getTotalStats().getIngestCount(), equalTo(2L));
    }

    // issue: https://github.com/elastic/elasticsearch/issues/18126
    public void testUpdatingStatsWhenRemovingPipelineWorks() {
        IngestService ingestService = createWithProcessors();
        Map<String, PipelineConfiguration> configurationMap = new HashMap<>();
        configurationMap.put("_id1", new PipelineConfiguration("_id1", new BytesArray("{}"), XContentType.JSON));
        configurationMap.put("_id2", new PipelineConfiguration("_id2", new BytesArray("{}"), XContentType.JSON));
        ingestService.updatePipelineStats(new IngestMetadata(configurationMap));
        assertThat(ingestService.stats().getStatsPerPipeline(), hasKey("_id1"));
        assertThat(ingestService.stats().getStatsPerPipeline(), hasKey("_id2"));

        configurationMap = new HashMap<>();
        configurationMap.put("_id3", new PipelineConfiguration("_id3", new BytesArray("{}"), XContentType.JSON));
        ingestService.updatePipelineStats(new IngestMetadata(configurationMap));
        assertThat(ingestService.stats().getStatsPerPipeline(), not(hasKey("_id1")));
        assertThat(ingestService.stats().getStatsPerPipeline(), not(hasKey("_id2")));
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
            return new Processor() {
                @Override
                public void execute(IngestDocument ingestDocument) {
                    ingestDocument.setFieldValue(field, value);
                }

                @Override
                public String getType() {
                    return "set";
                }

                @Override
                public String getTag() {
                    return tag;
                }
            };
        });
        processors.put("remove", (factories, tag, config) -> {
            String field = (String) config.remove("field");
            return new Processor() {
                @Override
                public void execute(IngestDocument ingestDocument) {
                    ingestDocument.removeField(field);
                }

                @Override
                public String getType() {
                    return "remove";
                }

                @Override
                public String getTag() {
                    return tag;
                }
            };
        });
        return createWithProcessors(processors);
    }

    private static IngestService createWithProcessors(Map<String, Processor.Factory> processors) {
        ThreadPool threadPool = mock(ThreadPool.class);
        final ExecutorService executorService = EsExecutors.newDirectExecutorService();
        when(threadPool.executor(anyString())).thenReturn(executorService);
        return new IngestService(mock(ClusterService.class), threadPool, null, null,
            null, Collections.singletonList(new IngestPlugin() {
            @Override
            public Map<String, Processor.Factory> getProcessors(final Processor.Parameters parameters) {
                return processors;
            }
        }));
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
}
