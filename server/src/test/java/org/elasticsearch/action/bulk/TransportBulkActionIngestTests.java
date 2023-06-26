/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.index.IndexAction;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.AutoCreateIndex;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateApplier;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexTemplateMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.ingest.IngestService;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportResponseHandler;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.sameInstance;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class TransportBulkActionIngestTests extends ESTestCase {

    /**
     * Index for which mock settings contain a default pipeline.
     */
    private static final String WITH_DEFAULT_PIPELINE = "index_with_default_pipeline";
    private static final String WITH_DEFAULT_PIPELINE_ALIAS = "alias_for_index_with_default_pipeline";

    private static final Settings SETTINGS = Settings.builder().put(AutoCreateIndex.AUTO_CREATE_INDEX_SETTING.getKey(), true).build();

    private static final Thread DUMMY_WRITE_THREAD = new Thread(ThreadPool.Names.WRITE);

    /** Services needed by bulk action */
    TransportService transportService;
    ClusterService clusterService;
    IngestService ingestService;
    ThreadPool threadPool;

    /** Arguments to callbacks we want to capture, but which require generics, so we must use @Captor */
    @Captor
    ArgumentCaptor<BiConsumer<Integer, Exception>> failureHandler;
    @Captor
    ArgumentCaptor<BiConsumer<Thread, Exception>> completionHandler;
    @Captor
    ArgumentCaptor<TransportResponseHandler<BulkResponse>> remoteResponseHandler;
    @Captor
    ArgumentCaptor<Iterable<DocWriteRequest<?>>> bulkDocsItr;

    /** The actual action we want to test, with real indexing mocked */
    TestTransportBulkAction action;

    /** Single item bulk write action that wraps index requests */
    TestSingleItemBulkWriteAction singleItemBulkWriteAction;

    /** True if the next call to the index action should act as an ingest node */
    boolean localIngest;

    /** The nodes that forwarded index requests should be cycled through. */
    DiscoveryNodes nodes;
    DiscoveryNode remoteNode1;
    DiscoveryNode remoteNode2;

    /** A subclass of the real bulk action to allow skipping real bulk indexing, and marking when it would have happened. */
    class TestTransportBulkAction extends TransportBulkAction {
        boolean isExecuted = false; // set when the "real" bulk execution happens

        boolean needToCheck; // pluggable return value for `needToCheck`

        boolean indexCreated = true; // If set to false, will be set to true by call to createIndex

        TestTransportBulkAction() {
            super(
                threadPool,
                transportService,
                clusterService,
                ingestService,
                null,
                new ActionFilters(Collections.emptySet()),
                TestIndexNameExpressionResolver.newInstance(),
                new IndexingPressure(SETTINGS),
                EmptySystemIndices.INSTANCE
            );
        }

        @Override
        void executeBulk(
            Task task,
            BulkRequest bulkRequest,
            long startTimeNanos,
            ActionListener<BulkResponse> listener,
            String executorName,
            AtomicArray<BulkItemResponse> responses,
            Map<String, IndexNotFoundException> indicesThatCannotBeCreated
        ) {
            assertTrue(indexCreated);
            isExecuted = true;
        }

        @Override
        void createIndex(String index, TimeValue timeout, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            listener.onResponse(null);
        }
    }

    class TestSingleItemBulkWriteAction extends TransportSingleItemBulkWriteAction<IndexRequest, IndexResponse> {

        TestSingleItemBulkWriteAction(TestTransportBulkAction bulkAction) {
            super(
                IndexAction.NAME,
                TransportBulkActionIngestTests.this.transportService,
                new ActionFilters(Collections.emptySet()),
                IndexRequest::new,
                bulkAction
            );
        }
    }

    @Before
    public void setupAction() {
        // initialize captors, which must be members to use @Capture because of generics
        threadPool = mock(ThreadPool.class);
        when(threadPool.executor(anyString())).thenReturn(EsExecutors.DIRECT_EXECUTOR_SERVICE);
        MockitoAnnotations.openMocks(this);
        // setup services that will be called by action
        transportService = mock(TransportService.class);
        clusterService = mock(ClusterService.class);
        localIngest = true;
        // setup nodes for local and remote
        DiscoveryNode localNode = mock(DiscoveryNode.class);
        when(localNode.isIngestNode()).thenAnswer(stub -> localIngest);
        when(clusterService.localNode()).thenReturn(localNode);
        when(clusterService.getSettings()).thenReturn(Settings.EMPTY);
        remoteNode1 = mock(DiscoveryNode.class);
        remoteNode2 = mock(DiscoveryNode.class);
        nodes = mock(DiscoveryNodes.class);
        Map<String, DiscoveryNode> ingestNodes = Map.of("node1", remoteNode1, "node2", remoteNode2);
        when(nodes.getIngestNodes()).thenReturn(ingestNodes);
        when(nodes.getMinNodeVersion()).thenReturn(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
        ClusterState state = mock(ClusterState.class);
        when(state.getNodes()).thenReturn(nodes);
        Metadata metadata = Metadata.builder()
            .indices(
                Map.of(
                    WITH_DEFAULT_PIPELINE,
                    IndexMetadata.builder(WITH_DEFAULT_PIPELINE)
                        .settings(settings(Version.CURRENT).put(IndexSettings.DEFAULT_PIPELINE.getKey(), "default_pipeline").build())
                        .putAlias(AliasMetadata.builder(WITH_DEFAULT_PIPELINE_ALIAS).build())
                        .numberOfShards(1)
                        .numberOfReplicas(1)
                        .build(),
                    ".system",
                    IndexMetadata.builder(".system")
                        .settings(settings(Version.CURRENT))
                        .system(true)
                        .numberOfShards(1)
                        .numberOfReplicas(0)
                        .build()
                )
            )
            .build();
        when(state.getMetadata()).thenReturn(metadata);
        when(state.metadata()).thenReturn(metadata);
        when(clusterService.state()).thenReturn(state);
        doAnswer(invocation -> {
            ClusterChangedEvent event = mock(ClusterChangedEvent.class);
            when(event.state()).thenReturn(state);
            ((ClusterStateApplier) invocation.getArguments()[0]).applyClusterState(event);
            return null;
        }).when(clusterService).addStateApplier(any(ClusterStateApplier.class));
        // setup the mocked ingest service for capturing calls
        ingestService = mock(IngestService.class);
        action = new TestTransportBulkAction();
        singleItemBulkWriteAction = new TestSingleItemBulkWriteAction(action);
        reset(transportService); // call on construction of action
    }

    public void testIngestSkipped() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index").id("id");
        indexRequest.source(Collections.emptyMap());
        bulkRequest.add(indexRequest);
        ActionTestUtils.execute(action, null, bulkRequest, ActionTestUtils.assertNoFailureListener(response -> {}));
        assertTrue(action.isExecuted);
        verifyNoMoreInteractions(ingestService);
    }

    public void testSingleItemBulkActionIngestSkipped() throws Exception {
        IndexRequest indexRequest = new IndexRequest("index").id("id");
        indexRequest.source(Collections.emptyMap());
        ActionTestUtils.execute(singleItemBulkWriteAction, null, indexRequest, ActionTestUtils.assertNoFailureListener(response -> {}));
        assertTrue(action.isExecuted);
        verifyNoMoreInteractions(ingestService);
    }

    public void testIngestLocal() throws Exception {
        Exception exception = new Exception("fake exception");
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("index").id("id");
        indexRequest1.source(Collections.emptyMap());
        indexRequest1.setPipeline("testpipeline");
        IndexRequest indexRequest2 = new IndexRequest("index").id("id");
        indexRequest2.source(Collections.emptyMap());
        indexRequest2.setPipeline("testpipeline");
        bulkRequest.add(indexRequest1);
        bulkRequest.add(indexRequest2);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(action, null, bulkRequest, ActionListener.wrap(response -> {
            BulkItemResponse itemResponse = response.iterator().next();
            assertThat(itemResponse.getFailure().getMessage(), containsString("fake exception"));
            responseCalled.set(true);
        }, e -> {
            assertThat(e, sameInstance(exception));
            failureCalled.set(true);
        }));

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(bulkRequest.numberOfActions()),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
        completionHandler.getValue().accept(null, exception);
        assertTrue(failureCalled.get());

        // now check success
        Iterator<DocWriteRequest<?>> req = bulkDocsItr.getValue().iterator();
        failureHandler.getValue().accept(0, exception); // have an exception for our one index request
        indexRequest2.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(DUMMY_WRITE_THREAD, null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyNoMoreInteractions(transportService);
    }

    public void testSingleItemBulkActionIngestLocal() throws Exception {
        Exception exception = new Exception("fake exception");
        IndexRequest indexRequest = new IndexRequest("index").id("id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(
            singleItemBulkWriteAction,
            null,
            indexRequest,
            ActionListener.wrap(response -> { responseCalled.set(true); }, e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            })
        );

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(1),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
        completionHandler.getValue().accept(null, exception);
        assertTrue(failureCalled.get());

        // now check success
        indexRequest.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(DUMMY_WRITE_THREAD, null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyNoMoreInteractions(transportService);
    }

    public void testIngestSystemLocal() throws Exception {
        Exception exception = new Exception("fake exception");
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest(".system").id("id");
        indexRequest1.source(Collections.emptyMap());
        indexRequest1.setPipeline("testpipeline");
        IndexRequest indexRequest2 = new IndexRequest(".system").id("id");
        indexRequest2.source(Collections.emptyMap());
        indexRequest2.setPipeline("testpipeline");
        bulkRequest.add(indexRequest1);
        bulkRequest.add(indexRequest2);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(action, null, bulkRequest, ActionListener.wrap(response -> {
            BulkItemResponse itemResponse = response.iterator().next();
            assertThat(itemResponse.getFailure().getMessage(), containsString("fake exception"));
            responseCalled.set(true);
        }, e -> {
            assertThat(e, sameInstance(exception));
            failureCalled.set(true);
        }));

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(bulkRequest.numberOfActions()),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.SYSTEM_WRITE)
        );
        completionHandler.getValue().accept(null, exception);
        assertTrue(failureCalled.get());

        // now check success
        Iterator<DocWriteRequest<?>> req = bulkDocsItr.getValue().iterator();
        failureHandler.getValue().accept(0, exception); // have an exception for our one index request
        indexRequest2.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(DUMMY_WRITE_THREAD, null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyNoMoreInteractions(transportService);
    }

    public void testIngestForward() throws Exception {
        localIngest = false;
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest = new IndexRequest("index").id("id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        bulkRequest.add(indexRequest);
        BulkResponse bulkResponse = mock(BulkResponse.class);
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        ActionListener<BulkResponse> listener = ActionTestUtils.assertNoFailureListener(response -> {
            responseCalled.set(true);
            assertSame(bulkResponse, response);
        });
        ActionTestUtils.execute(action, null, bulkRequest, listener);

        // should not have executed ingest locally
        verify(ingestService, never()).executeBulkRequest(anyInt(), any(), any(), any(), any(), any());
        // but instead should have sent to a remote node with the transport service
        ArgumentCaptor<DiscoveryNode> node = ArgumentCaptor.forClass(DiscoveryNode.class);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        boolean usedNode1 = node.getValue() == remoteNode1; // make sure we used one of the nodes
        if (usedNode1 == false) {
            assertSame(remoteNode2, node.getValue());
        }
        assertFalse(action.isExecuted); // no local index execution
        assertFalse(responseCalled.get()); // listener not called yet

        remoteResponseHandler.getValue().handleResponse(bulkResponse); // call the listener for the remote node
        assertTrue(responseCalled.get()); // now the listener we passed should have been delegated to by the remote listener
        assertFalse(action.isExecuted); // still no local index execution

        // now make sure ingest nodes are rotated through with a subsequent request
        reset(transportService);
        ActionTestUtils.execute(action, null, bulkRequest, listener);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        if (usedNode1) {
            assertSame(remoteNode2, node.getValue());
        } else {
            assertSame(remoteNode1, node.getValue());
        }
    }

    public void testSingleItemBulkActionIngestForward() throws Exception {
        localIngest = false;
        IndexRequest indexRequest = new IndexRequest("index").id("id");
        indexRequest.source(Collections.emptyMap());
        indexRequest.setPipeline("testpipeline");
        IndexResponse indexResponse = mock(IndexResponse.class);
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        ActionListener<IndexResponse> listener = ActionTestUtils.assertNoFailureListener(response -> {
            responseCalled.set(true);
            assertSame(indexResponse, response);
        });
        ActionTestUtils.execute(singleItemBulkWriteAction, null, indexRequest, listener);

        // should not have executed ingest locally
        verify(ingestService, never()).executeBulkRequest(anyInt(), any(), any(), any(), any(), any());
        // but instead should have sent to a remote node with the transport service
        ArgumentCaptor<DiscoveryNode> node = ArgumentCaptor.forClass(DiscoveryNode.class);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        boolean usedNode1 = node.getValue() == remoteNode1; // make sure we used one of the nodes
        if (usedNode1 == false) {
            assertSame(remoteNode2, node.getValue());
        }
        assertFalse(action.isExecuted); // no local index execution
        assertFalse(responseCalled.get()); // listener not called yet

        BulkItemResponse itemResponse = BulkItemResponse.success(0, DocWriteRequest.OpType.CREATE, indexResponse);
        BulkItemResponse[] bulkItemResponses = new BulkItemResponse[1];
        bulkItemResponses[0] = itemResponse;
        remoteResponseHandler.getValue().handleResponse(new BulkResponse(bulkItemResponses, 0)); // call the listener for the remote node
        assertTrue(responseCalled.get()); // now the listener we passed should have been delegated to by the remote listener
        assertFalse(action.isExecuted); // still no local index execution

        // now make sure ingest nodes are rotated through with a subsequent request
        reset(transportService);
        ActionTestUtils.execute(singleItemBulkWriteAction, null, indexRequest, listener);
        verify(transportService).sendRequest(node.capture(), eq(BulkAction.NAME), any(), remoteResponseHandler.capture());
        if (usedNode1) {
            assertSame(remoteNode2, node.getValue());
        } else {
            assertSame(remoteNode1, node.getValue());
        }
    }

    public void testUseDefaultPipeline() throws Exception {
        validateDefaultPipeline(new IndexRequest(WITH_DEFAULT_PIPELINE).id("id"));
    }

    public void testUseDefaultPipelineWithAlias() throws Exception {
        validateDefaultPipeline(new IndexRequest(WITH_DEFAULT_PIPELINE_ALIAS).id("id"));
    }

    public void testUseDefaultPipelineWithBulkUpsert() throws Exception {
        String indexRequestName = randomFrom(new String[] { null, WITH_DEFAULT_PIPELINE, WITH_DEFAULT_PIPELINE_ALIAS });
        validatePipelineWithBulkUpsert(indexRequestName, WITH_DEFAULT_PIPELINE);
    }

    public void testUseDefaultPipelineWithBulkUpsertWithAlias() throws Exception {
        String indexRequestName = randomFrom(new String[] { null, WITH_DEFAULT_PIPELINE, WITH_DEFAULT_PIPELINE_ALIAS });
        validatePipelineWithBulkUpsert(indexRequestName, WITH_DEFAULT_PIPELINE_ALIAS);
    }

    private void validatePipelineWithBulkUpsert(@Nullable String indexRequestIndexName, String updateRequestIndexName) throws Exception {
        Exception exception = new Exception("fake exception");
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest(indexRequestIndexName).id("id1").source(Collections.emptyMap());
        IndexRequest indexRequest2 = new IndexRequest(indexRequestIndexName).id("id2").source(Collections.emptyMap());
        IndexRequest indexRequest3 = new IndexRequest(indexRequestIndexName).id("id3").source(Collections.emptyMap());
        UpdateRequest upsertRequest = new UpdateRequest(updateRequestIndexName, "id1").upsert(indexRequest1).script(mockScript("1"));
        UpdateRequest docAsUpsertRequest = new UpdateRequest(updateRequestIndexName, "id2").doc(indexRequest2).docAsUpsert(true);
        // this test only covers the mechanics that scripted bulk upserts will execute a default pipeline. However, in practice scripted
        // bulk upserts with a default pipeline are a bit surprising since the script executes AFTER the pipeline.
        UpdateRequest scriptedUpsert = new UpdateRequest(updateRequestIndexName, "id2").upsert(indexRequest3)
            .script(mockScript("1"))
            .scriptedUpsert(true);
        bulkRequest.add(upsertRequest).add(docAsUpsertRequest).add(scriptedUpsert);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        assertNull(indexRequest1.getPipeline());
        assertNull(indexRequest2.getPipeline());
        assertNull(indexRequest3.getPipeline());
        ActionTestUtils.execute(action, null, bulkRequest, ActionListener.wrap(response -> {
            BulkItemResponse itemResponse = response.iterator().next();
            assertThat(itemResponse.getFailure().getMessage(), containsString("fake exception"));
            responseCalled.set(true);
        }, e -> {
            assertThat(e, sameInstance(exception));
            failureCalled.set(true);
        }));

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(bulkRequest.numberOfActions()),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
        assertEquals(indexRequest1.getPipeline(), "default_pipeline");
        assertEquals(indexRequest2.getPipeline(), "default_pipeline");
        assertEquals(indexRequest3.getPipeline(), "default_pipeline");
        completionHandler.getValue().accept(null, exception);
        assertTrue(failureCalled.get());

        // now check success of the transport bulk action
        indexRequest1.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        indexRequest2.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        indexRequest3.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(DUMMY_WRITE_THREAD, null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyNoMoreInteractions(transportService);
    }

    public void testDoExecuteCalledTwiceCorrectly() throws Exception {
        Exception exception = new Exception("fake exception");
        IndexRequest indexRequest = new IndexRequest("missing_index").id("id");
        indexRequest.setPipeline("testpipeline");
        indexRequest.source(Collections.emptyMap());
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        action.needToCheck = true;
        action.indexCreated = false;
        ActionTestUtils.execute(
            singleItemBulkWriteAction,
            null,
            indexRequest,
            ActionListener.wrap(response -> responseCalled.set(true), e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            })
        );

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(action.indexCreated); // no index yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(1),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
        completionHandler.getValue().accept(null, exception);
        assertFalse(action.indexCreated); // still no index yet, the ingest node failed.
        assertTrue(failureCalled.get());

        // now check success
        indexRequest.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(DUMMY_WRITE_THREAD, null);
        assertTrue(action.isExecuted);
        assertTrue(action.indexCreated); // now the index is created since we skipped the ingest node path.
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyNoMoreInteractions(transportService);
    }

    public void testNotFindDefaultPipelineFromTemplateMatches() {
        Exception exception = new Exception("fake exception");
        IndexRequest indexRequest = new IndexRequest("missing_index").id("id");
        indexRequest.source(Collections.emptyMap());
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(
            singleItemBulkWriteAction,
            null,
            indexRequest,
            ActionListener.wrap(response -> responseCalled.set(true), e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            })
        );
        assertEquals(IngestService.NOOP_PIPELINE_NAME, indexRequest.getPipeline());
        verifyNoMoreInteractions(ingestService);

    }

    public void testFindDefaultPipelineFromTemplateMatch() {
        Exception exception = new Exception("fake exception");
        ClusterState state = clusterService.state();

        Map<String, IndexTemplateMetadata> templateMetadata = new HashMap<>();
        templateMetadata.put(
            "template1",
            IndexTemplateMetadata.builder("template1")
                .patterns(Arrays.asList("missing_index"))
                .order(1)
                .settings(Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "pipeline1").build())
                .build()
        );
        templateMetadata.put(
            "template2",
            IndexTemplateMetadata.builder("template2")
                .patterns(Arrays.asList("missing_*"))
                .order(2)
                .settings(Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "pipeline2").build())
                .build()
        );
        templateMetadata.put("template3", IndexTemplateMetadata.builder("template3").patterns(Arrays.asList("missing*")).order(3).build());
        templateMetadata.put(
            "template4",
            IndexTemplateMetadata.builder("template4")
                .patterns(Arrays.asList("nope"))
                .order(4)
                .settings(Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "pipeline4").build())
                .build()
        );

        Metadata metadata = mock(Metadata.class);
        when(state.metadata()).thenReturn(metadata);
        when(state.getMetadata()).thenReturn(metadata);
        when(metadata.templates()).thenReturn(templateMetadata);
        when(metadata.getTemplates()).thenReturn(templateMetadata);
        when(metadata.indices()).thenReturn(Map.of());

        IndexRequest indexRequest = new IndexRequest("missing_index").id("id");
        indexRequest.source(Collections.emptyMap());
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(
            singleItemBulkWriteAction,
            null,
            indexRequest,
            ActionListener.wrap(response -> responseCalled.set(true), e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            })
        );

        assertEquals("pipeline2", indexRequest.getPipeline());
        verify(ingestService).executeBulkRequest(
            eq(1),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
    }

    public void testFindDefaultPipelineFromV2TemplateMatch() {
        Exception exception = new Exception("fake exception");

        ComposableIndexTemplate t1 = new ComposableIndexTemplate(
            Collections.singletonList("missing_*"),
            new Template(Settings.builder().put(IndexSettings.DEFAULT_PIPELINE.getKey(), "pipeline2").build(), null, null),
            null,
            null,
            null,
            null,
            null,
            null
        );

        ClusterState state = clusterService.state();
        Metadata metadata = Metadata.builder().put("my-template", t1).build();
        when(state.metadata()).thenReturn(metadata);
        when(state.getMetadata()).thenReturn(metadata);

        IndexRequest indexRequest = new IndexRequest("missing_index").id("id");
        indexRequest.source(Collections.emptyMap());
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(
            singleItemBulkWriteAction,
            null,
            indexRequest,
            ActionListener.wrap(response -> responseCalled.set(true), e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            })
        );

        assertEquals("pipeline2", indexRequest.getPipeline());
        verify(ingestService).executeBulkRequest(
            eq(1),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
    }

    public void testIngestCallbackExceptionHandled() throws Exception {
        BulkRequest bulkRequest = new BulkRequest();
        IndexRequest indexRequest1 = new IndexRequest("index");
        indexRequest1.source(Collections.emptyMap());
        indexRequest1.setPipeline("testpipeline");
        bulkRequest.add(indexRequest1);

        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        ActionTestUtils.execute(action, null, bulkRequest, ActionListener.wrap(response -> { responseCalled.set(true); }, e -> {
            failureCalled.set(true);
        }));

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(bulkRequest.numberOfActions()),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
        indexRequest1.autoGenerateId();
        completionHandler.getValue().accept(Thread.currentThread(), null);

        // check failure passed through to the listener
        assertFalse(action.isExecuted);
        assertFalse(responseCalled.get());
        assertTrue(failureCalled.get());
    }

    private void validateDefaultPipeline(IndexRequest indexRequest) {
        Exception exception = new Exception("fake exception");
        indexRequest.source(Collections.emptyMap());
        AtomicBoolean responseCalled = new AtomicBoolean(false);
        AtomicBoolean failureCalled = new AtomicBoolean(false);
        assertNull(indexRequest.getPipeline());
        ActionTestUtils.execute(
            singleItemBulkWriteAction,
            null,
            indexRequest,
            ActionListener.wrap(response -> { responseCalled.set(true); }, e -> {
                assertThat(e, sameInstance(exception));
                failureCalled.set(true);
            })
        );

        // check failure works, and passes through to the listener
        assertFalse(action.isExecuted); // haven't executed yet
        assertFalse(responseCalled.get());
        assertFalse(failureCalled.get());
        verify(ingestService).executeBulkRequest(
            eq(1),
            bulkDocsItr.capture(),
            any(),
            failureHandler.capture(),
            completionHandler.capture(),
            eq(Names.WRITE)
        );
        assertEquals(indexRequest.getPipeline(), "default_pipeline");
        completionHandler.getValue().accept(null, exception);
        assertTrue(failureCalled.get());

        // now check success
        indexRequest.setPipeline(IngestService.NOOP_PIPELINE_NAME); // this is done by the real pipeline execution service when processing
        completionHandler.getValue().accept(DUMMY_WRITE_THREAD, null);
        assertTrue(action.isExecuted);
        assertFalse(responseCalled.get()); // listener would only be called by real index action, not our mocked one
        verifyNoMoreInteractions(transportService);
    }
}
