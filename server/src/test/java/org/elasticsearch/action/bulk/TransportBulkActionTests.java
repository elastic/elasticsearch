/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstraction.ConcreteIndex;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.index.IndexNotFoundException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.IndexVersions;
import org.elasticsearch.index.IndexingPressure;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.indices.EmptySystemIndices;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.VersionUtils;
import org.elasticsearch.test.index.IndexVersionUtils;
import org.elasticsearch.test.transport.CapturingTransport;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.bulk.TransportBulkAction.prohibitCustomRoutingOnDataStream;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamServiceTests.createDataStream;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.junit.Assume.assumeThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportBulkActionTests extends ESTestCase {

    /** Services needed by bulk action */
    private TransportService transportService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;

    private TestTransportBulkAction bulkAction;
    private FeatureService mockFeatureService;

    class TestTransportBulkAction extends TransportBulkAction {

        volatile boolean failIndexCreation = false;
        boolean indexCreated = false; // set when the "real" index is created
        Runnable beforeIndexCreation = null;

        TestTransportBulkAction() {
            super(
                TransportBulkActionTests.this.threadPool,
                transportService,
                TransportBulkActionTests.this.clusterService,
                null,
                mockFeatureService,
                new NodeClient(Settings.EMPTY, TransportBulkActionTests.this.threadPool),
                new ActionFilters(Collections.emptySet()),
                new Resolver(),
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                FailureStoreMetrics.NOOP
            );
        }

        @Override
        void createIndex(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            if (beforeIndexCreation != null) {
                beforeIndexCreation.run();
            }
            if (failIndexCreation) {
                listener.onFailure(new ResourceAlreadyExistsException("index already exists"));
            } else {
                listener.onResponse(null);
            }
        }
    }

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getClass().getName());
        DiscoveryNode discoveryNode = DiscoveryNodeUtils.builder("node")
            .version(
                VersionUtils.randomCompatibleVersion(random(), Version.CURRENT),
                IndexVersions.MINIMUM_COMPATIBLE,
                IndexVersionUtils.randomCompatibleVersion(random())
            )
            .build();
        clusterService = createClusterService(threadPool, discoveryNode);
        CapturingTransport capturingTransport = new CapturingTransport();
        transportService = capturingTransport.createTransportService(
            clusterService.getSettings(),
            threadPool,
            TransportService.NOOP_TRANSPORT_INTERCEPTOR,
            boundAddress -> clusterService.localNode(),
            null,
            Collections.emptySet()
        );
        transportService.start();
        transportService.acceptIncomingRequests();
        mockFeatureService = mock(FeatureService.class);
        when(mockFeatureService.clusterHasFeature(any(), any())).thenReturn(true);
        bulkAction = new TestTransportBulkAction();
    }

    @After
    public void tearDown() throws Exception {
        ThreadPool.terminate(threadPool, 30, TimeUnit.SECONDS);
        threadPool = null;
        clusterService.close();
        super.tearDown();
    }

    public void testDeleteNonExistingDocDoesNotCreateIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index").id("id"));

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);

        BulkResponse response = future.actionGet();
        assertFalse(bulkAction.indexCreated);
        BulkItemResponse[] bulkResponses = response.getItems();
        assertEquals(bulkResponses.length, 1);
        assertTrue(bulkResponses[0].isFailed());
        assertTrue(bulkResponses[0].getFailure().getCause() instanceof IndexNotFoundException);
        assertEquals("index", bulkResponses[0].getFailure().getIndex());
    }

    public void testDeleteNonExistingDocExternalVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(new DeleteRequest("index").id("id").versionType(VersionType.EXTERNAL).version(0));

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
        future.actionGet();
        assertTrue(bulkAction.indexCreated);
    }

    public void testDeleteNonExistingDocExternalGteVersionCreatesIndex() throws Exception {
        BulkRequest bulkRequest = new BulkRequest().add(
            new DeleteRequest("index2").id("id").versionType(VersionType.EXTERNAL_GTE).version(0)
        );

        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
        future.actionGet();
        assertTrue(bulkAction.indexCreated);
    }

    public void testProhibitAppendWritesInBackingIndices() throws Exception {
        String dataStreamName = "logs-foobar";
        ClusterState clusterState = createDataStream(dataStreamName);
        Metadata metadata = clusterState.metadata();

        // Testing create op against backing index fails:
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexRequest invalidRequest1 = new IndexRequest(backingIndexName).opType(DocWriteRequest.OpType.CREATE);
        Exception e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportBulkAction.prohibitAppendWritesInBackingIndices(
                invalidRequest1,
                metadata.getIndicesLookup().get(invalidRequest1.index())
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "index request with op_type=create targeting backing indices is disallowed, "
                    + "target corresponding data stream [logs-foobar] instead"
            )
        );

        // Testing index op against backing index fails:
        IndexRequest invalidRequest2 = new IndexRequest(backingIndexName).opType(DocWriteRequest.OpType.INDEX);
        e = expectThrows(
            IllegalArgumentException.class,
            () -> TransportBulkAction.prohibitAppendWritesInBackingIndices(
                invalidRequest2,
                metadata.getIndicesLookup().get(invalidRequest2.index())
            )
        );
        assertThat(
            e.getMessage(),
            equalTo(
                "index request with op_type=index and no if_primary_term and if_seq_no set "
                    + "targeting backing indices is disallowed, target corresponding data stream [logs-foobar] instead"
            )
        );

        // Testing valid writes ops against a backing index:
        DocWriteRequest<?> validRequest = new IndexRequest(backingIndexName).opType(DocWriteRequest.OpType.INDEX)
            .setIfSeqNo(1)
            .setIfPrimaryTerm(1);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));
        validRequest = new DeleteRequest(backingIndexName);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));
        validRequest = new UpdateRequest(backingIndexName, "_id");
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));

        // Testing append only write via ds name
        validRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));

        validRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.INDEX);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));

        // Append only for a backing index that doesn't exist is allowed:
        validRequest = new IndexRequest(DataStream.getDefaultBackingIndexName("logs-barbaz", 1)).opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));

        // Some other index names:
        validRequest = new IndexRequest("my-index").opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));
        validRequest = new IndexRequest("foobar").opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(validRequest, metadata.getIndicesLookup().get(validRequest.index()));
    }

    public void testProhibitCustomRoutingOnDataStream() throws Exception {
        String dataStreamName = "logs-foobar";
        ClusterState clusterState = createDataStream(dataStreamName);
        Metadata metadata = clusterState.metadata();

        // custom routing requests against the data stream are prohibited
        DocWriteRequest<?> writeRequestAgainstDataStream = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.INDEX)
            .routing("custom");
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> prohibitCustomRoutingOnDataStream(
                writeRequestAgainstDataStream,
                metadata.getIndicesLookup().get(writeRequestAgainstDataStream.index())
            )
        );
        assertThat(
            exception.getMessage(),
            is(
                "index request targeting data stream [logs-foobar] specifies a custom routing "
                    + "but the [allow_custom_routing] setting was not enabled in the data stream's template."
            )
        );

        // test custom routing is allowed when the index request targets the backing index
        DocWriteRequest<?> writeRequestAgainstIndex = new IndexRequest(DataStream.getDefaultBackingIndexName(dataStreamName, 1L)).opType(
            DocWriteRequest.OpType.INDEX
        ).routing("custom");
        prohibitCustomRoutingOnDataStream(writeRequestAgainstIndex, metadata.getIndicesLookup().get(writeRequestAgainstIndex.index()));
    }

    public void testOnlySystem() throws IOException {
        SortedMap<String, IndexAbstraction> indicesLookup = new TreeMap<>();
        Settings settings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()).build();
        indicesLookup.put(
            ".foo",
            new ConcreteIndex(IndexMetadata.builder(".foo").settings(settings).system(true).numberOfShards(1).numberOfReplicas(0).build())
        );
        indicesLookup.put(
            ".bar",
            new ConcreteIndex(IndexMetadata.builder(".bar").settings(settings).system(true).numberOfShards(1).numberOfReplicas(0).build())
        );
        SystemIndices systemIndices = new SystemIndices(
            List.of(new SystemIndices.Feature("plugin", "test feature", List.of(SystemIndexDescriptorUtils.createUnmanaged(".test*", ""))))
        );
        List<String> onlySystem = List.of(".foo", ".bar");
        assertTrue(TransportBulkAction.isOnlySystem(buildBulkRequest(onlySystem), indicesLookup, systemIndices));
        /* Test forwarded bulk requests (that are serialized then deserialized) */
        assertTrue(TransportBulkAction.isOnlySystem(buildBulkStreamRequest(onlySystem), indicesLookup, systemIndices));

        onlySystem = List.of(".foo", ".bar", ".test");
        assertTrue(TransportBulkAction.isOnlySystem(buildBulkRequest(onlySystem), indicesLookup, systemIndices));
        /* Test forwarded bulk requests (that are serialized then deserialized) */
        assertTrue(TransportBulkAction.isOnlySystem(buildBulkStreamRequest(onlySystem), indicesLookup, systemIndices));

        List<String> nonSystem = List.of("foo", "bar");
        assertFalse(TransportBulkAction.isOnlySystem(buildBulkRequest(nonSystem), indicesLookup, systemIndices));
        /* Test forwarded bulk requests (that are serialized then deserialized) */
        assertFalse(TransportBulkAction.isOnlySystem(buildBulkStreamRequest(nonSystem), indicesLookup, systemIndices));

        List<String> mixed = List.of(".foo", ".test", "other");
        assertFalse(TransportBulkAction.isOnlySystem(buildBulkRequest(mixed), indicesLookup, systemIndices));
        /* Test forwarded bulk requests (that are serialized then deserialized) */
        assertFalse(TransportBulkAction.isOnlySystem(buildBulkStreamRequest(mixed), indicesLookup, systemIndices));
    }

    private void blockWriteThreadPool(CountDownLatch blockingLatch) {
        assertThat(blockingLatch.getCount(), greaterThan(0L));
        final var executor = threadPool.executor(ThreadPool.Names.WRITE);
        // Add tasks repeatedly until we get an EsRejectedExecutionException which indicates that the threadpool and its queue are full.
        expectThrows(EsRejectedExecutionException.class, () -> {
            // noinspection InfiniteLoopStatement
            while (true) {
                executor.execute(() -> safeAwait(blockingLatch));
            }
        });
    }

    public void testRejectCoordination() {
        BulkRequest bulkRequest = new BulkRequest().add(new IndexRequest("index").id("id").source(Collections.emptyMap()));

        final var blockingLatch = new CountDownLatch(1);
        try {
            blockWriteThreadPool(blockingLatch);
            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
            ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
            expectThrows(EsRejectedExecutionException.class, future);
        } finally {
            blockingLatch.countDown();
        }
    }

    public void testRejectionAfterCreateIndexIsPropagated() {
        BulkRequest bulkRequest = new BulkRequest().add(new IndexRequest("index").id("id").source(Collections.emptyMap()));

        bulkAction.failIndexCreation = randomBoolean();
        final var blockingLatch = new CountDownLatch(1);
        try {
            bulkAction.beforeIndexCreation = () -> blockWriteThreadPool(blockingLatch);
            PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
            ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
            expectThrows(EsRejectedExecutionException.class, future);
            assertTrue(bulkAction.indexCreated);
        } finally {
            blockingLatch.countDown();
        }
    }

    public void testResolveFailureStoreFromMetadata() throws Exception {
        assumeThat(DataStream.isFailureStoreFeatureFlagEnabled(), is(true));

        String dataStreamWithFailureStore = "test-data-stream-failure-enabled";
        String dataStreamWithoutFailureStore = "test-data-stream-failure-disabled";
        long testTime = randomMillisUpToYear9999();

        IndexMetadata backingIndex1 = DataStreamTestHelper.createFirstBackingIndex(dataStreamWithFailureStore, testTime).build();
        IndexMetadata backingIndex2 = DataStreamTestHelper.createFirstBackingIndex(dataStreamWithoutFailureStore, testTime).build();
        IndexMetadata failureStoreIndex1 = DataStreamTestHelper.createFirstFailureStore(dataStreamWithFailureStore, testTime).build();

        Metadata metadata = Metadata.builder()
            .dataStreams(
                Map.of(
                    dataStreamWithFailureStore,
                    DataStreamTestHelper.newInstance(
                        dataStreamWithFailureStore,
                        List.of(backingIndex1.getIndex()),
                        1L,
                        Map.of(),
                        false,
                        null,
                        List.of(failureStoreIndex1.getIndex())
                    ),
                    dataStreamWithoutFailureStore,
                    DataStreamTestHelper.newInstance(
                        dataStreamWithoutFailureStore,
                        List.of(backingIndex2.getIndex()),
                        1L,
                        Map.of(),
                        false,
                        null,
                        List.of()
                    )
                ),
                Map.of()
            )
            .indices(
                Map.of(
                    backingIndex1.getIndex().getName(),
                    backingIndex1,
                    backingIndex2.getIndex().getName(),
                    backingIndex2,
                    failureStoreIndex1.getIndex().getName(),
                    failureStoreIndex1
                )
            )
            .build();

        // Data stream with failure store should store failures
        assertThat(TransportBulkAction.resolveFailureInternal(dataStreamWithFailureStore, metadata, testTime), is(true));
        // Data stream without failure store should not
        assertThat(TransportBulkAction.resolveFailureInternal(dataStreamWithoutFailureStore, metadata, testTime), is(false));
        // An index should not be considered for failure storage
        assertThat(TransportBulkAction.resolveFailureInternal(backingIndex1.getIndex().getName(), metadata, testTime), is(nullValue()));
        // even if that index is itself a failure store
        assertThat(
            TransportBulkAction.resolveFailureInternal(failureStoreIndex1.getIndex().getName(), metadata, testTime),
            is(nullValue())
        );
    }

    public void testResolveFailureStoreFromTemplate() throws Exception {
        assumeThat(DataStream.isFailureStoreFeatureFlagEnabled(), is(true));

        String dsTemplateWithFailureStore = "test-data-stream-failure-enabled";
        String dsTemplateWithoutFailureStore = "test-data-stream-failure-disabled";
        String indexTemplate = "test-index";
        long testTime = randomMillisUpToYear9999();

        Metadata metadata = Metadata.builder()
            .indexTemplates(
                Map.of(
                    dsTemplateWithFailureStore,
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dsTemplateWithFailureStore + "-*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false, true))
                        .build(),
                    dsTemplateWithoutFailureStore,
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dsTemplateWithoutFailureStore + "-*"))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false, false))
                        .build(),
                    indexTemplate,
                    ComposableIndexTemplate.builder().indexPatterns(List.of(indexTemplate + "-*")).build()
                )
            )
            .build();

        // Data stream with failure store should store failures
        assertThat(TransportBulkAction.resolveFailureInternal(dsTemplateWithFailureStore + "-1", metadata, testTime), is(true));
        // Data stream without failure store should not
        assertThat(TransportBulkAction.resolveFailureInternal(dsTemplateWithoutFailureStore + "-1", metadata, testTime), is(false));
        // An index template should not be considered for failure storage
        assertThat(TransportBulkAction.resolveFailureInternal(indexTemplate + "-1", metadata, testTime), is(nullValue()));
    }

    private BulkRequest buildBulkRequest(List<String> indices) {
        BulkRequest request = new BulkRequest();
        for (String index : indices) {
            final DocWriteRequest<?> subRequest = switch (randomIntBetween(1, 3)) {
                case 1 -> new IndexRequest(index);
                case 2 -> new DeleteRequest(index).id("0");
                case 3 -> new UpdateRequest(index, "0");
                default -> throw new IllegalStateException("only have 3 cases");
            };
            request.add(subRequest);
        }
        return request;
    }

    private BulkRequest buildBulkStreamRequest(List<String> indices) throws IOException {
        BulkRequest request = buildBulkRequest(indices);
        BytesStreamOutput out = new BytesStreamOutput();
        request.writeTo(out);
        StreamInput streamInput = out.bytes().streamInput();
        return (new BulkRequest(streamInput));
    }
}
