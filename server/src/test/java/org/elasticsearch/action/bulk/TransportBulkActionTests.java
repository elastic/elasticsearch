/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.bulk.TransportBulkActionTookTests.Resolver;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.ActionTestUtils;
import org.elasticsearch.action.support.IndexComponentSelector;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexAbstraction.ConcreteIndex;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.project.ProjectResolver;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.routing.GlobalRoutingTableTestHelper;
import org.elasticsearch.cluster.routing.RoutingTable;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.EsRejectedExecutionException;
import org.elasticsearch.core.CheckedRunnable;
import org.elasticsearch.features.FeatureService;
import org.elasticsearch.features.NodeFeature;
import org.elasticsearch.index.Index;
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
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.action.bulk.TransportBulkAction.prohibitCustomRoutingOnDataStream;
import static org.elasticsearch.cluster.metadata.MetadataCreateDataStreamServiceTests.createDataStream;
import static org.elasticsearch.test.ClusterServiceUtils.createClusterService;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportBulkActionTests extends ESTestCase {

    private final ClusterSettings clusterSettings = ClusterSettings.createBuiltInClusterSettings();

    /** Services needed by bulk action */
    private TransportService transportService;
    private ClusterService clusterService;
    private TestThreadPool threadPool;

    private TestTransportBulkAction bulkAction;
    private FeatureService mockFeatureService;
    private AtomicReference<ProjectId> activeProjectId = new AtomicReference<>();

    class TestTransportBulkAction extends TransportBulkAction {

        volatile Exception failIndexCreationException;
        volatile Exception failDataStreamRolloverException;
        volatile Exception failFailureStoreRolloverException;
        boolean indexCreated = false; // set when the "real" index is created
        Runnable beforeIndexCreation = null;

        TestTransportBulkAction() {
            super(
                TransportBulkActionTests.this.threadPool,
                transportService,
                TransportBulkActionTests.this.clusterService,
                null,
                new NodeClient(Settings.EMPTY, TransportBulkActionTests.this.threadPool, TestProjectResolvers.alwaysThrow()),
                new ActionFilters(Collections.emptySet()),
                new Resolver(),
                new IndexingPressure(Settings.EMPTY),
                EmptySystemIndices.INSTANCE,
                new ProjectResolver() {
                    @Override
                    public <E extends Exception> void executeOnProject(ProjectId projectId, CheckedRunnable<E> body) throws E {
                        throw new UnsupportedOperationException("");
                    }

                    @Override
                    public ProjectId getProjectId() {
                        return activeProjectId.get();
                    }
                },
                FailureStoreMetrics.NOOP,
                DataStreamFailureStoreSettings.create(clusterSettings),
                new FeatureService(List.of()) {
                    @Override
                    public boolean clusterHasFeature(ClusterState state, NodeFeature feature) {
                        return DataStream.DATA_STREAM_FAILURE_STORE_FEATURE.equals(feature);
                    }
                }
            );
        }

        @Override
        void createIndex(CreateIndexRequest createIndexRequest, ActionListener<CreateIndexResponse> listener) {
            indexCreated = true;
            if (beforeIndexCreation != null) {
                beforeIndexCreation.run();
            }
            if (failIndexCreationException != null) {
                listener.onFailure(failIndexCreationException);
            } else {
                listener.onResponse(null);
            }
        }

        @Override
        void rollOver(RolloverRequest rolloverRequest, ActionListener<RolloverResponse> listener) {
            String selectorString = IndexNameExpressionResolver.splitSelectorExpression(rolloverRequest.getRolloverTarget()).v2();
            boolean isFailureStoreRollover = IndexComponentSelector.FAILURES.getKey().equals(selectorString);
            if (failDataStreamRolloverException != null && isFailureStoreRollover == false) {
                listener.onFailure(failDataStreamRolloverException);
            } else if (failFailureStoreRolloverException != null && isFailureStoreRollover) {
                listener.onFailure(failFailureStoreRolloverException);
            } else {
                listener.onResponse(
                    new RolloverResponse(null, null, Map.of(), rolloverRequest.isDryRun(), true, true, true, rolloverRequest.isLazy())
                );
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
        activeProjectId.set(Metadata.DEFAULT_PROJECT_ID);
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
                metadata.getProject().getIndicesLookup().get(invalidRequest1.index())
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
                metadata.getProject().getIndicesLookup().get(invalidRequest2.index())
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
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );
        validRequest = new DeleteRequest(backingIndexName);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );
        validRequest = new UpdateRequest(backingIndexName, "_id");
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );

        // Testing append only write via ds name
        validRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );

        validRequest = new IndexRequest(dataStreamName).opType(DocWriteRequest.OpType.INDEX);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );

        // Append only for a backing index that doesn't exist is allowed:
        validRequest = new IndexRequest(DataStream.getDefaultBackingIndexName("logs-barbaz", 1)).opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );

        // Some other index names:
        validRequest = new IndexRequest("my-index").opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );
        validRequest = new IndexRequest("foobar").opType(DocWriteRequest.OpType.CREATE);
        TransportBulkAction.prohibitAppendWritesInBackingIndices(
            validRequest,
            metadata.getProject().getIndicesLookup().get(validRequest.index())
        );
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
                metadata.getProject().getIndicesLookup().get(writeRequestAgainstDataStream.index())
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
        prohibitCustomRoutingOnDataStream(
            writeRequestAgainstIndex,
            metadata.getProject().getIndicesLookup().get(writeRequestAgainstIndex.index())
        );
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

        bulkAction.failIndexCreationException = randomBoolean() ? new ResourceAlreadyExistsException("index already exists") : null;
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
        String dataStreamWithFailureStoreEnabled = "test-data-stream-failure-enabled";
        String dataStreamWithFailureStoreDefault = "test-data-stream-failure-default";
        String dataStreamWithFailureStoreDisabled = "test-data-stream-failure-disabled";
        long testTime = randomMillisUpToYear9999();

        IndexMetadata backingIndex1 = DataStreamTestHelper.createFirstBackingIndex(dataStreamWithFailureStoreEnabled, testTime).build();
        IndexMetadata backingIndex2 = DataStreamTestHelper.createFirstBackingIndex(dataStreamWithFailureStoreDefault, testTime).build();
        IndexMetadata backingIndex3 = DataStreamTestHelper.createFirstBackingIndex(dataStreamWithFailureStoreDisabled, testTime).build();
        IndexMetadata failureStoreIndex1 = DataStreamTestHelper.createFirstFailureStore(dataStreamWithFailureStoreEnabled, testTime)
            .build();

        ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .dataStreams(
                Map.of(
                    dataStreamWithFailureStoreEnabled,
                    DataStreamTestHelper.newInstance(
                        dataStreamWithFailureStoreEnabled,
                        List.of(backingIndex1.getIndex()),
                        1L,
                        Map.of(),
                        false,
                        null,
                        List.of(),
                        DataStreamOptions.FAILURE_STORE_ENABLED
                    ),
                    dataStreamWithFailureStoreDefault,
                    DataStreamTestHelper.newInstance(
                        dataStreamWithFailureStoreDefault,
                        List.of(backingIndex2.getIndex()),
                        1L,
                        Map.of(),
                        false,
                        null,
                        List.of(),
                        DataStreamOptions.EMPTY
                    ),
                    dataStreamWithFailureStoreDisabled,
                    DataStreamTestHelper.newInstance(
                        dataStreamWithFailureStoreDisabled,
                        List.of(backingIndex3.getIndex()),
                        1L,
                        Map.of(),
                        false,
                        null,
                        List.of(),
                        DataStreamOptions.FAILURE_STORE_DISABLED
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
                    backingIndex3.getIndex().getName(),
                    backingIndex3,
                    failureStoreIndex1.getIndex().getName(),
                    failureStoreIndex1
                )
            )
            .build();

        // Data stream with failure store should store failures
        assertThat(bulkAction.resolveFailureInternal(dataStreamWithFailureStoreEnabled, projectMetadata, testTime), is(true));
        // Data stream with the default failure store options should not...
        assertThat(bulkAction.resolveFailureInternal(dataStreamWithFailureStoreDefault, projectMetadata, testTime), is(false));
        // ...unless we change the cluster setting to enable it that way.
        clusterSettings.applySettings(
            Settings.builder()
                .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), dataStreamWithFailureStoreDefault)
                .build()
        );
        assertThat(bulkAction.resolveFailureInternal(dataStreamWithFailureStoreDefault, projectMetadata, testTime), is(true));
        // Data stream with failure store explicitly disabled should not store failures even if it matches the cluster setting
        clusterSettings.applySettings(
            Settings.builder()
                .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), dataStreamWithFailureStoreDisabled)
                .build()
        );
        assertThat(bulkAction.resolveFailureInternal(dataStreamWithFailureStoreDisabled, projectMetadata, testTime), is(false));
        // An index should not be considered for failure storage
        assertThat(bulkAction.resolveFailureInternal(backingIndex1.getIndex().getName(), projectMetadata, testTime), is(nullValue()));
        // even if that index is itself a failure store
        assertThat(bulkAction.resolveFailureInternal(failureStoreIndex1.getIndex().getName(), projectMetadata, testTime), is(nullValue()));
    }

    public void testResolveFailureStoreFromTemplate() throws Exception {
        String dsTemplateWithFailureStoreEnabled = "test-data-stream-failure-enabled";
        String dsTemplateWithFailureStoreDefault = "test-data-stream-failure-default";
        String dsTemplateWithFailureStoreDisabled = "test-data-stream-failure-disabled";
        String indexTemplate = "test-index";
        long testTime = randomMillisUpToYear9999();

        ProjectMetadata projectMetadata = ProjectMetadata.builder(randomProjectIdOrDefault())
            .indexTemplates(
                Map.of(
                    dsTemplateWithFailureStoreEnabled,
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dsTemplateWithFailureStoreEnabled + "-*"))
                        .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true)))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build(),
                    dsTemplateWithFailureStoreDefault,
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dsTemplateWithFailureStoreDefault + "-*"))
                        .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(null)))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build(),
                    dsTemplateWithFailureStoreDisabled,
                    ComposableIndexTemplate.builder()
                        .indexPatterns(List.of(dsTemplateWithFailureStoreDisabled + "-*"))
                        .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(false)))
                        .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                        .build(),
                    indexTemplate,
                    ComposableIndexTemplate.builder().indexPatterns(List.of(indexTemplate + "-*")).build()
                )
            )
            .build();

        // Data stream with failure store should store failures
        assertThat(bulkAction.resolveFailureInternal(dsTemplateWithFailureStoreEnabled + "-1", projectMetadata, testTime), is(true));
        // Same if date math is used
        assertThat(
            bulkAction.resolveFailureInternal("<" + dsTemplateWithFailureStoreEnabled + "-{now}>", projectMetadata, testTime),
            is(true)
        );
        // Data stream with the default failure store options should not...
        assertThat(bulkAction.resolveFailureInternal(dsTemplateWithFailureStoreDefault + "-1", projectMetadata, testTime), is(false));
        assertThat(
            bulkAction.resolveFailureInternal("<" + dsTemplateWithFailureStoreDefault + "-{now}>", projectMetadata, testTime),
            is(false)
        );
        // ...unless we change the cluster setting to enable it that way.
        clusterSettings.applySettings(
            Settings.builder()
                .put(
                    DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(),
                    dsTemplateWithFailureStoreDefault + "*"
                )
                .build()
        );
        assertThat(bulkAction.resolveFailureInternal(dsTemplateWithFailureStoreDefault + "-1", projectMetadata, testTime), is(true));
        assertThat(
            bulkAction.resolveFailureInternal("<" + dsTemplateWithFailureStoreDefault + "-{now}>", projectMetadata, testTime),
            is(true)
        );
        // Data stream with failure store explicitly disabled should not store failures even if it matches the cluster setting
        clusterSettings.applySettings(
            Settings.builder()
                .put(
                    DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(),
                    dsTemplateWithFailureStoreDisabled + "*"
                )
                .build()
        );
        assertThat(bulkAction.resolveFailureInternal(dsTemplateWithFailureStoreDisabled + "-1", projectMetadata, testTime), is(false));
        assertThat(
            bulkAction.resolveFailureInternal("<" + dsTemplateWithFailureStoreDisabled + "-{now}>", projectMetadata, testTime),
            is(false)
        );
        // An index template should not be considered for failure storage
        assertThat(bulkAction.resolveFailureInternal(indexTemplate + "-1", projectMetadata, testTime), is(nullValue()));
    }

    /**
     * This test asserts that any failing prerequisite action that fails (i.e. index creation or data stream/failure store rollover)
     * results in a failed response.
     */
    public void testFailuresDuringPrerequisiteActions() throws InterruptedException {
        // One request for testing a failure during index creation.
        BulkRequest bulkRequest = new BulkRequest().add(new IndexRequest("index").source(Map.of()))
            // One request for testing a failure during data stream rollover.
            .add(new IndexRequest("data-stream").source(Map.of()))
            // One request for testing a failure during failure store rollover.
            .add(new IndexRequest("failure-store").source(Map.of()).setWriteToFailureStore(true));

        // Construct a cluster state that contains the required data streams.
        // using a single, non-default project
        final ClusterState oldState = clusterService.state();
        final ProjectId projectId = randomUniqueProjectId();
        final Metadata metadata = Metadata.builder(oldState.metadata())
            .removeProject(Metadata.DEFAULT_PROJECT_ID)
            .put(
                ProjectMetadata.builder(projectId)
                    .put(indexMetadata(".ds-data-stream-01"))
                    .put(indexMetadata(".ds-failure-store-01"))
                    .put(indexMetadata(".fs-failure-store-01"))
                    .put(
                        DataStream.builder(
                            "data-stream",
                            DataStream.DataStreamIndices.backingIndicesBuilder(List.of(new Index(".ds-data-stream-01", randomUUID())))
                                .setRolloverOnWrite(true)
                                .build()
                        ).build()
                    )
                    .put(
                        DataStream.builder("failure-store", List.of(new Index(".ds-failure-store-01", randomUUID())))
                            .setFailureIndices(
                                DataStream.DataStreamIndices.failureIndicesBuilder(List.of(new Index(".fs-failure-store-01", randomUUID())))
                                    .setRolloverOnWrite(true)
                                    .build()
                            )
                            .build()
                    )
            )
            .build();
        final ClusterState clusterState = ClusterState.builder(oldState)
            .metadata(metadata)
            .routingTable(GlobalRoutingTableTestHelper.buildRoutingTable(metadata, RoutingTable.Builder::addAsNew))
            .build();

        // Apply the cluster state.
        CountDownLatch latch = new CountDownLatch(1);
        clusterService.getClusterApplierService()
            .onNewClusterState("set-state", () -> clusterState, ActionListener.running(latch::countDown));
        // And wait for it to be applied.
        latch.await(10L, TimeUnit.SECONDS);

        activeProjectId.set(projectId);
        // Set the exceptions that the transport action should encounter.
        bulkAction.failIndexCreationException = new IndexNotFoundException("index");
        bulkAction.failDataStreamRolloverException = new RuntimeException("data-stream-rollover-exception");
        bulkAction.failFailureStoreRolloverException = new RuntimeException("failure-store-rollover-exception");

        // Execute the action and get the response.
        PlainActionFuture<BulkResponse> future = new PlainActionFuture<>();
        ActionTestUtils.execute(bulkAction, null, bulkRequest, future);
        BulkResponse response = future.actionGet();
        assertEquals(3, response.getItems().length);

        var indexFailure = response.getItems()[0];
        assertTrue(indexFailure.isFailed());
        assertTrue(indexFailure.getFailure().getCause() instanceof IndexNotFoundException);
        assertNull(bulkRequest.requests.get(0));

        var dataStreamFailure = response.getItems()[1];
        assertTrue(dataStreamFailure.isFailed());
        assertEquals("data-stream-rollover-exception", dataStreamFailure.getFailure().getCause().getMessage());
        assertNull(bulkRequest.requests.get(1));

        var failureStoreFailure = response.getItems()[2];
        assertTrue(failureStoreFailure.isFailed());
        assertEquals("failure-store-rollover-exception", failureStoreFailure.getFailure().getCause().getMessage());
        assertNull(bulkRequest.requests.get(2));
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

    private static IndexMetadata.Builder indexMetadata(String index) {
        return IndexMetadata.builder(index).settings(settings(IndexVersion.current())).numberOfShards(1).numberOfReplicas(1);
    }
}
