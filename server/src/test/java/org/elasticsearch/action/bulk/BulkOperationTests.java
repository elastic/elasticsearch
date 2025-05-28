/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.rollover.LazyRolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.SubscribableListener;
import org.elasticsearch.action.update.UpdateResponse;
import org.elasticsearch.client.internal.node.NodeClient;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateObserver;
import org.elasticsearch.cluster.block.ClusterBlockException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.coordination.NoMasterBlockService;
import org.elasticsearch.cluster.metadata.ComposableIndexTemplate;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamFailureStoreSettings;
import org.elasticsearch.cluster.metadata.DataStreamOptions;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.EsExecutors;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.MapperException;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.node.NodeClosedException;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.client.NoOpNodeClient;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Supplier;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BulkOperationTests extends ESTestCase {

    private final long millis = randomMillisUpToYear9999();
    private final String indexName = "my_index";
    private final String dataStreamName = "my_data_stream";
    private final String fsDataStreamName = "my_failure_store_data_stream";
    private final String fsRolloverDataStreamName = "my_failure_store_to_be_rolled_over_data_stream";
    private final String fsBySettingsDataStreamName = "my_failure_store_enabled_by_setting_data_stream";

    private final IndexMetadata indexMetadata = IndexMetadata.builder(indexName)
        .settings(
            Settings.builder()
                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                .build()
        )
        .build();
    private final IndexMetadata ds1BackingIndex1 = DataStreamTestHelper.createBackingIndex(dataStreamName, 1, millis)
        .numberOfShards(2)
        .build();
    private final IndexMetadata ds1BackingIndex2 = DataStreamTestHelper.createBackingIndex(dataStreamName, 2, millis + 1)
        .numberOfShards(2)
        .build();
    private final IndexMetadata ds2BackingIndex1 = DataStreamTestHelper.createBackingIndex(fsDataStreamName, 1, millis)
        .numberOfShards(2)
        .build();
    private final IndexMetadata ds2FailureStore1 = DataStreamTestHelper.createFailureStore(fsDataStreamName, 1, millis)
        .numberOfShards(1)
        .build();
    private final IndexMetadata ds3BackingIndex1 = DataStreamTestHelper.createBackingIndex(fsRolloverDataStreamName, 1, millis)
        .numberOfShards(2)
        .build();
    private final IndexMetadata ds3FailureStore1 = DataStreamTestHelper.createFailureStore(fsRolloverDataStreamName, 1, millis)
        .numberOfShards(1)
        .build();
    private final IndexMetadata ds3FailureStore2 = DataStreamTestHelper.createFailureStore(fsRolloverDataStreamName, 2, millis)
        .numberOfShards(1)
        .build();
    private final IndexMetadata ds4BackingIndex1 = DataStreamTestHelper.createBackingIndex(fsBySettingsDataStreamName, 1, millis)
        .numberOfShards(2)
        .build();
    private final IndexMetadata ds4FailureStore1 = DataStreamTestHelper.createFailureStore(fsBySettingsDataStreamName, 1, millis)
        .numberOfShards(1)
        .build();

    private final DataStream dataStream1 = DataStreamTestHelper.newInstance(
        dataStreamName,
        List.of(ds1BackingIndex1.getIndex(), ds1BackingIndex2.getIndex())
    );
    private final DataStream dataStream2 = DataStreamTestHelper.newInstance(
        fsDataStreamName,
        List.of(ds2BackingIndex1.getIndex()),
        List.of(ds2FailureStore1.getIndex())
    );
    private final DataStream dataStream3 = DataStream.builder(fsRolloverDataStreamName, List.of(ds3BackingIndex1.getIndex()))
        .setGeneration(1)
        .setDataStreamOptions(DataStreamOptions.FAILURE_STORE_ENABLED)
        .setFailureIndices(
            DataStream.DataStreamIndices.failureIndicesBuilder(List.of(ds3FailureStore1.getIndex())).setRolloverOnWrite(true).build()
        )
        .build();
    private final DataStream dataStream4 = DataStream.builder(fsBySettingsDataStreamName, List.of(ds4BackingIndex1.getIndex()))
        .setGeneration(1)
        .setDataStreamOptions(DataStreamOptions.EMPTY)
        .setFailureIndices(DataStream.DataStreamIndices.failureIndicesBuilder(List.of(ds4FailureStore1.getIndex())).build())
        .build();

    private final ClusterState DEFAULT_STATE = ClusterState.builder(ClusterName.DEFAULT)
        .metadata(
            Metadata.builder()
                .indexTemplates(
                    Map.of(
                        "ds-template",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(dataStreamName))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(false)))
                            .build(),
                        "ds-template-with-failure-store",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(fsDataStreamName, fsRolloverDataStreamName))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate())
                            .template(Template.builder().dataStreamOptions(DataStreamTestHelper.createDataStreamOptionsTemplate(true)))
                            .build()
                    )
                )
                .indices(
                    Map.of(
                        indexName,
                        indexMetadata,
                        ds1BackingIndex1.getIndex().getName(),
                        ds1BackingIndex1,
                        ds1BackingIndex2.getIndex().getName(),
                        ds1BackingIndex2,
                        ds2BackingIndex1.getIndex().getName(),
                        ds2BackingIndex1,
                        ds2FailureStore1.getIndex().getName(),
                        ds2FailureStore1,
                        ds3BackingIndex1.getIndex().getName(),
                        ds3BackingIndex1,
                        ds3FailureStore1.getIndex().getName(),
                        ds3FailureStore1,
                        ds4BackingIndex1.getIndex().getName(),
                        ds4BackingIndex1,
                        ds4FailureStore1.getIndex().getName(),
                        ds4FailureStore1
                    )
                )
                .dataStreams(
                    Map.of(
                        dataStreamName,
                        dataStream1,
                        fsDataStreamName,
                        dataStream2,
                        fsRolloverDataStreamName,
                        dataStream3,
                        fsBySettingsDataStreamName,
                        dataStream4
                    ),
                    Map.of()
                )
                .build()
        )
        .build();

    private TestThreadPool threadPool;

    @Before
    public void setupThreadpool() {
        threadPool = new TestThreadPool(getClass().getName());
    }

    @After
    public void tearDownThreadpool() {
        terminate(threadPool);
    }

    /**
     * If a bulk operation begins and the cluster is experiencing a non-retryable block, the bulk operation should fail
     */
    public void testClusterBlockedFailsBulk() {
        NodeClient client = getNodeClient(assertNoClientInteraction());

        // Not retryable
        ClusterState state = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK).build())
            .build();

        // Make sure we don't wait at all
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(state);
        when(observer.isTimedOut()).thenReturn(false);
        doThrow(new AssertionError("Should not wait")).when(observer).waitForNextChange(any());

        assertThat(
            safeAwaitFailure(BulkResponse.class, l -> newBulkOperation(client, new BulkRequest(), state, observer, l).run()),
            instanceOf(ClusterBlockException.class)
        );
    }

    /**
     * If a bulk operation times out while waiting for cluster blocks to be cleared, it should fail the request.
     */
    public void testTimeoutOnRetryableClusterBlockedFailsBulk() {
        NodeClient client = getNodeClient(assertNoClientInteraction());

        // Retryable
        final ClusterState state = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build())
            .build();

        // Always return cluster state, first observation: return same cluster state, second observation: time out, ensure no further wait
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(state);
        when(observer.isTimedOut()).thenReturn(false, true);
        doAnswer((i) -> {
            // Returning same state or timing out will result in one more attempt.
            if (randomBoolean()) {
                i.getArgument(0, ClusterStateObserver.Listener.class).onNewClusterState(state);
            } else {
                i.getArgument(0, ClusterStateObserver.Listener.class).onTimeout(null);
            }
            return null;
        }).doThrow(new AssertionError("Should not wait")).when(observer).waitForNextChange(any());

        assertThat(
            safeAwaitFailure(BulkResponse.class, l -> newBulkOperation(client, new BulkRequest(), state, observer, l).run()),
            instanceOf(ClusterBlockException.class)
        );

        verify(observer, times(2)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * If the cluster service closes while a bulk operation is waiting for cluster blocks to be cleared, it should fail the request.
     */
    public void testNodeClosedOnRetryableClusterBlockedFailsBulk() {
        NodeClient client = getNodeClient(assertNoClientInteraction());

        // Retryable
        final ClusterState state = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build())
            .build();

        // Always return cluster state, first observation: signal cluster service closed, ensure no further wait
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(state);
        when(observer.isTimedOut()).thenReturn(false);
        doAnswer((i) -> {
            i.getArgument(0, ClusterStateObserver.Listener.class).onClusterServiceClose();
            return null;
        }).doThrow(new AssertionError("Should not wait")).when(observer).waitForNextChange(any());

        assertThat(
            safeAwaitFailure(BulkResponse.class, l -> newBulkOperation(client, new BulkRequest(), state, observer, l).run()),
            instanceOf(NodeClosedException.class)
        );
        verify(observer, times(1)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * A bulk operation to an index should succeed if all of its shard level requests succeed
     */
    public void testBulkToIndex() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(indexName).id("1").source(Map.of("key", "val")));
        bulkRequest.add(new IndexRequest(indexName).id("3").source(Map.of("key", "val")));

        NodeClient client = getNodeClient(acceptAllShardWrites());

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(false));
    }

    /**
     * A bulk operation to an index should partially succeed if only some of its shard level requests fail
     */
    public void testBulkToIndexFailingEntireShard() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(indexName).id("1").source(Map.of("key", "val")));
        bulkRequest.add(new IndexRequest(indexName).id("3").source(Map.of("key", "val")));

        NodeClient client = getNodeClient(
            shardSpecificResponse(Map.of(new ShardId(indexMetadata.getIndex(), 0), failWithException(() -> new MapperException("test"))))
        );

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find failed item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("test")));
    }

    /**
     * A bulk operation to a data stream should succeed if all of its shard level requests succeed
     */
    public void testBulkToDataStream() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(dataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(dataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(acceptAllShardWrites());

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(false));
    }

    /**
     * A bulk operation to a data stream should partially succeed if only some of its shard level requests fail
     */
    public void testBulkToDataStreamFailingEntireShard() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(dataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(dataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            shardSpecificResponse(Map.of(new ShardId(ds1BackingIndex2.getIndex(), 0), failWithException(() -> new MapperException("test"))))
        );

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find failed item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("test")));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled should redirect any shard level failures to the failure store.
     */
    public void testFailingEntireShardRedirectsToFailureStore() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            shardSpecificResponse(Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), failWithException(() -> new MapperException("test"))))
        );

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(false));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(item -> item.getIndex().equals(ds2FailureStore1.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem, is(notNullValue()));
        // Ensure the status in the successful response gets through
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.USED));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled should redirect any documents that fail at a shard level to the
     * failure store.
     */
    public void testFailingDocumentRedirectsToFailureStore() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            thatFailsDocuments(Map.of(new IndexAndId(ds2BackingIndex1.getIndex().getName(), "3"), () -> new MapperException("test")))
        );

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(false));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(item -> item.getIndex().equals(ds2FailureStore1.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getIndex(), is(notNullValue()));
        // Ensure the status in the successful response gets through
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.USED));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled should NOT redirect any documents that fail at a shard level to the
     * failure store if the failure store node feature is not on every node in the cluster
     */
    public void testFailingDocumentIgnoredByFailureStoreWhenFeatureIsDisabled() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            thatFailsDocuments(Map.of(new IndexAndId(ds2BackingIndex1.getIndex().getName(), "3"), () -> new MapperException("test")))
        );

        BulkResponse bulkItemResponses = safeAwait(
            l -> newBulkOperation(
                DEFAULT_STATE,
                client,
                bulkRequest,
                new AtomicArray<>(bulkRequest.numberOfActions()),
                mockObserver(DEFAULT_STATE),
                l,
                new FailureStoreDocumentConverter(),
                DataStreamFailureStoreSettings.create(ClusterSettings.createBuiltInClusterSettings()),
                false
            ).run()
        );
        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("test")));
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN));
    }

    public void testFailingDocumentRedirectsToFailureStoreWhenEnabledByClusterSetting() {
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(fsBySettingsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE)
        );

        NodeClient client = getNodeClient(
            thatFailsDocuments(
                Map.of(new IndexAndId(ds4BackingIndex1.getIndex().getName(), "1"), () -> new MapperException("mapping go boom"))
            )
        );
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings = DataStreamFailureStoreSettings.create(
            ClusterSettings.createBuiltInClusterSettings(
                Settings.builder()
                    .put(DataStreamFailureStoreSettings.DATA_STREAM_FAILURE_STORED_ENABLED_SETTING.getKey(), fsBySettingsDataStreamName)
                    .build()
            )
        );

        // Without the cluster setting, this bulk request should fail:
        BulkResponse bulkItemResponsesWithoutClusterSetting = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponsesWithoutClusterSetting.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponsesWithoutClusterSetting.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("mapping go boom")));

        // With a cluster setting to enable the failure store for this data stream, the same request should be redirected:
        BulkResponse bulkItemResponsesUsingClusterSetting = safeAwait(
            l -> newBulkOperation(
                DEFAULT_STATE,
                client,
                bulkRequest,
                new AtomicArray<>(bulkRequest.numberOfActions()),
                mockObserver(DEFAULT_STATE),
                l,
                new FailureStoreDocumentConverter(),
                dataStreamFailureStoreSettings,
                true
            ).run()
        );
        assertThat(bulkItemResponsesUsingClusterSetting.hasFailures(), is(false));
        BulkItemResponse redirectedItem = Arrays.stream(bulkItemResponsesUsingClusterSetting.getItems())
            .filter(item -> item.getIndex().equals(ds4FailureStore1.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(redirectedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.USED));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may still partially fail if the redirected documents experience
     * a shard-level failure while writing to the failure store indices.
     */
    public void testFailureStoreShardFailureRejectsDocument() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(
                    new ShardId(ds2BackingIndex1.getIndex(), 0),
                    failWithException(() -> new MapperException("root cause")),
                    new ShardId(ds2FailureStore1.getIndex(), 0),
                    failWithException(() -> new MapperException("failure store test failure"))
                )
            )
        );

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("root cause")));
        assertThat(failedItem.getFailure().getCause().getSuppressed().length, is(not(equalTo(0))));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0], is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0].getMessage(), is(equalTo("failure store test failure")));
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.FAILED));
    }

    /**
     * A document that fails at the shard level will be converted into a failure document if an applicable failure store is present.
     * In the unlikely case that the failure document cannot be created, the document will not be redirected to the failure store and
     * instead will simply report its original failure in the response, with the conversion failure present as a suppressed exception.
     */
    public void testFailedDocumentCanNotBeConvertedFails() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            thatFailsDocuments(Map.of(new IndexAndId(ds2BackingIndex1.getIndex().getName(), "3"), () -> new MapperException("root cause")))
        );

        // Mock a failure store document converter that always fails
        FailureStoreDocumentConverter mockConverter = mock(FailureStoreDocumentConverter.class);
        when(mockConverter.transformFailedRequest(any(), any(), any(), any())).thenThrow(new IOException("Could not serialize json"));

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, mockConverter, l).run());

        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("root cause")));
        assertThat(failedItem.getFailure().getCause().getSuppressed().length, is(not(equalTo(0))));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0], is(instanceOf(IOException.class)));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0].getMessage(), is(equalTo("Could not serialize json")));
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.FAILED));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled could still succeed if the cluster is experiencing a
     * retryable block when the redirected documents would be sent to the shard-level action. If the cluster state observer
     * returns an unblocked cluster, the redirection of failure documents should proceed and not return early.
     */
    public void testRetryableBlockAcceptsFailureStoreDocument() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // We want to make sure that going async during the write operation won't cause correctness
        // issues, so use a real executor for the test
        ExecutorService writeExecutor = threadPool.executor(ThreadPool.Names.WRITE);

        // Create a pair of countdown latches to synchronize our test code and the write operation we're testing:
        // One to notify the test that the write operation has been reached, and one for the test to signal that
        // the write operation should proceed
        CountDownLatch readyToPerformFailureStoreWrite = new CountDownLatch(1);
        CountDownLatch beginFailureStoreWrite = new CountDownLatch(1);

        // A mock client that:
        // 1) Rejects an entire shard level request for the backing index and
        // 2) When the followup write is submitted for the failure store, will go async and wait until the above latch is counted down
        // before accepting the request.
        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(
                    new ShardId(ds2BackingIndex1.getIndex(), 0),
                    failWithException(() -> new MapperException("root cause")),
                    new ShardId(ds2FailureStore1.getIndex(), 0),
                    goAsyncAndWait(writeExecutor, readyToPerformFailureStoreWrite, beginFailureStoreWrite, acceptAllShardWrites())
                )
            )
        );

        // Create a new cluster state that has a retryable cluster block on it
        ClusterState blockedState = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build())
            .build();

        // Cluster state observer logic:
        // First time we will return the normal cluster state (before normal writes) which skips any further interactions,
        // Second time we will return a blocked cluster state (before the redirects) causing us to start observing the cluster
        // Then, when waiting for next state change, we will emulate the observer receiving an unblocked state to continue the processing
        // Finally, third time we will return the normal cluster state again since the cluster will be "unblocked" after waiting
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(DEFAULT_STATE).thenReturn(blockedState).thenReturn(DEFAULT_STATE);
        when(observer.isTimedOut()).thenReturn(false);
        doAnswer(invocation -> {
            ClusterStateObserver.Listener l = invocation.getArgument(0);
            l.onNewClusterState(DEFAULT_STATE);
            return null;
        }).when(observer).waitForNextChange(any());

        final SubscribableListener<BulkResponse> responseListener = SubscribableListener.newForked(
            l -> newBulkOperation(client, bulkRequest, DEFAULT_STATE, observer, l).run()
        );

        // The operation will attempt to write the documents in the request, receive a failure, wait for a stable cluster state, and then
        // redirect the failed documents to the failure store. Wait for that failure store write to start:
        if (readyToPerformFailureStoreWrite.await(30, TimeUnit.SECONDS) == false) {
            // we're going to fail the test, but be a good citizen and unblock the other thread first
            beginFailureStoreWrite.countDown();
            fail("timed out waiting for failure store write operation to begin");
        }

        // Check to make sure there is no response yet
        if (responseListener.isDone()) {
            // we're going to fail the test, but be a good citizen and unblock the other thread first
            beginFailureStoreWrite.countDown();
            fail("bulk operation completed prematurely");
        }

        // Operation is still correctly in flight. Allow the write operation to continue
        beginFailureStoreWrite.countDown();

        // Await final result and verify
        BulkResponse bulkItemResponses = safeAwait(responseListener);
        assertThat(bulkItemResponses.hasFailures(), is(false));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(item -> item.getIndex().equals(ds2FailureStore1.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem, is(notNullValue()));
        // Ensure the status in the successful response gets through
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.USED));

        verify(observer, times(1)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may still partially fail if the cluster is experiencing a
     * non-retryable block when the redirected documents would be sent to the shard-level action.
     */
    public void testBlockedClusterRejectsFailureStoreDocument() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), failWithException(() -> new MapperException("root cause")))
            )
        );

        // Create a new cluster state that has a non-retryable cluster block on it
        ClusterState blockedState = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(IndexMetadata.INDEX_READ_ONLY_BLOCK).build())
            .build();

        // First time we will return the normal cluster state (before normal writes) which skips any further interactions,
        // Second time we will return a blocked cluster state (before the redirects) causing us to start observing the cluster
        // Finally, we will simulate the observer timing out causing the redirects to fail.
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(DEFAULT_STATE).thenReturn(blockedState);
        when(observer.isTimedOut()).thenReturn(false);
        doThrow(new AssertionError("Should not wait on non retryable block")).when(observer).waitForNextChange(any());

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, DEFAULT_STATE, observer, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("root cause")));
        assertThat(failedItem.getFailure().getCause().getSuppressed().length, is(not(equalTo(0))));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0], is(instanceOf(ClusterBlockException.class)));
        assertThat(
            failedItem.getFailure().getCause().getSuppressed()[0].getMessage(),
            is(equalTo("blocked by: [FORBIDDEN/5/index read-only (api)];"))
        );
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.FAILED));

        verify(observer, times(0)).isTimedOut();
        verify(observer, times(0)).waitForNextChange(any());
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may still partially fail if the cluster times out while waiting for a
     * retryable block to clear when the redirected documents would be sent to the shard-level action.
     */
    public void testOperationTimeoutRejectsFailureStoreDocument() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), failWithException(() -> new MapperException("root cause")))
            )
        );

        // Create a new cluster state that has a retryable cluster block on it
        ClusterState blockedState = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build())
            .build();

        // First time we will return the normal cluster state (before normal writes) which skips any further interactions,
        // Second time we will return a blocked cluster state (before the redirects) causing us to start observing the cluster
        // Finally, we will simulate the observer timing out causing the redirects to fail.
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(DEFAULT_STATE).thenReturn(blockedState);
        when(observer.isTimedOut()).thenReturn(false, true);
        doAnswer((i) -> {
            // Returning same state or timing out will result in one more attempt.
            if (randomBoolean()) {
                i.getArgument(0, ClusterStateObserver.Listener.class).onNewClusterState(blockedState);
            } else {
                i.getArgument(0, ClusterStateObserver.Listener.class).onTimeout(null);
            }
            return null;
        }).doThrow(new AssertionError("Should not wait any longer")).when(observer).waitForNextChange(any());

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, DEFAULT_STATE, observer, l).run());
        assertThat(bulkItemResponses.hasFailures(), is(true));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("root cause")));
        assertThat(failedItem.getFailure().getCause().getSuppressed().length, is(not(equalTo(0))));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0], is(instanceOf(ClusterBlockException.class)));
        assertThat(
            failedItem.getFailure().getCause().getSuppressed()[0].getMessage(),
            is(equalTo("blocked by: [SERVICE_UNAVAILABLE/2/no master];"))
        );
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.FAILED));

        verify(observer, times(2)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may completely fail if the cluster service closes out while waiting
     * for a retryable block to clear when the redirected documents would be sent to the shard-level action.
     */
    public void testNodeClosureRejectsFailureStoreDocument() {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), failWithException(() -> new MapperException("root cause")))
            )
        );

        // Create a new cluster state that has a retryable cluster block on it
        ClusterState blockedState = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_WRITES).build())
            .build();

        // First time we will return the normal cluster state (before normal writes) which skips any further interactions,
        // Second time we will return a blocked cluster state (before the redirects) causing us to start observing the cluster
        // Finally, we will simulate the node closing causing the redirects to fail.
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(DEFAULT_STATE).thenReturn(blockedState);
        when(observer.isTimedOut()).thenReturn(false, true);
        doAnswer((i) -> {
            i.getArgument(0, ClusterStateObserver.Listener.class).onClusterServiceClose();
            return null;
        }).doThrow(new AssertionError("Should not wait any longer")).when(observer).waitForNextChange(any());

        assertThat(
            safeAwaitFailure(BulkResponse.class, l -> newBulkOperation(client, bulkRequest, DEFAULT_STATE, observer, l).run()),
            instanceOf(NodeClosedException.class)
        );

        verify(observer, times(1)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * When a bulk operation needs to redirect some documents that failed on the shard level, and that failure store is marked for lazy
     * rollover, it first needs to roll over the failure store and then redirect the failure to the <i>new</i> failure index.
     */
    public void testLazilyRollingOverFailureStore() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(fsRolloverDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE)
        );
        bulkRequest.add(
            new IndexRequest(fsRolloverDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE)
        );

        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(new ShardId(ds3BackingIndex1.getIndex(), 0), failWithException(() -> new MapperException("test")))
            ),
            (rolloverRequest, actionListener) -> actionListener.onResponse(
                new RolloverResponse(
                    ds3FailureStore1.getIndex().getName(),
                    ds3FailureStore2.getIndex().getName(),
                    Map.of(),
                    false,
                    true,
                    true,
                    true,
                    false
                )
            )
        );

        DataStream rolledOverDataStream = dataStream3.copy()
            .setFailureIndices(
                dataStream3.getFailureComponent()
                    .copy()
                    .setIndices(List.of(ds3FailureStore1.getIndex(), ds3FailureStore2.getIndex()))
                    .build()
            )
            .build();
        Metadata metadata = Metadata.builder(DEFAULT_STATE.metadata())
            .indices(Map.of(ds3FailureStore2.getIndex().getName(), ds3FailureStore2))
            .put(rolledOverDataStream)
            .build();
        ClusterState rolledOverState = ClusterState.builder(DEFAULT_STATE).metadata(metadata).build();
        ClusterStateObserver observer = mockObserver(DEFAULT_STATE, DEFAULT_STATE, rolledOverState);

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, DEFAULT_STATE, observer, l).run());
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(item -> item.getIndex().equals(ds3FailureStore2.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem, is(notNullValue()));
        // Ensure the status in the successful response gets through
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.USED));
    }

    /**
     * When a bulk operation faces a failure while trying to roll over a failure store that was marked for lazy rollover, the exception
     * should be added to the list of suppressed causes in the <code>BulkItemResponse</code>.
     */
    public void testFailureWhileRollingOverFailureStore() throws Exception {
        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(
            new IndexRequest(fsRolloverDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE)
        );
        bulkRequest.add(
            new IndexRequest(fsRolloverDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE)
        );

        NodeClient client = getNodeClient(
            shardSpecificResponse(
                Map.of(new ShardId(ds3BackingIndex1.getIndex(), 0), failWithException(() -> new MapperException("test")))
            ),
            ((rolloverRequest, actionListener) -> actionListener.onFailure(new Exception("rollover failed")))
        );

        DataStream rolledOverDataStream = dataStream3.copy()
            .setFailureIndices(
                dataStream3.getFailureComponent()
                    .copy()
                    .setIndices(List.of(ds3FailureStore1.getIndex(), ds3FailureStore2.getIndex()))
                    .build()
            )
            .build();
        Metadata metadata = Metadata.builder(DEFAULT_STATE.metadata())
            .indices(Map.of(ds3FailureStore2.getIndex().getName(), ds3FailureStore2))
            .put(rolledOverDataStream)
            .build();
        ClusterState rolledOverState = ClusterState.builder(DEFAULT_STATE).metadata(metadata).build();
        ClusterStateObserver observer = mockObserver(DEFAULT_STATE, DEFAULT_STATE, rolledOverState);

        BulkResponse bulkItemResponses = safeAwait(l -> newBulkOperation(client, bulkRequest, DEFAULT_STATE, observer, l).run());
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(BulkItemResponse::isFailed)
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getFailure().getCause(), is(instanceOf(MapperException.class)));
        assertThat(failedItem.getFailure().getCause().getMessage(), is(equalTo("test")));
        assertThat(failedItem.getFailure().getCause().getSuppressed().length, is(not(equalTo(0))));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0], is(instanceOf(Exception.class)));
        assertThat(failedItem.getFailure().getCause().getSuppressed()[0].getMessage(), is(equalTo("rollover failed")));
        assertThat(failedItem.getFailureStoreStatus(), equalTo(IndexDocFailureStoreStatus.FAILED));
    }

    /**
     * Throws an assertion error with the given message if the client operation executes
     */
    private static BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> assertNoClientInteraction() {
        return (r, l) -> fail("Should not have executed shard action on blocked cluster");
    }

    /**
     * Accepts all write operations from the given request object when it is encountered in the mock shard bulk action
     */
    private static BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> acceptAllShardWrites() {
        return (BulkShardRequest request, ActionListener<BulkShardResponse> listener) -> listener.onResponse(
            new BulkShardResponse(
                request.shardId(),
                Arrays.stream(request.items()).map(item -> requestToResponse(request.shardId(), item)).toArray(BulkItemResponse[]::new)
            )
        );
    }

    /**
     * When the request is received, it is marked as failed with an exception created by the supplier
     */
    private BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> failWithException(Supplier<Exception> exceptionSupplier) {
        return (BulkShardRequest request, ActionListener<BulkShardResponse> listener) -> { listener.onFailure(exceptionSupplier.get()); };
    }

    /**
     * Maps an entire shard id to a consumer when it is encountered in the mock shard bulk action
     */
    private BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> shardSpecificResponse(
        Map<ShardId, BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>>> shardsToResponders
    ) {
        return (BulkShardRequest request, ActionListener<BulkShardResponse> listener) -> {
            if (shardsToResponders.containsKey(request.shardId())) {
                shardsToResponders.get(request.shardId()).accept(request, listener);
            } else {
                acceptAllShardWrites().accept(request, listener);
            }
        };
    }

    /**
     * When the consumer is called, it goes async on the given executor. It will signal that it has reached the operation by counting down
     * the readyLatch, then wait on the provided continueLatch before executing the delegate consumer.
     */
    private BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> goAsyncAndWait(
        Executor executor,
        CountDownLatch readyLatch,
        CountDownLatch continueLatch,
        BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> delegate
    ) {
        return (final BulkShardRequest request, final ActionListener<BulkShardResponse> listener) -> {
            executor.execute(() -> {
                try {
                    readyLatch.countDown();
                    if (continueLatch.await(30, TimeUnit.SECONDS) == false) {
                        listener.onFailure(new RuntimeException("Timeout in client operation waiting for test to signal a continuation"));
                    }
                } catch (InterruptedException e) {
                    listener.onFailure(new RuntimeException(e));
                }
                delegate.accept(request, listener);
            });
        };
    }

    /**
     * Index name / id tuple
     */
    private record IndexAndId(String indexName, String id) {}

    /**
     * Maps a document to an exception to thrown when it is encountered in the mock shard bulk action
     */
    private BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> thatFailsDocuments(
        Map<IndexAndId, Supplier<Exception>> documentsToFail
    ) {
        return (BulkShardRequest request, ActionListener<BulkShardResponse> listener) -> {
            listener.onResponse(new BulkShardResponse(request.shardId(), Arrays.stream(request.items()).map(item -> {
                IndexAndId key = new IndexAndId(request.index(), item.request().id());
                if (documentsToFail.containsKey(key)) {
                    return requestToFailedResponse(item, documentsToFail.get(key).get());
                } else {
                    return requestToResponse(request.shardId(), item);
                }
            }).toArray(BulkItemResponse[]::new)));
        };
    }

    /**
     * Create a shard-level result given a bulk item
     */
    private static BulkItemResponse requestToResponse(ShardId shardId, BulkItemRequest itemRequest) {
        var failureStatus = itemRequest.request() instanceof IndexRequest ir && ir.isWriteToFailureStore()
            ? IndexDocFailureStoreStatus.USED
            : IndexDocFailureStoreStatus.NOT_APPLICABLE_OR_UNKNOWN;
        return BulkItemResponse.success(itemRequest.id(), itemRequest.request().opType(), switch (itemRequest.request().opType()) {
            case INDEX, CREATE -> new IndexResponse(shardId, itemRequest.request().id(), 1, 1, 1, true, null, failureStatus);
            case UPDATE -> new UpdateResponse(shardId, itemRequest.request().id(), 1, 1, 1, DocWriteResponse.Result.UPDATED);
            case DELETE -> new DeleteResponse(shardId, itemRequest.request().id(), 1, 1, 1, true);
        });
    }

    /**
     * Create a shard-level failure given a bulk item
     */
    private static BulkItemResponse requestToFailedResponse(BulkItemRequest itemRequest, Exception reason) {
        return BulkItemResponse.failure(
            itemRequest.id(),
            itemRequest.request().opType(),
            new BulkItemResponse.Failure(itemRequest.index(), itemRequest.request().id(), reason)
        );
    }

    /**
     * Create a client that redirects expected actions to the provided function and fails if an unexpected operation happens.
     *
     * @param onShardAction Called when TransportShardBulkAction is executed.
     * @return A node client for the test.
     */
    private NodeClient getNodeClient(BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> onShardAction) {
        return getNodeClient(onShardAction, null);
    }

    /**
     * Create a client that redirects expected actions to the provided function and fails if an unexpected operation happens.
     *
     * @param onShardAction Called when TransportShardBulkAction is executed.
     * @return A node client for the test.
     */
    private NodeClient getNodeClient(
        BiConsumer<BulkShardRequest, ActionListener<BulkShardResponse>> onShardAction,
        BiConsumer<RolloverRequest, ActionListener<RolloverResponse>> onRolloverAction
    ) {
        return new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportShardBulkAction.TYPE.equals(action)) {
                    ActionListener<BulkShardResponse> notifyOnceListener = ActionListener.notifyOnce(
                        (ActionListener<BulkShardResponse>) listener
                    );
                    try {
                        onShardAction.accept((BulkShardRequest) request, notifyOnceListener);
                    } catch (Exception responseException) {
                        notifyOnceListener.onFailure(responseException);
                    }
                } else {
                    fail("Unexpected client call to " + action.name());
                }
                return null;
            }

            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> void doExecute(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (LazyRolloverAction.INSTANCE.equals(action)) {
                    ActionListener<RolloverResponse> notifyOnceListener = ActionListener.notifyOnce(
                        (ActionListener<RolloverResponse>) listener
                    );
                    try {
                        onRolloverAction.accept((RolloverRequest) request, notifyOnceListener);
                    } catch (Exception responseException) {
                        notifyOnceListener.onFailure(responseException);
                    }
                } else {
                    fail("Unexpected client call to " + action.name());
                }
            }
        };
    }

    private BulkOperation newBulkOperation(NodeClient client, BulkRequest request, ActionListener<BulkResponse> listener) {
        return newBulkOperation(
            DEFAULT_STATE,
            client,
            request,
            new AtomicArray<>(request.numberOfActions()),
            mockObserver(DEFAULT_STATE),
            listener,
            new FailureStoreDocumentConverter()
        );
    }

    private BulkOperation newBulkOperation(
        NodeClient client,
        BulkRequest request,
        FailureStoreDocumentConverter failureStoreDocumentConverter,
        ActionListener<BulkResponse> listener
    ) {
        return newBulkOperation(
            DEFAULT_STATE,
            client,
            request,
            new AtomicArray<>(request.numberOfActions()),
            mockObserver(DEFAULT_STATE),
            listener,
            failureStoreDocumentConverter
        );
    }

    private BulkOperation newBulkOperation(
        NodeClient client,
        BulkRequest request,
        ClusterState state,
        ClusterStateObserver observer,
        ActionListener<BulkResponse> listener
    ) {
        return newBulkOperation(
            state,
            client,
            request,
            new AtomicArray<>(request.numberOfActions()),
            observer,
            listener,
            new FailureStoreDocumentConverter()
        );
    }

    private BulkOperation newBulkOperation(
        ClusterState state,
        NodeClient client,
        BulkRequest request,
        AtomicArray<BulkItemResponse> existingResponses,
        ClusterStateObserver observer,
        ActionListener<BulkResponse> listener,
        FailureStoreDocumentConverter failureStoreDocumentConverter
    ) {
        return newBulkOperation(
            state,
            client,
            request,
            existingResponses,
            observer,
            listener,
            failureStoreDocumentConverter,
            DataStreamFailureStoreSettings.create(ClusterSettings.createBuiltInClusterSettings()),
            true
        );
    }

    private BulkOperation newBulkOperation(
        ClusterState state,
        NodeClient client,
        BulkRequest request,
        AtomicArray<BulkItemResponse> existingResponses,
        ClusterStateObserver observer,
        ActionListener<BulkResponse> listener,
        FailureStoreDocumentConverter failureStoreDocumentConverter,
        DataStreamFailureStoreSettings dataStreamFailureStoreSettings,
        boolean failureStoreNodeFeatureEnabled
    ) {
        // Time provision
        long timeZero = TimeUnit.MILLISECONDS.toNanos(randomMillisUpToYear9999() - TimeUnit.DAYS.toMillis(1));
        long duration = TimeUnit.SECONDS.toNanos(randomLongBetween(1, 60));
        long endTime = timeZero + duration;

        // Expressions
        ThreadContext ctx = threadPool.getThreadContext();
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(ctx, new SystemIndices(List.of()));

        // Mocks
        final DiscoveryNode mockNode = mock(DiscoveryNode.class);
        when(mockNode.getId()).thenReturn(randomAlphaOfLength(10));
        final ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.state()).thenReturn(state);
        when(clusterService.localNode()).thenReturn(mockNode);

        return new BulkOperation(
            null,
            threadPool,
            EsExecutors.DIRECT_EXECUTOR_SERVICE,
            clusterService,
            request,
            client,
            existingResponses,
            indexNameExpressionResolver,
            () -> endTime,
            timeZero,
            listener,
            observer,
            failureStoreDocumentConverter,
            FailureStoreMetrics.NOOP,
            dataStreamFailureStoreSettings,
            failureStoreNodeFeatureEnabled
        );
    }

    /**
     * A default mock cluster state observer that simply returns the state
     */
    private ClusterStateObserver mockObserver(ClusterState state, ClusterState... states) {
        ClusterStateObserver mockObserver = mock(ClusterStateObserver.class);
        when(mockObserver.setAndGetObservedState()).thenReturn(state, states);
        when(mockObserver.isTimedOut()).thenReturn(false);
        return mockObserver;
    }
}
