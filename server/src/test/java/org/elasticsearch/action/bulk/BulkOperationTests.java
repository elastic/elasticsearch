/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.bulk;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.ActionType;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.delete.DeleteResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
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
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.Template;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.CheckedFunction;
import org.elasticsearch.index.IndexNotFoundException;
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
import org.junit.Assume;
import org.junit.Before;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
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

    private final DataStream dataStream1 = DataStreamTestHelper.newInstance(
        dataStreamName,
        List.of(ds1BackingIndex1.getIndex(), ds1BackingIndex2.getIndex())
    );
    private final DataStream dataStream2 = DataStreamTestHelper.newInstance(
        fsDataStreamName,
        List.of(ds2BackingIndex1.getIndex()),
        List.of(ds2FailureStore1.getIndex())
    );

    private final ClusterState DEFAULT_STATE = ClusterState.builder(ClusterName.DEFAULT)
        .metadata(
            Metadata.builder()
                .indexTemplates(
                    Map.of(
                        "ds-template",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(dataStreamName))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false, false))
                            .template(new Template(null, null, null, null))
                            .build(),
                        "ds-template-with-failure-store",
                        ComposableIndexTemplate.builder()
                            .indexPatterns(List.of(fsDataStreamName))
                            .dataStreamTemplate(new ComposableIndexTemplate.DataStreamTemplate(false, false, true))
                            .template(new Template(null, null, null, null))
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
                        ds2FailureStore1
                    )
                )
                .dataStreams(Map.of(dataStreamName, dataStream1, fsDataStreamName, dataStream2), Map.of())
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
        NodeClient client = getNodeClient((r) -> {
            fail("Should not have executed shard action on blocked cluster");
            return null;
        });

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        // Not retryable
        ClusterState state = ClusterState.builder(DEFAULT_STATE)
            .blocks(ClusterBlocks.builder().addGlobalBlock(Metadata.CLUSTER_READ_ONLY_BLOCK).build())
            .build();

        // Make sure we don't wait at all
        ClusterStateObserver observer = mock(ClusterStateObserver.class);
        when(observer.setAndGetObservedState()).thenReturn(state);
        when(observer.isTimedOut()).thenReturn(false);
        doThrow(new AssertionError("Should not wait")).when(observer).waitForNextChange(any());

        newBulkOperation(state, client, new BulkRequest(), new AtomicArray<>(0), Map.of(), observer, listener).run();

        expectThrows(ExecutionException.class, ClusterBlockException.class, future::get);
    }

    /**
     * If a bulk operation times out while waiting for cluster blocks to be cleared, it should fail the request.
     */
    public void testTimeoutOnRetryableClusterBlockedFailsBulk() {
        NodeClient client = getNodeClient((r) -> {
            fail("Should not have executed shard action on blocked cluster");
            return null;
        });

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

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

        newBulkOperation(state, client, new BulkRequest(), new AtomicArray<>(0), Map.of(), observer, listener).run();

        expectThrows(ExecutionException.class, ClusterBlockException.class, future::get);
        verify(observer, times(2)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * If the cluster service closes while a bulk operation is waiting for cluster blocks to be cleared, it should fail the request.
     */
    public void testNodeClosedOnRetryableClusterBlockedFailsBulk() {
        NodeClient client = getNodeClient((r) -> {
            fail("Should not have executed shard action on blocked cluster");
            return null;
        });

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

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

        newBulkOperation(state, client, new BulkRequest(), new AtomicArray<>(0), Map.of(), observer, listener).run();

        expectThrows(ExecutionException.class, NodeClosedException.class, future::get);
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

        NodeClient client = getNodeClient(this::acceptAllShardWrites);

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
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
            failingShards(Map.of(new ShardId(indexMetadata.getIndex(), 0), () -> new MapperException("test")))
        );

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
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

        NodeClient client = getNodeClient(this::acceptAllShardWrites);

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
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
            failingShards(Map.of(new ShardId(ds1BackingIndex2.getIndex(), 0), () -> new MapperException("test")))
        );

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
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
        Assume.assumeTrue(DataStream.isFailureStoreEnabled());

        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            failingShards(Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), () -> new MapperException("test")))
        );

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
        assertThat(bulkItemResponses.hasFailures(), is(false));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(item -> item.getIndex().equals(ds2FailureStore1.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem, is(notNullValue()));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled should redirect any documents that fail at a shard level to the
     * failure store.
     */
    public void testFailingDocumentRedirectsToFailureStore() throws Exception {
        Assume.assumeTrue(DataStream.isFailureStoreEnabled());

        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        NodeClient client = getNodeClient(
            thatFailsDocuments(Map.of(new IndexAndId(ds2BackingIndex1.getIndex().getName(), "3"), () -> new MapperException("test")))
        );

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
        assertThat(bulkItemResponses.hasFailures(), is(false));
        BulkItemResponse failedItem = Arrays.stream(bulkItemResponses.getItems())
            .filter(item -> item.getIndex().equals(ds2FailureStore1.getIndex().getName()))
            .findFirst()
            .orElseThrow(() -> new AssertionError("Could not find redirected item"));
        assertThat(failedItem.getIndex(), is(notNullValue()));
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may still partially fail if the redirected documents experience
     * a shard-level failure while writing to the failure store indices.
     */
    public void testFailureStoreShardFailureRejectsDocument() throws Exception {
        Assume.assumeTrue(DataStream.isFailureStoreEnabled());

        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            failingShards(
                Map.of(
                    new ShardId(ds2BackingIndex1.getIndex(), 0),
                    () -> new MapperException("root cause"),
                    new ShardId(ds2FailureStore1.getIndex(), 0),
                    () -> new MapperException("failure store test failure")
                )
            )
        );

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(client, bulkRequest, listener).run();

        BulkResponse bulkItemResponses = future.get();
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
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may still partially fail if the cluster is experiencing a
     * non-retryable block when the redirected documents would be sent to the shard-level action.
     */
    public void testBlockedClusterRejectsFailureStoreDocument() throws Exception {
        Assume.assumeTrue(DataStream.isFailureStoreEnabled());

        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            failingShards(Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), () -> new MapperException("root cause")))
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

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(DEFAULT_STATE, client, bulkRequest, new AtomicArray<>(bulkRequest.numberOfActions()), Map.of(), observer, listener)
            .run();

        BulkResponse bulkItemResponses = future.get();
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

        verify(observer, times(0)).isTimedOut();
        verify(observer, times(0)).waitForNextChange(any());
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may still partially fail if the cluster times out while waiting for a
     * retryable block to clear when the redirected documents would be sent to the shard-level action.
     */
    public void testOperationTimeoutRejectsFailureStoreDocument() throws Exception {
        Assume.assumeTrue(DataStream.isFailureStoreEnabled());

        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            failingShards(Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), () -> new MapperException("root cause")))
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

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(DEFAULT_STATE, client, bulkRequest, new AtomicArray<>(bulkRequest.numberOfActions()), Map.of(), observer, listener)
            .run();

        BulkResponse bulkItemResponses = future.get();
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

        verify(observer, times(2)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * A bulk operation to a data stream with a failure store enabled may completely fail if the cluster service closes out while waiting
     * for a retryable block to clear when the redirected documents would be sent to the shard-level action.
     */
    public void testNodeClosureRejectsFailureStoreDocument() {
        Assume.assumeTrue(DataStream.isFailureStoreEnabled());

        // Requests that go to two separate shards
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("1").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));
        bulkRequest.add(new IndexRequest(fsDataStreamName).id("3").source(Map.of("key", "val")).opType(DocWriteRequest.OpType.CREATE));

        // Mock client that rejects all shard requests on the first shard in the backing index, and all requests to the only shard of
        // the failure store index.
        NodeClient client = getNodeClient(
            failingShards(Map.of(new ShardId(ds2BackingIndex1.getIndex(), 0), () -> new MapperException("root cause")))
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

        CompletableFuture<BulkResponse> future = new CompletableFuture<>();
        ActionListener<BulkResponse> listener = ActionListener.wrap(future::complete, future::completeExceptionally);

        newBulkOperation(DEFAULT_STATE, client, bulkRequest, new AtomicArray<>(bulkRequest.numberOfActions()), Map.of(), observer, listener)
            .run();

        expectThrows(ExecutionException.class, NodeClosedException.class, future::get);

        verify(observer, times(1)).isTimedOut();
        verify(observer, times(1)).waitForNextChange(any());
    }

    /**
     * Accepts all write operations from the given request object when it is encountered in the mock shard bulk action
     */
    private BulkShardResponse acceptAllShardWrites(BulkShardRequest request) {
        return new BulkShardResponse(
            request.shardId(),
            Arrays.stream(request.items()).map(item -> requestToResponse(request.shardId(), item)).toArray(BulkItemResponse[]::new)
        );
    }

    /**
     * Maps an entire shard id to an exception to throw when it is encountered in the mock shard bulk action
     */
    private CheckedFunction<BulkShardRequest, BulkShardResponse, Exception> failingShards(Map<ShardId, Supplier<Exception>> shardsToFail) {
        return (BulkShardRequest request) -> {
            if (shardsToFail.containsKey(request.shardId())) {
                throw shardsToFail.get(request.shardId()).get();
            } else {
                return acceptAllShardWrites(request);
            }
        };
    }

    /**
     * Index name / id tuple
     */
    private record IndexAndId(String indexName, String id) {}

    /**
     * Maps a document to an exception to thrown when it is encountered in the mock shard bulk action
     */
    private CheckedFunction<BulkShardRequest, BulkShardResponse, Exception> thatFailsDocuments(
        Map<IndexAndId, Supplier<Exception>> documentsToFail
    ) {
        return (BulkShardRequest request) -> new BulkShardResponse(request.shardId(), Arrays.stream(request.items()).map(item -> {
            IndexAndId key = new IndexAndId(request.index(), item.request().id());
            if (documentsToFail.containsKey(key)) {
                return requestToFailedResponse(item, documentsToFail.get(key).get());
            } else {
                return requestToResponse(request.shardId(), item);
            }
        }).toArray(BulkItemResponse[]::new));
    }

    /**
     * Create a shard-level result given a bulk item
     */
    private static BulkItemResponse requestToResponse(ShardId shardId, BulkItemRequest itemRequest) {
        return BulkItemResponse.success(itemRequest.id(), itemRequest.request().opType(), switch (itemRequest.request().opType()) {
            case INDEX, CREATE -> new IndexResponse(shardId, itemRequest.request().id(), 1, 1, 1, true);
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
     * @param onShardAction Called when TransportShardBulkAction is executed.
     * @return A node client for the test.
     */
    private NodeClient getNodeClient(CheckedFunction<BulkShardRequest, BulkShardResponse, Exception> onShardAction) {
        return new NoOpNodeClient(threadPool) {
            @Override
            @SuppressWarnings("unchecked")
            public <Request extends ActionRequest, Response extends ActionResponse> Task executeLocally(
                ActionType<Response> action,
                Request request,
                ActionListener<Response> listener
            ) {
                if (TransportShardBulkAction.TYPE.equals(action)) {
                    Response response = null;
                    Exception exception = null;
                    try {
                        response = (Response) onShardAction.apply((BulkShardRequest) request);
                    } catch (Exception responseException) {
                        exception = responseException;
                    }
                    if (response != null) {
                        listener.onResponse(response);
                    } else {
                        listener.onFailure(exception);
                    }
                } else {
                    fail("Unexpected client call to " + action.name());
                }
                return null;
            }
        };
    }

    private BulkOperation newBulkOperation(NodeClient client, BulkRequest request, ActionListener<BulkResponse> listener) {
        return newBulkOperation(
            DEFAULT_STATE,
            client,
            request,
            new AtomicArray<>(request.numberOfActions()),
            Map.of(),
            mockObserver(DEFAULT_STATE),
            listener
        );
    }

    private BulkOperation newBulkOperation(
        ClusterState state,
        NodeClient client,
        BulkRequest request,
        AtomicArray<BulkItemResponse> existingResponses,
        Map<String, IndexNotFoundException> indicesThatCanNotBeCreated,
        ClusterStateObserver observer,
        ActionListener<BulkResponse> listener
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
            ThreadPool.Names.SAME,
            clusterService,
            request,
            client,
            existingResponses,
            indicesThatCanNotBeCreated,
            indexNameExpressionResolver,
            () -> endTime,
            timeZero,
            listener,
            observer
        );
    }

    /**
     * A default mock cluster state observer that simply returns the state
     */
    private ClusterStateObserver mockObserver(ClusterState state) {
        ClusterStateObserver mockObserver = mock(ClusterStateObserver.class);
        when(mockObserver.setAndGetObservedState()).thenReturn(state);
        when(mockObserver.isTimedOut()).thenReturn(false);
        return mockObserver;
    }
}
