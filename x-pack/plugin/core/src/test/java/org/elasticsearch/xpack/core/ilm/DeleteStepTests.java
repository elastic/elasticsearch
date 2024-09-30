/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.datastreams.DeleteDataStreamAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamTestHelper;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.test.ActionListenerUtils.anyActionListener;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;

public class DeleteStepTests extends AbstractStepTestCase<DeleteStep> {

    @Override
    public DeleteStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();

        return new DeleteStep(stepKey, nextStepKey, client);
    }

    @Override
    public DeleteStep mutateInstance(DeleteStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new DeleteStep(key, nextKey, instance.getClient());
    }

    @Override
    public DeleteStep copyInstance(DeleteStep instance) {
        return new DeleteStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    private static IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
    }

    public void testIndexSurvives() {
        assertFalse(createRandomInstance().indexSurvives());
    }

    public void testDeleted() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata();

        Mockito.doAnswer(invocation -> {
            DeleteIndexRequest request = (DeleteIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertNotNull(request);
            assertEquals(1, request.indices().length);
            assertEquals(indexMetadata.getIndex().getName(), request.indices()[0]);
            listener.onResponse(null);
            return null;
        }).when(indicesClient).delete(any(), any());

        DeleteStep step = createRandomInstance();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();
        performActionAndWait(step, indexMetadata, clusterState, null);

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).delete(any(), any());
    }

    public void testExceptionThrown() {
        IndexMetadata indexMetadata = getIndexMetadata();
        Exception exception = new RuntimeException();

        Mockito.doAnswer(invocation -> {
            DeleteIndexRequest request = (DeleteIndexRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertNotNull(request);
            assertEquals(1, request.indices().length);
            assertEquals(indexMetadata.getIndex().getName(), request.indices()[0]);
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).delete(any(), any());

        DeleteStep step = createRandomInstance();
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();
        assertSame(exception, expectThrows(Exception.class, () -> performActionAndWait(step, indexMetadata, clusterState, null)));
    }

    public void testPerformActionCallsFailureListenerIfIndexIsTheDataStreamWriteIndex() {
        doThrow(
            new IllegalStateException(
                "the client must not be called in this test as we should fail in the step validation phase before we call the delete API"
            )
        ).when(indicesClient).delete(any(DeleteIndexRequest.class), anyActionListener());

        String policyName = "test-ilm-policy";
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();

        IndexMetadata index1;
        {
            String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
            index1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }
        IndexMetadata sourceIndexMetadata;
        {
            String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 2, ts);
            sourceIndexMetadata = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }
        IndexMetadata failureIndex1;
        {
            String indexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
            failureIndex1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }
        IndexMetadata failureSourceIndexMetadata;
        {
            String indexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
            failureSourceIndexMetadata = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }

        DataStream dataStream = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(index1.getIndex(), sourceIndexMetadata.getIndex()),
            List.of(failureIndex1.getIndex(), failureSourceIndexMetadata.getIndex())
        );
        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(index1, false)
                    .put(sourceIndexMetadata, false)
                    .put(failureIndex1, false)
                    .put(failureSourceIndexMetadata, false)
                    .put(dataStream)
                    .build()
            )
            .build();

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        final boolean useFailureStore = randomBoolean();
        final IndexMetadata indexToOperateOn = useFailureStore ? failureSourceIndexMetadata : sourceIndexMetadata;
        createRandomInstance().performDuringNoSnapshot(indexToOperateOn, clusterState, new ActionListener<>() {
            @Override
            public void onResponse(Void complete) {
                listenerCalled.set(true);
                fail("unexpected listener callback");
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalled.set(true);
                assertThat(
                    e.getMessage(),
                    is(
                        "index ["
                            + indexToOperateOn.getIndex().getName()
                            + "] is the "
                            + (useFailureStore ? "failure store " : "")
                            + "write index for data stream ["
                            + dataStreamName
                            + "]. stopping execution of lifecycle [test-ilm-policy] as a data stream's write index cannot be deleted. "
                            + "manually rolling over the index will resume the execution of the policy as the index will not be the "
                            + "data stream's write index anymore"
                    )
                );
            }
        });

        assertThat(listenerCalled.get(), is(true));
    }

    public void testDeleteWorksIfWriteIndexIsTheOnlyIndexInDataStream() throws Exception {
        String policyName = "test-ilm-policy";
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();

        // Single backing index
        IndexMetadata index1;
        {
            String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
            index1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }

        DataStream dataStream = DataStreamTestHelper.newInstance(dataStreamName, List.of(index1.getIndex()), List.of());

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(index1, false).put(dataStream).build())
            .build();

        Mockito.doAnswer(invocation -> {
            DeleteDataStreamAction.Request request = (DeleteDataStreamAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertNotNull(request);
            assertEquals(1, request.getNames().length);
            assertEquals(dataStreamName, request.getNames()[0]);
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        // Try on the normal data stream - It should delete the data stream
        DeleteStep step = createRandomInstance();
        performActionAndWait(step, index1, clusterState, null);

        Mockito.verify(client, Mockito.only()).execute(any(), any(), any());
        Mockito.verify(adminClient, Mockito.never()).indices();
        Mockito.verify(indicesClient, Mockito.never()).delete(any(), any());
    }

    public void testDeleteWorksIfWriteIndexIsTheOnlyIndexInDataStreamWithFailureStore() throws Exception {
        String policyName = "test-ilm-policy";
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();

        // Single backing index
        IndexMetadata index1;
        {
            String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
            index1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }

        // Multiple failure indices
        IndexMetadata failureIndex1;
        {
            String indexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
            failureIndex1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }
        IndexMetadata failureSourceIndexMetadata;
        {
            String indexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
            failureSourceIndexMetadata = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }

        DataStream dataStreamWithFailureIndices = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(index1.getIndex()),
            List.of(failureIndex1.getIndex(), failureSourceIndexMetadata.getIndex())
        );

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(index1, false)
                    .put(failureIndex1, false)
                    .put(failureSourceIndexMetadata, false)
                    .put(dataStreamWithFailureIndices)
                    .build()
            )
            .build();

        Mockito.doAnswer(invocation -> {
            DeleteDataStreamAction.Request request = (DeleteDataStreamAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertNotNull(request);
            assertEquals(1, request.getNames().length);
            assertEquals(dataStreamName, request.getNames()[0]);
            listener.onResponse(null);
            return null;
        }).when(client).execute(any(), any(), any());

        // Again, the deletion should work since the data stream would be fully deleted anyway if the failure store were disabled.
        DeleteStep step = createRandomInstance();
        performActionAndWait(step, index1, clusterState, null);

        Mockito.verify(client, Mockito.only()).execute(any(), any(), any());
        Mockito.verify(adminClient, Mockito.never()).indices();
        Mockito.verify(indicesClient, Mockito.never()).delete(any(), any());
    }

    public void testDeletingFailureStoreWriteIndexOnDataStreamWithSingleBackingIndex() {
        doThrow(
            new IllegalStateException(
                "the client must not be called in this test as we should fail in the step validation phase before we call the delete API"
            )
        ).when(indicesClient).delete(any(DeleteIndexRequest.class), anyActionListener());

        String policyName = "test-ilm-policy";
        String dataStreamName = randomAlphaOfLength(10);
        long ts = System.currentTimeMillis();

        // Single backing index
        IndexMetadata index1;
        {
            String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1, ts);
            index1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }

        // Multiple failure indices
        IndexMetadata failureIndex1;
        {
            String indexName = DataStream.getDefaultFailureStoreName(dataStreamName, 1, ts);
            failureIndex1 = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }
        IndexMetadata failureSourceIndexMetadata;
        {
            String indexName = DataStream.getDefaultFailureStoreName(dataStreamName, 2, ts);
            failureSourceIndexMetadata = IndexMetadata.builder(indexName)
                .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5))
                .numberOfReplicas(randomIntBetween(0, 5))
                .build();
        }

        DataStream dataStreamWithFailureIndices = DataStreamTestHelper.newInstance(
            dataStreamName,
            List.of(index1.getIndex()),
            List.of(failureIndex1.getIndex(), failureSourceIndexMetadata.getIndex())
        );

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(
                Metadata.builder()
                    .put(index1, false)
                    .put(failureIndex1, false)
                    .put(failureSourceIndexMetadata, false)
                    .put(dataStreamWithFailureIndices)
                    .build()
            )
            .build();

        AtomicBoolean listenerCalled = new AtomicBoolean(false);
        createRandomInstance().performDuringNoSnapshot(failureSourceIndexMetadata, clusterState, new ActionListener<>() {
            @Override
            public void onResponse(Void complete) {
                listenerCalled.set(true);
                fail("unexpected listener callback");
            }

            @Override
            public void onFailure(Exception e) {
                listenerCalled.set(true);
                assertThat(
                    e.getMessage(),
                    is(
                        "index ["
                            + failureSourceIndexMetadata.getIndex().getName()
                            + "] is the failure store write index for data stream ["
                            + dataStreamName
                            + "]. stopping execution of lifecycle [test-ilm-policy] as a data stream's write index cannot be deleted. "
                            + "manually rolling over the index will resume the execution of the policy as the index will not be the "
                            + "data stream's write index anymore"
                    )
                );
            }
        });

        assertThat(listenerCalled.get(), is(true));
    }
}
