/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.List;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class DeleteStepTests extends AbstractStepMasterTimeoutTestCase<DeleteStep> {

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
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new DeleteStep(key, nextKey, instance.getClient());
    }

    @Override
    public DeleteStep copyInstance(DeleteStep instance) {
        return new DeleteStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    protected IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
    }

    public void testIndexSurvives() {
        assertFalse(createRandomInstance().indexSurvives());
    }

    public void testDeleted() {
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
        }).when(indicesClient).delete(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        DeleteStep step = createRandomInstance();
        ClusterState clusterState = ClusterState.builder(emptyClusterState()).metadata(
            Metadata.builder().put(indexMetadata, true).build()
        ).build();
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError(e);
            }
        });

        assertThat(actionCompleted.get(), equalTo(true));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).delete(Mockito.any(), Mockito.any());
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
        }).when(indicesClient).delete(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        DeleteStep step = createRandomInstance();
        ClusterState clusterState = ClusterState.builder(emptyClusterState()).metadata(
            Metadata.builder().put(indexMetadata, true).build()
        ).build();
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertEquals(exception, e);
                exceptionThrown.set(true);
            }
        });

        assertThat(exceptionThrown.get(), equalTo(true));
    }

    public void testPerformActionThrowsExceptionIfIndexIsTheDataStreamWriteIndex() {
        String dataStreamName = randomAlphaOfLength(10);
        String indexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        String policyName = "test-ilm-policy";
        IndexMetadata sourceIndexMetadata =
            IndexMetadata.builder(indexName).settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, policyName))
                .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        DataStream dataStream =
            new DataStream(dataStreamName, createTimestampField("@timestamp"), List.of(sourceIndexMetadata.getIndex()));
        ClusterState clusterState = ClusterState.builder(emptyClusterState()).metadata(
            Metadata.builder().put(sourceIndexMetadata, true).put(dataStream).build()
        ).build();

        IllegalStateException illegalStateException = expectThrows(IllegalStateException.class,
            () -> createRandomInstance().performDuringNoSnapshot(sourceIndexMetadata, clusterState, new AsyncActionStep.Listener() {
                @Override
                public void onResponse(boolean complete) {
                    fail("unexpected listener callback");
                }

                @Override
                public void onFailure(Exception e) {
                    fail("unexpected listener callback");
                }
            }));
        assertThat(illegalStateException.getMessage(),
            is("index [" + indexName + "] is the write index for data stream [" + dataStreamName + "]. stopping execution of lifecycle" +
                " [test-ilm-policy] as a data stream's write index cannot be deleted. manually rolling over the index will resume the " +
                "execution of the policy as the index will not be the data stream's write index anymore"));
    }
}
