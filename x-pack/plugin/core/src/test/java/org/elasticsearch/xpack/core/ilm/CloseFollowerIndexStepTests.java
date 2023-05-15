/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class CloseFollowerIndexStepTests extends AbstractStepTestCase<CloseFollowerIndexStep> {

    private static IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    public void testCloseFollowingIndex() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata();

        Mockito.doAnswer(invocation -> {
            CloseIndexRequest closeIndexRequest = (CloseIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            listener.onResponse(new CloseIndexResponse(true, true, Collections.emptyList()));
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f));
    }

    public void testRequestNotAcknowledged() {
        IndexMetadata indexMetadata = getIndexMetadata();

        Mockito.doAnswer(invocation -> {
            CloseIndexRequest closeIndexRequest = (CloseIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            listener.onResponse(new CloseIndexResponse(false, false, Collections.emptyList()));
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        Exception e = expectThrows(
            Exception.class,
            () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f))
        );
        assertThat(e.getMessage(), is("close index request failed to be acknowledged"));
    }

    public void testCloseFollowingIndexFailed() {
        IndexMetadata indexMetadata = getIndexMetadata();

        // Mock pause follow api call:
        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            CloseIndexRequest closeIndexRequest = (CloseIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[1];
            listener.onFailure(error);
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        assertSame(
            error,
            expectThrows(
                Exception.class,
                () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f))
            )
        );
        Mockito.verify(indicesClient).close(Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(indicesClient);
    }

    public void testCloseFollowerIndexIsNoopForAlreadyClosedIndex() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .state(IndexMetadata.State.CLOSE)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f));
        Mockito.verifyNoMoreInteractions(client);
    }

    @Override
    protected CloseFollowerIndexStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return new CloseFollowerIndexStep(stepKey, nextStepKey, client);
    }

    @Override
    protected CloseFollowerIndexStep mutateInstance(CloseFollowerIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
        }

        return new CloseFollowerIndexStep(key, nextKey, instance.getClient());
    }

    @Override
    protected CloseFollowerIndexStep copyInstance(CloseFollowerIndexStep instance) {
        return new CloseFollowerIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }
}
