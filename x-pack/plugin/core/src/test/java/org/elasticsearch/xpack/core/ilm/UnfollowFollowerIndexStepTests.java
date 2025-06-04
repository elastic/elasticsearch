/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;
import org.mockito.Mockito;

import java.util.Map;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class UnfollowFollowerIndexStepTests extends AbstractUnfollowIndexStepTestCase<UnfollowFollowerIndexStep> {

    @Override
    protected UnfollowFollowerIndexStep newInstance(Step.StepKey key, Step.StepKey nextKey) {
        return new UnfollowFollowerIndexStep(key, nextKey, client);
    }

    public void testUnFollow() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Mockito.doAnswer(invocation -> {
            UnfollowAction.Request request = (UnfollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowerIndex(), equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(client).execute(Mockito.same(UnfollowAction.INSTANCE), Mockito.any(), Mockito.any());

        UnfollowFollowerIndexStep step = new UnfollowFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        performActionAndWait(step, indexMetadata, null, null);
    }

    public void testRequestNotAcknowledged() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Mockito.doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(AcknowledgedResponse.FALSE);
            return null;
        }).when(client).execute(Mockito.same(UnfollowAction.INSTANCE), Mockito.any(), Mockito.any());

        UnfollowFollowerIndexStep step = new UnfollowFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        Exception e = expectThrows(Exception.class, () -> performActionAndWait(step, indexMetadata, null, null));
        assertThat(e.getMessage(), is("unfollow request failed to be acknowledged"));
    }

    public void testUnFollowUnfollowFailed() {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Mock unfollow api call:
        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            UnfollowAction.Request request = (UnfollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowerIndex(), equalTo("follower-index"));
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(error);
            return null;
        }).when(client).execute(Mockito.same(UnfollowAction.INSTANCE), Mockito.any(), Mockito.any());

        UnfollowFollowerIndexStep step = new UnfollowFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        assertSame(error, expectThrows(RuntimeException.class, () -> performActionAndWait(step, indexMetadata, null, null)));
    }

    public void testFailureToReleaseRetentionLeases() throws Exception {
        IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Map.of())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Mock unfollow api call:
        ElasticsearchException error = new ElasticsearchException("text exception");
        error.addMetadata("es.failed_to_remove_retention_leases", randomAlphaOfLength(10));
        Mockito.doAnswer(invocation -> {
            UnfollowAction.Request request = (UnfollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowerIndex(), equalTo("follower-index"));
            ActionListener<?> listener = (ActionListener<?>) invocation.getArguments()[2];
            listener.onFailure(error);
            return null;
        }).when(client).execute(Mockito.same(UnfollowAction.INSTANCE), Mockito.any(), Mockito.any());

        UnfollowFollowerIndexStep step = new UnfollowFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        performActionAndWait(step, indexMetadata, null, null);
    }
}
