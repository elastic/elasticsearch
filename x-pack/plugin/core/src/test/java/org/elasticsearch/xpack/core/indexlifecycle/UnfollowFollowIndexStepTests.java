/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.xpack.core.ccr.action.UnfollowAction;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.indexlifecycle.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UnfollowFollowIndexStepTests extends AbstractUnfollowIndexStepTestCase<UnfollowFollowIndexStep> {

    @Override
    protected UnfollowFollowIndexStep newInstance(Step.StepKey key, Step.StepKey nextKey, Client client) {
        return new UnfollowFollowIndexStep(key, nextKey, client);
    }

    public void testUnFollow() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Mockito.doAnswer(invocation -> {
            UnfollowAction.Request request = (UnfollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowerIndex(), equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(client).execute(Mockito.same(UnfollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        UnfollowFollowIndexStep step = new UnfollowFollowIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], is(true));
        assertThat(failure[0], nullValue());
    }

    public void testUnFollowUnfollowFailed() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        // Mock unfollow api call:
        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            UnfollowAction.Request request = (UnfollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowerIndex(), equalTo("follower-index"));
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onFailure(error);
            return null;
        }).when(client).execute(Mockito.same(UnfollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        UnfollowFollowIndexStep step = new UnfollowFollowIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                completed[0] = complete;
            }

            @Override
            public void onFailure(Exception e) {
                failure[0] = e;
            }
        });
        assertThat(completed[0], nullValue());
        assertThat(failure[0], sameInstance(error));
    }
}
