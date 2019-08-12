/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.xpack.core.ccr.action.PauseFollowAction;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class PauseFollowerIndexStepTests extends AbstractUnfollowIndexStepTestCase<PauseFollowerIndexStep> {

    @Override
    protected PauseFollowerIndexStep newInstance(Step.StepKey key, Step.StepKey nextKey, Client client) {
        return new PauseFollowerIndexStep(key, nextKey, client);
    }

    public void testPauseFollowingIndex() {
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
            PauseFollowAction.Request request = (PauseFollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowIndex(), equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        PauseFollowerIndexStep step = new PauseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, null, null, new AsyncActionStep.Listener() {
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

    public void testPauseFollowingIndexFailed() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Mock pause follow api call:
        Client client = Mockito.mock(Client.class);
        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            PauseFollowAction.Request request = (PauseFollowAction.Request) invocation.getArguments()[1];
            assertThat(request.getFollowIndex(), equalTo("follower-index"));
            ActionListener listener = (ActionListener) invocation.getArguments()[2];
            listener.onFailure(error);
            return null;
        }).when(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        PauseFollowerIndexStep step = new PauseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, null, null, new AsyncActionStep.Listener() {
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
        Mockito.verify(client).execute(Mockito.same(PauseFollowAction.INSTANCE), Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(client);
    }
}
