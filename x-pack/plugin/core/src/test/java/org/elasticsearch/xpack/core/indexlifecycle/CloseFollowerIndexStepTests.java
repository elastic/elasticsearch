/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.IndicesAdminClient;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.indexlifecycle.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CloseFollowerIndexStepTests extends AbstractUnfollowIndexStepTestCase<CloseFollowerIndexStep> {

    @Override
    protected CloseFollowerIndexStep newInstance(Step.StepKey key, Step.StepKey nextKey, Client client) {
        return new CloseFollowerIndexStep(key, nextKey, client);
    }

    public void testCloseFollowingIndex() {
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
            CloseIndexRequest closeIndexRequest = (CloseIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            listener.onResponse(new AcknowledgedResponse(true));
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
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

    public void testCloseFollowingIndexFailed() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        // Mock pause follow api call:
        Client client = Mockito.mock(Client.class);
        AdminClient adminClient = Mockito.mock(AdminClient.class);
        Mockito.when(client.admin()).thenReturn(adminClient);
        IndicesAdminClient indicesClient = Mockito.mock(IndicesAdminClient.class);
        Mockito.when(adminClient.indices()).thenReturn(indicesClient);

        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            CloseIndexRequest closeIndexRequest = (CloseIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            ActionListener listener = (ActionListener) invocation.getArguments()[1];
            listener.onFailure(error);
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
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
        Mockito.verify(indicesClient).close(Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(indicesClient);
    }
}
