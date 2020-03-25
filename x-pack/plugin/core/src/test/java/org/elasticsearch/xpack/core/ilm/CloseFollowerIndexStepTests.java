/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.close.CloseIndexRequest;
import org.elasticsearch.action.admin.indices.close.CloseIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class CloseFollowerIndexStepTests extends AbstractStepMasterTimeoutTestCase<CloseFollowerIndexStep> {

    @Override
    protected IndexMetaData getIndexMetaData() {
        return IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    public void testCloseFollowingIndex() {
        IndexMetaData indexMetadata = getIndexMetaData();

        Mockito.doAnswer(invocation -> {
            CloseIndexRequest closeIndexRequest = (CloseIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<CloseIndexResponse> listener = (ActionListener<CloseIndexResponse>) invocation.getArguments()[1];
            listener.onResponse(new CloseIndexResponse(true, true, Collections.emptyList()));
            return null;
        }).when(indicesClient).close(Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, emptyClusterState(), null, new AsyncActionStep.Listener() {
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
        IndexMetaData indexMetadata = getIndexMetaData();

        // Mock pause follow api call:
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
        step.performAction(indexMetadata, emptyClusterState(), null, new AsyncActionStep.Listener() {
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

    public void testCloseFollowerIndexIsNoopForAlreadyClosedIndex() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .state(IndexMetaData.State.CLOSE)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        CloseFollowerIndexStep step = new CloseFollowerIndexStep(randomStepKey(), randomStepKey(), client);
        step.performAction(indexMetadata, null, null, new AsyncActionStep.Listener() {
            @Override
            public void onResponse(boolean complete) {
                assertThat(complete, is(true));
            }

            @Override
            public void onFailure(Exception e) {
            }
        });

        Mockito.verifyZeroInteractions(client);
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
            key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new CloseFollowerIndexStep(key, nextKey, instance.getClient());
    }

    @Override
    protected CloseFollowerIndexStep copyInstance(CloseFollowerIndexStep instance) {
        return new CloseFollowerIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }
}
