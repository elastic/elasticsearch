/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.open.OpenIndexRequest;
import org.elasticsearch.action.admin.indices.open.OpenIndexResponse;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.mockito.Mockito;

import java.util.Collections;

import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class OpenFollowerIndexStepTests extends AbstractStepMasterTimeoutTestCase<OpenFollowerIndexStep> {

    @Override
    protected OpenFollowerIndexStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        return new OpenFollowerIndexStep(stepKey, nextStepKey, client);
    }

    @Override
    protected OpenFollowerIndexStep mutateInstance(OpenFollowerIndexStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        if (randomBoolean()) {
            key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        } else {
            nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
        }

        return new OpenFollowerIndexStep(key, nextKey, instance.getClient());
    }

    @Override
    protected OpenFollowerIndexStep copyInstance(OpenFollowerIndexStep instance) {
        return new OpenFollowerIndexStep(instance.getKey(), instance.getNextStepKey(), instance.getClient());
    }

    @Override
    protected IndexMetaData getIndexMetaData() {
        return IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .state(IndexMetaData.State.CLOSE)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
    }

    public void testOpenFollowerIndexIsNoopForAlreadyOpenIndex() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .state(IndexMetaData.State.OPEN)
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        OpenFollowerIndexStep step = new OpenFollowerIndexStep(randomStepKey(), randomStepKey(), client);
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

    public void testOpenFollowingIndex() {
        IndexMetaData indexMetadata = getIndexMetaData();

        Mockito.doAnswer(invocation -> {
            OpenIndexRequest closeIndexRequest = (OpenIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            @SuppressWarnings("unchecked")
            ActionListener<OpenIndexResponse> listener = (ActionListener<OpenIndexResponse>) invocation.getArguments()[1];
            listener.onResponse(new OpenIndexResponse(true, true));
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        OpenFollowerIndexStep step = new OpenFollowerIndexStep(randomStepKey(), randomStepKey(), client);
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

    public void testOpenFollowingIndexFailed() {
        IndexMetaData indexMetadata = IndexMetaData.builder("follower-index")
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true"))
            .state(IndexMetaData.State.CLOSE)
            .putCustom(CCR_METADATA_KEY, Collections.emptyMap())
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();

        Exception error = new RuntimeException();
        Mockito.doAnswer(invocation -> {
            OpenIndexRequest closeIndexRequest = (OpenIndexRequest) invocation.getArguments()[0];
            assertThat(closeIndexRequest.indices()[0], equalTo("follower-index"));
            ActionListener listener = (ActionListener) invocation.getArguments()[1];
            listener.onFailure(error);
            return null;
        }).when(indicesClient).open(Mockito.any(), Mockito.any());

        Boolean[] completed = new Boolean[1];
        Exception[] failure = new Exception[1];
        OpenFollowerIndexStep step = new OpenFollowerIndexStep(randomStepKey(), randomStepKey(), client);
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
        Mockito.verify(indicesClient).open(Mockito.any(), Mockito.any());
        Mockito.verifyNoMoreInteractions(indicesClient);
    }
}
