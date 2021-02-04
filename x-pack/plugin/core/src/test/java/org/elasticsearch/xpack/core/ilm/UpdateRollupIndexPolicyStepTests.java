/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;


import org.apache.lucene.util.SetOnce;
import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ilm.AsyncActionStep.Listener;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import static org.hamcrest.Matchers.equalTo;

public class UpdateRollupIndexPolicyStepTests extends AbstractStepMasterTimeoutTestCase<UpdateRollupIndexPolicyStep> {

    @Override
    public UpdateRollupIndexPolicyStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        String rollupPolicy = randomAlphaOfLength(10);

        return new UpdateRollupIndexPolicyStep(stepKey, nextStepKey, client, rollupPolicy);
    }

    @Override
    public UpdateRollupIndexPolicyStep mutateInstance(UpdateRollupIndexPolicyStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        String rollupPolicy = instance.getRollupPolicy();

        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            rollupPolicy = randomAlphaOfLength(5);
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new UpdateRollupIndexPolicyStep(key, nextKey, client, rollupPolicy);
    }

    @Override
    public UpdateRollupIndexPolicyStep copyInstance(UpdateRollupIndexPolicyStep instance) {
        return new UpdateRollupIndexPolicyStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(),
            instance.getRollupPolicy());
    }

    @Override
    protected IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
    }

    public void testPerformAction() {
        IndexMetadata indexMetadata = getIndexMetadata();
        String rollupIndex = RollupStep.getRollupIndexName(indexMetadata.getIndex().getName());
        UpdateRollupIndexPolicyStep step = createRandomInstance();
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, step.getRollupPolicy()).build();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertThat(request.settings(), equalTo(settings));
            assertThat(request.indices(), equalTo(new String[] { rollupIndex }));
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();

        step.performAction(indexMetadata, emptyClusterState(), null, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                actionCompleted.set(complete);
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        });

        assertEquals(true, actionCompleted.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = getIndexMetadata();
        String rollupIndex = RollupStep.getRollupIndexName(indexMetadata.getIndex().getName());
        Exception exception = new RuntimeException();
        UpdateRollupIndexPolicyStep step = createRandomInstance();
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, step.getRollupPolicy()).build();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertThat(request.settings(), equalTo(settings));
            assertThat(request.indices(), equalTo(new String[] { rollupIndex }));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        SetOnce<Boolean> exceptionThrown = new SetOnce<>();
        step.performAction(indexMetadata, emptyClusterState(), null, new Listener() {

            @Override
            public void onResponse(boolean complete) {
                throw new AssertionError("Unexpected method call");
            }

            @Override
            public void onFailure(Exception e) {
                assertSame(exception, e);
                exceptionThrown.set(true);
            }
        });

        assertEquals(true, exceptionThrown.get());

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }
}
