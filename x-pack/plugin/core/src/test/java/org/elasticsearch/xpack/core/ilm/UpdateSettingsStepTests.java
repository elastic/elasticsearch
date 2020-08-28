/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
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

public class UpdateSettingsStepTests extends AbstractStepMasterTimeoutTestCase<UpdateSettingsStep> {

    @Override
    public UpdateSettingsStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        Settings settings = Settings.builder().put(randomAlphaOfLength(10), randomAlphaOfLength(10)).build();

        return new UpdateSettingsStep(stepKey, nextStepKey, client, settings);
    }

    @Override
    public UpdateSettingsStep mutateInstance(UpdateSettingsStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        Settings settings = instance.getSettings();

        switch (between(0, 2)) {
        case 0:
            key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 1:
            nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            break;
        case 2:
            settings = Settings.builder().put(settings).put(randomAlphaOfLength(10), randomInt()).build();
            break;
        default:
            throw new AssertionError("Illegal randomisation branch");
        }

        return new UpdateSettingsStep(key, nextKey, client, settings);
    }

    @Override
    public UpdateSettingsStep copyInstance(UpdateSettingsStep instance) {
        return new UpdateSettingsStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getSettings());
    }

    @Override
    protected IndexMetadata getIndexMetadata() {
        return IndexMetadata.builder(randomAlphaOfLength(10)).settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
    }

    public void testPerformAction() {
        IndexMetadata indexMetadata = getIndexMetadata();

        UpdateSettingsStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertThat(request.settings(), equalTo(step.getSettings()));
            assertThat(request.indices(), equalTo(new String[] {indexMetadata.getIndex().getName()}));
            listener.onResponse(new AcknowledgedResponse(true));
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
        Exception exception = new RuntimeException();
        UpdateSettingsStep step = createRandomInstance();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertThat(request.settings(), equalTo(step.getSettings()));
            assertThat(request.indices(), equalTo(new String[] {indexMetadata.getIndex().getName()}));
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
