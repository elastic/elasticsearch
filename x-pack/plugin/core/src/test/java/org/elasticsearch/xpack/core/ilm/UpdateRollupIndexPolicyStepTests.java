/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;


import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class UpdateRollupIndexPolicyStepTests extends AbstractStepTestCase<UpdateRollupIndexPolicyStep> {

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

    private static IndexMetadata getIndexMetadata() {
        Map<String, String> ilmCustom = Collections.singletonMap("rollup_index_name", "rollup-index");
        return IndexMetadata.builder(randomAlphaOfLength(10)).settings(
            settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "test-ilm-policy"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .build();
    }

    public void testPerformAction() throws Exception {
        IndexMetadata indexMetadata = getIndexMetadata();
        UpdateRollupIndexPolicyStep step = createRandomInstance();
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, step.getRollupPolicy()).build();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertThat(request.settings(), equalTo(settings));
            assertThat(request.indices(), equalTo(new String[] { "rollup-index" }));
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, emptyClusterState(), null, f));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailure() {
        IndexMetadata indexMetadata = getIndexMetadata();
        ClusterState clusterState =
            ClusterState.builder(emptyClusterState()).metadata(Metadata.builder().put(indexMetadata, true).build()).build();
        Exception exception = new RuntimeException();
        UpdateRollupIndexPolicyStep step = createRandomInstance();
        Settings settings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, step.getRollupPolicy()).build();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            assertThat(request.settings(), equalTo(settings));
            assertThat(request.indices(), equalTo(new String[] { "rollup-index" }));
            listener.onFailure(exception);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        assertSame(exception, expectThrows(Exception.class,
            () -> PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f))));

        Mockito.verify(client, Mockito.only()).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionFailureIllegalExecutionState() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(
            settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "test-ilm-policy"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .build();
        String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        String indexName = indexMetadata.getIndex().getName();
        UpdateRollupIndexPolicyStep step = createRandomInstance();
        step.performAction(indexMetadata, emptyClusterState(), null, new ActionListener<Void>() {
            @Override
            public void onResponse(Void unused) {
                fail("expecting a failure as the index doesn't have any rollup index name in its ILM execution state");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(IllegalStateException.class));
                assertThat(e.getMessage(),
                    is("rollup index name was not generated for policy [" + policyName + "] and index [" + indexName + "]"));
            }
        });
    }
}
