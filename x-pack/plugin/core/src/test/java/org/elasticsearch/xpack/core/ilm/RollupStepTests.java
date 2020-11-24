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
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.v2.RollupAction;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.v2.RollupActionConfigTests;
import org.mockito.Mockito;

import java.util.List;

import static org.elasticsearch.cluster.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;

public class RollupStepTests extends AbstractStepTestCase<RollupStep> {

    @Override
    public RollupStep createRandomInstance() {
        if (randomBoolean()) {
            return createRandomInstanceWithPolicy();
        }
        return createRandomInstanceWithoutPolicy();
    }

    private RollupStep createRandomInstanceWithoutPolicy() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        RollupActionConfig config = RollupActionConfigTests.randomConfig(random());
        return new RollupStep(stepKey, nextStepKey, client, config, null);
    }

    private RollupStep createRandomInstanceWithPolicy() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        RollupActionConfig config = RollupActionConfigTests.randomConfig(random());
        return new RollupStep(stepKey, nextStepKey, client, config, randomAlphaOfLength(5));
    }

    @Override
    public RollupStep mutateInstance(RollupStep instance) {
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
                rollupPolicy = randomAlphaOfLength(3);
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new RollupStep(key, nextKey, instance.getClient(), instance.getConfig(), rollupPolicy);
    }

    @Override
    public RollupStep copyInstance(RollupStep instance) {
        return new RollupStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(),
            instance.getConfig(), instance.getRollupPolicy());
    }

    private IndexMetadata getIndexMetadata(String index) {
        return IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();
    }

    private static void assertRollupActionRequest(RollupAction.Request request, String sourceIndex) {
        assertNotNull(request);
        assertThat(request.getSourceIndex(), equalTo(sourceIndex));
    }

    public void testPerformAction() {
        String index = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = getIndexMetadata(index);

        RollupStep step = createRandomInstanceWithoutPolicy();

        mockClientRollupCall(index);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
            )
            .build();
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {

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
    }

    public void testPerformActionWithPolicy() {
        String index = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = getIndexMetadata(index);
        IndexMetadata rollupIndexMetadata = getIndexMetadata(index + RollupStep.ROLLUP_INDEX_NAME_POSTFIX);

        RollupStep step = createRandomInstanceWithPolicy();

        Mockito.doAnswer(invocation -> {
            UpdateSettingsRequest request = (UpdateSettingsRequest) invocation.getArguments()[0];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[1];
            Settings expectedSettings = Settings.builder().put(LifecycleSettings.LIFECYCLE_NAME, step.getRollupPolicy()).build();
            assertThat(request.settings(), equalTo(expectedSettings));
            assertThat(request.indices(), equalTo(new String[] {rollupIndexMetadata.getIndex().getName()}));
            listener.onResponse(AcknowledgedResponse.TRUE);
            return null;
        }).when(indicesClient).updateSettings(Mockito.any(), Mockito.any());

        Mockito.doAnswer(invocation -> {
            RollupAction.Request request = (RollupAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<RollupAction.Response> listener = (ActionListener<RollupAction.Response>) invocation.getArguments()[2];
            assertRollupActionRequest(request, index);
            listener.onResponse(new RollupAction.Response(true));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
            )
            .build();
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {

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

        Mockito.verify(client, Mockito.times(1)).execute(Mockito.any(), Mockito.any(), Mockito.any());
        Mockito.verify(client, Mockito.times(1)).admin();
        Mockito.verify(adminClient, Mockito.only()).indices();
        Mockito.verify(indicesClient, Mockito.only()).updateSettings(Mockito.any(), Mockito.any());
    }

    public void testPerformActionOnDataStream() {
        String dataStreamName = "test-datastream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata indexMetadata = IndexMetadata.builder(backingIndexName)
            .settings(settings(Version.CURRENT))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5)).build();

        RollupStep step = createRandomInstanceWithoutPolicy();

        mockClientRollupCall(backingIndexName);

        SetOnce<Boolean> actionCompleted = new SetOnce<>();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(new DataStream(dataStreamName, createTimestampField("@timestamp"),
                        List.of(indexMetadata.getIndex())))
                    .put(indexMetadata, true)
            )
            .build();
        step.performAction(indexMetadata, clusterState, null, new AsyncActionStep.Listener() {

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
    }

    private void mockClientRollupCall(String sourceIndex) {
        Mockito.doAnswer(invocation -> {
            RollupAction.Request request = (RollupAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<RollupAction.Response> listener = (ActionListener<RollupAction.Response>) invocation.getArguments()[2];
            assertRollupActionRequest(request, sourceIndex);
            listener.onResponse(new RollupAction.Response(true));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
