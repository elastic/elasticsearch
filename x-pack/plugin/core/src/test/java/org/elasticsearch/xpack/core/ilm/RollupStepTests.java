/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;
import org.elasticsearch.xpack.core.rollup.RollupActionConfigTests;
import org.elasticsearch.xpack.core.rollup.action.RollupAction;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.createTimestampField;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RollupStepTests extends AbstractStepTestCase<RollupStep> {

    @Override
    public RollupStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        RollupActionConfig config = RollupActionConfigTests.randomConfig(random());
        return new RollupStep(stepKey, nextStepKey, client, config);
    }

    @Override
    public RollupStep mutateInstance(RollupStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        return new RollupStep(key, nextKey, instance.getClient(), instance.getConfig());
    }

    @Override
    public RollupStep copyInstance(RollupStep instance) {
        return new RollupStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getConfig());
    }

    private IndexMetadata getIndexMetadata(String index) {
        Map<String, String> ilmCustom = Collections.singletonMap("rollup_index_name", "rollup-index");
        return IndexMetadata.builder(index).settings(
            settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "test-ilm-policy"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY, ilmCustom)
            .build();
    }

    private static void assertRollupActionRequest(RollupAction.Request request, String sourceIndex) {
        assertNotNull(request);
        assertThat(request.getSourceIndex(), equalTo(sourceIndex));
        assertThat(request.getRollupIndex(), equalTo("rollup-index"));
    }

    public void testPerformAction() throws Exception {
        String index = randomAlphaOfLength(5);
        IndexMetadata indexMetadata = getIndexMetadata(index);

        RollupStep step = createRandomInstance();

        mockClientRollupCall(index);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(indexMetadata, true)
            )
            .build();
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f));
    }

    public void testPerformActionFailureInvalidExecutionState() {
        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10)).settings(
            settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, "test-ilm-policy"))
            .numberOfShards(randomIntBetween(1, 5)).numberOfReplicas(randomIntBetween(0, 5))
            .build();
        String policyName = indexMetadata.getSettings().get(LifecycleSettings.LIFECYCLE_NAME);
        String indexName = indexMetadata.getIndex().getName();
        RollupStep step = createRandomInstance();
        step.performAction(indexMetadata, emptyClusterState(), null, new ActionListener<>() {
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

    public void testPerformActionOnDataStream() throws Exception {
        String dataStreamName = "test-datastream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata indexMetadata = getIndexMetadata(backingIndexName);

        RollupStep step = createRandomInstance();

        mockClientRollupCall(backingIndexName);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(
                Metadata.builder()
                    .put(new DataStream(dataStreamName, createTimestampField("@timestamp"),
                        List.of(indexMetadata.getIndex())))
                    .put(indexMetadata, true)
            )
            .build();
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f));
    }

    private void mockClientRollupCall(String sourceIndex) {
        Mockito.doAnswer(invocation -> {
            RollupAction.Request request = (RollupAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertRollupActionRequest(request, sourceIndex);
            listener.onResponse(AcknowledgedResponse.of(true));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
