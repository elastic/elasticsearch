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
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class RollupStepTests extends AbstractStepTestCase<RollupStep> {

    @Override
    public RollupStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        return new RollupStep(stepKey, nextStepKey, client, fixedInterval);
    }

    @Override
    public RollupStep mutateInstance(RollupStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        DateHistogramInterval fixedInterval = instance.getFixedInterval();

        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
            case 2 -> fixedInterval = ConfigTestHelpers.randomInterval();
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new RollupStep(key, nextKey, instance.getClient(), fixedInterval);
    }

    @Override
    public RollupStep copyInstance(RollupStep instance) {
        return new RollupStep(instance.getKey(), instance.getNextStepKey(), instance.getClient(), instance.getFixedInterval());
    }

    private IndexMetadata getIndexMetadata(String index, String lifecycleName, RollupStep step) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().getPhase());
        lifecycleState.setAction(step.getKey().getAction());
        lifecycleState.setStep(step.getKey().getName());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        lifecycleState.setRollupIndexName("rollup-index");

        return IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
    }

    private static void assertRollupActionRequest(DownsampleAction.Request request, String sourceIndex) {
        assertNotNull(request);
        assertThat(request.getSourceIndex(), equalTo(sourceIndex));
        assertThat(request.getTargetIndex(), equalTo("rollup-index"));
    }

    public void testPerformAction() throws Exception {
        String lifecycleName = randomAlphaOfLength(5);
        RollupStep step = createRandomInstance();
        String index = randomAlphaOfLength(5);

        IndexMetadata indexMetadata = getIndexMetadata(index, lifecycleName, step);
        mockClientRollupCall(index);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, true)).build();
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f));
    }

    public void testPerformActionFailureInvalidExecutionState() {
        String lifecycleName = randomAlphaOfLength(5);
        RollupStep step = createRandomInstance();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().getPhase());
        lifecycleState.setAction(step.getKey().getAction());
        lifecycleState.setStep(step.getKey().getName());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        IndexMetadata indexMetadata = IndexMetadata.builder(randomAlphaOfLength(10))
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();

        String policyName = indexMetadata.getLifecyclePolicyName();
        String indexName = indexMetadata.getIndex().getName();
        step.performAction(indexMetadata, emptyClusterState(), null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("expecting a failure as the index doesn't have any rollup index name in its ILM execution state");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(IllegalStateException.class));
                assertThat(
                    e.getMessage(),
                    is("rollup index name was not generated for policy [" + policyName + "] and index [" + indexName + "]")
                );
            }
        });
    }

    public void testPerformActionOnDataStream() throws Exception {
        RollupStep step = createRandomInstance();
        String lifecycleName = randomAlphaOfLength(5);
        String dataStreamName = "test-datastream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata indexMetadata = getIndexMetadata(backingIndexName, lifecycleName, step);

        mockClientRollupCall(backingIndexName);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(newInstance(dataStreamName, List.of(indexMetadata.getIndex()))).put(indexMetadata, true))
            .build();
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f));
    }

    /**
     * Test rollup step when a successfully completed rollup index already exists.
     */
    public void testPerformActionCompletedRollupIndexExists() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);
        RollupStep step = createRandomInstance();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().getPhase());
        lifecycleState.setAction(step.getKey().getAction());
        lifecycleState.setStep(step.getKey().getName());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        String rollupIndex = GenerateUniqueIndexNameStep.generateValidIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setRollupIndexName(rollupIndex);

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        // Create a successfully completed rollup index (index.rollup.status: success)
        IndexMetadata indexMetadata = IndexMetadata.builder(rollupIndex)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.SUCCESS)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Map<String, IndexMetadata> indices = Map.of(rollupIndex, indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices)).build();

        Mockito.doThrow(new IllegalStateException("Rollup action should not be invoked"))
            .when(client)
            .execute(Mockito.any(), Mockito.any(), Mockito.any());

        step.performAction(sourceIndexMetadata, clusterState, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {}

            @Override
            public void onFailure(Exception e) {
                fail("onFailure should not be called in this test, called with exception: " + e.getMessage());
            }
        });
    }

    /**
     * Test rollup step when an in-progress rollup index already exists.
     */
    public void testPerformActionRollupInProgressIndexExists() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);
        RollupStep step = createRandomInstance();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().getPhase());
        lifecycleState.setAction(step.getKey().getAction());
        lifecycleState.setStep(step.getKey().getName());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        String rollupIndex = GenerateUniqueIndexNameStep.generateValidIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setRollupIndexName(rollupIndex);

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        // Create an in-progress rollup index (index.rollup.status: started)
        IndexMetadata indexMetadata = IndexMetadata.builder(rollupIndex)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.STARTED)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Map<String, IndexMetadata> indices = Map.of(rollupIndex, indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices)).build();

        step.performAction(sourceIndexMetadata, clusterState, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("onResponse should not be called in this test, because there's an in-progress rollup index");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertTrue(e.getMessage().contains("already exists with rollup status [started]"));
            }
        });
    }

    private void mockClientRollupCall(String sourceIndex) {
        Mockito.doAnswer(invocation -> {
            DownsampleAction.Request request = (DownsampleAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertRollupActionRequest(request, sourceIndex);
            listener.onResponse(AcknowledgedResponse.of(true));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
