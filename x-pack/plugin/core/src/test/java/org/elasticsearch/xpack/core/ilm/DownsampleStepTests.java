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
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.downsample.DownsampleAction;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.common.IndexNameGenerator.generateValidIndexName;
import static org.elasticsearch.xpack.core.ilm.DownsampleAction.DOWNSAMPLED_INDEX_PREFIX;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

public class DownsampleStepTests extends AbstractStepTestCase<DownsampleStep> {

    @Override
    public DownsampleStep createRandomInstance() {
        StepKey stepKey = randomStepKey();
        StepKey nextStepKey = randomStepKey();
        DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
        return new DownsampleStep(stepKey, nextStepKey, null, client, fixedInterval);
    }

    @Override
    public DownsampleStep mutateInstance(DownsampleStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        DateHistogramInterval fixedInterval = instance.getFixedInterval();

        switch (between(0, 2)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> fixedInterval = randomValueOtherThan(instance.getFixedInterval(), ConfigTestHelpers::randomInterval);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new DownsampleStep(key, nextKey, null, instance.getClient(), fixedInterval);
    }

    @Override
    public DownsampleStep copyInstance(DownsampleStep instance) {
        return new DownsampleStep(instance.getKey(), instance.getNextStepKey(), null, instance.getClient(), instance.getFixedInterval());
    }

    private IndexMetadata getIndexMetadata(String index, String lifecycleName, DownsampleStep step) {
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        lifecycleState.setDownsampleIndexName("downsample-index");

        return IndexMetadata.builder(index)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();
    }

    private static void assertDownsampleActionRequest(DownsampleAction.Request request, String sourceIndex) {
        assertNotNull(request);
        assertThat(request.getSourceIndex(), equalTo(sourceIndex));
        assertThat(request.getTargetIndex(), equalTo("downsample-index"));
    }

    public void testPerformAction() throws Exception {
        String lifecycleName = randomAlphaOfLength(5);
        DownsampleStep step = createRandomInstance();
        String index = randomAlphaOfLength(5);

        IndexMetadata indexMetadata = getIndexMetadata(index, lifecycleName, step);
        mockClientDownsampleCall(index);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, true)).build();
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f));
    }

    public void testPerformActionFailureInvalidExecutionState() {
        String lifecycleName = randomAlphaOfLength(5);
        DownsampleStep step = createRandomInstance();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
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
                fail("expecting a failure as the index doesn't have any downsample index name in its ILM execution state");
            }

            @Override
            public void onFailure(Exception e) {
                assertThat(e, instanceOf(IllegalStateException.class));
                assertThat(
                    e.getMessage(),
                    is("downsample index name was not generated for policy [" + policyName + "] and index [" + indexName + "]")
                );
            }
        });
    }

    public void testPerformActionOnDataStream() throws Exception {
        DownsampleStep step = createRandomInstance();
        String lifecycleName = randomAlphaOfLength(5);
        String dataStreamName = "test-datastream";
        String backingIndexName = DataStream.getDefaultBackingIndexName(dataStreamName, 1);
        IndexMetadata indexMetadata = getIndexMetadata(backingIndexName, lifecycleName, step);

        mockClientDownsampleCall(backingIndexName);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT)
            .metadata(Metadata.builder().put(newInstance(dataStreamName, List.of(indexMetadata.getIndex()))).put(indexMetadata, true))
            .build();
        PlainActionFuture.<Void, Exception>get(f -> step.performAction(indexMetadata, clusterState, null, f));
    }

    /**
     * Test downsample step when a successfully completed downsample index already exists.
     */
    public void testPerformActionCompletedDownsampleIndexExists() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);
        DownsampleStep step = createRandomInstance();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        String downsampleIndex = generateValidIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setDownsampleIndexName(downsampleIndex);

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        // Create a successfully completed downsample index (index.downsample.status: success)
        IndexMetadata indexMetadata = IndexMetadata.builder(downsampleIndex)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.SUCCESS)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Map<String, IndexMetadata> indices = Map.of(downsampleIndex, indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices)).build();

        Mockito.doThrow(new IllegalStateException("Downsample action should not be invoked"))
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
     * Test downsample step when an in-progress downsample index already exists.
     */
    public void testPerformActionDownsampleInProgressIndexExists() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);
        DownsampleStep step = createRandomInstance();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        String downsampleIndex = generateValidIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setDownsampleIndexName(downsampleIndex);

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        // Create an in-progress downsample index (index.downsample.status: started)
        IndexMetadata indexMetadata = IndexMetadata.builder(downsampleIndex)
            .settings(
                settings(Version.CURRENT).put(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(), IndexMetadata.DownsampleTaskStatus.STARTED)
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Map<String, IndexMetadata> indices = Map.of(downsampleIndex, indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices)).build();

        step.performAction(sourceIndexMetadata, clusterState, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                fail("onResponse should not be called in this test, because there's an in-progress downsample index");
            }

            @Override
            public void onFailure(Exception e) {
                assertTrue(e instanceof IllegalStateException);
                assertTrue(e.getMessage().contains("already exists with downsample status [started]"));
            }
        });
    }

    public void testNextStepKey() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        String downsampleIndex = generateValidIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setDownsampleIndexName(downsampleIndex);

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(Version.CURRENT).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(sourceIndexMetadata, true).build())
            .build();
        {
            try (NoOpClient client = new NoOpClient(getTestName())) {
                StepKey nextKeyOnComplete = randomStepKey();
                StepKey nextKeyOnIncomplete = randomStepKey();
                DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
                DownsampleStep completeStep = new DownsampleStep(
                    randomStepKey(),
                    nextKeyOnComplete,
                    nextKeyOnIncomplete,
                    client,
                    fixedInterval
                ) {
                    void performDownsampleIndex(String indexName, String downsampleIndexName, ActionListener<Void> listener) {
                        listener.onResponse(null);
                    }
                };
                completeStep.performAction(sourceIndexMetadata, clusterState, null, ActionListener.noop());
                assertThat(completeStep.getNextStepKey(), is(nextKeyOnComplete));
            }
        }
        {
            try (NoOpClient client = new NoOpClient(getTestName())) {
                StepKey nextKeyOnComplete = randomStepKey();
                StepKey nextKeyOnIncomplete = randomStepKey();
                DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
                DownsampleStep doubleInvocationStep = new DownsampleStep(
                    randomStepKey(),
                    nextKeyOnComplete,
                    nextKeyOnIncomplete,
                    client,
                    fixedInterval
                ) {
                    void performDownsampleIndex(String indexName, String downsampleIndexName, ActionListener<Void> listener) {
                        listener.onFailure(
                            new IllegalStateException(
                                "failing ["
                                    + DownsampleStep.NAME
                                    + "] step for index ["
                                    + indexName
                                    + "] as part of policy ["
                                    + lifecycleName
                                    + "] because the downsample index ["
                                    + downsampleIndexName
                                    + "] already exists with downsample status ["
                                    + IndexMetadata.DownsampleTaskStatus.UNKNOWN
                                    + "]"
                            )
                        );
                    }
                };
                doubleInvocationStep.performAction(sourceIndexMetadata, clusterState, null, ActionListener.noop());
                assertThat(doubleInvocationStep.getNextStepKey(), is(nextKeyOnIncomplete));
            }
        }
    }

    private void mockClientDownsampleCall(String sourceIndex) {
        Mockito.doAnswer(invocation -> {
            DownsampleAction.Request request = (DownsampleAction.Request) invocation.getArguments()[1];
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = (ActionListener<AcknowledgedResponse>) invocation.getArguments()[2];
            assertDownsampleActionRequest(request, sourceIndex);
            listener.onResponse(AcknowledgedResponse.of(true));
            return null;
        }).when(client).execute(Mockito.any(), Mockito.any(), Mockito.any());
    }
}
