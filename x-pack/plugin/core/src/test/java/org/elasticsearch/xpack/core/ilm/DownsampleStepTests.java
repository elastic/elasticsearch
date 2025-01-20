/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.downsample.DownsampleAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.client.NoOpClient;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.mockito.Mockito;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.action.downsample.DownsampleConfig.generateDownsampleIndexName;
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
        return new DownsampleStep(
            stepKey,
            nextStepKey,
            client,
            fixedInterval,
            randomTimeValue(1, 1000, TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
        );
    }

    @Override
    public DownsampleStep mutateInstance(DownsampleStep instance) {
        StepKey key = instance.getKey();
        StepKey nextKey = instance.getNextStepKey();
        DateHistogramInterval fixedInterval = instance.getFixedInterval();
        TimeValue timeout = instance.getWaitTimeout();

        switch (between(0, 3)) {
            case 0 -> key = new StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> fixedInterval = randomValueOtherThan(instance.getFixedInterval(), ConfigTestHelpers::randomInterval);
            case 3 -> timeout = randomValueOtherThan(
                instance.getWaitTimeout(),
                () -> randomTimeValue(1, 1000, TimeUnit.DAYS, TimeUnit.HOURS, TimeUnit.MINUTES, TimeUnit.SECONDS, TimeUnit.MILLISECONDS)
            );
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new DownsampleStep(key, nextKey, instance.getClient(), fixedInterval, timeout);
    }

    @Override
    public DownsampleStep copyInstance(DownsampleStep instance) {
        return new DownsampleStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.getClient(),
            instance.getFixedInterval(),
            instance.getWaitTimeout()
        );
    }

    private IndexMetadata getIndexMetadata(String index, String lifecycleName, DownsampleStep step) {
        IndexMetadata im = IndexMetadata.builder(index)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        lifecycleState.setDownsampleIndexName(generateDownsampleIndexName(DOWNSAMPLED_INDEX_PREFIX, im, step.getFixedInterval()));
        return IndexMetadata.builder(im).putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap()).build();
    }

    private static void assertDownsampleActionRequest(DownsampleAction.Request request, String sourceIndex) {
        assertNotNull(request);
        assertThat(request.getSourceIndex(), equalTo(sourceIndex));
        assertThat(
            request.getTargetIndex(),
            equalTo(DOWNSAMPLED_INDEX_PREFIX + request.getDownsampleConfig().getFixedInterval() + "-" + sourceIndex)
        );
    }

    public void testPerformAction() throws Exception {
        String lifecycleName = randomAlphaOfLength(5);
        DownsampleStep step = createRandomInstance();
        String index = randomAlphaOfLength(5);

        IndexMetadata indexMetadata = getIndexMetadata(index, lifecycleName, step);
        mockClientDownsampleCall(index);

        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(indexMetadata, true)).build();
        performActionAndWait(step, indexMetadata, clusterState, null);
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
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
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
        performActionAndWait(step, indexMetadata, clusterState, null);
    }

    /**
     * Test downsample step when an in-progress downsample index already exists.
     */
    public void testPerformActionDownsampleInProgressIndexExists() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);
        DownsampleStep step = createRandomInstance();

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();
        String downsampleIndex = generateDownsampleIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexMetadata, step.getFixedInterval());
        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        lifecycleState.setDownsampleIndexName(downsampleIndex);

        sourceIndexMetadata = IndexMetadata.builder(sourceIndexMetadata)
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .build();

        // Create an in-progress downsample index (index.downsample.status: started)
        IndexMetadata indexMetadata = IndexMetadata.builder(downsampleIndex)
            .settings(
                settings(IndexVersion.current()).put(
                    IndexMetadata.INDEX_DOWNSAMPLE_STATUS.getKey(),
                    IndexMetadata.DownsampleTaskStatus.STARTED
                )
            )
            .numberOfShards(1)
            .numberOfReplicas(0)
            .build();
        Map<String, IndexMetadata> indices = Map.of(downsampleIndex, indexMetadata);
        ClusterState clusterState = ClusterState.builder(ClusterState.EMPTY_STATE).metadata(Metadata.builder().indices(indices)).build();
        mockClientDownsampleCall(sourceIndexName);

        final AtomicBoolean listenerIsCalled = new AtomicBoolean(false);
        step.performAction(sourceIndexMetadata, clusterState, null, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                listenerIsCalled.set(true);
            }

            @Override
            public void onFailure(Exception e) {
                listenerIsCalled.set(true);
                logger.error("-> " + e.getMessage(), e);
                fail("repeatedly calling the downsampling API is expected, but got exception [" + e.getMessage() + "]");
            }
        });
        assertThat(listenerIsCalled.get(), is(true));
    }

    public void testNextStepKey() {
        String sourceIndexName = randomAlphaOfLength(10);
        String lifecycleName = randomAlphaOfLength(5);

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());

        String downsampleIndex = generateValidIndexName(DOWNSAMPLED_INDEX_PREFIX, sourceIndexName);
        lifecycleState.setDownsampleIndexName(downsampleIndex);

        IndexMetadata sourceIndexMetadata = IndexMetadata.builder(sourceIndexName)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap())
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        ClusterState clusterState = ClusterState.builder(emptyClusterState())
            .metadata(Metadata.builder().put(sourceIndexMetadata, true).build())
            .build();
        {
            try (var threadPool = createThreadPool()) {
                final var client = new NoOpClient(threadPool);
                StepKey nextKey = randomStepKey();
                DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
                TimeValue timeout = DownsampleAction.DEFAULT_WAIT_TIMEOUT;
                DownsampleStep completeStep = new DownsampleStep(randomStepKey(), nextKey, client, fixedInterval, timeout) {
                    void performDownsampleIndex(String indexName, String downsampleIndexName, ActionListener<Void> listener) {
                        listener.onResponse(null);
                    }
                };
                completeStep.performAction(sourceIndexMetadata, clusterState, null, ActionListener.noop());
                assertThat(completeStep.getNextStepKey(), is(nextKey));
            }
        }
        {
            try (var threadPool = createThreadPool()) {
                final var client = new NoOpClient(threadPool);
                StepKey nextKey = randomStepKey();
                DateHistogramInterval fixedInterval = ConfigTestHelpers.randomInterval();
                TimeValue timeout = DownsampleAction.DEFAULT_WAIT_TIMEOUT;
                DownsampleStep doubleInvocationStep = new DownsampleStep(randomStepKey(), nextKey, client, fixedInterval, timeout) {
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
                assertThat(doubleInvocationStep.getNextStepKey(), is(nextKey));
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
