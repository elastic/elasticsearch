/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.ilm.step.info.SingleMessageFieldInfo;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneId;
import java.util.Map;
import java.util.UUID;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.ClusterStateWaitUntilThresholdStep.waitedMoreThanThresholdLevel;
import static org.elasticsearch.xpack.core.ilm.UnfollowAction.CCR_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class ClusterStateWaitUntilThresholdStepTests extends AbstractStepTestCase<ClusterStateWaitUntilThresholdStep> {

    @Override
    public ClusterStateWaitUntilThresholdStep createRandomInstance() {
        ClusterStateWaitStep stepToExecute = new WaitForActiveShardsStep(randomStepKey(), randomStepKey());
        return new ClusterStateWaitUntilThresholdStep(stepToExecute, randomStepKey());
    }

    @Override
    public ClusterStateWaitUntilThresholdStep mutateInstance(ClusterStateWaitUntilThresholdStep instance) {
        ClusterStateWaitStep stepToExecute = instance.getStepToExecute();
        StepKey nextKeyOnThreshold = instance.getNextKeyOnThreshold();

        switch (between(0, 1)) {
            case 0 -> stepToExecute = randomValueOtherThan(
                stepToExecute,
                () -> new WaitForActiveShardsStep(randomStepKey(), randomStepKey())
            );
            case 1 -> nextKeyOnThreshold = randomValueOtherThan(nextKeyOnThreshold, AbstractStepTestCase::randomStepKey);
            default -> throw new AssertionError("Illegal randomisation branch");
        }

        return new ClusterStateWaitUntilThresholdStep(stepToExecute, nextKeyOnThreshold);
    }

    @Override
    public ClusterStateWaitUntilThresholdStep copyInstance(ClusterStateWaitUntilThresholdStep instance) {
        return new ClusterStateWaitUntilThresholdStep(instance.getStepToExecute(), instance.getNextKeyOnThreshold());
    }

    public void testIndexIsMissingReturnsIncompleteResult() {
        WaitForIndexingCompleteStep stepToExecute = new WaitForIndexingCompleteStep(randomStepKey(), randomStepKey());
        ClusterStateWaitUntilThresholdStep underTest = new ClusterStateWaitUntilThresholdStep(stepToExecute, randomStepKey());
        ClusterStateWaitStep.Result result = underTest.isConditionMet(
            new Index("testName", UUID.randomUUID().toString()),
            ClusterState.EMPTY_STATE
        );
        assertThat(result.complete(), is(false));
        assertThat(result.informationContext(), nullValue());
    }

    public void testIsConditionMetForUnderlyingStep() {
        {
            // threshold is not breached and the underlying step condition is met
            IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
                .settings(
                    settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true")
                        .put(LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD, "480h")
                )
                .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("step_time", String.valueOf(System.currentTimeMillis())))
                .putCustom(CCR_METADATA_KEY, Map.of())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            WaitForIndexingCompleteStep stepToExecute = new WaitForIndexingCompleteStep(randomStepKey(), randomStepKey());
            ClusterStateWaitUntilThresholdStep underTest = new ClusterStateWaitUntilThresholdStep(stepToExecute, randomStepKey());

            ClusterStateWaitStep.Result result = underTest.isConditionMet(indexMetadata.getIndex(), clusterState);
            assertThat(result.complete(), is(true));
            assertThat(result.informationContext(), nullValue());
        }

        {
            // threshold is not breached and the underlying step condition is NOT met
            IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
                .settings(
                    settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "false")
                        .put(LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD, "48h")
                )
                .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("step_time", String.valueOf(System.currentTimeMillis())))
                .putCustom(CCR_METADATA_KEY, Map.of())
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

            ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            WaitForIndexingCompleteStep stepToExecute = new WaitForIndexingCompleteStep(randomStepKey(), randomStepKey());
            ClusterStateWaitUntilThresholdStep underTest = new ClusterStateWaitUntilThresholdStep(stepToExecute, randomStepKey());
            ClusterStateWaitStep.Result result = underTest.isConditionMet(indexMetadata.getIndex(), clusterState);

            assertThat(result.complete(), is(false));
            assertThat(result.informationContext(), notNullValue());
            WaitForIndexingCompleteStep.IndexingNotCompleteInfo info = (WaitForIndexingCompleteStep.IndexingNotCompleteInfo) result
                .informationContext();
            assertThat(
                info.getMessage(),
                equalTo(
                    "waiting for the [index.lifecycle.indexing_complete] setting to be set to "
                        + "true on the leader index, it is currently [false]"
                )
            );
        }

        {
            // underlying step is executed once even if the threshold is breached and the underlying complete result is returned
            IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
                .settings(
                    settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "true")
                        .put(LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD, "1s")
                )
                .putCustom(CCR_METADATA_KEY, Map.of())
                .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("step_time", String.valueOf(1234L)))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();
            ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            WaitForIndexingCompleteStep stepToExecute = new WaitForIndexingCompleteStep(randomStepKey(), randomStepKey());
            StepKey nextKeyOnThresholdBreach = randomStepKey();
            ClusterStateWaitUntilThresholdStep underTest = new ClusterStateWaitUntilThresholdStep(stepToExecute, nextKeyOnThresholdBreach);

            ClusterStateWaitStep.Result result = underTest.isConditionMet(indexMetadata.getIndex(), clusterState);
            assertThat(result.complete(), is(true));
            assertThat(result.informationContext(), nullValue());
            assertThat(underTest.getNextStepKey(), is(not(nextKeyOnThresholdBreach)));
            assertThat(underTest.getNextStepKey(), is(stepToExecute.getNextStepKey()));
        }

        {
            // underlying step is executed once even if the threshold is breached, but because the underlying step result is false the
            // step under test will return `complete` (becuase the threshold is breached and we don't want to wait anymore)
            IndexMetadata indexMetadata = IndexMetadata.builder("follower-index")
                .settings(
                    settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_INDEXING_COMPLETE, "false")
                        .put(LifecycleSettings.LIFECYCLE_STEP_WAIT_TIME_THRESHOLD, "1h")
                )
                .putCustom(CCR_METADATA_KEY, Map.of())
                .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("step_time", String.valueOf(1234L)))
                .numberOfShards(1)
                .numberOfReplicas(0)
                .build();

            ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
                .metadata(Metadata.builder().put(indexMetadata, true).build())
                .build();

            StepKey currentStepKey = randomStepKey();
            WaitForIndexingCompleteStep stepToExecute = new WaitForIndexingCompleteStep(currentStepKey, randomStepKey());
            StepKey nextKeyOnThresholdBreach = randomStepKey();
            ClusterStateWaitUntilThresholdStep underTest = new ClusterStateWaitUntilThresholdStep(stepToExecute, nextKeyOnThresholdBreach);
            ClusterStateWaitStep.Result result = underTest.isConditionMet(indexMetadata.getIndex(), clusterState);

            assertThat(result.complete(), is(true));
            assertThat(result.informationContext(), notNullValue());
            SingleMessageFieldInfo info = (SingleMessageFieldInfo) result.informationContext();
            assertThat(
                info.message(),
                equalTo(
                    "["
                        + currentStepKey.name()
                        + "] lifecycle step, as part of ["
                        + currentStepKey.action()
                        + "] "
                        + "action, for index [follower-index] executed for more than [1h]. Abandoning execution and moving to the next "
                        + "fallback step ["
                        + nextKeyOnThresholdBreach
                        + "]"
                )
            );

            // the next step must change to the provided one when the threshold is breached
            assertThat(underTest.getNextStepKey(), is(nextKeyOnThresholdBreach));
        }
    }

    public void testWaitedMoreThanThresholdLevelMath() {
        long epochMillis = 1552684146542L; // Friday, 15 March 2019 21:09:06.542
        Clock clock = Clock.fixed(Instant.ofEpochMilli(epochMillis), ZoneId.systemDefault());
        TimeValue retryThreshold = TimeValue.timeValueHours(1);

        {
            // step time is "2 hours ago" with a threshold of 1 hour - the threshold level is breached
            LifecycleExecutionState executionState = new LifecycleExecutionState.Builder().setStepTime(
                epochMillis - TimeValue.timeValueHours(2).millis()
            ).build();
            boolean thresholdBreached = waitedMoreThanThresholdLevel(retryThreshold, executionState, clock);
            assertThat(thresholdBreached, is(true));
        }

        {
            // step time is "10 minutes ago" with a threshold of 1 hour - the threshold level is NOT breached
            LifecycleExecutionState executionState = new LifecycleExecutionState.Builder().setStepTime(
                epochMillis - TimeValue.timeValueMinutes(10).millis()
            ).build();
            boolean thresholdBreached = waitedMoreThanThresholdLevel(retryThreshold, executionState, clock);
            assertThat(thresholdBreached, is(false));
        }

        {
            // if no threshold is configured we'll never report the threshold is breached
            LifecycleExecutionState executionState = new LifecycleExecutionState.Builder().setStepTime(
                epochMillis - TimeValue.timeValueHours(2).millis()
            ).build();
            boolean thresholdBreached = waitedMoreThanThresholdLevel(null, executionState, clock);
            assertThat(thresholdBreached, is(false));
        }
    }

    public void testIsCompletableBreaches() {
        IndexMetadata indexMetadata = IndexMetadata.builder("index")
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(0)
            .putCustom(ILM_CUSTOM_METADATA_KEY, Map.of("step_time", String.valueOf(Clock.systemUTC().millis())))
            .build();

        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster"))
            .metadata(Metadata.builder().put(indexMetadata, true).build())
            .build();

        ClusterStateWaitUntilThresholdStep step = new ClusterStateWaitUntilThresholdStep(
            new ClusterStateWaitStep(new StepKey("phase", "action", "key"), new StepKey("phase", "action", "next-key")) {
                @Override
                public Result isConditionMet(Index index, ClusterState clusterState) {
                    return new Result(false, new SingleMessageFieldInfo(""));
                }

                @Override
                public boolean isRetryable() {
                    return true;
                }
            },
            new StepKey("phase", "action", "breached")
        );

        assertFalse(step.isConditionMet(indexMetadata.getIndex(), clusterState).complete());

        assertThat(step.getNextStepKey().name(), equalTo("next-key"));

        step = new ClusterStateWaitUntilThresholdStep(
            new ClusterStateWaitStep(new StepKey("phase", "action", "key"), new StepKey("phase", "action", "next-key")) {
                @Override
                public Result isConditionMet(Index index, ClusterState clusterState) {
                    return new Result(false, new SingleMessageFieldInfo(""));
                }

                @Override
                public boolean isCompletable() {
                    return false;
                }

                @Override
                public boolean isRetryable() {
                    return true;
                }
            },
            new StepKey("phase", "action", "breached")
        );
        assertTrue(step.isConditionMet(indexMetadata.getIndex(), clusterState).complete());
        assertThat(step.getNextStepKey().name(), equalTo("breached"));
    }
}
