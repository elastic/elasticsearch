/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.LifecycleExecutionState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xpack.core.ilm.step.info.EmptyInfo;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.elasticsearch.xpack.core.ilm.WaitUntilReplicateForTimePassesStep.approximateTimeRemaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class WaitUntilReplicateForTimePassesStepTests extends AbstractStepTestCase<WaitUntilReplicateForTimePassesStep> {

    @Override
    protected WaitUntilReplicateForTimePassesStep createRandomInstance() {
        Step.StepKey stepKey = randomStepKey();
        Step.StepKey nextStepKey = randomStepKey();
        TimeValue replicateFor = randomPositiveTimeValue();
        return new WaitUntilReplicateForTimePassesStep(stepKey, nextStepKey, replicateFor, Instant::now);
    }

    @Override
    protected WaitUntilReplicateForTimePassesStep mutateInstance(WaitUntilReplicateForTimePassesStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();
        TimeValue replicateFor = instance.getReplicateFor();

        switch (between(0, 2)) {
            case 0 -> key = new Step.StepKey(key.phase(), key.action(), key.name() + randomAlphaOfLength(5));
            case 1 -> nextKey = new Step.StepKey(nextKey.phase(), nextKey.action(), nextKey.name() + randomAlphaOfLength(5));
            case 2 -> replicateFor = randomValueOtherThan(replicateFor, ESTestCase::randomPositiveTimeValue);
        }
        return new WaitUntilReplicateForTimePassesStep(key, nextKey, replicateFor, Instant::now);
    }

    @Override
    protected WaitUntilReplicateForTimePassesStep copyInstance(WaitUntilReplicateForTimePassesStep instance) {
        return new WaitUntilReplicateForTimePassesStep(
            instance.getKey(),
            instance.getNextStepKey(),
            instance.getReplicateFor(),
            Instant::now
        );
    }

    public void testEvaluateCondition() {
        // a mutable box that we can put Instants into
        final AtomicReference<Instant> returnVal = new AtomicReference<>();

        final WaitUntilReplicateForTimePassesStep step = new WaitUntilReplicateForTimePassesStep(
            randomStepKey(),
            randomStepKey(),
            TimeValue.timeValueHours(1),
            () -> returnVal.get()
        );

        final Instant now = Instant.now().truncatedTo(ChronoUnit.MILLIS);
        final Instant t1 = now.minus(2, ChronoUnit.HOURS);
        final Instant t2 = now.plus(2, ChronoUnit.HOURS);

        final IndexMetadata indexMeta = getIndexMetadata(randomAlphaOfLengthBetween(10, 30), randomAlphaOfLengthBetween(10, 30), step);
        final Metadata metadata = Metadata.builder().put(indexMeta, true).build();
        final Index index = indexMeta.getIndex();

        // if we evaluate the condition now, it hasn't been met, because it hasn't been an hour
        returnVal.set(now);
        step.evaluateCondition(metadata, index, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                assertThat(complete, is(false));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        returnVal.set(t1); // similarly, if we were in the past, enough time also wouldn't have passed
        step.evaluateCondition(metadata, index, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                assertThat(complete, is(false));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);

        returnVal.set(t2); // but two hours from now in the future, an hour will have passed
        step.evaluateCondition(metadata, index, new AsyncWaitStep.Listener() {
            @Override
            public void onResponse(boolean complete, ToXContentObject informationContext) {
                assertThat(complete, is(true));
                assertThat(informationContext, is(EmptyInfo.INSTANCE));
            }

            @Override
            public void onFailure(Exception e) {
                throw new AssertionError("Unexpected method call", e);
            }
        }, MASTER_TIMEOUT);
    }

    public void testApproximateTimeRemaining() {
        assertThat(approximateTimeRemaining(TimeValue.ZERO), equalTo("less than 1d"));

        for (int i : new int[] { -2000, 0, 2000 }) {
            assertThat(
                approximateTimeRemaining(TimeValue.timeValueMillis(TimeValue.timeValueDays(2).millis() + i)),
                equalTo("approximately 2d")
            );
        }

        assertThat(approximateTimeRemaining(TimeValue.timeValueHours(24)), equalTo("approximately 1d"));
        assertThat(approximateTimeRemaining(TimeValue.timeValueMillis(TimeValue.timeValueHours(24).millis() - 1)), equalTo("less than 1d"));
    }

    private IndexMetadata getIndexMetadata(String index, String lifecycleName, WaitUntilReplicateForTimePassesStep step) {
        IndexMetadata idxMetadata = IndexMetadata.builder(index)
            .settings(settings(IndexVersion.current()).put(LifecycleSettings.LIFECYCLE_NAME, lifecycleName))
            .numberOfShards(randomIntBetween(1, 5))
            .numberOfReplicas(randomIntBetween(0, 5))
            .build();

        LifecycleExecutionState.Builder lifecycleState = LifecycleExecutionState.builder();
        lifecycleState.setPhase(step.getKey().phase());
        lifecycleState.setAction(step.getKey().action());
        lifecycleState.setStep(step.getKey().name());
        long stateTimes = System.currentTimeMillis();
        lifecycleState.setPhaseTime(stateTimes);
        lifecycleState.setActionTime(stateTimes);
        lifecycleState.setStepTime(stateTimes);
        lifecycleState.setIndexCreationDate(randomNonNegativeLong());
        return IndexMetadata.builder(idxMetadata).putCustom(ILM_CUSTOM_METADATA_KEY, lifecycleState.build().asMap()).build();
    }
}
