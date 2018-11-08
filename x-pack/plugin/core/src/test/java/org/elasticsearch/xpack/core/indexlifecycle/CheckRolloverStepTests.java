/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.Version;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ToXContentObject;

import static org.elasticsearch.xpack.core.indexlifecycle.LifecycleExecutionState.ILM_CUSTOM_METADATA_KEY;
import static org.hamcrest.Matchers.equalTo;

public class CheckRolloverStepTests extends AbstractStepTestCase<CheckRolloverStep> {

    private final long stepTime = randomLongBetween(0, Long.MAX_VALUE - (CheckRolloverStep.TIMEOUT_MILLIS + 1));
    private final String indexName = randomAlphaOfLengthBetween(5,15);
    private final String aliasName = randomAlphaOfLengthBetween(5,15);

    public void testOverTimeout() {
        logger.info(indexName);
        final long millisOverTimeout = randomLongBetween(1, Long.MAX_VALUE - stepTime);
        CheckRolloverStep step = new CheckRolloverStep(randomStepKey(), randomStepKey(), null,
            () -> stepTime + CheckRolloverStep.TIMEOUT_MILLIS + millisOverTimeout);
        IndexMetaData indexMetaData = createIndexMetaData(false);

        ExpectFailureListener listener = new ExpectFailureListener("over-timeout");
        step.evaluateCondition(indexMetaData, listener);
        assertThat(listener.getException().getMessage(), equalTo("index [" + indexName + "] was not rolled over using the configured " +
            "rollover alias [" + aliasName + "] or a subsequent index was created outside of Index Lifecycle Management"));
    }

    public void testUnderTimeout() {
        final long millisUnderTimeout = randomLongBetween(1, CheckRolloverStep.TIMEOUT_MILLIS);
        CheckRolloverStep step = new CheckRolloverStep(randomStepKey(), randomStepKey(), null,
            () -> stepTime + CheckRolloverStep.TIMEOUT_MILLIS - millisUnderTimeout);
        IndexMetaData indexMetaData = createIndexMetaData(false);

        ExpectResponseListener listener = new ExpectResponseListener("under-timeout", false);
        step.evaluateCondition(indexMetaData, listener);
    }

    public void testConditionMet() {
        final long millisDifferenceFromTimeout = randomLongBetween(-stepTime, Long.MAX_VALUE - stepTime);
        CheckRolloverStep step = new CheckRolloverStep(randomStepKey(), randomStepKey(), null,
            () -> stepTime + CheckRolloverStep.TIMEOUT_MILLIS + millisDifferenceFromTimeout);
        IndexMetaData indexMetaData = createIndexMetaData(true);

        ExpectResponseListener listener = new ExpectResponseListener("under-timeout", true);
        step.evaluateCondition(indexMetaData, listener);
    }

    @Override
    protected CheckRolloverStep createRandomInstance() {
        final Step.StepKey stepKey = randomStepKey();
        final Step.StepKey nextStepKey = randomStepKey();
        return new CheckRolloverStep(stepKey, nextStepKey, null, () -> stepTime);
    }

    @Override
    protected CheckRolloverStep mutateInstance(CheckRolloverStep instance) {
        Step.StepKey key = instance.getKey();
        Step.StepKey nextKey = instance.getNextStepKey();

        switch (between(0, 1)) {
            case 0:
                key = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            case 1:
                nextKey = new Step.StepKey(key.getPhase(), key.getAction(), key.getName() + randomAlphaOfLength(5));
                break;
            default:
                throw new AssertionError("Illegal randomisation branch");
        }
        return new CheckRolloverStep(key, nextKey, null, () -> stepTime);
    }

    @Override
    protected CheckRolloverStep copyInstance(CheckRolloverStep instance) {
        return new CheckRolloverStep(instance.getKey(), instance.getNextStepKey(), null, () -> stepTime);
    }

    private ClusterState createClusterState(boolean hasRolloverInfo) {
        LifecycleExecutionState executionState = LifecycleExecutionState.builder()
            .setStepTime(stepTime).build();

        Settings.Builder indexSettingsBuilder = Settings.builder();
        Settings indexSettings = indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, aliasName).build();
        IndexMetaData.Builder indexMetadata = IndexMetaData.builder(indexName)
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, executionState.asMap());
        if (hasRolloverInfo) {
            RolloverInfo rolloverInfo = new RolloverInfo(aliasName, null, stepTime);
            indexMetadata.putRolloverInfo(rolloverInfo);
        }

        MetaData metadata = MetaData.builder().put(indexMetadata.build(), true).build();
        return ClusterState.builder(new ClusterName("my_cluster")).metaData(metadata).build();
    }

    private IndexMetaData createIndexMetaData(boolean hasRolloverInfo) {
        LifecycleExecutionState executionState = LifecycleExecutionState.builder()
            .setStepTime(stepTime).build();

        Settings.Builder indexSettingsBuilder = Settings.builder();
        Settings indexSettings = indexSettingsBuilder.put(IndexMetaData.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetaData.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(RolloverAction.LIFECYCLE_ROLLOVER_ALIAS, aliasName).build();
        IndexMetaData.Builder indexMetadata = IndexMetaData.builder(indexName)
            .settings(indexSettings)
            .putCustom(ILM_CUSTOM_METADATA_KEY, executionState.asMap());
        if (hasRolloverInfo) {
            RolloverInfo rolloverInfo = new RolloverInfo(aliasName, null, stepTime);
            indexMetadata.putRolloverInfo(rolloverInfo);
        }

        return indexMetadata.build();
    }

    private static class ExpectFailureListener implements AsyncWaitStep.Listener {
        private final String description;

        private Exception exception;

        private ExpectFailureListener(String description) {
            this.description = description;
        }

        @Override
        public void onResponse(boolean conditionMet, ToXContentObject infomationContext) {
            fail(description + " should fail when called, but onResponse was called with conditionMet = " + conditionMet);
        }

        @Override
        public void onFailure(Exception e) {
            exception = e;
        }

        public Exception getException() {
            return exception;
        }
    }

    private static class ExpectResponseListener implements AsyncWaitStep.Listener {
        private final String description;
        private final boolean expectedConditionMet;

        private ExpectResponseListener(String description, boolean expectedConditionMet) {
            this.description = description;
            this.expectedConditionMet = expectedConditionMet;
        }

        @Override
        public void onResponse(boolean conditionMet, ToXContentObject infomationContext) {
            if (conditionMet != expectedConditionMet) {
                fail(description + " expected conditionMet = " + expectedConditionMet + " but got " + conditionMet);
            }
        }

        @Override
        public void onFailure(Exception e) {
            fail(description + " should not fail, but got exception: " + e);
        }
    }
}
