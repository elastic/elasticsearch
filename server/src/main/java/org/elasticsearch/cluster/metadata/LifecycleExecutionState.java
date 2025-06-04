/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.metadata;

import org.elasticsearch.exception.ElasticsearchException;
import org.elasticsearch.common.Strings;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Contains information about the execution of a lifecycle policy for a single
 * index, and serializes/deserializes this information to and from custom
 * index metadata.
 */
public record LifecycleExecutionState(
    String phase,
    String action,
    String step,
    String failedStep,
    Boolean isAutoRetryableError,
    Integer failedStepRetryCount,
    String stepInfo,
    String previousStepInfo,
    String phaseDefinition,
    Long lifecycleDate,
    Long phaseTime,
    Long actionTime,
    Long stepTime,
    String snapshotRepository,
    String snapshotName,
    String shrinkIndexName,
    String snapshotIndexName,
    String downsampleIndexName
) {

    public static final String ILM_CUSTOM_METADATA_KEY = "ilm";
    public static final int MAXIMUM_STEP_INFO_STRING_LENGTH = 1024;

    private static final String PHASE = "phase";
    private static final String ACTION = "action";
    private static final String STEP = "step";
    private static final String INDEX_CREATION_DATE = "creation_date";
    private static final String PHASE_TIME = "phase_time";
    private static final String ACTION_TIME = "action_time";
    private static final String STEP_TIME = "step_time";
    private static final String FAILED_STEP = "failed_step";
    private static final String IS_AUTO_RETRYABLE_ERROR = "is_auto_retryable_error";
    private static final String FAILED_STEP_RETRY_COUNT = "failed_step_retry_count";
    private static final String STEP_INFO = "step_info";
    private static final String PREVIOUS_STEP_INFO = "previous_step_info";
    private static final String PHASE_DEFINITION = "phase_definition";
    private static final String SNAPSHOT_NAME = "snapshot_name";
    private static final String SNAPSHOT_REPOSITORY = "snapshot_repository";
    private static final String SNAPSHOT_INDEX_NAME = "snapshot_index_name";
    private static final String SHRINK_INDEX_NAME = "shrink_index_name";
    private static final String DOWNSAMPLE_INDEX_NAME = "rollup_index_name";

    public static final LifecycleExecutionState EMPTY_STATE = LifecycleExecutionState.builder().build();

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(LifecycleExecutionState state) {
        return new Builder().setPhase(state.phase)
            .setAction(state.action)
            .setStep(state.step)
            .setFailedStep(state.failedStep)
            .setIsAutoRetryableError(state.isAutoRetryableError)
            .setFailedStepRetryCount(state.failedStepRetryCount)
            .setStepInfo(state.stepInfo)
            .setPreviousStepInfo(state.previousStepInfo)
            .setPhaseDefinition(state.phaseDefinition)
            .setIndexCreationDate(state.lifecycleDate)
            .setPhaseTime(state.phaseTime)
            .setActionTime(state.actionTime)
            .setSnapshotRepository(state.snapshotRepository)
            .setSnapshotName(state.snapshotName)
            .setShrinkIndexName(state.shrinkIndexName)
            .setSnapshotIndexName(state.snapshotIndexName)
            .setDownsampleIndexName(state.downsampleIndexName)
            .setStepTime(state.stepTime);
    }

    public static LifecycleExecutionState fromCustomMetadata(Map<String, String> customData) {
        Builder builder = builder();
        String phase = customData.get(PHASE);
        if (phase != null) {
            builder.setPhase(phase);
        }
        String action = customData.get(ACTION);
        if (action != null) {
            builder.setAction(action);
        }
        String step = customData.get(STEP);
        if (step != null) {
            builder.setStep(step);
        }
        String failedStep = customData.get(FAILED_STEP);
        if (failedStep != null) {
            builder.setFailedStep(failedStep);
        }
        String isAutoRetryableError = customData.get(IS_AUTO_RETRYABLE_ERROR);
        if (isAutoRetryableError != null) {
            builder.setIsAutoRetryableError(Boolean.parseBoolean(isAutoRetryableError));
        }
        String failedStepRetryCount = customData.get(FAILED_STEP_RETRY_COUNT);
        if (failedStepRetryCount != null) {
            builder.setFailedStepRetryCount(Integer.parseInt(failedStepRetryCount));
        }
        String stepInfo = customData.get(STEP_INFO);
        if (stepInfo != null) {
            builder.setStepInfo(stepInfo);
        }
        String previousStepInfo = customData.get(PREVIOUS_STEP_INFO);
        if (previousStepInfo != null) {
            builder.setPreviousStepInfo(previousStepInfo);
        }
        String phaseDefinition = customData.get(PHASE_DEFINITION);
        if (phaseDefinition != null) {
            builder.setPhaseDefinition(phaseDefinition);
        }
        String snapShotRepository = customData.get(SNAPSHOT_REPOSITORY);
        if (snapShotRepository != null) {
            builder.setSnapshotRepository(snapShotRepository);
        }
        String snapshotName = customData.get(SNAPSHOT_NAME);
        if (snapshotName != null) {
            builder.setSnapshotName(snapshotName);
        }
        String shrinkIndexName = customData.get(SHRINK_INDEX_NAME);
        if (shrinkIndexName != null) {
            builder.setShrinkIndexName(shrinkIndexName);
        }
        String indexCreationDate = customData.get(INDEX_CREATION_DATE);
        if (indexCreationDate != null) {
            try {
                builder.setIndexCreationDate(Long.parseLong(indexCreationDate));
            } catch (NumberFormatException e) {
                throw new ElasticsearchException(
                    "Custom metadata field [{}] does not contain a valid long. Actual value: [{}]",
                    e,
                    INDEX_CREATION_DATE,
                    customData.get(INDEX_CREATION_DATE)
                );
            }
        }
        String phaseTime = customData.get(PHASE_TIME);
        if (phaseTime != null) {
            try {
                builder.setPhaseTime(Long.parseLong(phaseTime));
            } catch (NumberFormatException e) {
                throw new ElasticsearchException(
                    "Custom metadata field [{}] does not contain a valid long. Actual value: [{}]",
                    e,
                    PHASE_TIME,
                    customData.get(PHASE_TIME)
                );
            }
        }
        String actionTime = customData.get(ACTION_TIME);
        if (actionTime != null) {
            try {
                builder.setActionTime(Long.parseLong(actionTime));
            } catch (NumberFormatException e) {
                throw new ElasticsearchException(
                    "Custom metadata field [{}] does not contain a valid long. Actual value: [{}]",
                    e,
                    ACTION_TIME,
                    customData.get(ACTION_TIME)
                );
            }
        }
        String stepTime = customData.get(STEP_TIME);
        if (stepTime != null) {
            try {
                builder.setStepTime(Long.parseLong(stepTime));
            } catch (NumberFormatException e) {
                throw new ElasticsearchException(
                    "Custom metadata field [{}] does not contain a valid long. Actual value: [{}]",
                    e,
                    STEP_TIME,
                    customData.get(STEP_TIME)
                );
            }
        }
        String snapshotIndexName = customData.get(SNAPSHOT_INDEX_NAME);
        if (snapshotIndexName != null) {
            builder.setSnapshotIndexName(snapshotIndexName);
        }
        String downsampleIndexName = customData.get(DOWNSAMPLE_INDEX_NAME);
        if (downsampleIndexName != null) {
            builder.setDownsampleIndexName(downsampleIndexName);
        }
        return builder.build();
    }

    /**
     * Converts this object to an immutable map representation for use with
     * {@link IndexMetadata.Builder#putCustom(String, Map)}.
     *
     * @return An immutable Map representation of this execution state.
     */
    public Map<String, String> asMap() {
        Map<String, String> result = new HashMap<>();
        if (phase != null) {
            result.put(PHASE, phase);
        }
        if (action != null) {
            result.put(ACTION, action);
        }
        if (step != null) {
            result.put(STEP, step);
        }
        if (failedStep != null) {
            result.put(FAILED_STEP, failedStep);
        }
        if (isAutoRetryableError != null) {
            result.put(IS_AUTO_RETRYABLE_ERROR, String.valueOf(isAutoRetryableError));
        }
        if (failedStepRetryCount != null) {
            result.put(FAILED_STEP_RETRY_COUNT, String.valueOf(failedStepRetryCount));
        }
        if (stepInfo != null) {
            result.put(STEP_INFO, stepInfo);
        }
        if (previousStepInfo != null) {
            result.put(PREVIOUS_STEP_INFO, previousStepInfo);
        }
        if (lifecycleDate != null) {
            result.put(INDEX_CREATION_DATE, String.valueOf(lifecycleDate));
        }
        if (phaseTime != null) {
            result.put(PHASE_TIME, String.valueOf(phaseTime));
        }
        if (actionTime != null) {
            result.put(ACTION_TIME, String.valueOf(actionTime));
        }
        if (stepTime != null) {
            result.put(STEP_TIME, String.valueOf(stepTime));
        }
        if (phaseDefinition != null) {
            result.put(PHASE_DEFINITION, phaseDefinition);
        }
        if (snapshotRepository != null) {
            result.put(SNAPSHOT_REPOSITORY, snapshotRepository);
        }
        if (snapshotName != null) {
            result.put(SNAPSHOT_NAME, snapshotName);
        }
        if (shrinkIndexName != null) {
            result.put(SHRINK_INDEX_NAME, shrinkIndexName);
        }
        if (snapshotIndexName != null) {
            result.put(SNAPSHOT_INDEX_NAME, snapshotIndexName);
        }
        if (downsampleIndexName != null) {
            result.put(DOWNSAMPLE_INDEX_NAME, downsampleIndexName);
        }
        return Collections.unmodifiableMap(result);
    }

    public static String truncateWithExplanation(String input) {
        if (input != null && input.length() > MAXIMUM_STEP_INFO_STRING_LENGTH) {
            return Strings.cleanTruncate(input, MAXIMUM_STEP_INFO_STRING_LENGTH)
                + "... ("
                + (input.length() - MAXIMUM_STEP_INFO_STRING_LENGTH)
                + " chars truncated)";
        } else {
            return input;
        }
    }

    public static class Builder {
        private String phase;
        private String action;
        private String step;
        private String failedStep;
        private String stepInfo;
        private String previousStepInfo;
        private String phaseDefinition;
        private Long indexCreationDate;
        private Long phaseTime;
        private Long actionTime;
        private Long stepTime;
        private Boolean isAutoRetryableError;
        private Integer failedStepRetryCount;
        private String snapshotName;
        private String snapshotRepository;
        private String shrinkIndexName;
        private String snapshotIndexName;
        private String downsampleIndexName;

        public Builder setPhase(String phase) {
            this.phase = phase;
            return this;
        }

        public Builder setAction(String action) {
            this.action = action;
            return this;
        }

        public Builder setStep(String step) {
            this.step = step;
            return this;
        }

        public Builder setFailedStep(String failedStep) {
            this.failedStep = failedStep;
            return this;
        }

        public Builder setStepInfo(String stepInfo) {
            this.stepInfo = truncateWithExplanation(stepInfo);
            return this;
        }

        public Builder setPreviousStepInfo(String previousStepInfo) {
            this.previousStepInfo = previousStepInfo;
            return this;
        }

        public Builder setPhaseDefinition(String phaseDefinition) {
            this.phaseDefinition = phaseDefinition;
            return this;
        }

        public Builder setIndexCreationDate(Long indexCreationDate) {
            this.indexCreationDate = indexCreationDate;
            return this;
        }

        public Builder setPhaseTime(Long phaseTime) {
            this.phaseTime = phaseTime;
            return this;
        }

        public Builder setActionTime(Long actionTime) {
            this.actionTime = actionTime;
            return this;
        }

        public Builder setStepTime(Long stepTime) {
            this.stepTime = stepTime;
            return this;
        }

        public Builder setIsAutoRetryableError(Boolean isAutoRetryableError) {
            this.isAutoRetryableError = isAutoRetryableError;
            return this;
        }

        public Builder setFailedStepRetryCount(Integer failedStepRetryCount) {
            this.failedStepRetryCount = failedStepRetryCount;
            return this;
        }

        public Builder setSnapshotRepository(String snapshotRepository) {
            this.snapshotRepository = snapshotRepository;
            return this;
        }

        public Builder setSnapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
            return this;
        }

        public Builder setShrinkIndexName(String shrinkIndexName) {
            this.shrinkIndexName = shrinkIndexName;
            return this;
        }

        public Builder setSnapshotIndexName(String snapshotIndexName) {
            this.snapshotIndexName = snapshotIndexName;
            return this;
        }

        public Builder setDownsampleIndexName(String downsampleIndexName) {
            this.downsampleIndexName = downsampleIndexName;
            return this;
        }

        public LifecycleExecutionState build() {
            return new LifecycleExecutionState(
                phase,
                action,
                step,
                failedStep,
                isAutoRetryableError,
                failedStepRetryCount,
                stepInfo,
                previousStepInfo,
                phaseDefinition,
                indexCreationDate,
                phaseTime,
                actionTime,
                stepTime,
                snapshotRepository,
                snapshotName,
                shrinkIndexName,
                snapshotIndexName,
                downsampleIndexName
            );
        }
    }

}
