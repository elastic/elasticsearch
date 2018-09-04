/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */

package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.cluster.metadata.IndexMetaData;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class LifecycleExecutionState {
    public static final String ILM_CUSTOM_METADATA_KEY = "ilm";

    private static final String PHASE = "phase";
    private static final String ACTION = "action";
    private static final String STEP = "step";
    private static final String INDEX_CREATION_DATE = "creation_date";
    private static final String PHASE_TIME = "phase_time";
    private static final String ACTION_TIME = "action_time";
    private static final String STEP_TIME = "step_time";
    private static final String FAILED_STEP = "failed_step";
    private static final String STEP_INFO = "step_info";
    private static final String PHASE_DEFINITION = "phase_definition";

    private final String phase;
    private final String action;
    private final String step;
    private final String failedStep;
    private final String stepInfo;
    private final String phaseDefinition;
    private final long indexCreationDate;
    private final long phaseTime;
    private final long actionTime;
    private final long stepTime;

    private LifecycleExecutionState(String phase, String action, String step, String failedStep,
                                    String stepInfo, String phaseDefinition, long indexCreationDate,
                                    long phaseTime, long actionTime, long stepTime) {
        this.phase = phase;
        this.action = action;
        this.step = step;
        this.failedStep = failedStep;
        this.stepInfo = stepInfo;
        this.phaseDefinition = phaseDefinition;
        this.indexCreationDate = indexCreationDate;
        this.phaseTime = phaseTime;
        this.actionTime = actionTime;
        this.stepTime = stepTime;
    }

    /**
     * Retrieves the execution state from an {@link IndexMetaData} based on the
     * custom metadata.
     * @param indexMetaData The metadata of the index to retrieve the execution
     *                      state from.
     * @return The execution state of that index.
     */
    public static LifecycleExecutionState fromIndexMetadata(IndexMetaData indexMetaData) {
        Map<String, String> customData = indexMetaData.getCustomData(ILM_CUSTOM_METADATA_KEY);
        customData = customData == null ? new HashMap<>() : customData;
        return fromCustomMetadata(customData);
    }

    public static Builder builder() {
        return new Builder();
    }

    public static Builder builder(LifecycleExecutionState state) {
        return new Builder()
            .setPhase(state.phase)
            .setAction(state.action)
            .setStep(state.step)
            .setFailedStep(state.failedStep)
            .setStepInfo(state.stepInfo)
            .setPhaseDefinition(state.phaseDefinition)
            .setIndexCreationDate(state.indexCreationDate)
            .setPhaseTime(state.phaseTime)
            .setActionTime(state.actionTime)
            .setStepTime(state.stepTime);
    }

    static LifecycleExecutionState fromCustomMetadata(Map<String, String> customData) {
        return new Builder()
            .setPhase(customData.getOrDefault(PHASE, ""))
            .setAction(customData.getOrDefault(ACTION, ""))
            .setStep(customData.getOrDefault(STEP, ""))
            .setFailedStep(customData.getOrDefault(FAILED_STEP, ""))
            .setStepInfo(customData.getOrDefault(STEP_INFO, ""))
            .setPhaseDefinition(customData.getOrDefault(PHASE_DEFINITION, ""))
            .setIndexCreationDate(Long.parseLong(customData.getOrDefault(INDEX_CREATION_DATE, "-1")))
            .setPhaseTime(Long.parseLong(customData.getOrDefault(PHASE_TIME, "-1")))
            .setActionTime(Long.parseLong(customData.getOrDefault(ACTION_TIME, "-1")))
            .setStepTime(Long.parseLong(customData.getOrDefault(STEP_TIME, "-1")))
            .build();
    }

    /**
     * Converts this object to an immutable map representation for use with
     * {@link IndexMetaData.Builder#putCustom(String, Map)}.
     * @return An immutable Map representation of this execution state.
     */
    public Map<String, String> asMap() {
        Map<String, String> result = new HashMap<>();
        result.put(PHASE, phase);
        result.put(ACTION, action);
        result.put(STEP, step);
        result.put(FAILED_STEP, failedStep);
        result.put(STEP_INFO, stepInfo);
        result.put(INDEX_CREATION_DATE, String.valueOf(indexCreationDate));
        result.put(PHASE_TIME, String.valueOf(phaseTime));
        result.put(ACTION_TIME, String.valueOf(actionTime));
        result.put(STEP_TIME, String.valueOf(stepTime));
        result.put(PHASE_DEFINITION, String.valueOf(phaseDefinition));
        return Collections.unmodifiableMap(result);
    }

    public String getPhase() {
        return phase;
    }

    public String getAction() {
        return action;
    }

    public String getStep() {
        return step;
    }

    public String getFailedStep() {
        return failedStep;
    }

    public String getStepInfo() {
        return stepInfo;
    }

    public String getPhaseDefinition() {
        return phaseDefinition;
    }

    public long getIndexCreationDate() {
        return indexCreationDate;
    }

    public long getPhaseTime() {
        return phaseTime;
    }

    public long getActionTime() {
        return actionTime;
    }

    public long getStepTime() {
        return stepTime;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        LifecycleExecutionState that = (LifecycleExecutionState) o;
        return getIndexCreationDate() == that.getIndexCreationDate() &&
            getPhaseTime() == that.getPhaseTime() &&
            getActionTime() == that.getActionTime() &&
            getStepTime() == that.getStepTime() &&
            Objects.equals(getPhase(), that.getPhase()) &&
            Objects.equals(getAction(), that.getAction()) &&
            Objects.equals(getStep(), that.getStep()) &&
            Objects.equals(getFailedStep(), that.getFailedStep()) &&
            Objects.equals(getStepInfo(), that.getStepInfo()) &&
            Objects.equals(getPhaseDefinition(), that.getPhaseDefinition());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getPhase(), getAction(), getStep(), getFailedStep(), getStepInfo(), getPhaseDefinition(),
            getIndexCreationDate(), getPhaseTime(), getActionTime(), getStepTime());
    }

    public static class Builder {
        private String phase = "";
        private String action = "";
        private String step = "";
        private String failedStep = "";
        private String stepInfo = "";
        private String phaseDefinition = "";
        private long indexCreationDate = -1;
        private long phaseTime = -1;
        private long actionTime = -1;
        private long stepTime = -1;

        public Builder setPhase(String phase) {
            this.phase = Objects.requireNonNull(phase);
            return this;
        }

        public Builder setAction(String action) {
            this.action = Objects.requireNonNull(action);
            return this;
        }

        public Builder setStep(String step) {
            this.step = Objects.requireNonNull(step);
            return this;
        }

        public Builder setFailedStep(String failedStep) {
            this.failedStep = Objects.requireNonNull(failedStep);
            return this;
        }

        public Builder setStepInfo(String stepInfo) {
            this.stepInfo = Objects.requireNonNull(stepInfo);
            return this;
        }

        public Builder setPhaseDefinition(String phaseDefinition) {
            this.phaseDefinition = Objects.requireNonNull(phaseDefinition);
            return this;
        }

        public Builder setIndexCreationDate(long indexCreationDate) {
            this.indexCreationDate = indexCreationDate;
            return this;
        }

        public Builder setPhaseTime(long phaseTime) {
            this.phaseTime = phaseTime;
            return this;
        }

        public Builder setActionTime(long actionTime) {
            this.actionTime = actionTime;
            return this;
        }

        public Builder setStepTime(long stepTime) {
            this.stepTime = stepTime;
            return this;
        }

        public LifecycleExecutionState build() {
            return new LifecycleExecutionState(phase, action, step, failedStep, stepInfo, phaseDefinition, indexCreationDate,
                phaseTime, actionTime, stepTime);
        }
    }

}
