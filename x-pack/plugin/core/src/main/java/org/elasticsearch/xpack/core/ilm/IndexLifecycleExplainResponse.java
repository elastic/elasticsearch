/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentType;
import org.elasticsearch.xcontent.json.JsonXContent;

import java.io.IOException;
import java.util.Objects;
import java.util.function.Supplier;
import java.util.stream.Stream;

public class IndexLifecycleExplainResponse implements ToXContentObject, Writeable {

    private static final ParseField INDEX_FIELD = new ParseField("index");
    private static final ParseField INDEX_CREATION_DATE_MILLIS_FIELD = new ParseField("index_creation_date_millis");
    private static final ParseField INDEX_CREATION_DATE_FIELD = new ParseField("index_creation_date");
    private static final ParseField MANAGED_BY_ILM_FIELD = new ParseField("managed");
    private static final ParseField POLICY_NAME_FIELD = new ParseField("policy");
    private static final ParseField LIFECYCLE_DATE_MILLIS_FIELD = new ParseField("lifecycle_date_millis");
    private static final ParseField LIFECYCLE_DATE_FIELD = new ParseField("lifecycle_date");
    private static final ParseField PHASE_FIELD = new ParseField("phase");
    private static final ParseField ACTION_FIELD = new ParseField("action");
    private static final ParseField STEP_FIELD = new ParseField("step");
    private static final ParseField FAILED_STEP_FIELD = new ParseField("failed_step");
    private static final ParseField IS_AUTO_RETRYABLE_ERROR_FIELD = new ParseField("is_auto_retryable_error");
    private static final ParseField FAILED_STEP_RETRY_COUNT_FIELD = new ParseField("failed_step_retry_count");
    private static final ParseField PHASE_TIME_MILLIS_FIELD = new ParseField("phase_time_millis");
    private static final ParseField PHASE_TIME_FIELD = new ParseField("phase_time");
    private static final ParseField ACTION_TIME_MILLIS_FIELD = new ParseField("action_time_millis");
    private static final ParseField ACTION_TIME_FIELD = new ParseField("action_time");
    private static final ParseField STEP_TIME_MILLIS_FIELD = new ParseField("step_time_millis");
    private static final ParseField STEP_TIME_FIELD = new ParseField("step_time");
    private static final ParseField STEP_INFO_FIELD = new ParseField("step_info");
    private static final ParseField PREVIOUS_STEP_INFO_FIELD = new ParseField("previous_step_info");
    private static final ParseField PHASE_EXECUTION_INFO = new ParseField("phase_execution");
    private static final ParseField AGE_FIELD = new ParseField("age");
    private static final ParseField TIME_SINCE_INDEX_CREATION_FIELD = new ParseField("time_since_index_creation");
    private static final ParseField REPOSITORY_NAME = new ParseField("repository_name");
    private static final ParseField SHRINK_INDEX_NAME = new ParseField("shrink_index_name");
    private static final ParseField SNAPSHOT_NAME = new ParseField("snapshot_name");
    private static final ParseField SKIP_NAME = new ParseField("skip");

    public static final ConstructingObjectParser<IndexLifecycleExplainResponse, Void> PARSER = new ConstructingObjectParser<>(
        "index_lifecycle_explain_response",
        a -> new IndexLifecycleExplainResponse(
            (String) a[0],
            (Long) (a[19]),
            (boolean) a[1],
            (String) a[2],
            (Long) (a[3]),
            (String) a[4],
            (String) a[5],
            (String) a[6],
            (String) a[7],
            (Boolean) a[14],
            (Integer) a[15],
            (Long) (a[8]),
            (Long) (a[9]),
            (Long) (a[10]),
            (String) a[16],
            (String) a[17],
            (String) a[18],
            (BytesReference) a[11],
            (BytesReference) a[21],
            (PhaseExecutionInfo) a[12],
            Objects.requireNonNullElse((Boolean) a[22], false)
            // a[13] == "age"
            // a[20] == "time_since_index_creation"
        )
    );
    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), INDEX_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.constructorArg(), MANAGED_BY_ILM_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), POLICY_NAME_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), LIFECYCLE_DATE_MILLIS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PHASE_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ACTION_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), STEP_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FAILED_STEP_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), PHASE_TIME_MILLIS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), ACTION_TIME_MILLIS_FIELD);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), STEP_TIME_MILLIS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.copyCurrentStructure(p);
            return BytesReference.bytes(builder);
        }, STEP_INFO_FIELD);
        PARSER.declareObject(
            ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> PhaseExecutionInfo.parse(p, ""),
            PHASE_EXECUTION_INFO
        );
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), AGE_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), IS_AUTO_RETRYABLE_ERROR_FIELD);
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), FAILED_STEP_RETRY_COUNT_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), REPOSITORY_NAME);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SNAPSHOT_NAME);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), SHRINK_INDEX_NAME);
        PARSER.declareLong(ConstructingObjectParser.optionalConstructorArg(), INDEX_CREATION_DATE_MILLIS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), TIME_SINCE_INDEX_CREATION_FIELD);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> {
            XContentBuilder builder = JsonXContent.contentBuilder();
            builder.copyCurrentStructure(p);
            return BytesReference.bytes(builder);
        }, PREVIOUS_STEP_INFO_FIELD);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), SKIP_NAME);
    }

    private final String index;
    private final Long indexCreationDate;
    private final String policyName;
    private final String phase;
    private final String action;
    private final String step;
    private final String failedStep;
    private final Long lifecycleDate;
    private final Long phaseTime;
    private final Long actionTime;
    private final Long stepTime;
    private final boolean managedByILM;
    private final BytesReference stepInfo;
    private final BytesReference previousStepInfo;
    private final PhaseExecutionInfo phaseExecutionInfo;
    private final Boolean isAutoRetryableError;
    private final Integer failedStepRetryCount;
    private final String repositoryName;
    private final String snapshotName;
    private final String shrinkIndexName;
    private final boolean skip;

    Supplier<Long> nowSupplier = System::currentTimeMillis; // Can be changed for testing

    public static IndexLifecycleExplainResponse newManagedIndexResponse(
        String index,
        Long indexCreationDate,
        String policyName,
        Long lifecycleDate,
        String phase,
        String action,
        String step,
        String failedStep,
        Boolean isAutoRetryableError,
        Integer failedStepRetryCount,
        Long phaseTime,
        Long actionTime,
        Long stepTime,
        String repositoryName,
        String snapshotName,
        String shrinkIndexName,
        BytesReference stepInfo,
        BytesReference previousStepInfo,
        PhaseExecutionInfo phaseExecutionInfo,
        boolean skip
    ) {
        return new IndexLifecycleExplainResponse(
            index,
            indexCreationDate,
            true,
            policyName,
            lifecycleDate,
            phase,
            action,
            step,
            failedStep,
            isAutoRetryableError,
            failedStepRetryCount,
            phaseTime,
            actionTime,
            stepTime,
            repositoryName,
            snapshotName,
            shrinkIndexName,
            stepInfo,
            previousStepInfo,
            phaseExecutionInfo,
            skip
        );
    }

    public static IndexLifecycleExplainResponse newUnmanagedIndexResponse(String index) {
        return new IndexLifecycleExplainResponse(
            index,
            null,
            false,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            null,
            false
        );
    }

    private IndexLifecycleExplainResponse(
        String index,
        Long indexCreationDate,
        boolean managedByILM,
        String policyName,
        Long lifecycleDate,
        String phase,
        String action,
        String step,
        String failedStep,
        Boolean isAutoRetryableError,
        Integer failedStepRetryCount,
        Long phaseTime,
        Long actionTime,
        Long stepTime,
        String repositoryName,
        String snapshotName,
        String shrinkIndexName,
        BytesReference stepInfo,
        BytesReference previousStepInfo,
        PhaseExecutionInfo phaseExecutionInfo,
        boolean skip
    ) {
        if (managedByILM) {
            if (policyName == null) {
                throw new IllegalArgumentException("[" + POLICY_NAME_FIELD.getPreferredName() + "] cannot be null for managed index");
            }

            // If at least one detail is null, but not *all* are null
            if (Stream.of(phase, action, step).anyMatch(Objects::isNull)
                && Stream.of(phase, action, step).allMatch(Objects::isNull) == false) {
                // …and it's not in the error step
                if (ErrorStep.NAME.equals(step) == false) {
                    throw new IllegalArgumentException(
                        "managed index response must have complete step details ["
                            + PHASE_FIELD.getPreferredName()
                            + "="
                            + phase
                            + ", "
                            + ACTION_FIELD.getPreferredName()
                            + "="
                            + action
                            + ", "
                            + STEP_FIELD.getPreferredName()
                            + "="
                            + step
                            + "]"
                    );
                }
            }
        } else {
            if (policyName != null
                || indexCreationDate != null
                || lifecycleDate != null
                || phase != null
                || action != null
                || step != null
                || failedStep != null
                || phaseTime != null
                || actionTime != null
                || stepTime != null
                || stepInfo != null
                || previousStepInfo != null
                || phaseExecutionInfo != null) {
                throw new IllegalArgumentException(
                    "Unmanaged index response must only contain fields: [" + MANAGED_BY_ILM_FIELD + ", " + INDEX_FIELD + "]"
                );
            }
        }
        this.index = index;
        this.indexCreationDate = indexCreationDate;
        this.policyName = policyName;
        this.managedByILM = managedByILM;
        this.lifecycleDate = lifecycleDate;
        this.phase = phase;
        this.action = action;
        this.step = step;
        this.phaseTime = phaseTime;
        this.actionTime = actionTime;
        this.stepTime = stepTime;
        this.failedStep = failedStep;
        this.isAutoRetryableError = isAutoRetryableError;
        this.failedStepRetryCount = failedStepRetryCount;
        this.stepInfo = stepInfo;
        this.previousStepInfo = previousStepInfo;
        this.phaseExecutionInfo = phaseExecutionInfo;
        this.repositoryName = repositoryName;
        this.snapshotName = snapshotName;
        this.shrinkIndexName = shrinkIndexName;
        this.skip = skip;
    }

    public IndexLifecycleExplainResponse(StreamInput in) throws IOException {
        index = in.readString();
        managedByILM = in.readBoolean();
        if (managedByILM) {
            policyName = in.readString();
            lifecycleDate = in.readOptionalLong();
            phase = in.readOptionalString();
            action = in.readOptionalString();
            step = in.readOptionalString();
            failedStep = in.readOptionalString();
            phaseTime = in.readOptionalLong();
            actionTime = in.readOptionalLong();
            stepTime = in.readOptionalLong();
            stepInfo = in.readOptionalBytesReference();
            phaseExecutionInfo = in.readOptionalWriteable(PhaseExecutionInfo::new);
            isAutoRetryableError = in.readOptionalBoolean();
            failedStepRetryCount = in.readOptionalVInt();
            repositoryName = in.readOptionalString();
            snapshotName = in.readOptionalString();
            shrinkIndexName = in.readOptionalString();
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
                indexCreationDate = in.readOptionalLong();
            } else {
                indexCreationDate = null;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                previousStepInfo = in.readOptionalBytesReference();
            } else {
                previousStepInfo = null;
            }
            if (in.getTransportVersion().isPatchFrom(TransportVersions.ILM_ADD_SKIP_SETTING_8_19)
                || in.getTransportVersion().onOrAfter(TransportVersions.ILM_ADD_SKIP_SETTING)) {
                skip = in.readBoolean();
            } else {
                skip = false;
            }
        } else {
            policyName = null;
            lifecycleDate = null;
            phase = null;
            action = null;
            step = null;
            failedStep = null;
            isAutoRetryableError = null;
            failedStepRetryCount = null;
            phaseTime = null;
            actionTime = null;
            stepTime = null;
            stepInfo = null;
            previousStepInfo = null;
            phaseExecutionInfo = null;
            repositoryName = null;
            snapshotName = null;
            shrinkIndexName = null;
            indexCreationDate = null;
            skip = false;
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeBoolean(managedByILM);
        if (managedByILM) {
            out.writeString(policyName);
            out.writeOptionalLong(lifecycleDate);
            out.writeOptionalString(phase);
            out.writeOptionalString(action);
            out.writeOptionalString(step);
            out.writeOptionalString(failedStep);
            out.writeOptionalLong(phaseTime);
            out.writeOptionalLong(actionTime);
            out.writeOptionalLong(stepTime);
            out.writeOptionalBytesReference(stepInfo);
            out.writeOptionalWriteable(phaseExecutionInfo);
            out.writeOptionalBoolean(isAutoRetryableError);
            out.writeOptionalVInt(failedStepRetryCount);
            out.writeOptionalString(repositoryName);
            out.writeOptionalString(snapshotName);
            out.writeOptionalString(shrinkIndexName);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_1_0)) {
                out.writeOptionalLong(indexCreationDate);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
                out.writeOptionalBytesReference(previousStepInfo);
            }
            if (out.getTransportVersion().isPatchFrom(TransportVersions.ILM_ADD_SKIP_SETTING_8_19)
                || out.getTransportVersion().onOrAfter(TransportVersions.ILM_ADD_SKIP_SETTING)) {
                out.writeBoolean(skip);
            }
        }
    }

    public String getIndex() {
        return index;
    }

    public Long getIndexCreationDate() {
        return indexCreationDate;
    }

    public TimeValue getTimeSinceIndexCreation(Supplier<Long> now) {
        if (indexCreationDate == null) {
            return null;
        } else {
            return TimeValue.timeValueMillis(Math.max(0L, now.get() - indexCreationDate));
        }
    }

    public boolean managedByILM() {
        return managedByILM;
    }

    public String getPolicyName() {
        return policyName;
    }

    public Long getLifecycleDate() {
        return lifecycleDate;
    }

    public String getPhase() {
        return phase;
    }

    public Long getPhaseTime() {
        return phaseTime;
    }

    public String getAction() {
        return action;
    }

    public Long getActionTime() {
        return actionTime;
    }

    public String getStep() {
        return step;
    }

    public Long getStepTime() {
        return stepTime;
    }

    public String getFailedStep() {
        return failedStep;
    }

    public BytesReference getStepInfo() {
        return stepInfo;
    }

    public BytesReference getPreviousStepInfo() {
        return previousStepInfo;
    }

    public PhaseExecutionInfo getPhaseExecutionInfo() {
        return phaseExecutionInfo;
    }

    public Boolean isAutoRetryableError() {
        return isAutoRetryableError;
    }

    public Integer getFailedStepRetryCount() {
        return failedStepRetryCount;
    }

    public TimeValue getAge(Supplier<Long> now) {
        if (lifecycleDate == null) {
            return TimeValue.MINUS_ONE;
        } else {
            return TimeValue.timeValueMillis(Math.max(0L, now.get() - lifecycleDate));
        }
    }

    public String getRepositoryName() {
        return repositoryName;
    }

    public String getSnapshotName() {
        return snapshotName;
    }

    public String getShrinkIndexName() {
        return shrinkIndexName;
    }

    public boolean getSkip() {
        return skip;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(INDEX_FIELD.getPreferredName(), index);
        builder.field(MANAGED_BY_ILM_FIELD.getPreferredName(), managedByILM);
        if (managedByILM) {
            builder.field(POLICY_NAME_FIELD.getPreferredName(), policyName);
            if (indexCreationDate != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    INDEX_CREATION_DATE_MILLIS_FIELD.getPreferredName(),
                    INDEX_CREATION_DATE_FIELD.getPreferredName(),
                    indexCreationDate
                );
                builder.field(
                    TIME_SINCE_INDEX_CREATION_FIELD.getPreferredName(),
                    getTimeSinceIndexCreation(nowSupplier).toHumanReadableString(2)
                );
            }
            if (lifecycleDate != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    LIFECYCLE_DATE_MILLIS_FIELD.getPreferredName(),
                    LIFECYCLE_DATE_FIELD.getPreferredName(),
                    lifecycleDate
                );
                builder.field(AGE_FIELD.getPreferredName(), getAge(nowSupplier).toHumanReadableString(2));
            }
            if (phase != null) {
                builder.field(PHASE_FIELD.getPreferredName(), phase);
            }
            if (phaseTime != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    PHASE_TIME_MILLIS_FIELD.getPreferredName(),
                    PHASE_TIME_FIELD.getPreferredName(),
                    phaseTime
                );
            }
            if (action != null) {
                builder.field(ACTION_FIELD.getPreferredName(), action);
            }
            if (actionTime != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    ACTION_TIME_MILLIS_FIELD.getPreferredName(),
                    ACTION_TIME_FIELD.getPreferredName(),
                    actionTime
                );
            }
            if (step != null) {
                builder.field(STEP_FIELD.getPreferredName(), step);
            }
            if (stepTime != null) {
                builder.timestampFieldsFromUnixEpochMillis(
                    STEP_TIME_MILLIS_FIELD.getPreferredName(),
                    STEP_TIME_FIELD.getPreferredName(),
                    stepTime
                );
            }
            if (Strings.hasLength(failedStep)) {
                builder.field(FAILED_STEP_FIELD.getPreferredName(), failedStep);
            }
            if (isAutoRetryableError != null) {
                builder.field(IS_AUTO_RETRYABLE_ERROR_FIELD.getPreferredName(), isAutoRetryableError);
            }
            if (failedStepRetryCount != null) {
                builder.field(FAILED_STEP_RETRY_COUNT_FIELD.getPreferredName(), failedStepRetryCount);
            }
            if (repositoryName != null) {
                builder.field(REPOSITORY_NAME.getPreferredName(), repositoryName);
            }
            if (snapshotName != null) {
                builder.field(SNAPSHOT_NAME.getPreferredName(), snapshotName);
            }
            if (shrinkIndexName != null) {
                builder.field(SHRINK_INDEX_NAME.getPreferredName(), shrinkIndexName);
            }
            if (stepInfo != null && stepInfo.length() > 0) {
                builder.rawField(STEP_INFO_FIELD.getPreferredName(), stepInfo.streamInput(), XContentType.JSON);
            }
            if (previousStepInfo != null && previousStepInfo.length() > 0) {
                builder.rawField(PREVIOUS_STEP_INFO_FIELD.getPreferredName(), previousStepInfo.streamInput(), XContentType.JSON);
            }
            if (phaseExecutionInfo != null) {
                builder.field(PHASE_EXECUTION_INFO.getPreferredName(), phaseExecutionInfo);
            }
            builder.field(SKIP_NAME.getPreferredName(), skip);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            index,
            indexCreationDate,
            managedByILM,
            policyName,
            lifecycleDate,
            phase,
            action,
            step,
            failedStep,
            isAutoRetryableError,
            failedStepRetryCount,
            phaseTime,
            actionTime,
            stepTime,
            repositoryName,
            snapshotName,
            shrinkIndexName,
            stepInfo,
            previousStepInfo,
            phaseExecutionInfo,
            skip
        );
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        IndexLifecycleExplainResponse other = (IndexLifecycleExplainResponse) obj;
        return Objects.equals(index, other.index)
            && Objects.equals(indexCreationDate, other.indexCreationDate)
            && Objects.equals(managedByILM, other.managedByILM)
            && Objects.equals(policyName, other.policyName)
            && Objects.equals(lifecycleDate, other.lifecycleDate)
            && Objects.equals(phase, other.phase)
            && Objects.equals(action, other.action)
            && Objects.equals(step, other.step)
            && Objects.equals(failedStep, other.failedStep)
            && Objects.equals(isAutoRetryableError, other.isAutoRetryableError)
            && Objects.equals(failedStepRetryCount, other.failedStepRetryCount)
            && Objects.equals(phaseTime, other.phaseTime)
            && Objects.equals(actionTime, other.actionTime)
            && Objects.equals(stepTime, other.stepTime)
            && Objects.equals(repositoryName, other.repositoryName)
            && Objects.equals(snapshotName, other.snapshotName)
            && Objects.equals(shrinkIndexName, other.shrinkIndexName)
            && Objects.equals(stepInfo, other.stepInfo)
            && Objects.equals(previousStepInfo, other.previousStepInfo)
            && Objects.equals(phaseExecutionInfo, other.phaseExecutionInfo)
            && Objects.equals(skip, other.skip);
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, true);
    }

}
