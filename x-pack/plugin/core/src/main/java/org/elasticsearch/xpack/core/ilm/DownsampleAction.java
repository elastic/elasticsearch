/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.action.downsample.DownsampleConfig.generateDownsampleIndexName;
import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@link LifecycleAction} which calls {@link org.elasticsearch.action.downsample.DownsampleAction} on an index
 */
public class DownsampleAction implements LifecycleAction {

    private static final Logger logger = LogManager.getLogger(DownsampleAction.class);

    public static final String NAME = "downsample";
    public static final String DOWNSAMPLED_INDEX_PREFIX = "downsample-";
    public static final String CONDITIONAL_TIME_SERIES_CHECK_KEY = BranchingStep.NAME + "-on-timeseries-check";
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";
    public static final TimeValue DEFAULT_WAIT_TIMEOUT = new TimeValue(1, TimeUnit.DAYS);
    private static final ParseField FIXED_INTERVAL_FIELD = new ParseField(DownsampleConfig.FIXED_INTERVAL);
    private static final ParseField WAIT_TIMEOUT_FIELD = new ParseField("wait_timeout");
    static final String BWC_CLEANUP_TARGET_INDEX_NAME = "cleanup-target-index";

    private static final ConstructingObjectParser<DownsampleAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new DownsampleAction((DateHistogramInterval) a[0], (TimeValue) a[1])
    );

    static {
        PARSER.declareField(
            constructorArg(),
            p -> new DateHistogramInterval(p.text()),
            FIXED_INTERVAL_FIELD,
            ObjectParser.ValueType.STRING
        );
        PARSER.declareField(
            optionalConstructorArg(),
            p -> TimeValue.parseTimeValue(p.textOrNull(), WAIT_TIMEOUT_FIELD.getPreferredName()),
            WAIT_TIMEOUT_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    private final DateHistogramInterval fixedInterval;
    private final TimeValue waitTimeout;

    public static DownsampleAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public DownsampleAction(final DateHistogramInterval fixedInterval, final TimeValue waitTimeout) {
        if (fixedInterval == null) {
            throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL_FIELD.getPreferredName() + "] is required.");
        }
        this.fixedInterval = fixedInterval;
        this.waitTimeout = waitTimeout == null ? DEFAULT_WAIT_TIMEOUT : waitTimeout;
    }

    public DownsampleAction(StreamInput in) throws IOException {
        this(
            new DateHistogramInterval(in),
            in.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)
                ? TimeValue.parseTimeValue(in.readString(), WAIT_TIMEOUT_FIELD.getPreferredName())
                : DEFAULT_WAIT_TIMEOUT
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fixedInterval.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_10_X)) {
            out.writeString(waitTimeout.getStringRep());
        } else {
            out.writeString(DEFAULT_WAIT_TIMEOUT.getStringRep());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIXED_INTERVAL_FIELD.getPreferredName(), fixedInterval.toString());
        builder.field(WAIT_TIMEOUT_FIELD.getPreferredName(), waitTimeout.getStringRep());
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public DateHistogramInterval fixedInterval() {
        return fixedInterval;
    }

    public TimeValue waitTimeout() {
        return waitTimeout;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey timeSeriesIndexCheckBranchKey = new StepKey(phase, NAME, CONDITIONAL_TIME_SERIES_CHECK_KEY);
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey waitTimeSeriesEndTimePassesKey = new StepKey(phase, NAME, WaitUntilTimeSeriesEndTimePassesStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyStep.NAME);
        StepKey cleanupDownsampleIndexKey = new StepKey(phase, NAME, BWC_CLEANUP_TARGET_INDEX_NAME);
        StepKey generateDownsampleIndexNameKey = new StepKey(phase, NAME, DownsamplePrepareLifeCycleStateStep.NAME);
        StepKey downsampleKey = new StepKey(phase, NAME, DownsampleStep.NAME);
        StepKey waitForDownsampleIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey copyIndexLifecycleKey = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey dataStreamCheckBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);
        StepKey swapAliasesKey = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);

        // If the source index is not a time-series index, we should skip the downsample action completely
        BranchingStep isTimeSeriesIndexBranchingStep = new BranchingStep(
            timeSeriesIndexCheckBranchKey,
            nextStepKey,
            checkNotWriteIndex,
            (index, project) -> {
                IndexMetadata indexMetadata = project.index(index);
                assert indexMetadata != null : "invalid cluster metadata. index [" + index.getName() + "] metadata not found";
                if (IndexSettings.MODE.get(indexMetadata.getSettings()) != IndexMode.TIME_SERIES) {
                    return false;
                }

                if (index.getName().equals(generateDownsampleIndexName(DOWNSAMPLED_INDEX_PREFIX, indexMetadata, fixedInterval))) {
                    var downsampleStatus = IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
                    if (downsampleStatus == IndexMetadata.DownsampleTaskStatus.UNKNOWN) {
                        // This isn't a downsample index, but it has the name of our target downsample index - very bad, we'll skip the
                        // downsample action to avoid blocking the lifecycle of this index - if there
                        // is another downsample action configured in the next phase, it'll be able to proceed successfully
                        logger.warn(
                            "index [{}] as part of policy [{}] cannot be downsampled at interval [{}] in phase [{}] because it has"
                                + " the name of the target downsample index and is itself not a downsampled index. Skipping the downsample "
                                + "action.",
                            index.getName(),
                            indexMetadata.getLifecyclePolicyName(),
                            fixedInterval,
                            phase
                        );
                    }
                    return false;
                }

                return true;
            }
        );

        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNotWriteIndex,
            waitForNoFollowerStepKey
        );
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(
            waitForNoFollowerStepKey,
            waitTimeSeriesEndTimePassesKey,
            client
        );

        WaitUntilTimeSeriesEndTimePassesStep waitUntilTimeSeriesEndTimeStep = new WaitUntilTimeSeriesEndTimePassesStep(
            waitTimeSeriesEndTimePassesKey,
            readOnlyKey,
            Instant::now
        );
        // Mark source index as read-only
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, generateDownsampleIndexNameKey, client, true);

        // Before the downsample action was retry-able, we used to generate a unique downsample index name and delete the previous index in
        // case a failure occurred. The downsample action can now retry execution in case of failure and start where it left off, so no
        // unique name needs to be generated and the target index is now predictable and generated in the downsample step.
        // (This noop step exists so deployments that are in this step (that has been converted to a noop) when the Elasticsearch
        // upgrade was performed resume the ILM execution and complete the downsample action after upgrade.)
        NoopStep cleanupDownsampleIndexStep = new NoopStep(cleanupDownsampleIndexKey, downsampleKey);

        // Prepare the lifecycleState by generating the name of the target index, that subsequent steps will use.
        DownsamplePrepareLifeCycleStateStep generateDownsampleIndexNameStep = new DownsamplePrepareLifeCycleStateStep(
            generateDownsampleIndexNameKey,
            downsampleKey,
            fixedInterval
        );

        // Here is where the actual downsample action takes place
        DownsampleStep downsampleStep = new DownsampleStep(downsampleKey, waitForDownsampleIndexKey, client, fixedInterval, waitTimeout);

        // Wait until the downsampled index is recovered. We again wait until the configured threshold is breached and
        // if the downsampled index has not successfully recovered until then, we rewind to the "cleanup-downsample-index"
        // step to delete this unsuccessful downsampled index and retry the operation by generating a new downsample index
        // name and attempting to downsample again.
        ClusterStateWaitUntilThresholdStep downsampleAllocatedStep = new ClusterStateWaitUntilThresholdStep(
            new WaitForIndexColorStep(
                waitForDownsampleIndexKey,
                copyMetadataKey,
                ClusterHealthStatus.YELLOW,
                (indexName, lifecycleState) -> lifecycleState.downsampleIndexName()
            ),
            cleanupDownsampleIndexKey
        );

        CopyExecutionStateStep copyExecutionStateStep = new CopyExecutionStateStep(
            copyMetadataKey,
            copyIndexLifecycleKey,
            (indexName, lifecycleState) -> lifecycleState.downsampleIndexName(),
            nextStepKey
        );

        CopySettingsStep copyLifecycleSettingsStep = new CopySettingsStep(
            copyIndexLifecycleKey,
            dataStreamCheckBranchingKey,
            (indexName, lifecycleState) -> lifecycleState.downsampleIndexName(),
            LifecycleSettings.LIFECYCLE_NAME_SETTING.getKey()
        );

        // By the time we get to this step we have 2 indices, the source and the downsampled one. We now need to choose an index
        // swapping strategy such that the downsampled index takes the place of the source index (which will also be deleted).
        // If the source index is part of a data stream it's a matter of replacing it with the downsampled index one in the data stream and
        // then deleting the source index.
        BranchingStep isDataStreamBranchingStep = new BranchingStep(
            dataStreamCheckBranchingKey,
            swapAliasesKey,
            replaceDataStreamIndexKey,
            (index, project) -> {
                IndexAbstraction indexAbstraction = project.getIndicesLookup().get(index.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
                return indexAbstraction.getParentDataStream() != null;
            }
        );

        ReplaceDataStreamBackingIndexStep replaceDataStreamBackingIndex = new ReplaceDataStreamBackingIndexStep(
            replaceDataStreamIndexKey,
            deleteIndexKey,
            (sourceIndexName, lifecycleState) -> lifecycleState.downsampleIndexName()
        );
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteIndexKey, nextStepKey, client);

        SwapAliasesAndDeleteSourceIndexStep swapAliasesAndDeleteSourceIndexStep = new SwapAliasesAndDeleteSourceIndexStep(
            swapAliasesKey,
            nextStepKey,
            client,
            (indexName, lifecycleState) -> lifecycleState.downsampleIndexName(),
            false
        );

        return List.of(
            isTimeSeriesIndexBranchingStep,
            checkNotWriteIndexStep,
            waitForNoFollowersStep,
            waitUntilTimeSeriesEndTimeStep,
            readOnlyStep,
            cleanupDownsampleIndexStep,
            generateDownsampleIndexNameStep,
            downsampleStep,
            downsampleAllocatedStep,
            copyExecutionStateStep,
            copyLifecycleSettingsStep,
            isDataStreamBranchingStep,
            replaceDataStreamBackingIndex,
            deleteSourceIndexStep,
            swapAliasesAndDeleteSourceIndexStep
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        DownsampleAction that = (DownsampleAction) o;
        return Objects.equals(this.fixedInterval, that.fixedInterval);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fixedInterval);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
