/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.TransportVersion;
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
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * A {@link LifecycleAction} which calls {@link org.elasticsearch.xpack.core.downsample.DownsampleAction} on an index
 */
public class DownsampleAction implements LifecycleAction {

    public static final String NAME = "downsample";
    public static final String DOWNSAMPLED_INDEX_PREFIX = "downsample-";
    public static final String CONDITIONAL_TIME_SERIES_CHECK_KEY = BranchingStep.NAME + "-on-timeseries-check";
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";
    public static final String GENERATE_DOWNSAMPLE_STEP_NAME = "generate-downsampled-index-name";
    private static final ParseField FIXED_INTERVAL_FIELD = new ParseField(DownsampleConfig.FIXED_INTERVAL);
    private static final ParseField TIMEOUT_FIELD = new ParseField(DownsampleConfig.TIMEOUT);

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
            p -> TimeValue.parseTimeValue(p.textOrNull(), TIMEOUT_FIELD.getPreferredName()),
            TIMEOUT_FIELD,
            ObjectParser.ValueType.STRING
        );
    }

    private final DateHistogramInterval fixedInterval;
    private final TimeValue timeout;

    public static DownsampleAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public DownsampleAction(final DateHistogramInterval fixedInterval, final TimeValue timeout) {
        if (fixedInterval == null) {
            throw new IllegalArgumentException("Parameter [" + FIXED_INTERVAL_FIELD.getPreferredName() + "] is required.");
        }
        this.fixedInterval = fixedInterval;
        this.timeout = timeout == null ? DownsampleConfig.DEFAULT_TIMEOUT : timeout;
    }

    public DownsampleAction(StreamInput in) throws IOException {
        this(
            new DateHistogramInterval(in),
            in.getTransportVersion().onOrAfter(TransportVersion.V_8_500_037)
                ? TimeValue.parseTimeValue(in.readString(), TIMEOUT_FIELD.getPreferredName())
                : DownsampleConfig.DEFAULT_TIMEOUT
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        fixedInterval.writeTo(out);
        if (out.getTransportVersion().onOrAfter(TransportVersion.V_8_500_037)) {
            out.writeString(timeout.getStringRep());
        } else {
            out.writeString(DownsampleConfig.DEFAULT_TIMEOUT.getStringRep());
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(FIXED_INTERVAL_FIELD.getPreferredName(), fixedInterval.toString());
        builder.field(TIMEOUT_FIELD.getPreferredName(), timeout.getStringRep());
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

    public TimeValue timeout() {
        return timeout;
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
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyStep.NAME);
        StepKey cleanupDownsampleIndexKey = new StepKey(phase, NAME, CleanupTargetIndexStep.NAME);
        StepKey generateDownsampleIndexNameKey = new StepKey(phase, NAME, GENERATE_DOWNSAMPLE_STEP_NAME);
        StepKey downsampleKey = new StepKey(phase, NAME, DownsampleStep.NAME);
        StepKey waitForDownsampleIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey dataStreamCheckBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);
        StepKey swapAliasesKey = new StepKey(phase, NAME, SwapAliasesAndDeleteSourceIndexStep.NAME);

        // If the source index is not a time-series index, we should skip the downsample action completely
        BranchingStep isTimeSeriesIndexBranchingStep = new BranchingStep(
            timeSeriesIndexCheckBranchKey,
            nextStepKey,
            checkNotWriteIndex,
            (index, clusterState) -> {
                IndexMetadata indexMetadata = clusterState.metadata().index(index);
                assert indexMetadata != null : "invalid cluster metadata. index [" + index.getName() + "] metadata not found";
                return IndexSettings.MODE.get(indexMetadata.getSettings()) == IndexMode.TIME_SERIES;
            }
        );

        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNotWriteIndex,
            waitForNoFollowerStepKey
        );
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, readOnlyKey, client);

        // Mark source index as read-only
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, cleanupDownsampleIndexKey, client);

        // We generate a unique downsample index name, but we also retry if the allocation of the downsample index
        // is not possible, so we want to delete the "previously generated" downsample index (this is a no-op if it's
        // the first run of the action, and we haven't generated a downsample index name)
        CleanupTargetIndexStep cleanupDownsampleIndexStep = new CleanupTargetIndexStep(
            cleanupDownsampleIndexKey,
            generateDownsampleIndexNameKey,
            client,
            (indexMetadata) -> IndexMetadata.INDEX_DOWNSAMPLE_SOURCE_NAME.get(indexMetadata.getSettings()),
            (indexMetadata) -> indexMetadata.getLifecycleExecutionState().downsampleIndexName()
        );

        // Generate a unique downsample index name and store it in the ILM execution state
        GenerateUniqueIndexNameStep generateDownsampleIndexNameStep = new GenerateDownsampleIndexNameStep(
            generateDownsampleIndexNameKey,
            downsampleKey,
            DOWNSAMPLED_INDEX_PREFIX,
            fixedInterval,
            (downsampleIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setDownsampleIndexName(downsampleIndexName)
        );

        // Here is where the actual downsample action takes place
        DownsampleStep downsampleStep = new DownsampleStep(
            downsampleKey,
            waitForDownsampleIndexKey,
            cleanupDownsampleIndexKey,
            client,
            fixedInterval,
            timeout
        );

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
            dataStreamCheckBranchingKey,
            (indexName, lifecycleState) -> lifecycleState.downsampleIndexName(),
            nextStepKey
        );

        // By the time we get to this step we have 2 indices, the source and the downsampled one. We now need to choose an index
        // swapping strategy such that the downsampled index takes the place of the source index (which will also be deleted).
        // If the source index is part of a data stream it's a matter of replacing it with the downsampled index one in the data stream and
        // then deleting the source index.
        BranchingStep isDataStreamBranchingStep = new BranchingStep(
            dataStreamCheckBranchingKey,
            swapAliasesKey,
            replaceDataStreamIndexKey,
            (index, clusterState) -> {
                IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(index.getName());
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
            readOnlyStep,
            cleanupDownsampleIndexStep,
            generateDownsampleIndexNameStep,
            downsampleStep,
            downsampleAllocatedStep,
            copyExecutionStateStep,
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
