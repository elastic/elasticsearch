/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.health.ClusterHealthStatus;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.index.codec.CodecService;
import org.elasticsearch.index.engine.EngineConfig;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which force-merges the index.
 */
public class ForceMergeAction implements LifecycleAction {
    private static final Logger logger = LogManager.getLogger(ForceMergeAction.class);

    private static final Settings BEST_COMPRESSION_SETTINGS = Settings.builder()
        .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
        .build();

    public static final String NAME = "forcemerge";
    public static final ParseField MAX_NUM_SEGMENTS_FIELD = new ParseField("max_num_segments");
    public static final ParseField CODEC = new ParseField("index_codec");
    public static final String CONDITIONAL_SKIP_FORCE_MERGE_STEP = BranchingStep.NAME + "-forcemerge-check-prerequisites";

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME, false, a -> {
        int maxNumSegments = (int) a[0];
        String codec = a[1] != null ? (String) a[1] : null;
        return new ForceMergeAction(maxNumSegments, codec);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CODEC);
    }

    private final int maxNumSegments;
    private final String codec;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(int maxNumSegments, @Nullable String codec) {
        if (maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName() + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
        if (codec != null && CodecService.BEST_COMPRESSION_CODEC.equals(codec) == false) {
            throw new IllegalArgumentException("unknown index codec: [" + codec + "]");
        }
        this.codec = codec;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        this.maxNumSegments = in.readVInt();
        this.codec = in.readOptionalString();
    }

    public int getMaxNumSegments() {
        return maxNumSegments;
    }

    public String getCodec() {
        return this.codec;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(maxNumSegments);
        out.writeOptionalString(codec);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        if (codec != null) {
            builder.field(CODEC.getPreferredName(), codec);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        final boolean codecChange = codec != null && codec.equals(CodecService.BEST_COMPRESSION_CODEC);

        StepKey preForceMergeBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_FORCE_MERGE_STEP);
        StepKey checkNotWriteIndexKey = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);

        StepKey closeKey = new StepKey(phase, NAME, CloseIndexStep.NAME);
        StepKey updateCompressionKey = new StepKey(phase, NAME, UpdateSettingsStep.NAME);
        StepKey openKey = new StepKey(phase, NAME, OpenIndexStep.NAME);
        StepKey waitForGreenIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);

        StepKey forceMergeKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey countKey = new StepKey(phase, NAME, SegmentCountStep.NAME);

        BranchingStep conditionalSkipShrinkStep = new BranchingStep(
            preForceMergeBranchingKey,
            checkNotWriteIndexKey,
            nextStepKey,
            (index, clusterState) -> {
                IndexMetadata indexMetadata = clusterState.metadata().index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                if (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null) {
                    String policyName = indexMetadata.getLifecyclePolicyName();
                    logger.warn(
                        "[{}] action is configured for index [{}] in policy [{}] which is mounted as searchable snapshot. "
                            + "Skipping this action",
                        ForceMergeAction.NAME,
                        index.getName(),
                        policyName
                    );
                    return true;
                }
                return false;
            }
        );

        // Indices in this step key can skip the no-op step and jump directly to the step with closeKey/forcemergeKey key
        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNotWriteIndexKey,
            codecChange ? closeKey : forceMergeKey
        );

        // Indices already in this step key when upgrading need to know how to move forward but stop making the index
        // read-only. In order to achieve this we introduce a no-op step with the same key as the read-only step so that
        // the index can safely move to the next step without performing any read-only action nor getting stuck in this step
        NoopStep noopStep = new NoopStep(readOnlyKey, codecChange ? closeKey : forceMergeKey);

        CloseIndexStep closeIndexStep = new CloseIndexStep(closeKey, updateCompressionKey, client);
        UpdateSettingsStep updateBestCompressionSettings = new UpdateSettingsStep(
            updateCompressionKey,
            openKey,
            client,
            BEST_COMPRESSION_SETTINGS
        );
        OpenIndexStep openIndexStep = new OpenIndexStep(openKey, waitForGreenIndexKey, client);
        WaitForIndexColorStep waitForIndexGreenStep = new WaitForIndexColorStep(
            waitForGreenIndexKey,
            forceMergeKey,
            ClusterHealthStatus.GREEN
        );

        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, countKey, client, maxNumSegments);
        SegmentCountStep segmentCountStep = new SegmentCountStep(countKey, nextStepKey, client, maxNumSegments);

        List<Step> mergeSteps = new ArrayList<>();
        mergeSteps.add(conditionalSkipShrinkStep);
        mergeSteps.add(checkNotWriteIndexStep);
        mergeSteps.add(noopStep);

        if (codecChange) {
            mergeSteps.add(closeIndexStep);
            mergeSteps.add(updateBestCompressionSettings);
            mergeSteps.add(openIndexStep);
            mergeSteps.add(waitForIndexGreenStep);
        }

        mergeSteps.add(forceMergeStep);
        mergeSteps.add(segmentCountStep);
        return mergeSteps;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments, codec);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ForceMergeAction other = (ForceMergeAction) obj;
        return Objects.equals(this.maxNumSegments, other.maxNumSegments) && Objects.equals(this.codec, other.codec);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
