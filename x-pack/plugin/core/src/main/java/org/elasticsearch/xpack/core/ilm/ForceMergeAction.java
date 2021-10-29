/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
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

    private static final Settings READ_ONLY_SETTINGS = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();

    private static final Settings BEST_COMPRESSION_SETTINGS = Settings.builder()
        .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), CodecService.BEST_COMPRESSION_CODEC)
        .build();

    public static final String NAME = "forcemerge";
    public static final ParseField MAX_NUM_SEGMENTS_FIELD = new ParseField("max_num_segments");
    public static final ParseField CODEC = new ParseField("index_codec");
    public static final ParseField ONLY_EXPUNGE_DELETES = new ParseField("only_expunge_deletes");
    public static final String CONDITIONAL_SKIP_FORCE_MERGE_STEP = BranchingStep.NAME + "-forcemerge-check-prerequisites";

    private static final ConstructingObjectParser<ForceMergeAction, Void> PARSER = new ConstructingObjectParser<>(NAME, false, a -> {
        Integer maxNumSegments = (Integer) a[0];
        String codec = a[1] != null ? (String) a[1] : null;
        boolean onlyExpungeDeletes = a[2] != null && (boolean) a[2];
        return new ForceMergeAction(maxNumSegments, codec, onlyExpungeDeletes);
    });

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_NUM_SEGMENTS_FIELD);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), CODEC);
        PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ONLY_EXPUNGE_DELETES);
    }

    private final Integer maxNumSegments;
    private final String codec;
    private final boolean onlyExpungeDeletes;

    public static ForceMergeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ForceMergeAction(@Nullable Integer maxNumSegments, @Nullable String codec, boolean onlyExpungeDeletes) {
        if (maxNumSegments != null && onlyExpungeDeletes) {
            throw new IllegalArgumentException(
                "cannot set [max_num_segments] and [only_expunge_deletes] at the same time,"
                    + " those two parameters are mutually exclusive"
            );
        }
        if (maxNumSegments == null && onlyExpungeDeletes == false) {
            throw new IllegalArgumentException("Either [max_num_segments] or [only_expunge_deletes] must be set");
        }
        if (maxNumSegments != null && maxNumSegments <= 0) {
            throw new IllegalArgumentException("[" + MAX_NUM_SEGMENTS_FIELD.getPreferredName() + "] must be a positive integer");
        }
        this.maxNumSegments = maxNumSegments;
        if (codec != null && CodecService.BEST_COMPRESSION_CODEC.equals(codec) == false) {
            throw new IllegalArgumentException("unknown index codec: [" + codec + "]");
        }
        this.codec = codec;
        this.onlyExpungeDeletes = onlyExpungeDeletes;
    }

    public ForceMergeAction(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            this.maxNumSegments = in.readOptionalVInt();
        } else {
            this.maxNumSegments = in.readVInt();
        }
        this.codec = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_7_16_0)) {
            this.onlyExpungeDeletes = in.readBoolean();
        } else {
            this.onlyExpungeDeletes = false;
        }
    }

    public Integer getMaxNumSegments() {
        return maxNumSegments;
    }

    public String getCodec() {
        return this.codec;
    }

    public Boolean getOnlyExpungeDeletes() {
        return this.onlyExpungeDeletes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeOptionalVInt(maxNumSegments);
        } else {
            out.writeVInt(maxNumSegments);
        }

        out.writeOptionalString(codec);
        if (out.getVersion().onOrAfter(Version.V_7_16_0)) {
            out.writeBoolean(onlyExpungeDeletes);
        }
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
        if (maxNumSegments != null) {
            builder.field(MAX_NUM_SEGMENTS_FIELD.getPreferredName(), maxNumSegments);
        }
        if (codec != null) {
            builder.field(CODEC.getPreferredName(), codec);
        }
        builder.field(ONLY_EXPUNGE_DELETES.getPreferredName(), onlyExpungeDeletes);
        builder.endObject();
        return builder;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        final boolean codecChange = codec != null && codec.equals(CodecService.BEST_COMPRESSION_CODEC);

        StepKey preForceMergeBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_FORCE_MERGE_STEP);
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);

        StepKey closeKey = new StepKey(phase, NAME, CloseIndexStep.NAME);
        StepKey updateCompressionKey = new StepKey(phase, NAME, UpdateSettingsStep.NAME);
        StepKey openKey = new StepKey(phase, NAME, OpenIndexStep.NAME);
        StepKey waitForGreenIndexKey = new StepKey(phase, NAME, WaitForIndexColorStep.NAME);

        StepKey forceMergeKey = new StepKey(phase, NAME, ForceMergeStep.NAME);
        StepKey countKey = new StepKey(phase, NAME, SegmentCountStep.NAME);

        BranchingStep conditionalSkipShrinkStep = new BranchingStep(
            preForceMergeBranchingKey,
            checkNotWriteIndex,
            nextStepKey,
            (index, clusterState) -> {
                IndexMetadata indexMetadata = clusterState.metadata().index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                if (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null) {
                    String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings());
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
        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNotWriteIndex, readOnlyKey);
        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(
            readOnlyKey,
            codecChange ? closeKey : forceMergeKey,
            client,
            READ_ONLY_SETTINGS
        );

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

        StepKey forceMergeNextKey = nextStepKey;
        if (maxNumSegments != null) {
            forceMergeNextKey = countKey;
        }
        ForceMergeStep forceMergeStep = new ForceMergeStep(forceMergeKey, forceMergeNextKey, client, maxNumSegments, onlyExpungeDeletes);

        List<Step> mergeSteps = new ArrayList<>();
        mergeSteps.add(conditionalSkipShrinkStep);
        mergeSteps.add(checkNotWriteIndexStep);
        mergeSteps.add(readOnlyStep);

        if (codecChange) {
            mergeSteps.add(closeIndexStep);
            mergeSteps.add(updateBestCompressionSettings);
            mergeSteps.add(openIndexStep);
            mergeSteps.add(waitForIndexGreenStep);
        }

        mergeSteps.add(forceMergeStep);

        if (maxNumSegments != null) {
            SegmentCountStep segmentCountStep = new SegmentCountStep(countKey, nextStepKey, client, maxNumSegments);
            mergeSteps.add(segmentCountStep);
        }

        return mergeSteps;
    }

    @Override
    public int hashCode() {
        return Objects.hash(maxNumSegments, codec, onlyExpungeDeletes);
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
        return Objects.equals(this.maxNumSegments, other.maxNumSegments)
            && Objects.equals(this.codec, other.codec)
            && Objects.equals(this.onlyExpungeDeletes, other.onlyExpungeDeletes);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
