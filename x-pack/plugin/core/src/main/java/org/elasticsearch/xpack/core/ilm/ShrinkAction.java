/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which shrinks the index.
 */
public class ShrinkAction implements LifecycleAction {
    private static final Logger logger = LogManager.getLogger(ShrinkAction.class);

    public static final String NAME = "shrink";
    public static final String SHRUNKEN_INDEX_PREFIX = "shrink-";
    public static final ParseField NUMBER_OF_SHARDS_FIELD = new ParseField("number_of_shards");
    private static final ParseField MAX_SINGLE_PRIMARY_SIZE = new ParseField("max_single_primary_size");
    public static final String CONDITIONAL_SKIP_SHRINK_STEP = BranchingStep.NAME + "-check-prerequisites";
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";

    private static final ConstructingObjectParser<ShrinkAction, Void> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ShrinkAction((Integer) a[0], (ByteSizeValue) a[1]));

    static {
        PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), NUMBER_OF_SHARDS_FIELD);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_SINGLE_PRIMARY_SIZE.getPreferredName()),
            MAX_SINGLE_PRIMARY_SIZE, ObjectParser.ValueType.STRING);
    }

    private Integer numberOfShards;
    private ByteSizeValue maxSinglePrimarySize;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ShrinkAction(@Nullable Integer numberOfShards, @Nullable ByteSizeValue maxSinglePrimarySize) {
        if (numberOfShards != null && maxSinglePrimarySize != null) {
            throw new IllegalArgumentException("Cannot set both [number_of_shards] and [max_single_primary_size]");
        }
        if (numberOfShards == null && maxSinglePrimarySize == null) {
            throw new IllegalArgumentException("Either [number_of_shards] or [max_single_primary_size] must be set");
        }
        if (maxSinglePrimarySize != null) {
            if (maxSinglePrimarySize.getBytes() <= 0) {
                throw new IllegalArgumentException("[max_single_primary_size] must be greater than 0");
            }
            this.maxSinglePrimarySize = maxSinglePrimarySize;
        } else {
            if (numberOfShards <= 0) {
                throw new IllegalArgumentException("[" + NUMBER_OF_SHARDS_FIELD.getPreferredName() + "] must be greater than 0");
            }
            this.numberOfShards = numberOfShards;
        }
    }

    public ShrinkAction(StreamInput in) throws IOException {
        if (in.getVersion().onOrAfter(Version.V_8_0_0)) {
            if (in.readBoolean()) {
                this.numberOfShards = in.readVInt();
                this.maxSinglePrimarySize = null;
            } else {
                this.numberOfShards = null;
                this.maxSinglePrimarySize = new ByteSizeValue(in);
            }
        } else {
            this.numberOfShards = in.readVInt();
            this.maxSinglePrimarySize = null;
        }
    }

    Integer getNumberOfShards() {
        return numberOfShards;
    }

    ByteSizeValue getMaxSinglePrimarySize() {
        return maxSinglePrimarySize;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_8_0_0)) {
            boolean hasNumberOfShards = numberOfShards != null;
            out.writeBoolean(hasNumberOfShards);
            if (hasNumberOfShards) {
                out.writeVInt(numberOfShards);
            } else {
                maxSinglePrimarySize.writeTo(out);
            }
        } else {
            out.writeVInt(numberOfShards);
        }
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        if (numberOfShards != null) {
            builder.field(NUMBER_OF_SHARDS_FIELD.getPreferredName(), numberOfShards);
        }
        if (maxSinglePrimarySize != null) {
            builder.field(MAX_SINGLE_PRIMARY_SIZE.getPreferredName(), maxSinglePrimarySize);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Settings readOnlySettings = Settings.builder().put(IndexMetadata.SETTING_BLOCKS_WRITE, true).build();

        StepKey preShrinkBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_SHRINK_STEP);
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);
        StepKey setSingleNodeKey = new StepKey(phase, NAME, SetSingleNodeAllocateStep.NAME);
        StepKey allocationRoutedKey = new StepKey(phase, NAME, CheckShrinkReadyStep.NAME);
        StepKey shrinkKey = new StepKey(phase, NAME, ShrinkStep.NAME);
        StepKey enoughShardsKey = new StepKey(phase, NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey dataStreamCheckBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey aliasKey = new StepKey(phase, NAME, ShrinkSetAliasStep.NAME);
        StepKey isShrunkIndexKey = new StepKey(phase, NAME, ShrunkenIndexCheckStep.NAME);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);

        BranchingStep conditionalSkipShrinkStep = new BranchingStep(preShrinkBranchingKey, checkNotWriteIndex, nextStepKey,
            (index, clusterState) -> {
                IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
                if (numberOfShards != null && indexMetadata.getNumberOfShards() == numberOfShards) {
                    return true;
                }
                if (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null) {
                    logger.warn("[{}] action is configured for index [{}] in policy [{}] which is mounted as searchable snapshot. " +
                            "Skipping this action", ShrinkAction.NAME, indexMetadata.getIndex().getName(),
                        LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings()));
                    return true;
                }
                return false;
            });
        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNotWriteIndex,
            waitForNoFollowerStepKey);
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, readOnlyKey, client);
        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(readOnlyKey, setSingleNodeKey, client, readOnlySettings);
        SetSingleNodeAllocateStep setSingleNodeStep = new SetSingleNodeAllocateStep(setSingleNodeKey, allocationRoutedKey, client);
        CheckShrinkReadyStep checkShrinkReadyStep = new CheckShrinkReadyStep(allocationRoutedKey, shrinkKey);
        ShrinkStep shrink = new ShrinkStep(shrinkKey, enoughShardsKey, client, numberOfShards, maxSinglePrimarySize,
            SHRUNKEN_INDEX_PREFIX);
        ShrunkShardsAllocatedStep allocated = new ShrunkShardsAllocatedStep(enoughShardsKey, copyMetadataKey, SHRUNKEN_INDEX_PREFIX);
        CopyExecutionStateStep copyMetadata = new CopyExecutionStateStep(copyMetadataKey, dataStreamCheckBranchingKey,
            SHRUNKEN_INDEX_PREFIX, ShrunkenIndexCheckStep.NAME);
        // by the time we get to this step we have 2 indices, the source and the shrunken one. we now need to choose an index
        // swapping strategy such that the shrunken index takes the place of the source index (which is also deleted).
        // if the source index is part of a data stream it's a matter of replacing it with the shrunken index one in the data stream and
        // then deleting the source index; otherwise we'll use the alias management api to atomically transfer the aliases from source to
        // the shrunken index and delete the source
        BranchingStep isDataStreamBranchingStep = new BranchingStep(dataStreamCheckBranchingKey, aliasKey, replaceDataStreamIndexKey,
            (index, clusterState) -> {
                IndexAbstraction indexAbstraction = clusterState.metadata().getIndicesLookup().get(index.getName());
                assert indexAbstraction != null : "invalid cluster metadata. index [" + index.getName() + "] was not found";
                return indexAbstraction.getParentDataStream() != null;
            });
        ShrinkSetAliasStep aliasSwapAndDelete = new ShrinkSetAliasStep(aliasKey, isShrunkIndexKey, client, SHRUNKEN_INDEX_PREFIX);
        ReplaceDataStreamBackingIndexStep replaceDataStreamBackingIndex = new ReplaceDataStreamBackingIndexStep(replaceDataStreamIndexKey,
            deleteIndexKey, SHRUNKEN_INDEX_PREFIX);
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteIndexKey, isShrunkIndexKey, client);
        ShrunkenIndexCheckStep waitOnShrinkTakeover = new ShrunkenIndexCheckStep(isShrunkIndexKey, nextStepKey, SHRUNKEN_INDEX_PREFIX);
        return Arrays.asList(conditionalSkipShrinkStep, checkNotWriteIndexStep, waitForNoFollowersStep, readOnlyStep, setSingleNodeStep,
            checkShrinkReadyStep, shrink, allocated, copyMetadata, isDataStreamBranchingStep, aliasSwapAndDelete, waitOnShrinkTakeover,
            replaceDataStreamBackingIndex, deleteSourceIndexStep);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShrinkAction that = (ShrinkAction) o;
        return Objects.equals(numberOfShards, that.numberOfShards) &&
            Objects.equals(maxSinglePrimarySize, that.maxSinglePrimarySize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfShards, maxSinglePrimarySize);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
