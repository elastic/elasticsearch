/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
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
    public static final String NAME = "shrink";
    public static final String SHRUNKEN_INDEX_PREFIX = "shrink-";
    public static final ParseField NUMBER_OF_SHARDS_FIELD = new ParseField("number_of_shards");

    private static final ConstructingObjectParser<ShrinkAction, Void> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ShrinkAction((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_SHARDS_FIELD);
    }

    private int numberOfShards;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    public ShrinkAction(int numberOfShards) {
        if (numberOfShards <= 0) {
            throw new IllegalArgumentException("[" + NUMBER_OF_SHARDS_FIELD.getPreferredName() + "] must be greater than 0");
        }
        this.numberOfShards = numberOfShards;
    }

    public ShrinkAction(StreamInput in) throws IOException {
        this.numberOfShards = in.readVInt();
    }

    int getNumberOfShards() {
        return numberOfShards;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfShards);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUMBER_OF_SHARDS_FIELD.getPreferredName(), numberOfShards);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        Settings readOnlySettings = Settings.builder().put(IndexMetaData.SETTING_BLOCKS_WRITE, true).build();

        StepKey branchingKey = new StepKey(phase, NAME, BranchingStep.NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyAction.NAME);
        StepKey setSingleNodeKey = new StepKey(phase, NAME, SetSingleNodeAllocateStep.NAME);
        StepKey allocationRoutedKey = new StepKey(phase, NAME, CheckShrinkReadyStep.NAME);
        StepKey shrinkKey = new StepKey(phase, NAME, ShrinkStep.NAME);
        StepKey enoughShardsKey = new StepKey(phase, NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey aliasKey = new StepKey(phase, NAME, ShrinkSetAliasStep.NAME);
        StepKey isShrunkIndexKey = new StepKey(phase, NAME, ShrunkenIndexCheckStep.NAME);

        BranchingStep conditionalSkipShrinkStep = new BranchingStep(branchingKey, waitForNoFollowerStepKey, nextStepKey,
            (index, clusterState) -> clusterState.getMetaData().index(index).getNumberOfShards() == numberOfShards);
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, readOnlyKey, client);
        UpdateSettingsStep readOnlyStep = new UpdateSettingsStep(readOnlyKey, setSingleNodeKey, client, readOnlySettings);
        SetSingleNodeAllocateStep setSingleNodeStep = new SetSingleNodeAllocateStep(setSingleNodeKey, allocationRoutedKey, client);
        CheckShrinkReadyStep checkShrinkReadyStep = new CheckShrinkReadyStep(allocationRoutedKey, shrinkKey);
        ShrinkStep shrink = new ShrinkStep(shrinkKey, enoughShardsKey, client, numberOfShards, SHRUNKEN_INDEX_PREFIX);
        ShrunkShardsAllocatedStep allocated = new ShrunkShardsAllocatedStep(enoughShardsKey, copyMetadataKey, SHRUNKEN_INDEX_PREFIX);
        CopyExecutionStateStep copyMetadata = new CopyExecutionStateStep(copyMetadataKey, aliasKey, SHRUNKEN_INDEX_PREFIX);
        ShrinkSetAliasStep aliasSwapAndDelete = new ShrinkSetAliasStep(aliasKey, isShrunkIndexKey, client, SHRUNKEN_INDEX_PREFIX);
        ShrunkenIndexCheckStep waitOnShrinkTakeover = new ShrunkenIndexCheckStep(isShrunkIndexKey, nextStepKey, SHRUNKEN_INDEX_PREFIX);
        return Arrays.asList(conditionalSkipShrinkStep, waitForNoFollowersStep, readOnlyStep, setSingleNodeStep, checkShrinkReadyStep,
            shrink, allocated, copyMetadata, aliasSwapAndDelete, waitOnShrinkTakeover);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ShrinkAction that = (ShrinkAction) o;
        return Objects.equals(numberOfShards, that.numberOfShards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(numberOfShards);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
