/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

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

    private static final ConstructingObjectParser<ShrinkAction, CreateIndexRequest> PARSER =
        new ConstructingObjectParser<>(NAME, a -> new ShrinkAction((Integer) a[0]));

    static {
        PARSER.declareInt(ConstructingObjectParser.constructorArg(), NUMBER_OF_SHARDS_FIELD);
    }

    private int numberOfShards;

    public static ShrinkAction parse(XContentParser parser) throws IOException {
        return PARSER.parse(parser, new CreateIndexRequest());
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
        StepKey setSingleNodeKey = new StepKey(phase, NAME, SetSingleNodeAllocateStep.NAME);
        StepKey allocationRoutedKey = new StepKey(phase, NAME, AllocationRoutedStep.NAME);
        StepKey shrinkKey = new StepKey(phase, NAME, ShrinkStep.NAME);
        StepKey enoughShardsKey = new StepKey(phase, NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey aliasKey = new StepKey(phase, NAME, ShrinkSetAliasStep.NAME);
        StepKey isShrunkIndexKey = new StepKey(phase, NAME, ShrunkenIndexCheckStep.NAME);
        SetSingleNodeAllocateStep setSingleNodeStep = new SetSingleNodeAllocateStep(setSingleNodeKey, allocationRoutedKey, client);
        AllocationRoutedStep allocationStep = new AllocationRoutedStep(allocationRoutedKey, shrinkKey, false);
        ShrinkStep shrink = new ShrinkStep(shrinkKey, enoughShardsKey, client, numberOfShards, SHRUNKEN_INDEX_PREFIX);
        ShrunkShardsAllocatedStep allocated = new ShrunkShardsAllocatedStep(enoughShardsKey, aliasKey, SHRUNKEN_INDEX_PREFIX);
        ShrinkSetAliasStep aliasSwapAndDelete = new ShrinkSetAliasStep(aliasKey, isShrunkIndexKey, client, SHRUNKEN_INDEX_PREFIX);
        ShrunkenIndexCheckStep waitOnShrinkTakeover = new ShrunkenIndexCheckStep(isShrunkIndexKey, nextStepKey, SHRUNKEN_INDEX_PREFIX);
        return Arrays.asList(setSingleNodeStep, allocationStep, shrink, allocated, aliasSwapAndDelete, waitOnShrinkTakeover);
    }

    @Override
    public List<StepKey> toStepKeys(String phase) {
        StepKey setSingleNodeKey = new StepKey(phase, NAME, SetSingleNodeAllocateStep.NAME);
        StepKey allocationRoutedKey = new StepKey(phase, NAME, AllocationRoutedStep.NAME);
        StepKey shrinkKey = new StepKey(phase, NAME, ShrinkStep.NAME);
        StepKey enoughShardsKey = new StepKey(phase, NAME, ShrunkShardsAllocatedStep.NAME);
        StepKey aliasKey = new StepKey(phase, NAME, ShrinkSetAliasStep.NAME);
        StepKey isShrunkIndexKey = new StepKey(phase, NAME, ShrunkenIndexCheckStep.NAME);
        return Arrays.asList(setSingleNodeKey, allocationRoutedKey, shrinkKey, enoughShardsKey, aliasKey, isShrunkIndexKey);
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
