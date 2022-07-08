/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;
import org.elasticsearch.xpack.core.rollup.RollupActionConfig;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * A {@link LifecycleAction} which calls {@link org.elasticsearch.xpack.core.rollup.action.RollupAction} on an index
 */
public class RollupILMAction implements LifecycleAction {

    public static final String NAME = "rollup";
    public static final String ROLLUP_INDEX_PREFIX = "rollup-";
    public static final String CONDITIONAL_DATASTREAM_CHECK_KEY = BranchingStep.NAME + "-on-datastream-check";

    private static final ParseField CONFIG_FIELD = new ParseField("config");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<RollupILMAction, Void> PARSER = new ConstructingObjectParser<>(
        NAME,
        a -> new RollupILMAction((RollupActionConfig) a[0])
    );
    public static final String GENERATE_ROLLUP_STEP_NAME = "generate-rollup-name";

    private final RollupActionConfig config;

    static {
        PARSER.declareField(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> RollupActionConfig.fromXContent(p),
            CONFIG_FIELD,
            ObjectParser.ValueType.OBJECT
        );
    }

    public static RollupILMAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public RollupILMAction(RollupActionConfig config) {
        this.config = config;
    }

    public RollupILMAction(StreamInput in) throws IOException {
        this(new RollupActionConfig(in));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        config.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(CONFIG_FIELD.getPreferredName(), config);
        builder.endObject();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    public RollupActionConfig config() {
        return config;
    }

    @Override
    public boolean isSafeAction() {
        return false;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey waitForNoFollowerStepKey = new StepKey(phase, NAME, WaitForNoFollowersStep.NAME);
        StepKey readOnlyKey = new StepKey(phase, NAME, ReadOnlyStep.NAME);
        StepKey cleanupRollupIndexKey = new StepKey(phase, NAME, CleanupTargetIndexStep.NAME);
        StepKey generateRollupIndexNameKey = new StepKey(phase, NAME, GENERATE_ROLLUP_STEP_NAME);
        StepKey rollupKey = new StepKey(phase, NAME, NAME);
        StepKey copyMetadataKey = new StepKey(phase, NAME, CopyExecutionStateStep.NAME);
        StepKey copyLifecyclePolicySettingKey = new StepKey(phase, NAME, CopySettingsStep.NAME);
        StepKey dataStreamCheckBranchingKey = new StepKey(phase, NAME, CONDITIONAL_DATASTREAM_CHECK_KEY);
        StepKey replaceDataStreamIndexKey = new StepKey(phase, NAME, ReplaceDataStreamBackingIndexStep.NAME);
        StepKey deleteIndexKey = new StepKey(phase, NAME, DeleteStep.NAME);

        CheckNotDataStreamWriteIndexStep checkNotWriteIndexStep = new CheckNotDataStreamWriteIndexStep(
            checkNotWriteIndex,
            waitForNoFollowerStepKey
        );
        WaitForNoFollowersStep waitForNoFollowersStep = new WaitForNoFollowersStep(waitForNoFollowerStepKey, cleanupRollupIndexKey, client);

        // We generate a unique rollup index name, but we also retry if the allocation of the rollup index is not possible, so we want to
        // delete the "previously generated" rollup index (this is a no-op if it's the first run of the action, and we haven't generated a
        // rollup index name)
        CleanupTargetIndexStep cleanupRollupIndexStep = new CleanupTargetIndexStep(
            cleanupRollupIndexKey,
            readOnlyKey,
            client,
            (indexMetadata) -> IndexMetadata.INDEX_ROLLUP_SOURCE_NAME.get(indexMetadata.getSettings()),
            (indexMetadata) -> indexMetadata.getLifecycleExecutionState().rollupIndexName()
        );
        // Mark source index as read-only
        ReadOnlyStep readOnlyStep = new ReadOnlyStep(readOnlyKey, generateRollupIndexNameKey, client);

        // Generate a unique rollup index name and store it in the ILM execution state
        GenerateUniqueIndexNameStep generateRollupIndexNameStep = new GenerateUniqueIndexNameStep(
            generateRollupIndexNameKey,
            rollupKey,
            ROLLUP_INDEX_PREFIX,
            (rollupIndexName, lifecycleStateBuilder) -> lifecycleStateBuilder.setRollupIndexName(rollupIndexName)
        );

        // Here is where the actual rollup action takes place
        RollupStep rollupStep = new RollupStep(rollupKey, copyMetadataKey, client, config);
        CopyExecutionStateStep copyExecutionStateStep = new CopyExecutionStateStep(
            copyMetadataKey,
            copyLifecyclePolicySettingKey,
            (indexName, lifecycleState) -> lifecycleState.rollupIndexName(),
            nextStepKey
        );

        // Copy the index.lifecycle.name setting to the rollup index settings
        CopySettingsStep copySettingsStep = new CopySettingsStep(
            copyLifecyclePolicySettingKey,
            dataStreamCheckBranchingKey,
            (indexName, lifecycleState) -> lifecycleState.rollupIndexName(),
            LifecycleSettings.LIFECYCLE_NAME
        );

        // By the time we get to this step we have 2 indices, the source and the rollup one. We now need to choose an index
        // swapping strategy such that the rollup index takes the place of the source index (which will also be deleted).
        // If the source index is part of a data stream it's a matter of replacing it with the rollup index one in the data stream and
        // then deleting the source index.
        BranchingStep isDataStreamBranchingStep = new BranchingStep(
            dataStreamCheckBranchingKey,
            deleteIndexKey,
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
            (sourceIndexName, lifecycleState) -> lifecycleState.rollupIndexName()
        );
        DeleteStep deleteSourceIndexStep = new DeleteStep(deleteIndexKey, nextStepKey, client);

        return List.of(
            checkNotWriteIndexStep,
            waitForNoFollowersStep,
            cleanupRollupIndexStep,
            readOnlyStep,
            generateRollupIndexNameStep,
            rollupStep,
            copyExecutionStateStep,
            copySettingsStep,
            isDataStreamBranchingStep,
            replaceDataStreamBackingIndex,
            deleteSourceIndexStep
        );
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        RollupILMAction that = (RollupILMAction) o;
        return Objects.equals(this.config, that.config);
    }

    @Override
    public int hashCode() {
        return Objects.hash(config);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }
}
