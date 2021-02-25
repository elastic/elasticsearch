/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ilm;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ilm.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * A {@link LifecycleAction} which freezes the index.
 */
public class FreezeAction implements LifecycleAction {
    private static final Logger logger = LogManager.getLogger(FreezeAction.class);

    public static final String NAME = "freeze";
    public static final String CONDITIONAL_SKIP_FREEZE_STEP = BranchingStep.NAME + "-freeze-check-prerequisites";

    private static final ObjectParser<FreezeAction, Void> PARSER = new ObjectParser<>(NAME, FreezeAction::new);

    public static FreezeAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public FreezeAction() {
    }

    public FreezeAction(StreamInput in) {
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, StepKey nextStepKey) {
        StepKey preFreezeMergeBranchingKey = new StepKey(phase, NAME, CONDITIONAL_SKIP_FREEZE_STEP);
        StepKey checkNotWriteIndex = new StepKey(phase, NAME, CheckNotDataStreamWriteIndexStep.NAME);
        StepKey freezeStepKey = new StepKey(phase, NAME, FreezeStep.NAME);

        BranchingStep conditionalSkipFreezeStep = new BranchingStep(preFreezeMergeBranchingKey, checkNotWriteIndex, nextStepKey,
            (index, clusterState) -> {
                IndexMetadata indexMetadata = clusterState.getMetadata().index(index);
                assert indexMetadata != null : "index " + index.getName() + " must exist in the cluster state";
                String policyName = LifecycleSettings.LIFECYCLE_NAME_SETTING.get(indexMetadata.getSettings());
                if (indexMetadata.getSettings().get(LifecycleSettings.SNAPSHOT_INDEX_NAME) != null) {
                    logger.warn("[{}] action is configured for index [{}] in policy [{}] which is mounted as searchable snapshot. " +
                        "Skipping this action", FreezeAction.NAME, index.getName(), policyName);
                    return true;
                }
                if (indexMetadata.getSettings().getAsBoolean("index.frozen", false)) {
                    logger.debug("skipping [{}] action for index [{}] in policy [{}] as the index is already frozen", FreezeAction.NAME,
                        index.getName(), policyName);
                    return true;
                }
                return false;
            });
        CheckNotDataStreamWriteIndexStep checkNoWriteIndexStep = new CheckNotDataStreamWriteIndexStep(checkNotWriteIndex,
            freezeStepKey);
        FreezeStep freezeStep = new FreezeStep(freezeStepKey, nextStepKey, client);
        return Arrays.asList(conditionalSkipFreezeStep, checkNoWriteIndexStep, freezeStep);
    }

    @Override
    public int hashCode() {
        return 1;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        return true;
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
