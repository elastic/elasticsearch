/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

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
import org.elasticsearch.xpack.core.indexlifecycle.Step.StepKey;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * A {@link LifecycleAction} that changes the number of replicas for the index.
 */
public class ReplicasAction implements LifecycleAction {
    public static final String NAME = "replicas";

    public static final ParseField NUMBER_OF_REPLICAS_FIELD = new ParseField("number_of_replicas");
    private static final ConstructingObjectParser<ReplicasAction, Void> PARSER = new ConstructingObjectParser<>(NAME,
        false, a -> new ReplicasAction((Integer) a[0]));

    static {
        PARSER.declareInt(constructorArg(), NUMBER_OF_REPLICAS_FIELD);
    }

    private int numberOfReplicas;

    public static ReplicasAction parse(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    public ReplicasAction(int numberOfReplicas) {
        if (numberOfReplicas < 0) {
            throw new IllegalArgumentException("[" + NUMBER_OF_REPLICAS_FIELD.getPreferredName() + "] must be >= 0");
        }
        this.numberOfReplicas = numberOfReplicas;
    }

    public ReplicasAction(StreamInput in) throws IOException {
        this.numberOfReplicas = in.readVInt();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(numberOfReplicas);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(NUMBER_OF_REPLICAS_FIELD.getPreferredName(), numberOfReplicas);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isSafeAction() {
        return true;
    }

    @Override
    public List<Step> toSteps(Client client, String phase, Step.StepKey nextStepKey) {
        StepKey updateReplicasKey = new StepKey(phase, NAME, UpdateSettingsStep.NAME);
        StepKey enoughKey = new StepKey(phase, NAME, ReplicasAllocatedStep.NAME);
        Settings replicaSettings = Settings.builder().put(IndexMetaData.SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        return Arrays.asList(new UpdateSettingsStep(updateReplicasKey, enoughKey, client, replicaSettings),
                new ReplicasAllocatedStep(enoughKey, nextStepKey));
    }

    @Override
    public List<StepKey> toStepKeys(String phase) {
        StepKey updateReplicasKey = new StepKey(phase, NAME, UpdateSettingsStep.NAME);
        StepKey enoughKey = new StepKey(phase, NAME, ReplicasAllocatedStep.NAME);
        return Arrays.asList(updateReplicasKey, enoughKey);
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(numberOfReplicas);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (obj.getClass() != getClass()) {
            return false;
        }
        ReplicasAction other = (ReplicasAction) obj;
        return Objects.equals(numberOfReplicas, other.numberOfReplicas);
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

}
