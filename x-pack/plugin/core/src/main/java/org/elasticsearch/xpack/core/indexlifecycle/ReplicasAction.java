/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.indexlifecycle;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.support.ActiveShardCount;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.index.Index;
import org.elasticsearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.function.LongSupplier;

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
    public List<Step> toSteps(String phase, Index index, Client client, ThreadPool threadPool, LongSupplier nowSupplier) {
        ClusterStateUpdateStep updateAllocationSettings = new ClusterStateUpdateStep(
            "update_replica_count", NAME, phase, index.getName(), (currentState) ->
            ClusterState.builder(currentState).metaData(MetaData.builder(currentState.metaData())
                .updateNumberOfReplicas(numberOfReplicas, index.getName())).build());
        ConditionalWaitStep isReplicatedCheck = new ConditionalWaitStep("wait_replicas_allocated", NAME,
            phase, index.getName(), (currentState) -> ActiveShardCount.ALL.enoughShardsActive(currentState, index.getName()) );
        return Arrays.asList(updateAllocationSettings, isReplicatedCheck);
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
