/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.cluster.health;

import org.elasticsearch.TransportVersions;
import org.elasticsearch.action.ClusterStatsLevel;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

public final class ClusterIndexHealth implements Writeable, ToXContentFragment {
    static final String STATUS = "status";
    static final String NUMBER_OF_SHARDS = "number_of_shards";
    static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    static final String ACTIVE_SHARDS = "active_shards";
    static final String RELOCATING_SHARDS = "relocating_shards";
    static final String INITIALIZING_SHARDS = "initializing_shards";
    static final String UNASSIGNED_SHARDS = "unassigned_shards";
    static final String UNASSIGNED_PRIMARY_SHARDS = "unassigned_primary_shards";
    static final String SHARDS = "shards";

    private final String index;
    private final int numberOfShards;
    private final int numberOfReplicas;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final int unassignedPrimaryShards;
    private final int activePrimaryShards;
    private final ClusterHealthStatus status;
    private final Map<Integer, ClusterShardHealth> shards;

    public ClusterIndexHealth(final IndexMetadata indexMetadata, final IndexRoutingTable indexRoutingTable) {
        this.index = indexMetadata.getIndex().getName();
        this.numberOfShards = indexMetadata.getNumberOfShards();
        this.numberOfReplicas = indexMetadata.getNumberOfReplicas();

        shards = new HashMap<>();
        for (int i = 0; i < indexRoutingTable.size(); i++) {
            IndexShardRoutingTable shardRoutingTable = indexRoutingTable.shard(i);
            int shardId = shardRoutingTable.shardId().id();
            shards.put(shardId, new ClusterShardHealth(shardId, shardRoutingTable));
        }

        // update the index status
        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedPrimaryShards = 0;
        int computeUnassignedShards = 0;
        for (ClusterShardHealth shardHealth : shards.values()) {
            if (shardHealth.isPrimaryActive()) {
                computeActivePrimaryShards++;
            }
            computeActiveShards += shardHealth.getActiveShards();
            computeRelocatingShards += shardHealth.getRelocatingShards();
            computeInitializingShards += shardHealth.getInitializingShards();
            computeUnassignedShards += shardHealth.getUnassignedShards();
            computeUnassignedPrimaryShards += shardHealth.getUnassignedPrimaryShards();

            if (shardHealth.getStatus() == ClusterHealthStatus.RED) {
                computeStatus = ClusterHealthStatus.RED;
            } else if (shardHealth.getStatus() == ClusterHealthStatus.YELLOW && computeStatus != ClusterHealthStatus.RED) {
                // do not override an existing red
                computeStatus = ClusterHealthStatus.YELLOW;
            }
        }
        if (shards.isEmpty()) { // might be since none has been created yet (two phase index creation)
            computeStatus = ClusterHealthStatus.RED;
        }

        this.status = computeStatus;
        this.activePrimaryShards = computeActivePrimaryShards;
        this.activeShards = computeActiveShards;
        this.relocatingShards = computeRelocatingShards;
        this.initializingShards = computeInitializingShards;
        this.unassignedShards = computeUnassignedShards;
        this.unassignedPrimaryShards = computeUnassignedPrimaryShards;
    }

    public ClusterIndexHealth(final StreamInput in) throws IOException {
        index = in.readString();
        numberOfShards = in.readVInt();
        numberOfReplicas = in.readVInt();
        activePrimaryShards = in.readVInt();
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        status = ClusterHealthStatus.readFrom(in);
        shards = in.readMapValues(ClusterShardHealth::new, ClusterShardHealth::getShardId);
        if (in.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            unassignedPrimaryShards = in.readVInt();
        } else {
            unassignedPrimaryShards = 0;
        }
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterIndexHealth(
        String index,
        int numberOfShards,
        int numberOfReplicas,
        int activeShards,
        int relocatingShards,
        int initializingShards,
        int unassignedShards,
        int unassignedPrimaryShards,
        int activePrimaryShards,
        ClusterHealthStatus status,
        Map<Integer, ClusterShardHealth> shards
    ) {
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.unassignedPrimaryShards = unassignedPrimaryShards;
        this.activePrimaryShards = activePrimaryShards;
        this.status = status;
        this.shards = shards;
    }

    public String getIndex() {
        return index;
    }

    public int getNumberOfShards() {
        return numberOfShards;
    }

    public int getNumberOfReplicas() {
        return numberOfReplicas;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActivePrimaryShards() {
        return activePrimaryShards;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    public int getUnassignedPrimaryShards() {
        return unassignedPrimaryShards;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<Integer, ClusterShardHealth> getShards() {
        return this.shards;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeString(index);
        out.writeVInt(numberOfShards);
        out.writeVInt(numberOfReplicas);
        out.writeVInt(activePrimaryShards);
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeByte(status.value());
        out.writeMapValues(shards);
        if (out.getTransportVersion().onOrAfter(TransportVersions.V_8_16_0)) {
            out.writeVInt(unassignedPrimaryShards);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.startObject(getIndex());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(NUMBER_OF_SHARDS, getNumberOfShards());
        builder.field(NUMBER_OF_REPLICAS, getNumberOfReplicas());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.field(UNASSIGNED_PRIMARY_SHARDS, getUnassignedPrimaryShards());

        ClusterStatsLevel level = ClusterStatsLevel.of(params, ClusterStatsLevel.INDICES);
        if (level == ClusterStatsLevel.SHARDS) {
            builder.startObject(SHARDS);
            for (ClusterShardHealth shardHealth : shards.values()) {
                shardHealth.toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "ClusterIndexHealth{"
            + "index='"
            + index
            + '\''
            + ", numberOfShards="
            + numberOfShards
            + ", numberOfReplicas="
            + numberOfReplicas
            + ", activeShards="
            + activeShards
            + ", relocatingShards="
            + relocatingShards
            + ", initializingShards="
            + initializingShards
            + ", unassignedShards="
            + unassignedShards
            + ", unassignedPrimaryShards="
            + unassignedPrimaryShards
            + ", activePrimaryShards="
            + activePrimaryShards
            + ", status="
            + status
            + ", shards.size="
            + (shards == null ? "null" : shards.size())
            + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterIndexHealth that = (ClusterIndexHealth) o;
        return Objects.equals(index, that.index)
            && numberOfShards == that.numberOfShards
            && numberOfReplicas == that.numberOfReplicas
            && activeShards == that.activeShards
            && relocatingShards == that.relocatingShards
            && initializingShards == that.initializingShards
            && unassignedShards == that.unassignedShards
            && unassignedPrimaryShards == that.unassignedPrimaryShards
            && activePrimaryShards == that.activePrimaryShards
            && status == that.status
            && Objects.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            index,
            numberOfShards,
            numberOfReplicas,
            activeShards,
            relocatingShards,
            initializingShards,
            unassignedShards,
            unassignedPrimaryShards,
            activePrimaryShards,
            status,
            shards
        );
    }
}
