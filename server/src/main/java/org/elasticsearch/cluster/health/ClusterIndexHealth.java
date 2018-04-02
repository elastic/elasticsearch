/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.cluster.health;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import static java.util.Collections.emptyMap;
import static org.elasticsearch.common.util.CollectionUtils.isEmpty;
import static org.elasticsearch.common.util.CollectionUtils.newHashMapWithExpectedSize;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

public final class ClusterIndexHealth implements Iterable<ClusterShardHealth>, Writeable, ToXContentFragment {
    private static final String INDEX = "index";
    private static final String STATUS = "status";
    private static final String NUMBER_OF_SHARDS = "number_of_shards";
    private static final String NUMBER_OF_REPLICAS = "number_of_replicas";
    private static final String ACTIVE_PRIMARY_SHARDS = "active_primary_shards";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String SHARDS = "shards";

    private static final ConstructingObjectParser<ClusterIndexHealth, Void> PARSER =
        new ConstructingObjectParser<>("cluster_index_health", true,
            a -> {
                int i = 0;
                String index = (String) a[i++];
                int numberOfShards = (int) a[i++];
                int numberOfReplicas = (int) a[i++];
                int activeShards = (int) a[i++];
                int relocatingShards = (int) a[i++];
                int initializingShards = (int) a[i++];
                int unassignedShards = (int) a[i++];
                int activePrimaryShards = (int) a[i++];
                String statusStr = (String) a[i++];
                ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
                List<ClusterShardHealth> shardList = (List<ClusterShardHealth>) a[i++];
                final Map<Integer, ClusterShardHealth> shards;
                if (isEmpty(shardList)) {
                    shards = emptyMap();
                } else {
                    shards = newHashMapWithExpectedSize(shardList.size());
                    for (ClusterShardHealth shardHealth : shardList) {
                        shards.put(shardHealth.getShardId(), shardHealth);
                    }
                }
                return new ClusterIndexHealth(index, numberOfShards, numberOfReplicas, activeShards, relocatingShards, initializingShards,
                    unassignedShards, activePrimaryShards, status, shards);
            });

    public static final ObjectParser.NamedObjectParser<ClusterShardHealth, Void> SHARD_PARSER =
        (XContentParser p, Void c, String nameIgnored) -> ClusterShardHealth.fromXContent(p);

    static {
        PARSER.declareString(constructorArg(), new ParseField(INDEX));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(NUMBER_OF_REPLICAS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_PRIMARY_SHARDS));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
        // Can be absent if LEVEL == 'indices' or 'cluster'
        PARSER.declareNamedObjects(optionalConstructorArg(), SHARD_PARSER, new ParseField(SHARDS));
    }

    private final String index;
    private final int numberOfShards;
    private final int numberOfReplicas;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final int activePrimaryShards;
    private final ClusterHealthStatus status;
    private final Map<Integer, ClusterShardHealth> shards;

    public ClusterIndexHealth(final IndexMetaData indexMetaData, final IndexRoutingTable indexRoutingTable) {
        this.index = indexMetaData.getIndex().getName();
        this.numberOfShards = indexMetaData.getNumberOfShards();
        this.numberOfReplicas = indexMetaData.getNumberOfReplicas();

        shards = new HashMap<>();
        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            int shardId = shardRoutingTable.shardId().id();
            shards.put(shardId, new ClusterShardHealth(shardId, shardRoutingTable));
        }

        // update the index status
        ClusterHealthStatus computeStatus = ClusterHealthStatus.GREEN;
        int computeActivePrimaryShards = 0;
        int computeActiveShards = 0;
        int computeRelocatingShards = 0;
        int computeInitializingShards = 0;
        int computeUnassignedShards = 0;
        for (ClusterShardHealth shardHealth : shards.values()) {
            if (shardHealth.isPrimaryActive()) {
                computeActivePrimaryShards++;
            }
            computeActiveShards += shardHealth.getActiveShards();
            computeRelocatingShards += shardHealth.getRelocatingShards();
            computeInitializingShards += shardHealth.getInitializingShards();
            computeUnassignedShards += shardHealth.getUnassignedShards();

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
        status = ClusterHealthStatus.fromValue(in.readByte());

        int size = in.readVInt();
        shards = newHashMapWithExpectedSize(size);
        for (int i = 0; i < size; i++) {
            ClusterShardHealth shardHealth = new ClusterShardHealth(in);
            shards.put(shardHealth.getShardId(), shardHealth);
        }
    }

    /**
     * For XContent Parser and serialization tests
     */
    ClusterIndexHealth(String index, int numberOfShards, int numberOfReplicas, int activeShards, int relocatingShards,
            int initializingShards, int unassignedShards, int activePrimaryShards, ClusterHealthStatus status,
            Map<Integer, ClusterShardHealth> shards) {
        this.index = index;
        this.numberOfShards = numberOfShards;
        this.numberOfReplicas = numberOfReplicas;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
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

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public Map<Integer, ClusterShardHealth> getShards() {
        return this.shards;
    }

    @Override
    public Iterator<ClusterShardHealth> iterator() {
        return shards.values().iterator();
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

        out.writeVInt(shards.size());
        for (ClusterShardHealth shardHealth : this) {
            shardHealth.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        builder.field(INDEX, getIndex());
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(NUMBER_OF_SHARDS, getNumberOfShards());
        builder.field(NUMBER_OF_REPLICAS, getNumberOfReplicas());
        builder.field(ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());

        if ("shards".equals(params.param("level", "indices"))) {
            builder.startObject(SHARDS);
            for (ClusterShardHealth shardHealth : shards.values()) {
                builder.startObject(Integer.toString(shardHealth.getShardId()));
                shardHealth.toXContent(builder, params);
                builder.endObject();
            }
            builder.endObject();
        }
        return builder;
    }

    public static ClusterIndexHealth fromXContent(XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    @Override
    public String toString() {
        return "ClusterIndexHealth{" +
            "index='" + index + '\'' +
            ", numberOfShards=" + numberOfShards +
            ", numberOfReplicas=" + numberOfReplicas +
            ", activeShards=" + activeShards +
            ", relocatingShards=" + relocatingShards +
            ", initializingShards=" + initializingShards +
            ", unassignedShards=" + unassignedShards +
            ", activePrimaryShards=" + activePrimaryShards +
            ", status=" + status +
            ", shards.size=" + (shards == null ? "null" : shards.size()) +
            '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ClusterIndexHealth that = (ClusterIndexHealth) o;
        return numberOfShards == that.numberOfShards &&
            numberOfReplicas == that.numberOfReplicas &&
            activeShards == that.activeShards &&
            relocatingShards == that.relocatingShards &&
            initializingShards == that.initializingShards &&
            unassignedShards == that.unassignedShards &&
            activePrimaryShards == that.activePrimaryShards &&
            Objects.equals(index, that.index) &&
            status == that.status &&
            Objects.equals(shards, that.shards);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, numberOfShards, numberOfReplicas, activeShards, relocatingShards, initializingShards, unassignedShards,
            activePrimaryShards, status, shards);
    }
}
