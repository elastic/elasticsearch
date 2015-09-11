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

package org.elasticsearch.action.admin.cluster.health;

import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.routing.IndexRoutingTable;
import org.elasticsearch.cluster.routing.IndexShardRoutingTable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderString;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.elasticsearch.action.admin.cluster.health.ClusterShardHealth.readClusterShardHealth;

/**
 *
 */
public class ClusterIndexHealth implements Iterable<ClusterShardHealth>, Streamable, ToXContent {

    private String index;

    private int numberOfShards;

    private int numberOfReplicas;

    private int activeShards = 0;

    private int relocatingShards = 0;

    private int initializingShards = 0;

    private int unassignedShards = 0;

    private int activePrimaryShards = 0;

    private ClusterHealthStatus status = ClusterHealthStatus.RED;

    private final Map<Integer, ClusterShardHealth> shards = new HashMap<>();

    private List<String> validationFailures;

    private ClusterIndexHealth() {
    }

    public ClusterIndexHealth(IndexMetaData indexMetaData, IndexRoutingTable indexRoutingTable) {
        this.index = indexMetaData.index();
        this.numberOfShards = indexMetaData.getNumberOfShards();
        this.numberOfReplicas = indexMetaData.getNumberOfReplicas();
        this.validationFailures = indexRoutingTable.validate(indexMetaData);

        for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
            int shardId = shardRoutingTable.shardId().id();
            shards.put(shardId, new ClusterShardHealth(shardId, shardRoutingTable));
        }

        // update the index status
        status = ClusterHealthStatus.GREEN;

        for (ClusterShardHealth shardHealth : shards.values()) {
            if (shardHealth.isPrimaryActive()) {
                activePrimaryShards++;
            }
            activeShards += shardHealth.getActiveShards();
            relocatingShards += shardHealth.getRelocatingShards();
            initializingShards += shardHealth.getInitializingShards();
            unassignedShards += shardHealth.getUnassignedShards();

            if (shardHealth.getStatus() == ClusterHealthStatus.RED) {
                status = ClusterHealthStatus.RED;
            } else if (shardHealth.getStatus() == ClusterHealthStatus.YELLOW && status != ClusterHealthStatus.RED) {
                // do not override an existing red
                status = ClusterHealthStatus.YELLOW;
            }
        }
        if (!validationFailures.isEmpty()) {
            status = ClusterHealthStatus.RED;
        } else if (shards.isEmpty()) { // might be since none has been created yet (two phase index creation)
            status = ClusterHealthStatus.RED;
        }
    }

    public String getIndex() {
        return index;
    }

    public List<String> getValidationFailures() {
        return this.validationFailures;
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

    public static ClusterIndexHealth readClusterIndexHealth(StreamInput in) throws IOException {
        ClusterIndexHealth indexHealth = new ClusterIndexHealth();
        indexHealth.readFrom(in);
        return indexHealth;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
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
        for (int i = 0; i < size; i++) {
            ClusterShardHealth shardHealth = readClusterShardHealth(in);
            shards.put(shardHealth.getId(), shardHealth);
        }
        validationFailures = Arrays.asList(in.readStringArray());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
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

        out.writeVInt(validationFailures.size());
        for (String failure : validationFailures) {
            out.writeString(failure);
        }
    }

    static final class Fields {
        static final XContentBuilderString STATUS = new XContentBuilderString("status");
        static final XContentBuilderString NUMBER_OF_SHARDS = new XContentBuilderString("number_of_shards");
        static final XContentBuilderString NUMBER_OF_REPLICAS = new XContentBuilderString("number_of_replicas");
        static final XContentBuilderString ACTIVE_PRIMARY_SHARDS = new XContentBuilderString("active_primary_shards");
        static final XContentBuilderString ACTIVE_SHARDS = new XContentBuilderString("active_shards");
        static final XContentBuilderString RELOCATING_SHARDS = new XContentBuilderString("relocating_shards");
        static final XContentBuilderString INITIALIZING_SHARDS = new XContentBuilderString("initializing_shards");
        static final XContentBuilderString UNASSIGNED_SHARDS = new XContentBuilderString("unassigned_shards");
        static final XContentBuilderString VALIDATION_FAILURES = new XContentBuilderString("validation_failures");
        static final XContentBuilderString SHARDS = new XContentBuilderString("shards");
        static final XContentBuilderString PRIMARY_ACTIVE = new XContentBuilderString("primary_active");
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(Fields.STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(Fields.NUMBER_OF_SHARDS, getNumberOfShards());
        builder.field(Fields.NUMBER_OF_REPLICAS, getNumberOfReplicas());
        builder.field(Fields.ACTIVE_PRIMARY_SHARDS, getActivePrimaryShards());
        builder.field(Fields.ACTIVE_SHARDS, getActiveShards());
        builder.field(Fields.RELOCATING_SHARDS, getRelocatingShards());
        builder.field(Fields.INITIALIZING_SHARDS, getInitializingShards());
        builder.field(Fields.UNASSIGNED_SHARDS, getUnassignedShards());

        if (!getValidationFailures().isEmpty()) {
            builder.startArray(Fields.VALIDATION_FAILURES);
            for (String validationFailure : getValidationFailures()) {
                builder.value(validationFailure);
            }
            builder.endArray();
        }

        if ("shards".equals(params.param("level", "indices"))) {
            builder.startObject(Fields.SHARDS);

            for (ClusterShardHealth shardHealth : shards.values()) {
                builder.startObject(Integer.toString(shardHealth.getId()));

                builder.field(Fields.STATUS, shardHealth.getStatus().name().toLowerCase(Locale.ROOT));
                builder.field(Fields.PRIMARY_ACTIVE, shardHealth.isPrimaryActive());
                builder.field(Fields.ACTIVE_SHARDS, shardHealth.getActiveShards());
                builder.field(Fields.RELOCATING_SHARDS, shardHealth.getRelocatingShards());
                builder.field(Fields.INITIALIZING_SHARDS, shardHealth.getInitializingShards());
                builder.field(Fields.UNASSIGNED_SHARDS, shardHealth.getUnassignedShards());

                builder.endObject();
            }

            builder.endObject();
        }
        return builder;
    }
}
