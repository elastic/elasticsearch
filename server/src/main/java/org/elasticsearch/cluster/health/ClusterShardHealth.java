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

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.elasticsearch.common.xcontent.XContentParserUtils.ensureExpectedToken;

public final class ClusterShardHealth implements Writeable, ToXContentFragment {
    private static final String STATUS = "status";
    private static final String ACTIVE_SHARDS = "active_shards";
    private static final String RELOCATING_SHARDS = "relocating_shards";
    private static final String INITIALIZING_SHARDS = "initializing_shards";
    private static final String UNASSIGNED_SHARDS = "unassigned_shards";
    private static final String PRIMARY_ACTIVE = "primary_active";

    public static final ConstructingObjectParser<ClusterShardHealth, Integer> PARSER =
        new ConstructingObjectParser<>("cluster_shard_health", true,
                (parsedObjects, shardId) -> {
                    int i = 0;
                    boolean primaryActive = (boolean) parsedObjects[i++];
                    int activeShards = (int) parsedObjects[i++];
                    int relocatingShards = (int) parsedObjects[i++];
                    int initializingShards = (int) parsedObjects[i++];
                    int unassignedShards = (int) parsedObjects[i++];
                    String statusStr = (String) parsedObjects[i];
                    ClusterHealthStatus status = ClusterHealthStatus.fromString(statusStr);
                    return new ClusterShardHealth(shardId, status, activeShards, relocatingShards, initializingShards, unassignedShards,
                        primaryActive);
                });

    static {
//        PARSER.declareInt(constructorArg(), new ParseField(SHARD_ID));
        PARSER.declareBoolean(constructorArg(), new ParseField(PRIMARY_ACTIVE));
        PARSER.declareInt(constructorArg(), new ParseField(ACTIVE_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(RELOCATING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(INITIALIZING_SHARDS));
        PARSER.declareInt(constructorArg(), new ParseField(UNASSIGNED_SHARDS));
        PARSER.declareString(constructorArg(), new ParseField(STATUS));
    }

    private final int shardId;
    private final ClusterHealthStatus status;
    private final int activeShards;
    private final int relocatingShards;
    private final int initializingShards;
    private final int unassignedShards;
    private final boolean primaryActive;

    public ClusterShardHealth(int shardId, ClusterHealthStatus status, int activeShards, int relocatingShards, int initializingShards,
            int unassignedShards, boolean primaryActive) {
        this.shardId = shardId;
        this.status = status;
        this.activeShards = activeShards;
        this.relocatingShards = relocatingShards;
        this.initializingShards = initializingShards;
        this.unassignedShards = unassignedShards;
        this.primaryActive = primaryActive;
    }

    public ClusterShardHealth(final StreamInput in) throws IOException {
        shardId = in.readVInt();
        status = ClusterHealthStatus.fromValue(in.readByte());
        activeShards = in.readVInt();
        relocatingShards = in.readVInt();
        initializingShards = in.readVInt();
        unassignedShards = in.readVInt();
        primaryActive = in.readBoolean();
    }

    public int getShardId() {
        return shardId;
    }

    public ClusterHealthStatus getStatus() {
        return status;
    }

    public int getRelocatingShards() {
        return relocatingShards;
    }

    public int getActiveShards() {
        return activeShards;
    }

    public boolean isPrimaryActive() {
        return primaryActive;
    }

    public int getInitializingShards() {
        return initializingShards;
    }

    public int getUnassignedShards() {
        return unassignedShards;
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        out.writeVInt(shardId);
        out.writeByte(status.value());
        out.writeVInt(activeShards);
        out.writeVInt(relocatingShards);
        out.writeVInt(initializingShards);
        out.writeVInt(unassignedShards);
        out.writeBoolean(primaryActive);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Integer.toString(getShardId()));
        builder.field(STATUS, getStatus().name().toLowerCase(Locale.ROOT));
        builder.field(PRIMARY_ACTIVE, isPrimaryActive());
        builder.field(ACTIVE_SHARDS, getActiveShards());
        builder.field(RELOCATING_SHARDS, getRelocatingShards());
        builder.field(INITIALIZING_SHARDS, getInitializingShards());
        builder.field(UNASSIGNED_SHARDS, getUnassignedShards());
        builder.endObject();
        return builder;
    }

    static ClusterShardHealth innerFromXContent(XContentParser parser, Integer shardId) {
        return PARSER.apply(parser, shardId);
    }

    public static ClusterShardHealth fromXContent(XContentParser parser) {
        try {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser::getTokenLocation);
            XContentParser.Token token = parser.nextToken();
            ensureExpectedToken(XContentParser.Token.FIELD_NAME, token, parser::getTokenLocation);
            String shardIdStr = parser.currentName();
            ClusterShardHealth parsed = innerFromXContent(parser, Integer.valueOf(shardIdStr));
            ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser::getTokenLocation);
            return parsed;
        } catch (IOException e) {
            throw new ParsingException(parser.getTokenLocation(), "[ClusterShardHealth::fromXContent] failed to parse object", e);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof ClusterShardHealth)) return false;
        ClusterShardHealth that = (ClusterShardHealth) o;
        return shardId == that.shardId &&
                activeShards == that.activeShards &&
                relocatingShards == that.relocatingShards &&
                initializingShards == that.initializingShards &&
                unassignedShards == that.unassignedShards &&
                primaryActive == that.primaryActive &&
                status == that.status;
    }

    @Override
    public int hashCode() {
        return Objects.hash(shardId, status, activeShards, relocatingShards, initializingShards, unassignedShards, primaryActive);
    }
}
