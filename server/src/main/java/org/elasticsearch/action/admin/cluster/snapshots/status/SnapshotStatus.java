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

package org.elasticsearch.action.admin.cluster.snapshots.status;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.SnapshotsInProgress.State;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static java.util.Collections.unmodifiableMap;

/**
 * Status of a snapshot
 */
public class SnapshotStatus implements ToXContentObject, Streamable {

    private Snapshot snapshot;

    private State state;

    private List<SnapshotIndexShardStatus> shards;

    private Map<String, SnapshotIndexStatus> indicesStatus;

    private SnapshotShardsStats shardsStats;

    private SnapshotStats stats;

    @Nullable
    private Boolean includeGlobalState;

    SnapshotStatus(final Snapshot snapshot, final State state, final List<SnapshotIndexShardStatus> shards,
                   final Boolean includeGlobalState) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.state = Objects.requireNonNull(state);
        this.shards = Objects.requireNonNull(shards);
        this.includeGlobalState = includeGlobalState;
        shardsStats = new SnapshotShardsStats(shards);
        updateShardStats();
    }

    private SnapshotStatus(Snapshot snapshot, State state, List<SnapshotIndexShardStatus> shards,
                          Map<String, SnapshotIndexStatus> indicesStatus, SnapshotShardsStats shardsStats,
                          SnapshotStats stats, Boolean includeGlobalState) {
        this.snapshot = snapshot;
        this.state = state;
        this.shards = shards;
        this.indicesStatus = indicesStatus;
        this.shardsStats = shardsStats;
        this.stats = stats;
        this.includeGlobalState = includeGlobalState;
    }

    SnapshotStatus() {
    }

    /**
     * Returns snapshot
     */
    public Snapshot getSnapshot() {
        return snapshot;
    }

    /**
     * Returns snapshot state
     */
    public State getState() {
        return state;
    }

    /**
     * Returns true if global state is included in the snapshot, false otherwise.
     * Can be null if this information is unknown.
     */
    public Boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * Returns list of snapshot shards
     */
    public List<SnapshotIndexShardStatus> getShards() {
        return shards;
    }

    public SnapshotShardsStats getShardsStats() {
        return shardsStats;
    }

    /**
     * Returns list of snapshot indices
     */
    public Map<String, SnapshotIndexStatus> getIndices() {
        if (this.indicesStatus != null) {
            return this.indicesStatus;
        }

        Map<String, SnapshotIndexStatus> indicesStatus = new HashMap<>();

        Set<String> indices = new HashSet<>();
        for (SnapshotIndexShardStatus shard : shards) {
            indices.add(shard.getIndex());
        }

        for (String index : indices) {
            List<SnapshotIndexShardStatus> shards = new ArrayList<>();
            for (SnapshotIndexShardStatus shard : this.shards) {
                if (shard.getIndex().equals(index)) {
                    shards.add(shard);
                }
            }
            indicesStatus.put(index, new SnapshotIndexStatus(index, shards));
        }
        this.indicesStatus = unmodifiableMap(indicesStatus);
        return this.indicesStatus;

    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        snapshot = new Snapshot(in);
        state = State.fromValue(in.readByte());
        int size = in.readVInt();
        List<SnapshotIndexShardStatus> builder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            builder.add(SnapshotIndexShardStatus.readShardSnapshotStatus(in));
        }
        shards = Collections.unmodifiableList(builder);
        if (in.getVersion().onOrAfter(Version.V_6_2_0)) {
            includeGlobalState = in.readOptionalBoolean();
        }
        updateShardStats();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        snapshot.writeTo(out);
        out.writeByte(state.value());
        out.writeVInt(shards.size());
        for (SnapshotIndexShardStatus shard : shards) {
            shard.writeTo(out);
        }
        if (out.getVersion().onOrAfter(Version.V_6_2_0)) {
            out.writeOptionalBoolean(includeGlobalState);
        }
    }

    /**
     * Reads snapshot status from stream input
     *
     * @param in stream input
     * @return deserialized snapshot status
     */
    public static SnapshotStatus readSnapshotStatus(StreamInput in) throws IOException {
        SnapshotStatus snapshotInfo = new SnapshotStatus();
        snapshotInfo.readFrom(in);
        return snapshotInfo;
    }

    @Override
    public String toString() {
        return Strings.toString(this, true, false);
    }

    /**
     * Returns number of files in the snapshot
     */
    public SnapshotStats getStats() {
        return stats;
    }

    private static final String SNAPSHOT = "snapshot";
    private static final String REPOSITORY = "repository";
    private static final String UUID = "uuid";
    private static final String STATE = "state";
    private static final String INDICES = "indices";
    private static final String INCLUDE_GLOBAL_STATE = "include_global_state";

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SNAPSHOT, snapshot.getSnapshotId().getName());
        builder.field(REPOSITORY, snapshot.getRepository());
        builder.field(UUID, snapshot.getSnapshotId().getUUID());
        builder.field(STATE, state.name());
        if (includeGlobalState != null) {
            builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState);
        }
        shardsStats.toXContent(builder, params);
        stats.toXContent(builder, params);
        builder.startObject(INDICES);
        for (SnapshotIndexStatus indexStatus : getIndices().values()) {
            indexStatus.toXContent(builder, params);
        }
        builder.endObject();
        builder.endObject();
        return builder;
    }

    public static SnapshotStatus fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser::getTokenLocation);
        String name = null;
        String repository = null;
        String uuid = null;
        SnapshotsInProgress.State state = null;
        Boolean includeGlobalState = null;
        SnapshotShardsStats shardsStats = null;
        SnapshotStats stats = null;
        List<SnapshotIndexShardStatus> shards = new ArrayList<>();
        Map<String, SnapshotIndexStatus> indices = new HashMap<>();
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                String currentFieldName = parser.currentName();
                if (SNAPSHOT.equals(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(),
                        parser::getTokenLocation);
                    name = parser.text();
                } else if (REPOSITORY.equals(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(),
                        parser::getTokenLocation);
                    repository = parser.text();
                } else if (UUID.equals(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(),
                        parser::getTokenLocation);
                    uuid = parser.text();
                } else if (STATE.equals(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_STRING, parser.nextToken(),
                        parser::getTokenLocation);
                    String stateRaw = parser.text();
                    try {
                        state = SnapshotsInProgress.State.valueOf(stateRaw);
                    } catch (IllegalArgumentException iae) {
                        throw new ElasticsearchParseException("failed to parse snapshot status, unknown state value [{}]", iae, stateRaw);
                    }
                } else if (INCLUDE_GLOBAL_STATE.equals(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.VALUE_BOOLEAN, parser.nextToken(),
                        parser::getTokenLocation);
                    includeGlobalState = parser.booleanValue();
                } else if (SnapshotShardsStats.Fields.SHARDS_STATS.equals(currentFieldName)) {
                    shardsStats = SnapshotShardsStats.fromXContent(parser);
                } else if (SnapshotStats.Fields.STATS.equals(currentFieldName)) {
                    stats = SnapshotStats.fromXContent(parser);
                } else if (INDICES.equals(currentFieldName)) {
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(),
                        parser::getTokenLocation);
                    while (parser.nextToken() != XContentParser.Token.END_OBJECT) {
                        SnapshotIndexStatus indexStatus = SnapshotIndexStatus.fromXContent(parser);
                        indices.put(indexStatus.getIndex(), indexStatus);
                        shards.addAll(indexStatus.getShards().values());
                    }
                } else {
                    throw new ElasticsearchParseException("failed to parse snapshot status, unknown field [{}]", currentFieldName);
                }
            } else {
                throw new ElasticsearchParseException("failed to parse snapshot status");
            }
        }
        if (name == null) {
            throw new ElasticsearchParseException("failed to parse snapshot status, missing snapshot name");
        } else if (uuid == null) {
            throw new ElasticsearchParseException("failed to parse snapshot status, missing snapshot uuid");
        } else if (repository == null) {
            throw new ElasticsearchParseException("failed to parse snapshot status, missing snapshot repository");
        } else if (state == null) {
            throw new ElasticsearchParseException("failed to parse snapshot status, missing snapshot state");
        }
        Snapshot snapshot = new Snapshot(repository, new SnapshotId(name, uuid));
        return new SnapshotStatus(snapshot, state, shards, indices, shardsStats, stats, includeGlobalState);
    }

    private void updateShardStats() {
        stats = new SnapshotStats();
        shardsStats = new SnapshotShardsStats(shards);
        for (SnapshotIndexShardStatus shard : shards) {
            stats.add(shard.getStats());
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SnapshotStatus that = (SnapshotStatus) o;

        if (snapshot != null ? !snapshot.equals(that.snapshot) : that.snapshot != null) return false;
        if (state != that.state) return false;
        if (indicesStatus != null ? !indicesStatus.equals(that.indicesStatus) : that.indicesStatus != null)
            return false;
        if (shardsStats != null ? !shardsStats.equals(that.shardsStats) : that.shardsStats != null) return false;
        if (stats != null ? !stats.equals(that.stats) : that.stats != null) return false;
        return includeGlobalState != null ? includeGlobalState.equals(that.includeGlobalState) : that.includeGlobalState == null;
    }

    @Override
    public int hashCode() {
        int result = snapshot != null ? snapshot.hashCode() : 0;
        result = 31 * result + (state != null ? state.hashCode() : 0);
        result = 31 * result + (indicesStatus != null ? indicesStatus.hashCode() : 0);
        result = 31 * result + (shardsStats != null ? shardsStats.hashCode() : 0);
        result = 31 * result + (stats != null ? stats.hashCode() : 0);
        result = 31 * result + (includeGlobalState != null ? includeGlobalState.hashCode() : 0);
        return result;
    }
}
