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
package org.elasticsearch.snapshots;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.rest.RestStatus;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Information about a snapshot
 */
public final class SnapshotInfo implements Comparable<SnapshotInfo>, ToXContent, Writeable {

    public static final Version DATA_STREAMS_IN_SNAPSHOT = Version.V_7_9_0;

    public static final String CONTEXT_MODE_PARAM = "context_mode";
    public static final String CONTEXT_MODE_SNAPSHOT = "SNAPSHOT";
    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("strictDateOptionalTime");
    private static final String SNAPSHOT = "snapshot";
    private static final String UUID = "uuid";
    private static final String INDICES = "indices";
    private static final String DATA_STREAMS = "data_streams";
    private static final String STATE = "state";
    private static final String REASON = "reason";
    private static final String START_TIME = "start_time";
    private static final String START_TIME_IN_MILLIS = "start_time_in_millis";
    private static final String END_TIME = "end_time";
    private static final String END_TIME_IN_MILLIS = "end_time_in_millis";
    private static final String DURATION = "duration";
    private static final String DURATION_IN_MILLIS = "duration_in_millis";
    private static final String FAILURES = "failures";
    private static final String SHARDS = "shards";
    private static final String TOTAL = "total";
    private static final String FAILED = "failed";
    private static final String SUCCESSFUL = "successful";
    private static final String VERSION_ID = "version_id";
    private static final String VERSION = "version";
    private static final String NAME = "name";
    private static final String TOTAL_SHARDS = "total_shards";
    private static final String SUCCESSFUL_SHARDS = "successful_shards";
    private static final String INCLUDE_GLOBAL_STATE = "include_global_state";
    private static final String USER_METADATA = "metadata";

    private static final Comparator<SnapshotInfo> COMPARATOR =
        Comparator.comparing(SnapshotInfo::startTime).thenComparing(SnapshotInfo::snapshotId);

    public static final class SnapshotInfoBuilder {
        private String snapshotName = null;
        private String snapshotUUID = null;
        private String state = null;
        private String reason = null;
        private List<String> indices = null;
        private List<String> dataStreams = null;
        private long startTime = 0L;
        private long endTime = 0L;
        private ShardStatsBuilder shardStatsBuilder = null;
        private Boolean includeGlobalState = null;
        private Map<String, Object> userMetadata = null;
        private int version = -1;
        private List<SnapshotShardFailure> shardFailures = null;

        private void setSnapshotName(String snapshotName) {
            this.snapshotName = snapshotName;
        }

        private void setSnapshotUUID(String snapshotUUID) {
            this.snapshotUUID = snapshotUUID;
        }

        private void setState(String state) {
            this.state = state;
        }

        private void setReason(String reason) {
            this.reason = reason;
        }

        private void setIndices(List<String> indices) {
            this.indices = indices;
        }

        private void setDataStreams(List<String> dataStreams) {
            this.dataStreams = dataStreams;
        }

        private void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        private void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        private void setShardStatsBuilder(ShardStatsBuilder shardStatsBuilder) {
            this.shardStatsBuilder = shardStatsBuilder;
        }

        private void setIncludeGlobalState(Boolean includeGlobalState) {
            this.includeGlobalState = includeGlobalState;
        }

        private void setUserMetadata(Map<String, Object> userMetadata) {
            this.userMetadata = userMetadata;
        }

        private void setVersion(int version) {
            this.version = version;
        }

        private void setShardFailures(List<SnapshotShardFailure> shardFailures) {
            this.shardFailures = shardFailures;
        }

        public SnapshotInfo build() {
            SnapshotId snapshotId = new SnapshotId(snapshotName, snapshotUUID);

            if (indices == null) {
                indices = Collections.emptyList();
            }

            if (dataStreams == null) {
                dataStreams = Collections.emptyList();
            }

            SnapshotState snapshotState = state == null ? null : SnapshotState.valueOf(state);
            Version version = this.version == -1 ? Version.CURRENT : Version.fromId(this.version);

            int totalShards = shardStatsBuilder == null ? 0 : shardStatsBuilder.getTotalShards();
            int successfulShards = shardStatsBuilder == null ? 0 : shardStatsBuilder.getSuccessfulShards();

            if (shardFailures == null) {
                shardFailures = new ArrayList<>();
            }

            return new SnapshotInfo(snapshotId, indices, dataStreams, snapshotState, reason, version, startTime, endTime,
                    totalShards, successfulShards, shardFailures, includeGlobalState, userMetadata);
        }
    }

    private static final class ShardStatsBuilder {
        private int totalShards;
        private int successfulShards;

        private void setTotalShards(int totalShards) {
            this.totalShards = totalShards;
        }

        int getTotalShards() {
            return totalShards;
        }

        private void setSuccessfulShards(int successfulShards) {
            this.successfulShards = successfulShards;
        }

        int getSuccessfulShards() {
            return successfulShards;
        }
    }

    public static final ObjectParser<SnapshotInfoBuilder, Void> SNAPSHOT_INFO_PARSER =
            new ObjectParser<>(SnapshotInfoBuilder.class.getName(), true, SnapshotInfoBuilder::new);

    private static final ObjectParser<ShardStatsBuilder, Void> SHARD_STATS_PARSER =
        new ObjectParser<>(ShardStatsBuilder.class.getName(), true, ShardStatsBuilder::new);

    static {
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setSnapshotName, new ParseField(SNAPSHOT));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setSnapshotUUID, new ParseField(UUID));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setState, new ParseField(STATE));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setReason, new ParseField(REASON));
        SNAPSHOT_INFO_PARSER.declareStringArray(SnapshotInfoBuilder::setIndices, new ParseField(INDICES));
        SNAPSHOT_INFO_PARSER.declareStringArray(SnapshotInfoBuilder::setDataStreams, new ParseField(DATA_STREAMS));
        SNAPSHOT_INFO_PARSER.declareLong(SnapshotInfoBuilder::setStartTime, new ParseField(START_TIME_IN_MILLIS));
        SNAPSHOT_INFO_PARSER.declareLong(SnapshotInfoBuilder::setEndTime, new ParseField(END_TIME_IN_MILLIS));
        SNAPSHOT_INFO_PARSER.declareObject(SnapshotInfoBuilder::setShardStatsBuilder, SHARD_STATS_PARSER, new ParseField(SHARDS));
        SNAPSHOT_INFO_PARSER.declareBoolean(SnapshotInfoBuilder::setIncludeGlobalState, new ParseField(INCLUDE_GLOBAL_STATE));
        SNAPSHOT_INFO_PARSER.declareObject(SnapshotInfoBuilder::setUserMetadata, (p, c) -> p.map() , new ParseField(USER_METADATA));
        SNAPSHOT_INFO_PARSER.declareInt(SnapshotInfoBuilder::setVersion, new ParseField(VERSION_ID));
        SNAPSHOT_INFO_PARSER.declareObjectArray(SnapshotInfoBuilder::setShardFailures, SnapshotShardFailure.SNAPSHOT_SHARD_FAILURE_PARSER,
            new ParseField(FAILURES));

        SHARD_STATS_PARSER.declareInt(ShardStatsBuilder::setTotalShards, new ParseField(TOTAL));
        SHARD_STATS_PARSER.declareInt(ShardStatsBuilder::setSuccessfulShards, new ParseField(SUCCESSFUL));
    }

    private final SnapshotId snapshotId;

    @Nullable
    private final SnapshotState state;

    @Nullable
    private final String reason;

    private final List<String> indices;

    private final List<String> dataStreams;

    private final long startTime;

    private final long endTime;

    private final int totalShards;

    private final int successfulShards;

    @Nullable
    private final Boolean includeGlobalState;

    @Nullable
    private final Map<String, Object> userMetadata;

    @Nullable
    private final Version version;

    private final List<SnapshotShardFailure> shardFailures;

    public SnapshotInfo(SnapshotId snapshotId, List<String> indices, List<String> dataStreams, SnapshotState state) {
        this(snapshotId, indices, dataStreams, state, null, null, 0L, 0L, 0, 0, Collections.emptyList(), null, null);
    }

    public SnapshotInfo(SnapshotId snapshotId, List<String> indices, List<String> dataStreams, SnapshotState state, Version version) {
        this(snapshotId, indices, dataStreams, state, null, version, 0L, 0L, 0, 0, Collections.emptyList(), null, null);
    }

    public SnapshotInfo(SnapshotsInProgress.Entry entry) {
        this(entry.snapshot().getSnapshotId(),
            entry.indices().stream().map(IndexId::getName).collect(Collectors.toList()), entry.dataStreams(), SnapshotState.IN_PROGRESS,
            null, Version.CURRENT, entry.startTime(), 0L, 0, 0, Collections.emptyList(), entry.includeGlobalState(), entry.userMetadata());
    }

    public SnapshotInfo(SnapshotId snapshotId, List<String> indices, List<String> dataStreams, long startTime, String reason,
                        long endTime, int totalShards, List<SnapshotShardFailure> shardFailures, Boolean includeGlobalState,
                        Map<String, Object> userMetadata) {
        this(snapshotId, indices, dataStreams, snapshotState(reason, shardFailures), reason, Version.CURRENT,
             startTime, endTime, totalShards, totalShards - shardFailures.size(), shardFailures, includeGlobalState, userMetadata);
    }

    private SnapshotInfo(SnapshotId snapshotId, List<String> indices, List<String> dataStreams, SnapshotState state, String reason,
                         Version version, long startTime, long endTime, int totalShards, int successfulShards,
                         List<SnapshotShardFailure> shardFailures, Boolean includeGlobalState, Map<String, Object> userMetadata) {
        this.snapshotId = Objects.requireNonNull(snapshotId);
        this.indices = Collections.unmodifiableList(Objects.requireNonNull(indices));
        this.dataStreams = Collections.unmodifiableList(Objects.requireNonNull(dataStreams));
        this.state = state;
        this.reason = reason;
        this.version = version;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.shardFailures = Objects.requireNonNull(shardFailures);
        this.includeGlobalState = includeGlobalState;
        this.userMetadata = userMetadata;
    }

    /**
     * Constructs snapshot information from stream input
     */
    public SnapshotInfo(final StreamInput in) throws IOException {
        snapshotId = new SnapshotId(in);
        int size = in.readVInt();
        List<String> indicesListBuilder = new ArrayList<>();
        for (int i = 0; i < size; i++) {
            indicesListBuilder.add(in.readString());
        }
        indices = Collections.unmodifiableList(indicesListBuilder);
        state = in.readBoolean() ? SnapshotState.fromValue(in.readByte()) : null;
        reason = in.readOptionalString();
        startTime = in.readVLong();
        endTime = in.readVLong();
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        size = in.readVInt();
        if (size > 0) {
            List<SnapshotShardFailure> failureBuilder = new ArrayList<>();
            for (int i = 0; i < size; i++) {
                failureBuilder.add(new SnapshotShardFailure(in));
            }
            shardFailures = Collections.unmodifiableList(failureBuilder);
        } else {
            shardFailures = Collections.emptyList();
        }
        version = in.readBoolean() ? Version.readVersion(in) : null;
        includeGlobalState = in.readOptionalBoolean();
        userMetadata = in.readMap();
        if (in.getVersion().onOrAfter(DATA_STREAMS_IN_SNAPSHOT)) {
            dataStreams = in.readStringList();
        } else {
            dataStreams = Collections.emptyList();
        }
    }

    /**
     * Gets a new {@link SnapshotInfo} instance from the given {@link SnapshotInfo} with
     * all information stripped out except the snapshot id, state, and indices.
     */
    public SnapshotInfo basic() {
        return new SnapshotInfo(snapshotId, indices, Collections.emptyList(), state);
    }

    /**
     * Returns snapshot id
     *
     * @return snapshot id
     */
    public SnapshotId snapshotId() {
        return snapshotId;
    }

    /**
     * Returns snapshot state; {@code null} if the state is unknown.
     *
     * @return snapshot state
     */
    @Nullable
    public SnapshotState state() {
        return state;
    }

    /**
     * Returns snapshot failure reason; {@code null} if the snapshot succeeded.
     *
     * @return snapshot failure reason
     */
    @Nullable
    public String reason() {
        return reason;
    }

    /**
     * Returns indices that were included in this snapshot.
     *
     * @return list of indices
     */
    public List<String> indices() {
        return indices;
    }

    /**
     * @return list of data streams that were included in this snapshot.
     */
    public List<String> dataStreams(){
        return dataStreams;
    }

    /**
     * Returns time when snapshot started; a value of {@code 0L} will be returned if
     * {@link #state()} returns {@code null}.
     *
     * @return snapshot start time
     */
    public long startTime() {
        return startTime;
    }

    /**
     * Returns time when snapshot ended; a value of {@code 0L} will be returned if the
     * snapshot is still running or if {@link #state()} returns {@code null}.
     *
     * @return snapshot end time
     */
    public long endTime() {
        return endTime;
    }

    /**
     * Returns total number of shards that were snapshotted; a value of {@code 0} will
     * be returned if {@link #state()} returns {@code null}.
     *
     * @return number of shards
     */
    public int totalShards() {
        return totalShards;
    }

    /**
     * Number of failed shards; a value of {@code 0} will be returned if there were no
     * failed shards, or if {@link #state()} returns {@code null}.
     *
     * @return number of failed shards
     */
    public int failedShards() {
        return totalShards - successfulShards;
    }

    /**
     * Returns total number of shards that were successfully snapshotted; a value of
     * {@code 0} will be returned if {@link #state()} returns {@code null}.
     *
     * @return number of successful shards
     */
    public int successfulShards() {
        return successfulShards;
    }

    public Boolean includeGlobalState() {
        return includeGlobalState;
    }

    /**
     * Returns shard failures; an empty list will be returned if there were no shard
     * failures, or if {@link #state()} returns {@code null}.
     *
     * @return shard failures
     */
    public List<SnapshotShardFailure> shardFailures() {
        return shardFailures;
    }

    /**
     * Returns the version of elasticsearch that the snapshot was created with.  Will only
     * return {@code null} if {@link #state()} returns {@code null} or {@link SnapshotState#INCOMPATIBLE}.
     *
     * @return version of elasticsearch that the snapshot was created with
     */
    @Nullable
    public Version version() {
        return version;
    }

    /**
     * Returns the custom metadata that was attached to this snapshot at creation time.
     * @return custom metadata
     */
    @Nullable
    public Map<String, Object> userMetadata() {
        return userMetadata;
    }

    /**
     * Compares two snapshots by their start time; if the start times are the same, then
     * compares the two snapshots by their snapshot ids.
     */
    @Override
    public int compareTo(final SnapshotInfo o) {
        return COMPARATOR.compare(this, o);
    }

    @Override
    public String toString() {
        return "SnapshotInfo{" +
            "snapshotId=" + snapshotId +
            ", state=" + state +
            ", reason='" + reason + '\'' +
            ", indices=" + indices +
            ", startTime=" + startTime +
            ", endTime=" + endTime +
            ", totalShards=" + totalShards +
            ", successfulShards=" + successfulShards +
            ", includeGlobalState=" + includeGlobalState +
            ", version=" + version +
            ", shardFailures=" + shardFailures +
            '}';
    }

    /**
     * Returns snapshot REST status
     */
    public RestStatus status() {
        if (state == SnapshotState.FAILED) {
            return RestStatus.INTERNAL_SERVER_ERROR;
        }
        if (shardFailures.size() == 0) {
            return RestStatus.OK;
        }
        return RestStatus.status(successfulShards, totalShards,
                                 shardFailures.toArray(new ShardOperationFailedException[shardFailures.size()]));
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        // write snapshot info to repository snapshot blob format
        if (CONTEXT_MODE_SNAPSHOT.equals(params.param(CONTEXT_MODE_PARAM))) {
            return toXContentInternal(builder, params);
        }

        final boolean verbose = params.paramAsBoolean("verbose", GetSnapshotsRequest.DEFAULT_VERBOSE_MODE);
        // write snapshot info for the API and any other situations
        builder.startObject();
        builder.field(SNAPSHOT, snapshotId.getName());
        builder.field(UUID, snapshotId.getUUID());
        if (version != null) {
            builder.field(VERSION_ID, version.id);
            builder.field(VERSION, version.toString());
        }
        builder.startArray(INDICES);
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.startArray(DATA_STREAMS);
        for (String dataStream : dataStreams) {
            builder.value(dataStream);
        }
        builder.endArray();
        if (includeGlobalState != null) {
            builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState);
        }
        if (userMetadata != null) {
            builder.field(USER_METADATA, userMetadata);
        }
        if (verbose || state != null) {
            builder.field(STATE, state);
        }
        if (reason != null) {
            builder.field(REASON, reason);
        }
        if (verbose || startTime != 0) {
            builder.field(START_TIME, DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(startTime).atZone(ZoneOffset.UTC)));
            builder.field(START_TIME_IN_MILLIS, startTime);
        }
        if (verbose || endTime != 0) {
            builder.field(END_TIME, DATE_TIME_FORMATTER.format(Instant.ofEpochMilli(endTime).atZone(ZoneOffset.UTC)));
            builder.field(END_TIME_IN_MILLIS, endTime);
            builder.humanReadableField(DURATION_IN_MILLIS, DURATION, new TimeValue(Math.max(0L, endTime - startTime)));
        }
        if (verbose || !shardFailures.isEmpty()) {
            builder.startArray(FAILURES);
            for (SnapshotShardFailure shardFailure : shardFailures) {
                shardFailure.toXContent(builder, params);
            }
            builder.endArray();
        }
        if (verbose || totalShards != 0) {
            builder.startObject(SHARDS);
            builder.field(TOTAL, totalShards);
            builder.field(FAILED, failedShards());
            builder.field(SUCCESSFUL, successfulShards);
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }

    private XContentBuilder toXContentInternal(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        builder.startObject(SNAPSHOT);
        builder.field(NAME, snapshotId.getName());
        builder.field(UUID, snapshotId.getUUID());
        assert version != null : "version must always be known when writing a snapshot metadata blob";
        builder.field(VERSION_ID, version.id);
        builder.startArray(INDICES);
        for (String index : indices) {
            builder.value(index);
        }
        builder.endArray();
        builder.startArray(DATA_STREAMS);
        for (String dataStream : dataStreams) {
            builder.value(dataStream);
        }
        builder.endArray();
        builder.field(STATE, state);
        if (reason != null) {
            builder.field(REASON, reason);
        }
        if (includeGlobalState != null) {
            builder.field(INCLUDE_GLOBAL_STATE, includeGlobalState);
        }
        builder.field(USER_METADATA, userMetadata);
        builder.field(START_TIME, startTime);
        builder.field(END_TIME, endTime);
        builder.field(TOTAL_SHARDS, totalShards);
        builder.field(SUCCESSFUL_SHARDS, successfulShards);
        builder.startArray(FAILURES);
        for (SnapshotShardFailure shardFailure : shardFailures) {
            shardFailure.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * This method creates a SnapshotInfo from internal x-content.  It does not
     * handle x-content written with the external version as external x-content
     * is only for display purposes and does not need to be parsed.
     */
    public static SnapshotInfo fromXContentInternal(final XContentParser parser) throws IOException {
        String name = null;
        String uuid = null;
        Version version = Version.CURRENT;
        SnapshotState state = SnapshotState.IN_PROGRESS;
        String reason = null;
        List<String> indices = Collections.emptyList();
        List<String> dataStreams = Collections.emptyList();
        long startTime = 0;
        long endTime = 0;
        int totalShards = 0;
        int successfulShards = 0;
        Boolean includeGlobalState = null;
        Map<String, Object> userMetadata = null;
        List<SnapshotShardFailure> shardFailures = Collections.emptyList();
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        XContentParser.Token token;
        if ((token = parser.nextToken()) == XContentParser.Token.START_OBJECT) {
            String currentFieldName = parser.currentName();
            if (SNAPSHOT.equals(currentFieldName)) {
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        currentFieldName = parser.currentName();
                        token = parser.nextToken();
                        if (token.isValue()) {
                            if (NAME.equals(currentFieldName)) {
                                name = parser.text();
                            } else if (UUID.equals(currentFieldName)) {
                                uuid = parser.text();
                            } else if (STATE.equals(currentFieldName)) {
                                state = SnapshotState.valueOf(parser.text());
                            } else if (REASON.equals(currentFieldName)) {
                                reason = parser.text();
                            } else if (START_TIME.equals(currentFieldName)) {
                                startTime = parser.longValue();
                            } else if (END_TIME.equals(currentFieldName)) {
                                endTime = parser.longValue();
                            } else if (TOTAL_SHARDS.equals(currentFieldName)) {
                                totalShards = parser.intValue();
                            } else if (SUCCESSFUL_SHARDS.equals(currentFieldName)) {
                                successfulShards = parser.intValue();
                            } else if (VERSION_ID.equals(currentFieldName)) {
                                version = Version.fromId(parser.intValue());
                            } else if (INCLUDE_GLOBAL_STATE.equals(currentFieldName)) {
                                includeGlobalState = parser.booleanValue();
                            }
                        } else if (token == XContentParser.Token.START_ARRAY) {
                            if (DATA_STREAMS.equals(currentFieldName)){
                                dataStreams = new ArrayList<>();
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    dataStreams.add(parser.text());
                                }
                            } else if (INDICES.equals(currentFieldName)) {
                                ArrayList<String> indicesArray = new ArrayList<>();
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    indicesArray.add(parser.text());
                                }
                                indices = Collections.unmodifiableList(indicesArray);
                            } else if (FAILURES.equals(currentFieldName)) {
                                ArrayList<SnapshotShardFailure> shardFailureArrayList = new ArrayList<>();
                                while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                                    shardFailureArrayList.add(SnapshotShardFailure.fromXContent(parser));
                                }
                                shardFailures = Collections.unmodifiableList(shardFailureArrayList);
                            } else {
                                // It was probably created by newer version - ignoring
                                parser.skipChildren();
                            }
                        } else if (token == XContentParser.Token.START_OBJECT) {
                            if (USER_METADATA.equals(currentFieldName)) {
                                userMetadata = parser.map();
                            } else {
                                // It was probably created by newer version - ignoring
                                parser.skipChildren();
                            }
                        }
                    }
                }
            }
        } else {
            throw new ElasticsearchParseException("unexpected token  [" + token + "]");
        }
        if (uuid == null) {
            // the old format where there wasn't a UUID
            uuid = name;
        }
        return new SnapshotInfo(new SnapshotId(name, uuid),
                                indices,
                                dataStreams,
                                state,
                                reason,
                                version,
                                startTime,
                                endTime,
                                totalShards,
                                successfulShards,
                                shardFailures,
                                includeGlobalState,
                                userMetadata);
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        snapshotId.writeTo(out);
        out.writeVInt(indices.size());
        for (String index : indices) {
            out.writeString(index);
        }
        if (state != null) {
            out.writeBoolean(true);
            out.writeByte(state.value());
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalString(reason);
        out.writeVLong(startTime);
        out.writeVLong(endTime);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);
        out.writeVInt(shardFailures.size());
        for (SnapshotShardFailure failure : shardFailures) {
            failure.writeTo(out);
        }
        if (version != null) {
            out.writeBoolean(true);
            Version.writeVersion(version, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalBoolean(includeGlobalState);
        out.writeMap(userMetadata);
        if(out.getVersion().onOrAfter(DATA_STREAMS_IN_SNAPSHOT)){
            out.writeStringCollection(dataStreams);
        }
    }

    private static SnapshotState snapshotState(final String reason, final List<SnapshotShardFailure> shardFailures) {
        if (reason == null) {
            if (shardFailures.isEmpty()) {
                return SnapshotState.SUCCESS;
            } else {
                return SnapshotState.PARTIAL;
            }
        } else {
            return SnapshotState.FAILED;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SnapshotInfo that = (SnapshotInfo) o;
        return startTime == that.startTime &&
            endTime == that.endTime &&
            totalShards == that.totalShards &&
            successfulShards == that.successfulShards &&
            Objects.equals(snapshotId, that.snapshotId) &&
            state == that.state &&
            Objects.equals(reason, that.reason) &&
            Objects.equals(indices, that.indices) &&
            Objects.equals(dataStreams, that.dataStreams) &&
            Objects.equals(includeGlobalState, that.includeGlobalState) &&
            Objects.equals(version, that.version) &&
            Objects.equals(shardFailures, that.shardFailures) &&
            Objects.equals(userMetadata, that.userMetadata);
    }

    @Override
    public int hashCode() {

        return Objects.hash(snapshotId, state, reason, indices, dataStreams, startTime, endTime,
                totalShards, successfulShards, includeGlobalState, version, shardFailures, userMetadata);
    }
}
