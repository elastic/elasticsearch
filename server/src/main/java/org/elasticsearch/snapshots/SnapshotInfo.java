/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.snapshots;

import org.elasticsearch.Version;
import org.elasticsearch.action.ShardOperationFailedException;
import org.elasticsearch.action.admin.cluster.snapshots.get.GetSnapshotsRequest;
import org.elasticsearch.cluster.SnapshotsInProgress;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.time.DateFormatter;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.repositories.RepositoryShardId;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Information about a snapshot
 */
public final class SnapshotInfo implements Comparable<SnapshotInfo>, ToXContentFragment, Writeable {

    public static final String INDEX_DETAILS_XCONTENT_PARAM = "index_details";

    public static final String INDEX_NAMES_XCONTENT_PARAM = "index_names";
    public static final String INCLUDE_REPOSITORY_XCONTENT_PARAM = "include_repository";

    private static final DateFormatter DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_optional_time");
    private static final String SNAPSHOT = "snapshot";
    private static final String UUID = "uuid";
    private static final String REPOSITORY = "repository";
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
    private static final String FEATURE_STATES = "feature_states";
    private static final String INDEX_DETAILS = "index_details";

    private static final String UNKNOWN_REPO_NAME = "_na_";

    private static final Comparator<SnapshotInfo> COMPARATOR = Comparator.comparing(SnapshotInfo::startTime)
        .thenComparing(SnapshotInfo::snapshotId);

    public static final class SnapshotInfoBuilder {
        private String snapshotName = null;
        private String snapshotUUID = null;
        private String repository = UNKNOWN_REPO_NAME;
        private String state = null;
        private String reason = null;
        private List<String> indices = null;
        private List<String> dataStreams = null;
        private List<SnapshotFeatureInfo> featureStates = null;
        private Map<String, IndexSnapshotDetails> indexSnapshotDetails = null;
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

        private void setRepository(String repository) {
            this.repository = repository;
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

        private void setFeatureStates(List<SnapshotFeatureInfo> featureStates) {
            this.featureStates = featureStates;
        }

        private void setIndexSnapshotDetails(Map<String, IndexSnapshotDetails> indexSnapshotDetails) {
            this.indexSnapshotDetails = indexSnapshotDetails;
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
            final Snapshot snapshot = new Snapshot(repository, new SnapshotId(snapshotName, snapshotUUID));

            if (indices == null) {
                indices = Collections.emptyList();
            }

            if (dataStreams == null) {
                dataStreams = Collections.emptyList();
            }

            if (featureStates == null) {
                featureStates = Collections.emptyList();
            }

            if (indexSnapshotDetails == null) {
                indexSnapshotDetails = Collections.emptyMap();
            }

            SnapshotState snapshotState = state == null ? null : SnapshotState.valueOf(state);
            Version version = this.version == -1 ? Version.CURRENT : Version.fromId(this.version);

            int totalShards = shardStatsBuilder == null ? 0 : shardStatsBuilder.getTotalShards();
            int successfulShards = shardStatsBuilder == null ? 0 : shardStatsBuilder.getSuccessfulShards();

            if (shardFailures == null) {
                shardFailures = new ArrayList<>();
            }

            return new SnapshotInfo(
                snapshot,
                indices,
                dataStreams,
                featureStates,
                reason,
                version,
                startTime,
                endTime,
                totalShards,
                successfulShards,
                shardFailures,
                includeGlobalState,
                userMetadata,
                snapshotState,
                indexSnapshotDetails
            );
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

    public static final ObjectParser<SnapshotInfoBuilder, Void> SNAPSHOT_INFO_PARSER = new ObjectParser<>(
        SnapshotInfoBuilder.class.getName(),
        true,
        SnapshotInfoBuilder::new
    );

    private static final ObjectParser<ShardStatsBuilder, Void> SHARD_STATS_PARSER = new ObjectParser<>(
        ShardStatsBuilder.class.getName(),
        true,
        ShardStatsBuilder::new
    );

    static {
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setSnapshotName, new ParseField(SNAPSHOT));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setSnapshotUUID, new ParseField(UUID));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setRepository, new ParseField(REPOSITORY));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setState, new ParseField(STATE));
        SNAPSHOT_INFO_PARSER.declareString(SnapshotInfoBuilder::setReason, new ParseField(REASON));
        SNAPSHOT_INFO_PARSER.declareStringArray(SnapshotInfoBuilder::setIndices, new ParseField(INDICES));
        SNAPSHOT_INFO_PARSER.declareStringArray(SnapshotInfoBuilder::setDataStreams, new ParseField(DATA_STREAMS));
        SNAPSHOT_INFO_PARSER.declareObjectArray(
            SnapshotInfoBuilder::setFeatureStates,
            SnapshotFeatureInfo.SNAPSHOT_FEATURE_INFO_PARSER,
            new ParseField(FEATURE_STATES)
        );
        SNAPSHOT_INFO_PARSER.declareObject(
            SnapshotInfoBuilder::setIndexSnapshotDetails,
            (p, c) -> p.map(HashMap::new, p2 -> IndexSnapshotDetails.PARSER.parse(p2, c)),
            new ParseField(INDEX_DETAILS)
        );
        SNAPSHOT_INFO_PARSER.declareLong(SnapshotInfoBuilder::setStartTime, new ParseField(START_TIME_IN_MILLIS));
        SNAPSHOT_INFO_PARSER.declareLong(SnapshotInfoBuilder::setEndTime, new ParseField(END_TIME_IN_MILLIS));
        SNAPSHOT_INFO_PARSER.declareObject(SnapshotInfoBuilder::setShardStatsBuilder, SHARD_STATS_PARSER, new ParseField(SHARDS));
        SNAPSHOT_INFO_PARSER.declareBoolean(SnapshotInfoBuilder::setIncludeGlobalState, new ParseField(INCLUDE_GLOBAL_STATE));
        SNAPSHOT_INFO_PARSER.declareObject(SnapshotInfoBuilder::setUserMetadata, (p, c) -> p.map(), new ParseField(USER_METADATA));
        SNAPSHOT_INFO_PARSER.declareInt(SnapshotInfoBuilder::setVersion, new ParseField(VERSION_ID));
        SNAPSHOT_INFO_PARSER.declareObjectArray(
            SnapshotInfoBuilder::setShardFailures,
            SnapshotShardFailure.SNAPSHOT_SHARD_FAILURE_PARSER,
            new ParseField(FAILURES)
        );

        SHARD_STATS_PARSER.declareInt(ShardStatsBuilder::setTotalShards, new ParseField(TOTAL));
        SHARD_STATS_PARSER.declareInt(ShardStatsBuilder::setSuccessfulShards, new ParseField(SUCCESSFUL));
    }

    private final Snapshot snapshot;

    @Nullable
    private final SnapshotState state;

    @Nullable
    private final String reason;

    private final List<String> indices;

    private final List<String> dataStreams;

    private final List<SnapshotFeatureInfo> featureStates;

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

    private final Map<String, IndexSnapshotDetails> indexSnapshotDetails;

    public SnapshotInfo(
        Snapshot snapshot,
        List<String> indices,
        List<String> dataStreams,
        List<SnapshotFeatureInfo> featureStates,
        SnapshotState state
    ) {
        this(
            snapshot,
            indices,
            dataStreams,
            featureStates,
            null,
            null,
            0L,
            0L,
            0,
            0,
            Collections.emptyList(),
            null,
            null,
            state,
            Collections.emptyMap()
        );
    }

    public SnapshotInfo(
        Snapshot snapshot,
        List<String> indices,
        List<String> dataStreams,
        List<SnapshotFeatureInfo> featureStates,
        Version version,
        SnapshotState state
    ) {
        this(
            snapshot,
            indices,
            dataStreams,
            featureStates,
            null,
            version,
            0L,
            0L,
            0,
            0,
            Collections.emptyList(),
            null,
            null,
            state,
            Collections.emptyMap()
        );
    }

    public static SnapshotInfo inProgress(SnapshotsInProgress.Entry entry) {
        int successfulShards = 0;
        List<SnapshotShardFailure> shardFailures = new ArrayList<>();
        for (Map.Entry<RepositoryShardId, SnapshotsInProgress.ShardSnapshotStatus> c : entry.shardsByRepoShardId().entrySet()) {
            if (c.getValue().state() == SnapshotsInProgress.ShardState.SUCCESS) {
                successfulShards++;
            } else if (c.getValue().state().failed() && c.getValue().state().completed()) {
                shardFailures.add(new SnapshotShardFailure(c.getValue().nodeId(), entry.shardId(c.getKey()), c.getValue().reason()));
            }
        }
        int totalShards = entry.shardsByRepoShardId().size();
        return new SnapshotInfo(
            entry.snapshot(),
            List.copyOf(entry.indices().keySet()),
            entry.dataStreams(),
            entry.featureStates(),
            null,
            Version.CURRENT,
            entry.startTime(),
            0L,
            totalShards,
            successfulShards,
            shardFailures,
            entry.includeGlobalState(),
            entry.userMetadata(),
            SnapshotState.IN_PROGRESS,
            Collections.emptyMap()
        );
    }

    public SnapshotInfo(
        Snapshot snapshot,
        List<String> indices,
        List<String> dataStreams,
        List<SnapshotFeatureInfo> featureStates,
        String reason,
        long endTime,
        int totalShards,
        List<SnapshotShardFailure> shardFailures,
        Boolean includeGlobalState,
        Map<String, Object> userMetadata,
        long startTime,
        Map<String, IndexSnapshotDetails> indexSnapshotDetails
    ) {
        this(
            snapshot,
            indices,
            dataStreams,
            featureStates,
            reason,
            Version.CURRENT,
            startTime,
            endTime,
            totalShards,
            totalShards - shardFailures.size(),
            shardFailures,
            includeGlobalState,
            userMetadata,
            snapshotState(reason, shardFailures),
            indexSnapshotDetails
        );
    }

    public SnapshotInfo(
        Snapshot snapshot,
        List<String> indices,
        List<String> dataStreams,
        List<SnapshotFeatureInfo> featureStates,
        String reason,
        Version version,
        long startTime,
        long endTime,
        int totalShards,
        int successfulShards,
        List<SnapshotShardFailure> shardFailures,
        Boolean includeGlobalState,
        Map<String, Object> userMetadata,
        SnapshotState state,
        Map<String, IndexSnapshotDetails> indexSnapshotDetails
    ) {
        this.snapshot = Objects.requireNonNull(snapshot);
        this.indices = List.copyOf(indices);
        this.dataStreams = List.copyOf(dataStreams);
        this.featureStates = List.copyOf(featureStates);
        this.state = state;
        this.reason = reason;
        this.version = version;
        this.startTime = startTime;
        this.endTime = endTime;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.shardFailures = List.copyOf(shardFailures);
        this.includeGlobalState = includeGlobalState;
        this.userMetadata = userMetadata == null ? null : Map.copyOf(userMetadata);
        this.indexSnapshotDetails = Map.copyOf(indexSnapshotDetails);
    }

    public SnapshotInfo withoutIndices() {
        if (indices.isEmpty()) {
            return this;
        }
        return new SnapshotInfo(
            snapshot,
            List.of(),
            dataStreams,
            featureStates,
            reason,
            version,
            startTime,
            endTime,
            totalShards,
            successfulShards,
            shardFailures,
            includeGlobalState,
            userMetadata,
            state,
            indexSnapshotDetails
        );
    }

    /**
     * Constructs snapshot information from stream input
     */
    public static SnapshotInfo readFrom(final StreamInput in) throws IOException {
        final Snapshot snapshot;
        if (in.getTransportVersion().onOrAfter(GetSnapshotsRequest.PAGINATED_GET_SNAPSHOTS_VERSION)) {
            snapshot = new Snapshot(in);
        } else {
            snapshot = new Snapshot(UNKNOWN_REPO_NAME, new SnapshotId(in));
        }
        final List<String> indices = in.readImmutableStringList();
        final SnapshotState state = in.readBoolean() ? SnapshotState.fromValue(in.readByte()) : null;
        final String reason = in.readOptionalString();
        final long startTime = in.readVLong();
        final long endTime = in.readVLong();
        final int totalShards = in.readVInt();
        final int successfulShards = in.readVInt();
        final List<SnapshotShardFailure> shardFailures = in.readImmutableList(SnapshotShardFailure::new);
        final Version version = in.readBoolean() ? Version.readVersion(in) : null;
        final Boolean includeGlobalState = in.readOptionalBoolean();
        final Map<String, Object> userMetadata = in.readMap();
        final List<String> dataStreams = in.readImmutableStringList();
        final List<SnapshotFeatureInfo> featureStates = in.readImmutableList(SnapshotFeatureInfo::new);
        final Map<String, IndexSnapshotDetails> indexSnapshotDetails = in.readImmutableMap(
            StreamInput::readString,
            IndexSnapshotDetails::new
        );
        return new SnapshotInfo(
            snapshot,
            indices,
            dataStreams,
            featureStates,
            reason,
            version,
            startTime,
            endTime,
            totalShards,
            successfulShards,
            shardFailures,
            includeGlobalState,
            userMetadata,
            state,
            indexSnapshotDetails
        );
    }

    /**
     * Gets a new {@link SnapshotInfo} instance from the given {@link SnapshotInfo} with
     * all information stripped out except the snapshot id, state, and indices.
     */
    public SnapshotInfo basic() {
        return new SnapshotInfo(snapshot, indices, Collections.emptyList(), featureStates, state);
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    /**
     * Returns snapshot id
     *
     * @return snapshot id
     */
    public SnapshotId snapshotId() {
        return snapshot.getSnapshotId();
    }

    public String repository() {
        return snapshot.getRepository();
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
    public List<String> dataStreams() {
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
     * Number of failed shards.
     *
     * @return number of failed shards
     */
    public int failedShards() {
        return shardFailures.size();
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

    public List<SnapshotFeatureInfo> featureStates() {
        return featureStates;
    }

    /**
     * @return details of each index in the snapshot, if available, or an empty map otherwise.
     */
    public Map<String, IndexSnapshotDetails> indexSnapshotDetails() {
        return indexSnapshotDetails;
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
        return "SnapshotInfo{"
            + "snapshot="
            + snapshot
            + ", state="
            + state
            + ", reason='"
            + reason
            + '\''
            + ", indices="
            + indices
            + ", startTime="
            + startTime
            + ", endTime="
            + endTime
            + ", totalShards="
            + totalShards
            + ", successfulShards="
            + successfulShards
            + ", includeGlobalState="
            + includeGlobalState
            + ", version="
            + version
            + ", shardFailures="
            + shardFailures
            + ", featureStates="
            + featureStates
            + ", indexSnapshotDetails="
            + indexSnapshotDetails
            + '}';
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
        return RestStatus.status(
            successfulShards,
            totalShards,
            shardFailures.toArray(new ShardOperationFailedException[shardFailures.size()])
        );
    }

    /**
     * Serialize this {@link SnapshotInfo} for external consumption, i.e. REST responses, from which we don't need to be able to read it
     * back again. This method builds a well-formed object, not a fragment like {@link #toXContent} does.
     */
    public XContentBuilder toXContentExternal(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        assert Metadata.CONTEXT_MODE_SNAPSHOT.equals(params.param(Metadata.CONTEXT_MODE_PARAM)) == false
            : "use toXContent() in SNAPSHOT context";

        final boolean verbose = params.paramAsBoolean("verbose", GetSnapshotsRequest.DEFAULT_VERBOSE_MODE);
        // write snapshot info for the API and any other situations
        builder.startObject();
        final SnapshotId snapshotId = snapshot.getSnapshotId();
        builder.field(SNAPSHOT, snapshotId.getName());
        builder.field(UUID, snapshotId.getUUID());

        if (params.paramAsBoolean(INCLUDE_REPOSITORY_XCONTENT_PARAM, true) && UNKNOWN_REPO_NAME.equals(snapshot.getRepository()) == false) {
            builder.field(REPOSITORY, snapshot.getRepository());
        }

        if (version != null) {
            builder.field(VERSION_ID, version.id);
            builder.field(VERSION, version.toString());
        }

        if (params.paramAsBoolean(INDEX_NAMES_XCONTENT_PARAM, true)) {
            builder.stringListField(INDICES, indices);
        }

        if (params.paramAsBoolean(INDEX_DETAILS_XCONTENT_PARAM, false) && indexSnapshotDetails.isEmpty() == false) {
            builder.startObject(INDEX_DETAILS);
            for (Map.Entry<String, IndexSnapshotDetails> entry : indexSnapshotDetails.entrySet()) {
                builder.field(entry.getKey());
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }

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
        if (verbose || shardFailures.isEmpty() == false) {
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
        if (verbose || featureStates.isEmpty() == false) {
            builder.startArray(FEATURE_STATES);
            for (SnapshotFeatureInfo snapshotFeatureInfo : featureStates) {
                builder.value(snapshotFeatureInfo);
            }
            builder.endArray();

        }
        builder.endObject();
        return builder;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final ToXContent.Params params) throws IOException {
        assert Metadata.CONTEXT_MODE_SNAPSHOT.equals(params.param(Metadata.CONTEXT_MODE_PARAM))
            : "use toXContentExternal() in external context";

        builder.startObject(SNAPSHOT);
        final SnapshotId snapshotId = snapshot.getSnapshotId();
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
        if (userMetadata != null) {
            builder.field(USER_METADATA, userMetadata);
        }
        builder.field(START_TIME, startTime);
        builder.field(END_TIME, endTime);
        builder.field(TOTAL_SHARDS, totalShards);
        builder.field(SUCCESSFUL_SHARDS, successfulShards);
        builder.startArray(FAILURES);
        for (SnapshotShardFailure shardFailure : shardFailures) {
            shardFailure.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray(FEATURE_STATES);
        for (SnapshotFeatureInfo snapshotFeatureInfo : featureStates) {
            builder.value(snapshotFeatureInfo);
        }
        builder.endArray();

        builder.startObject(INDEX_DETAILS);
        for (Map.Entry<String, IndexSnapshotDetails> entry : indexSnapshotDetails.entrySet()) {
            builder.field(entry.getKey());
            entry.getValue().toXContent(builder, params);
        }
        builder.endObject();

        builder.endObject();
        return builder;
    }

    /**
     * This method creates a SnapshotInfo from internal x-content.  It does not
     * handle x-content written with the external version as external x-content
     * is only for display purposes and does not need to be parsed.
     */
    public static SnapshotInfo fromXContentInternal(final String repoName, final XContentParser parser) throws IOException {
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
        List<SnapshotFeatureInfo> featureStates = Collections.emptyList();
        Map<String, IndexSnapshotDetails> indexSnapshotDetails = null;
        if (parser.currentToken() == null) { // fresh parser? move to the first token
            parser.nextToken();
        }
        if (parser.currentToken() == XContentParser.Token.START_OBJECT) {  // on a start object move to next token
            parser.nextToken();
        }
        XContentParserUtils.ensureFieldName(parser, parser.currentToken(), SNAPSHOT);
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
        while (parser.nextToken() == XContentParser.Token.FIELD_NAME) {
            final String currentFieldName = parser.currentName();
            final XContentParser.Token token = parser.nextToken();
            switch (currentFieldName) {
                case NAME:
                    name = parser.text();
                    break;
                case UUID:
                    uuid = parser.text();
                    break;
                case STATE:
                    state = SnapshotState.valueOf(parser.text());
                    break;
                case REASON:
                    reason = parser.text();
                    break;
                case START_TIME:
                    startTime = parser.longValue();
                    break;
                case END_TIME:
                    endTime = parser.longValue();
                    break;
                case TOTAL_SHARDS:
                    totalShards = parser.intValue();
                    break;
                case SUCCESSFUL_SHARDS:
                    successfulShards = parser.intValue();
                    break;
                case VERSION_ID:
                    version = Version.fromId(parser.intValue());
                    break;
                case INCLUDE_GLOBAL_STATE:
                    includeGlobalState = parser.booleanValue();
                    break;
                case DATA_STREAMS:
                    dataStreams = XContentParserUtils.parseList(parser, XContentParser::text);
                    break;
                case INDICES:
                    indices = XContentParserUtils.parseList(parser, XContentParser::text);
                    break;
                case FAILURES:
                    shardFailures = XContentParserUtils.parseList(parser, SnapshotShardFailure::fromXContent);
                    break;
                case FEATURE_STATES:
                    featureStates = XContentParserUtils.parseList(parser, SnapshotFeatureInfo::fromXContent);
                    break;
                case USER_METADATA:
                    if (token != XContentParser.Token.VALUE_NULL) {
                        // some older versions a redundant null value for this field
                        XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                        userMetadata = parser.map();
                    }
                    break;
                case INDEX_DETAILS:
                    XContentParserUtils.ensureExpectedToken(XContentParser.Token.START_OBJECT, token, parser);
                    indexSnapshotDetails = parser.map(HashMap::new, p -> IndexSnapshotDetails.PARSER.parse(p, null));
                    break;
                default:
                    // It was probably created by newer version - ignoring
                    parser.skipChildren();
                    break;
            }
        }
        // closing bracket of the object containing the "snapshot" field should be there
        XContentParserUtils.ensureExpectedToken(XContentParser.Token.END_OBJECT, parser.nextToken(), parser);
        if (uuid == null) {
            // the old format where there wasn't a UUID
            uuid = name;
        }
        return new SnapshotInfo(
            new Snapshot(repoName, new SnapshotId(name, uuid)),
            indices,
            dataStreams,
            featureStates,
            reason,
            version,
            startTime,
            endTime,
            totalShards,
            successfulShards,
            shardFailures,
            includeGlobalState,
            userMetadata,
            state,
            indexSnapshotDetails == null ? Collections.emptyMap() : indexSnapshotDetails
        );
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        if (out.getTransportVersion().onOrAfter(GetSnapshotsRequest.PAGINATED_GET_SNAPSHOTS_VERSION)) {
            snapshot.writeTo(out);
        } else {
            snapshot.getSnapshotId().writeTo(out);
        }
        out.writeStringCollection(indices);
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
        out.writeList(shardFailures);
        if (version != null) {
            out.writeBoolean(true);
            Version.writeVersion(version, out);
        } else {
            out.writeBoolean(false);
        }
        out.writeOptionalBoolean(includeGlobalState);
        out.writeGenericMap(userMetadata);
        out.writeStringCollection(dataStreams);
        out.writeList(featureStates);

        out.writeMap(indexSnapshotDetails, StreamOutput::writeString, (stream, value) -> value.writeTo(stream));
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
        return startTime == that.startTime
            && endTime == that.endTime
            && totalShards == that.totalShards
            && successfulShards == that.successfulShards
            && Objects.equals(snapshot, that.snapshot)
            && state == that.state
            && Objects.equals(reason, that.reason)
            && Objects.equals(indices, that.indices)
            && Objects.equals(dataStreams, that.dataStreams)
            && Objects.equals(includeGlobalState, that.includeGlobalState)
            && Objects.equals(version, that.version)
            && Objects.equals(shardFailures, that.shardFailures)
            && Objects.equals(userMetadata, that.userMetadata)
            && Objects.equals(featureStates, that.featureStates)
            && Objects.equals(indexSnapshotDetails, that.indexSnapshotDetails);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            snapshot,
            state,
            reason,
            indices,
            dataStreams,
            startTime,
            endTime,
            totalShards,
            successfulShards,
            includeGlobalState,
            version,
            shardFailures,
            userMetadata,
            featureStates,
            indexSnapshotDetails
        );
    }

    public static class IndexSnapshotDetails implements ToXContentObject, Writeable {
        private static final String SHARD_COUNT = "shard_count";
        private static final String SIZE = "size_in_bytes";
        private static final String MAX_SEGMENTS_PER_SHARD = "max_segments_per_shard";

        public static final IndexSnapshotDetails SKIPPED = new IndexSnapshotDetails(0, ByteSizeValue.ZERO, 0);

        public static final ConstructingObjectParser<IndexSnapshotDetails, Void> PARSER = new ConstructingObjectParser<>(
            IndexSnapshotDetails.class.getName(),
            true,
            a -> new IndexSnapshotDetails((int) a[0], ByteSizeValue.ofBytes((long) a[1]), (int) a[2])
        );

        static {
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(SHARD_COUNT));
            PARSER.declareLong(ConstructingObjectParser.constructorArg(), new ParseField(SIZE));
            PARSER.declareInt(ConstructingObjectParser.constructorArg(), new ParseField(MAX_SEGMENTS_PER_SHARD));
        }

        private final int shardCount;
        private final ByteSizeValue size;
        private final int maxSegmentsPerShard;

        public IndexSnapshotDetails(int shardCount, ByteSizeValue size, int maxSegmentsPerShard) {
            this.shardCount = shardCount;
            this.size = Objects.requireNonNull(size);
            this.maxSegmentsPerShard = maxSegmentsPerShard;
        }

        public IndexSnapshotDetails(StreamInput in) throws IOException {
            shardCount = in.readVInt();
            size = ByteSizeValue.readFrom(in);
            maxSegmentsPerShard = in.readVInt();
        }

        public int getShardCount() {
            return shardCount;
        }

        public ByteSizeValue getSize() {
            return size;
        }

        public int getMaxSegmentsPerShard() {
            return maxSegmentsPerShard;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            IndexSnapshotDetails that = (IndexSnapshotDetails) o;
            return shardCount == that.shardCount && maxSegmentsPerShard == that.maxSegmentsPerShard && size.equals(that.size);
        }

        @Override
        public int hashCode() {
            return Objects.hash(shardCount, size, maxSegmentsPerShard);
        }

        @Override
        public String toString() {
            return "IndexSnapshotDetails{"
                + "shardCount="
                + shardCount
                + ", size="
                + size
                + ", maxSegmentsPerShard="
                + maxSegmentsPerShard
                + '}';
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(shardCount);
            size.writeTo(out);
            out.writeVInt(maxSegmentsPerShard);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(SHARD_COUNT, shardCount);
            builder.humanReadableField(SIZE, "size", size);
            builder.field(MAX_SEGMENTS_PER_SHARD, maxSegmentsPerShard);
            builder.endObject();
            return builder;
        }
    }

}
