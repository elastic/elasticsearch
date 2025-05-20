/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.snapshots;

import org.elasticsearch.action.admin.cluster.snapshots.create.CreateSnapshotResponse;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.elasticsearch.snapshots.SnapshotInfo.DATA_STREAMS;
import static org.elasticsearch.snapshots.SnapshotInfo.END_TIME_IN_MILLIS;
import static org.elasticsearch.snapshots.SnapshotInfo.FAILURES;
import static org.elasticsearch.snapshots.SnapshotInfo.FEATURE_STATES;
import static org.elasticsearch.snapshots.SnapshotInfo.INCLUDE_GLOBAL_STATE;
import static org.elasticsearch.snapshots.SnapshotInfo.INDEX_DETAILS;
import static org.elasticsearch.snapshots.SnapshotInfo.INDICES;
import static org.elasticsearch.snapshots.SnapshotInfo.REASON;
import static org.elasticsearch.snapshots.SnapshotInfo.REPOSITORY;
import static org.elasticsearch.snapshots.SnapshotInfo.SHARDS;
import static org.elasticsearch.snapshots.SnapshotInfo.START_TIME_IN_MILLIS;
import static org.elasticsearch.snapshots.SnapshotInfo.STATE;
import static org.elasticsearch.snapshots.SnapshotInfo.SUCCESSFUL;
import static org.elasticsearch.snapshots.SnapshotInfo.TOTAL;
import static org.elasticsearch.snapshots.SnapshotInfo.UNKNOWN_REPO_NAME;
import static org.elasticsearch.snapshots.SnapshotInfo.USER_METADATA;
import static org.elasticsearch.snapshots.SnapshotInfo.UUID;
import static org.elasticsearch.snapshots.SnapshotInfo.VERSION_ID;
import static org.elasticsearch.threadpool.ThreadPool.Names.SNAPSHOT;
import static org.elasticsearch.xcontent.ConstructingObjectParser.optionalConstructorArg;

public class SnapshotInfoUtils {

    private SnapshotInfoUtils() {/* no instances */}

    static final ConstructingObjectParser<CreateSnapshotResponse, Void> CREATE_SNAPSHOT_RESPONSE_PARSER = new ConstructingObjectParser<>(
        CreateSnapshotResponse.class.getName(),
        true,
        args -> new CreateSnapshotResponse(((SnapshotInfoBuilder) args[0]).build())
    );

    static final ObjectParser<SnapshotInfoBuilder, Void> SNAPSHOT_INFO_PARSER = new ObjectParser<>(
        SnapshotInfoBuilder.class.getName(),
        true,
        SnapshotInfoBuilder::new
    );

    static final ConstructingObjectParser<ShardStatsBuilder, Void> SHARD_STATS_PARSER = new ConstructingObjectParser<>(
        ShardStatsBuilder.class.getName(),
        true,
        args -> new ShardStatsBuilder((int) Objects.requireNonNullElse(args[0], 0), (int) Objects.requireNonNullElse(args[1], 0))
    );

    static {
        SHARD_STATS_PARSER.declareInt(optionalConstructorArg(), new ParseField(TOTAL));
        SHARD_STATS_PARSER.declareInt(optionalConstructorArg(), new ParseField(SUCCESSFUL));

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
            (p, c) -> p.map(HashMap::new, p2 -> SnapshotInfo.IndexSnapshotDetails.PARSER.parse(p2, c)),
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

        CREATE_SNAPSHOT_RESPONSE_PARSER.declareObject(optionalConstructorArg(), SNAPSHOT_INFO_PARSER, new ParseField("snapshot"));
    }

    private record ShardStatsBuilder(int totalShards, int successfulShards) {}

    public static final class SnapshotInfoBuilder {
        private String snapshotName = null;
        private String snapshotUUID = null;
        private String repository = UNKNOWN_REPO_NAME;
        private String state = null;
        private String reason = null;
        private List<String> indices = null;
        private List<String> dataStreams = null;
        private List<SnapshotFeatureInfo> featureStates = null;
        private Map<String, SnapshotInfo.IndexSnapshotDetails> indexSnapshotDetails = null;
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

        private void setIndexSnapshotDetails(Map<String, SnapshotInfo.IndexSnapshotDetails> indexSnapshotDetails) {
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
            IndexVersion version = this.version == -1 ? IndexVersion.current() : IndexVersion.fromId(this.version);

            int totalShards = shardStatsBuilder == null ? 0 : shardStatsBuilder.totalShards();
            int successfulShards = shardStatsBuilder == null ? 0 : shardStatsBuilder.successfulShards();

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

    public static CreateSnapshotResponse createSnapshotResponseFromXContent(XContentParser parser) {
        return CREATE_SNAPSHOT_RESPONSE_PARSER.apply(parser, null);
    }

    public static SnapshotInfo snapshotInfoFromXContent(XContentParser parser) {
        return SNAPSHOT_INFO_PARSER.apply(parser, null).build();
    }
}
