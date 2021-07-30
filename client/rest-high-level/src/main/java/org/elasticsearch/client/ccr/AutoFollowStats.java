/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;

import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class AutoFollowStats {

    static final ParseField NUMBER_OF_SUCCESSFUL_INDICES_AUTO_FOLLOWED = new ParseField("number_of_successful_follow_indices");
    static final ParseField NUMBER_OF_FAILED_INDICES_AUTO_FOLLOWED = new ParseField("number_of_failed_follow_indices");
    static final ParseField NUMBER_OF_FAILED_REMOTE_CLUSTER_STATE_REQUESTS =
        new ParseField("number_of_failed_remote_cluster_state_requests");
    static final ParseField RECENT_AUTO_FOLLOW_ERRORS = new ParseField("recent_auto_follow_errors");
    static final ParseField LEADER_INDEX = new ParseField("leader_index");
    static final ParseField TIMESTAMP = new ParseField("timestamp");
    static final ParseField AUTO_FOLLOW_EXCEPTION = new ParseField("auto_follow_exception");
    static final ParseField AUTO_FOLLOWED_CLUSTERS = new ParseField("auto_followed_clusters");
    static final ParseField CLUSTER_NAME = new ParseField("cluster_name");
    static final ParseField TIME_SINCE_LAST_CHECK_MILLIS = new ParseField("time_since_last_check_millis");
    static final ParseField LAST_SEEN_METADATA_VERSION = new ParseField("last_seen_metadata_version");

    @SuppressWarnings("unchecked")
    static final ConstructingObjectParser<AutoFollowStats, Void> STATS_PARSER = new ConstructingObjectParser<>(
        "auto_follow_stats",
        true,
        args -> new AutoFollowStats(
            (Long) args[0],
            (Long) args[1],
            (Long) args[2],
            new TreeMap<>(
                ((List<Map.Entry<String, Tuple<Long, ElasticsearchException>>>) args[3])
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))),
            new TreeMap<>(
                ((List<Map.Entry<String, AutoFollowedCluster>>) args[4])
                    .stream()
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))
        ));

    static final ConstructingObjectParser<Map.Entry<String, Tuple<Long, ElasticsearchException>>, Void> AUTO_FOLLOW_EXCEPTIONS_PARSER =
        new ConstructingObjectParser<>(
            "auto_follow_stats_errors",
            true,
            args -> new AbstractMap.SimpleEntry<>((String) args[0], Tuple.tuple((Long) args[1], (ElasticsearchException) args[2])));

    private static final ConstructingObjectParser<Map.Entry<String, AutoFollowedCluster>, Void> AUTO_FOLLOWED_CLUSTERS_PARSER =
        new ConstructingObjectParser<>(
            "auto_followed_clusters",
            true,
            args -> new AbstractMap.SimpleEntry<>((String) args[0], new AutoFollowedCluster((Long) args[1], (Long) args[2])));

    static {
        AUTO_FOLLOW_EXCEPTIONS_PARSER.declareString(ConstructingObjectParser.constructorArg(), LEADER_INDEX);
        AUTO_FOLLOW_EXCEPTIONS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIMESTAMP);
        AUTO_FOLLOW_EXCEPTIONS_PARSER.declareObject(
            ConstructingObjectParser.constructorArg(),
            (p, c) -> ElasticsearchException.fromXContent(p),
            AUTO_FOLLOW_EXCEPTION);

        AUTO_FOLLOWED_CLUSTERS_PARSER.declareString(ConstructingObjectParser.constructorArg(), CLUSTER_NAME);
        AUTO_FOLLOWED_CLUSTERS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), TIME_SINCE_LAST_CHECK_MILLIS);
        AUTO_FOLLOWED_CLUSTERS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), LAST_SEEN_METADATA_VERSION);

        STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FAILED_INDICES_AUTO_FOLLOWED);
        STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_FAILED_REMOTE_CLUSTER_STATE_REQUESTS);
        STATS_PARSER.declareLong(ConstructingObjectParser.constructorArg(), NUMBER_OF_SUCCESSFUL_INDICES_AUTO_FOLLOWED);
        STATS_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), AUTO_FOLLOW_EXCEPTIONS_PARSER,
            RECENT_AUTO_FOLLOW_ERRORS);
        STATS_PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), AUTO_FOLLOWED_CLUSTERS_PARSER,
            AUTO_FOLLOWED_CLUSTERS);
    }

    private final long numberOfFailedFollowIndices;
    private final long numberOfFailedRemoteClusterStateRequests;
    private final long numberOfSuccessfulFollowIndices;
    private final NavigableMap<String, Tuple<Long, ElasticsearchException>> recentAutoFollowErrors;
    private final NavigableMap<String, AutoFollowedCluster> autoFollowedClusters;

    AutoFollowStats(long numberOfFailedFollowIndices,
                    long numberOfFailedRemoteClusterStateRequests,
                    long numberOfSuccessfulFollowIndices,
                    NavigableMap<String, Tuple<Long, ElasticsearchException>> recentAutoFollowErrors,
                    NavigableMap<String, AutoFollowedCluster> autoFollowedClusters) {
        this.numberOfFailedFollowIndices = numberOfFailedFollowIndices;
        this.numberOfFailedRemoteClusterStateRequests = numberOfFailedRemoteClusterStateRequests;
        this.numberOfSuccessfulFollowIndices = numberOfSuccessfulFollowIndices;
        this.recentAutoFollowErrors = recentAutoFollowErrors;
        this.autoFollowedClusters = autoFollowedClusters;
    }

    public long getNumberOfFailedFollowIndices() {
        return numberOfFailedFollowIndices;
    }

    public long getNumberOfFailedRemoteClusterStateRequests() {
        return numberOfFailedRemoteClusterStateRequests;
    }

    public long getNumberOfSuccessfulFollowIndices() {
        return numberOfSuccessfulFollowIndices;
    }

    public NavigableMap<String, Tuple<Long, ElasticsearchException>> getRecentAutoFollowErrors() {
        return recentAutoFollowErrors;
    }

    public NavigableMap<String, AutoFollowedCluster> getAutoFollowedClusters() {
        return autoFollowedClusters;
    }

    public static class AutoFollowedCluster {

        private final long timeSinceLastCheckMillis;
        private final long lastSeenMetadataVersion;

        public AutoFollowedCluster(long timeSinceLastCheckMillis, long lastSeenMetadataVersion) {
            this.timeSinceLastCheckMillis = timeSinceLastCheckMillis;
            this.lastSeenMetadataVersion = lastSeenMetadataVersion;
        }

        public long getTimeSinceLastCheckMillis() {
            return timeSinceLastCheckMillis;
        }

        public long getLastSeenMetadataVersion() {
            return lastSeenMetadataVersion;
        }
    }

}
