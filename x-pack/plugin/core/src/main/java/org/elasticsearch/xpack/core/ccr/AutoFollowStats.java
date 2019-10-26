/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.collect.Tuple;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public class AutoFollowStats implements Writeable, ToXContentObject {

    private static final ParseField NUMBER_OF_SUCCESSFUL_INDICES_AUTO_FOLLOWED = new ParseField("number_of_successful_follow_indices");
    private static final ParseField NUMBER_OF_FAILED_INDICES_AUTO_FOLLOWED = new ParseField("number_of_failed_follow_indices");
    private static final ParseField NUMBER_OF_FAILED_REMOTE_CLUSTER_STATE_REQUESTS =
        new ParseField("number_of_failed_remote_cluster_state_requests");
    private static final ParseField RECENT_AUTO_FOLLOW_ERRORS = new ParseField("recent_auto_follow_errors");
    private static final ParseField LEADER_INDEX = new ParseField("leader_index");
    private static final ParseField AUTO_FOLLOW_EXCEPTION = new ParseField("auto_follow_exception");
    private static final ParseField TIMESTAMP = new ParseField("timestamp");
    private static final ParseField AUTO_FOLLOWED_CLUSTERS = new ParseField("auto_followed_clusters");
    private static final ParseField CLUSTER_NAME = new ParseField("cluster_name");
    private static final ParseField TIME_SINCE_LAST_CHECK_MILLIS = new ParseField("time_since_last_check_millis");
    private static final ParseField LAST_SEEN_METADATA_VERSION = new ParseField("last_seen_metadata_version");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AutoFollowStats, Void> STATS_PARSER = new ConstructingObjectParser<>("auto_follow_stats",
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
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue)))));

    private static final ConstructingObjectParser<Map.Entry<String,
        Tuple<Long, ElasticsearchException>>, Void> AUTO_FOLLOW_EXCEPTIONS_PARSER =
        new ConstructingObjectParser<>(
            "auto_follow_stats_errors",
            args -> new AbstractMap.SimpleEntry<>((String) args[0], new Tuple<>((Long) args[1], (ElasticsearchException) args[2])));

    private static final ConstructingObjectParser<Map.Entry<String, AutoFollowedCluster>, Void> AUTO_FOLLOWED_CLUSTERS_PARSER =
        new ConstructingObjectParser<>(
            "auto_followed_clusters",
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

    public static AutoFollowStats fromXContent(final XContentParser parser) {
        return STATS_PARSER.apply(parser, null);
    }

    private final long numberOfFailedFollowIndices;
    private final long numberOfFailedRemoteClusterStateRequests;
    private final long numberOfSuccessfulFollowIndices;
    private final NavigableMap<String, Tuple<Long, ElasticsearchException>> recentAutoFollowErrors;
    private final NavigableMap<String, AutoFollowedCluster> autoFollowedClusters;

    public AutoFollowStats(
        long numberOfFailedFollowIndices,
        long numberOfFailedRemoteClusterStateRequests,
        long numberOfSuccessfulFollowIndices,
        NavigableMap<String, Tuple<Long, ElasticsearchException>> recentAutoFollowErrors,
        NavigableMap<String, AutoFollowedCluster> autoFollowedClusters
    ) {
        this.numberOfFailedFollowIndices = numberOfFailedFollowIndices;
        this.numberOfFailedRemoteClusterStateRequests = numberOfFailedRemoteClusterStateRequests;
        this.numberOfSuccessfulFollowIndices = numberOfSuccessfulFollowIndices;
        this.recentAutoFollowErrors = recentAutoFollowErrors;
        this.autoFollowedClusters = autoFollowedClusters;
    }

    public AutoFollowStats(StreamInput in) throws IOException {
        numberOfFailedFollowIndices = in.readVLong();
        numberOfFailedRemoteClusterStateRequests = in.readVLong();
        numberOfSuccessfulFollowIndices = in.readVLong();
        // note: the casts to the following Writeable.Reader<T> instances are needed by some IDEs (e.g. Eclipse 4.8) as a compiler help
        recentAutoFollowErrors = new TreeMap<>(in.readMap(StreamInput::readString,
            in1 -> new Tuple<>(in1.readZLong(), in1.readException())));
        autoFollowedClusters = new TreeMap<>(in.readMap(StreamInput::readString, AutoFollowedCluster::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(numberOfFailedFollowIndices);
        out.writeVLong(numberOfFailedRemoteClusterStateRequests);
        out.writeVLong(numberOfSuccessfulFollowIndices);
        out.writeMap(recentAutoFollowErrors, StreamOutput::writeString, (out1, value) -> {
            out1.writeZLong(value.v1());
            out1.writeException(value.v2());
        });
        out.writeMap(autoFollowedClusters, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
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

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        {
            toXContentFragment(builder, params);
        }
        builder.endObject();
        return builder;
    }

    public XContentBuilder toXContentFragment(final XContentBuilder builder, final Params params) throws IOException {
        builder.field(NUMBER_OF_FAILED_INDICES_AUTO_FOLLOWED.getPreferredName(), numberOfFailedFollowIndices);
        builder.field(NUMBER_OF_FAILED_REMOTE_CLUSTER_STATE_REQUESTS.getPreferredName(), numberOfFailedRemoteClusterStateRequests);
        builder.field(NUMBER_OF_SUCCESSFUL_INDICES_AUTO_FOLLOWED.getPreferredName(), numberOfSuccessfulFollowIndices);
        builder.startArray(RECENT_AUTO_FOLLOW_ERRORS.getPreferredName());
        {
            for (final Map.Entry<String, Tuple<Long, ElasticsearchException>> entry : recentAutoFollowErrors.entrySet()) {
                builder.startObject();
                {
                    builder.field(LEADER_INDEX.getPreferredName(), entry.getKey());
                    builder.field(TIMESTAMP.getPreferredName(), entry.getValue().v1());
                    builder.field(AUTO_FOLLOW_EXCEPTION.getPreferredName());
                    builder.startObject();
                    {
                        ElasticsearchException.generateThrowableXContent(builder, params, entry.getValue().v2());
                    }
                    builder.endObject();
                }
                builder.endObject();
            }
        }
        builder.endArray();
        builder.startArray(AUTO_FOLLOWED_CLUSTERS.getPreferredName());
        {
            for (final Map.Entry<String, AutoFollowedCluster> entry : autoFollowedClusters.entrySet()) {
                builder.startObject();
                {
                    builder.field(CLUSTER_NAME.getPreferredName(), entry.getKey());
                    builder.field(TIME_SINCE_LAST_CHECK_MILLIS.getPreferredName(), entry.getValue().getTimeSinceLastCheckMillis());
                    builder.field(LAST_SEEN_METADATA_VERSION.getPreferredName(), entry.getValue().getLastSeenMetadataVersion());
                }
                builder.endObject();
            }
        }
        builder.endArray();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoFollowStats that = (AutoFollowStats) o;
        return numberOfFailedFollowIndices == that.numberOfFailedFollowIndices &&
            numberOfFailedRemoteClusterStateRequests == that.numberOfFailedRemoteClusterStateRequests &&
            numberOfSuccessfulFollowIndices == that.numberOfSuccessfulFollowIndices &&
            /*
             * ElasticsearchException does not implement equals so we will assume the fetch exceptions are equal if they are equal
             * up to the key set and their messages.  Note that we are relying on the fact that the auto follow exceptions are ordered by
             * keys.
             */
            recentAutoFollowErrors.keySet().equals(that.recentAutoFollowErrors.keySet()) &&
            getFetchExceptionMessages(this).equals(getFetchExceptionMessages(that)) &&
            Objects.equals(autoFollowedClusters, that.autoFollowedClusters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            numberOfFailedFollowIndices,
            numberOfFailedRemoteClusterStateRequests,
            numberOfSuccessfulFollowIndices,
            /*
             * ElasticsearchException does not implement hash code so we will compute the hash code based on the key set and the
             * messages. Note that we are relying on the fact that the auto follow exceptions are ordered by keys.
             */
            recentAutoFollowErrors.keySet(),
            getFetchExceptionMessages(this),
            autoFollowedClusters
        );
    }

    private static List<String> getFetchExceptionMessages(final AutoFollowStats status) {
        return status.getRecentAutoFollowErrors().values()
            .stream()
            .map(Tuple::v2)
            .map(ElasticsearchException::getMessage)
            .collect(Collectors.toList());
    }

    @Override
    public String toString() {
        return "AutoFollowStats{" +
            "numberOfFailedFollowIndices=" + numberOfFailedFollowIndices +
            ", numberOfFailedRemoteClusterStateRequests=" + numberOfFailedRemoteClusterStateRequests +
            ", numberOfSuccessfulFollowIndices=" + numberOfSuccessfulFollowIndices +
            ", recentAutoFollowErrors=" + recentAutoFollowErrors +
            ", autoFollowedClusters=" + autoFollowedClusters +
            '}';
    }

    public static class AutoFollowedCluster implements Writeable {

        private final long timeSinceLastCheckMillis;
        private final long lastSeenMetadataVersion;

        public AutoFollowedCluster(long timeSinceLastCheckMillis, long lastSeenMetadataVersion) {
            this.timeSinceLastCheckMillis = timeSinceLastCheckMillis;
            this.lastSeenMetadataVersion = lastSeenMetadataVersion;
        }

        public AutoFollowedCluster(StreamInput in) throws IOException {
            this(in.readZLong(), in.readVLong());
        }

        public long getTimeSinceLastCheckMillis() {
            return timeSinceLastCheckMillis;
        }

        public long getLastSeenMetadataVersion() {
            return lastSeenMetadataVersion;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(timeSinceLastCheckMillis);
            out.writeVLong(lastSeenMetadataVersion);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AutoFollowedCluster that = (AutoFollowedCluster) o;
            return timeSinceLastCheckMillis == that.timeSinceLastCheckMillis &&
                lastSeenMetadataVersion == that.lastSeenMetadataVersion;
        }

        @Override
        public int hashCode() {
            return Objects.hash(timeSinceLastCheckMillis, lastSeenMetadataVersion);
        }

        @Override
        public String toString() {
            return "AutoFollowedCluster{" +
                "timeSinceLastCheckMillis=" + timeSinceLastCheckMillis +
                ", lastSeenMetadataVersion=" + lastSeenMetadataVersion +
                '}';
        }
    }
}
