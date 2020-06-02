/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ToXContentFragment;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.ImmutableFollowParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Custom metadata that contains auto follow patterns and what leader indices an auto follow pattern has already followed.
 */
public class AutoFollowMetadata extends AbstractNamedDiffable<Metadata.Custom> implements Metadata.Custom {

    public static final String TYPE = "ccr_auto_follow";

    private static final ParseField PATTERNS_FIELD = new ParseField("patterns");
    private static final ParseField FOLLOWED_LEADER_INDICES_FIELD = new ParseField("followed_leader_indices");
    private static final ParseField HEADERS = new ParseField("headers");

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<AutoFollowMetadata, Void> PARSER = new ConstructingObjectParser<>("auto_follow",
        args -> new AutoFollowMetadata(
            (Map<String, AutoFollowPattern>) args[0],
            (Map<String, List<String>>) args[1],
            (Map<String, Map<String, String>>) args[2]
        ));

    static {
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> {
            Map<String, AutoFollowPattern> patterns = new HashMap<>();
            String fieldName = null;
            for (XContentParser.Token token = p.nextToken(); token != XContentParser.Token.END_OBJECT; token = p.nextToken()) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = p.currentName();
                } else if (token == XContentParser.Token.START_OBJECT) {
                    patterns.put(fieldName, AutoFollowPattern.PARSER.parse(p, c));
                } else {
                    throw new ElasticsearchParseException("unexpected token [" + token + "]");
                }
            }
            return patterns;
        }, PATTERNS_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), FOLLOWED_LEADER_INDICES_FIELD);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), (p, c) -> p.map(), HEADERS);
    }

    public static AutoFollowMetadata fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    private final Map<String, AutoFollowPattern> patterns;
    private final Map<String, List<String>> followedLeaderIndexUUIDs;
    private final Map<String, Map<String, String>> headers;

    public AutoFollowMetadata(Map<String, AutoFollowPattern> patterns,
                              Map<String, List<String>> followedLeaderIndexUUIDs,
                              Map<String, Map<String, String>> headers) {
        this.patterns = Collections.unmodifiableMap(patterns);
        this.followedLeaderIndexUUIDs = Collections.unmodifiableMap(followedLeaderIndexUUIDs.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableList(e.getValue()))));
        this.headers = Collections.unmodifiableMap(headers.entrySet().stream()
            .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableMap(e.getValue()))));
    }

    public AutoFollowMetadata(StreamInput in) throws IOException {
        this(
            in.readMap(StreamInput::readString, AutoFollowPattern::readFrom),
            in.readMapOfLists(StreamInput::readString, StreamInput::readString),
            in.readMap(StreamInput::readString, valIn -> valIn.readMap(StreamInput::readString, StreamInput::readString))
        );
    }

    public Map<String, AutoFollowPattern> getPatterns() {
        return patterns;
    }

    public Map<String, List<String>> getFollowedLeaderIndexUUIDs() {
        return followedLeaderIndexUUIDs;
    }

    public Map<String, Map<String, String>> getHeaders() {
        return headers;
    }

    @Override
    public EnumSet<Metadata.XContentContext> context() {
        // No XContentContext.API, because the headers should not be serialized as part of clusters state api
        return EnumSet.of(Metadata.XContentContext.SNAPSHOT, Metadata.XContentContext.GATEWAY);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.CURRENT.minimumCompatibilityVersion();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(patterns, StreamOutput::writeString, (out1, value) -> value.writeTo(out1));
        out.writeMapOfLists(followedLeaderIndexUUIDs, StreamOutput::writeString, StreamOutput::writeString);
        out.writeMap(headers, StreamOutput::writeString,
            (valOut, header) -> valOut.writeMap(header, StreamOutput::writeString, StreamOutput::writeString));
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(PATTERNS_FIELD.getPreferredName());
        for (Map.Entry<String, AutoFollowPattern> entry : patterns.entrySet()) {
            builder.startObject(entry.getKey());
            builder.value(entry.getValue());
            builder.endObject();
        }
        builder.endObject();

        builder.startObject(FOLLOWED_LEADER_INDICES_FIELD.getPreferredName());
        for (Map.Entry<String, List<String>> entry : followedLeaderIndexUUIDs.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        builder.startObject(HEADERS.getPreferredName());
        for (Map.Entry<String, Map<String, String>> entry : headers.entrySet()) {
            builder.field(entry.getKey(), entry.getValue());
        }
        builder.endObject();
        return builder;
    }

    @Override
    public boolean isFragment() {
        return true;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoFollowMetadata that = (AutoFollowMetadata) o;
        return Objects.equals(patterns, that.patterns) &&
               Objects.equals(followedLeaderIndexUUIDs, that.followedLeaderIndexUUIDs) &&
               Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patterns, followedLeaderIndexUUIDs, headers);
    }

    public static class AutoFollowPattern extends ImmutableFollowParameters implements ToXContentFragment {

        public static final ParseField ACTIVE = new ParseField("active");
        public static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
        public static final ParseField LEADER_PATTERNS_FIELD = new ParseField("leader_index_patterns");
        public static final ParseField FOLLOW_PATTERN_FIELD = new ParseField("follow_index_pattern");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AutoFollowPattern, Void> PARSER =
            new ConstructingObjectParser<>("auto_follow_pattern",
                args -> new AutoFollowPattern((String) args[0], (List<String>) args[1], (String) args[2],
                    args[3] == null || (boolean) args[3], (Integer) args[4], (Integer) args[5], (Integer) args[6], (Integer) args[7],
                    (ByteSizeValue) args[8], (ByteSizeValue) args[9], (Integer) args[10], (ByteSizeValue) args[11], (TimeValue) args[12],
                    (TimeValue) args[13]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LEADER_PATTERNS_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FOLLOW_PATTERN_FIELD);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ACTIVE);
            ImmutableFollowParameters.initParser(PARSER);
        }

        private final String remoteCluster;
        private final List<String> leaderIndexPatterns;
        private final String followIndexPattern;
        private final boolean active;

        public AutoFollowPattern(String remoteCluster,
                                 List<String> leaderIndexPatterns,
                                 String followIndexPattern,
                                 boolean active,
                                 Integer maxReadRequestOperationCount,
                                 Integer maxWriteRequestOperationCount,
                                 Integer maxOutstandingReadRequests,
                                 Integer maxOutstandingWriteRequests,
                                 ByteSizeValue maxReadRequestSize,
                                 ByteSizeValue maxWriteRequestSize,
                                 Integer maxWriteBufferCount,
                                 ByteSizeValue maxWriteBufferSize,
                                 TimeValue maxRetryDelay,
                                 TimeValue pollTimeout) {
            super(maxReadRequestOperationCount, maxWriteRequestOperationCount, maxOutstandingReadRequests, maxOutstandingWriteRequests,
                maxReadRequestSize, maxWriteRequestSize, maxWriteBufferCount, maxWriteBufferSize, maxRetryDelay, pollTimeout);
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.followIndexPattern = followIndexPattern;
            this.active = active;
        }

        public static AutoFollowPattern readFrom(StreamInput in) throws IOException {
            return new AutoFollowPattern(in.readString(), in.readStringList(), in.readOptionalString(), in);
        }

        private AutoFollowPattern(String remoteCluster, List<String> leaderIndexPatterns,
                                  String followIndexPattern, StreamInput in) throws IOException {
            super(in);
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.followIndexPattern = followIndexPattern;
            if (in.getVersion().onOrAfter(Version.V_7_5_0)) {
                this.active = in.readBoolean();
            } else {
                this.active = true;
            }
        }

        public boolean match(String indexName) {
            return match(leaderIndexPatterns, indexName);
        }

        public static boolean match(List<String> leaderIndexPatterns, String indexName) {
            return Regex.simpleMatch(leaderIndexPatterns, indexName);
        }

        public String getRemoteCluster() {
            return remoteCluster;
        }

        public List<String> getLeaderIndexPatterns() {
            return leaderIndexPatterns;
        }

        public String getFollowIndexPattern() {
            return followIndexPattern;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(remoteCluster);
            out.writeStringCollection(leaderIndexPatterns);
            out.writeOptionalString(followIndexPattern);
            super.writeTo(out);
            if (out.getVersion().onOrAfter(Version.V_7_5_0)) {
                out.writeBoolean(active);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(ACTIVE.getPreferredName(), active);
            builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
            builder.array(LEADER_PATTERNS_FIELD.getPreferredName(), leaderIndexPatterns.toArray(new String[0]));
            if (followIndexPattern != null) {
                builder.field(FOLLOW_PATTERN_FIELD.getPreferredName(), followIndexPattern);
            }
            toXContentFragment(builder);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            AutoFollowPattern pattern = (AutoFollowPattern) o;
            return active == pattern.active &&
                remoteCluster.equals(pattern.remoteCluster) &&
                leaderIndexPatterns.equals(pattern.leaderIndexPatterns) &&
                followIndexPattern.equals(pattern.followIndexPattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), remoteCluster, leaderIndexPatterns, followIndexPattern, active);
        }
    }

}
