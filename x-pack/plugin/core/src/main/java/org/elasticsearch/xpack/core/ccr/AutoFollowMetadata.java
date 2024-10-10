/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.TransportVersion;
import org.elasticsearch.TransportVersions;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexAbstraction;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.xcontent.ChunkedToXContent;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.xcontent.ConstructingObjectParser;
import org.elasticsearch.xcontent.ParseField;
import org.elasticsearch.xcontent.ToXContent;
import org.elasticsearch.xcontent.ToXContentFragment;
import org.elasticsearch.xcontent.XContentBuilder;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.ImmutableFollowParameters;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Iterator;
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
    private static final ConstructingObjectParser<AutoFollowMetadata, Void> PARSER = new ConstructingObjectParser<>(
        "auto_follow",
        args -> new AutoFollowMetadata(
            (Map<String, AutoFollowPattern>) args[0],
            (Map<String, List<String>>) args[1],
            (Map<String, Map<String, String>>) args[2]
        )
    );

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

    public static final AutoFollowMetadata EMPTY = new AutoFollowMetadata(Map.of(), Map.of(), Map.of());

    private final Map<String, AutoFollowPattern> patterns;
    private final Map<String, List<String>> followedLeaderIndexUUIDs;
    private final Map<String, Map<String, String>> headers;

    public AutoFollowMetadata(
        Map<String, AutoFollowPattern> patterns,
        Map<String, List<String>> followedLeaderIndexUUIDs,
        Map<String, Map<String, String>> headers
    ) {
        this.patterns = Collections.unmodifiableMap(patterns);
        this.followedLeaderIndexUUIDs = Collections.unmodifiableMap(
            followedLeaderIndexUUIDs.entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableList(e.getValue())))
        );
        this.headers = Collections.unmodifiableMap(
            headers.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> Collections.unmodifiableMap(e.getValue())))
        );
    }

    public AutoFollowMetadata(StreamInput in) throws IOException {
        this(
            in.readMap(AutoFollowPattern::readFrom),
            in.readMapOfLists(StreamInput::readString),
            in.readMap(valIn -> valIn.readMap(StreamInput::readString))
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
    public TransportVersion getMinimalSupportedVersion() {
        return TransportVersions.MINIMUM_COMPATIBLE;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(patterns, StreamOutput::writeWriteable);
        out.writeMap(followedLeaderIndexUUIDs, StreamOutput::writeStringCollection);
        out.writeMap(headers, (valOut, header) -> valOut.writeMap(header, StreamOutput::writeString));
    }

    @Override
    public Iterator<? extends ToXContent> toXContentChunked(ToXContent.Params params) {
        return ChunkedToXContent.builder(params)
            .xContentObjectFieldObjects(PATTERNS_FIELD.getPreferredName(), patterns)
            .object(FOLLOWED_LEADER_INDICES_FIELD.getPreferredName(), followedLeaderIndexUUIDs)
            .object(HEADERS.getPreferredName(), headers);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AutoFollowMetadata that = (AutoFollowMetadata) o;
        return Objects.equals(patterns, that.patterns)
            && Objects.equals(followedLeaderIndexUUIDs, that.followedLeaderIndexUUIDs)
            && Objects.equals(headers, that.headers);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patterns, followedLeaderIndexUUIDs, headers);
    }

    public static class AutoFollowPattern extends ImmutableFollowParameters implements ToXContentFragment {

        public static final ParseField ACTIVE = new ParseField("active");
        public static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
        public static final ParseField LEADER_PATTERNS_FIELD = new ParseField("leader_index_patterns");
        public static final ParseField LEADER_EXCLUSION_PATTERNS_FIELD = new ParseField("leader_index_exclusion_patterns");
        public static final ParseField FOLLOW_PATTERN_FIELD = new ParseField("follow_index_pattern");
        public static final ParseField SETTINGS_FIELD = new ParseField("settings");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AutoFollowPattern, Void> PARSER = new ConstructingObjectParser<>(
            "auto_follow_pattern",
            args -> new AutoFollowPattern(
                (String) args[0],
                (List<String>) args[1],
                args[2] == null ? Collections.emptyList() : (List<String>) args[2],
                (String) args[3],
                args[4] == null ? Settings.EMPTY : (Settings) args[4],
                args[5] == null || (boolean) args[5],
                (Integer) args[6],
                (Integer) args[7],
                (Integer) args[8],
                (Integer) args[9],
                (ByteSizeValue) args[10],
                (ByteSizeValue) args[11],
                (Integer) args[12],
                (ByteSizeValue) args[13],
                (TimeValue) args[14],
                (TimeValue) args[15]
            )
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LEADER_PATTERNS_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(), LEADER_EXCLUSION_PATTERNS_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FOLLOW_PATTERN_FIELD);
            PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), (p, c) -> Settings.fromXContent(p), SETTINGS_FIELD);
            PARSER.declareBoolean(ConstructingObjectParser.optionalConstructorArg(), ACTIVE);
            ImmutableFollowParameters.initParser(PARSER);
        }

        private final String remoteCluster;
        private final List<String> leaderIndexPatterns;
        private final List<String> leaderIndexExclusionPatterns;
        private final String followIndexPattern;
        private final Settings settings;
        private final boolean active;

        public AutoFollowPattern(
            String remoteCluster,
            List<String> leaderIndexPatterns,
            List<String> leaderIndexExclusionPatterns,
            String followIndexPattern,
            Settings settings,
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
            TimeValue pollTimeout
        ) {
            super(
                maxReadRequestOperationCount,
                maxWriteRequestOperationCount,
                maxOutstandingReadRequests,
                maxOutstandingWriteRequests,
                maxReadRequestSize,
                maxWriteRequestSize,
                maxWriteBufferCount,
                maxWriteBufferSize,
                maxRetryDelay,
                pollTimeout
            );
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.leaderIndexExclusionPatterns = Objects.requireNonNull(leaderIndexExclusionPatterns);
            this.followIndexPattern = followIndexPattern;
            this.settings = Objects.requireNonNull(settings);
            this.active = active;
        }

        public static AutoFollowPattern readFrom(StreamInput in) throws IOException {
            final String remoteCluster = in.readString();
            final List<String> leaderIndexPatterns = in.readStringCollectionAsList();
            final String followIndexPattern = in.readOptionalString();
            final Settings settings;
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_9_0)) {
                settings = Settings.readSettingsFromStream(in);
            } else {
                settings = Settings.EMPTY;
            }
            return new AutoFollowPattern(remoteCluster, leaderIndexPatterns, followIndexPattern, settings, in);
        }

        private AutoFollowPattern(
            String remoteCluster,
            List<String> leaderIndexPatterns,
            String followIndexPattern,
            Settings settings,
            StreamInput in
        ) throws IOException {
            super(in);
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.followIndexPattern = followIndexPattern;
            this.settings = Objects.requireNonNull(settings);
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_5_0)) {
                this.active = in.readBoolean();
            } else {
                this.active = true;
            }
            if (in.getTransportVersion().onOrAfter(TransportVersions.V_7_14_0)) {
                this.leaderIndexExclusionPatterns = in.readStringCollectionAsList();
            } else {
                this.leaderIndexExclusionPatterns = Collections.emptyList();
            }
        }

        public boolean match(IndexAbstraction indexAbstraction) {
            return match(leaderIndexPatterns, leaderIndexExclusionPatterns, indexAbstraction);
        }

        public static boolean match(
            List<String> leaderIndexPatterns,
            List<String> leaderIndexExclusionPatterns,
            IndexAbstraction indexAbstraction
        ) {
            boolean matches = indexAbstraction.isSystem() == false
                && Regex.simpleMatch(leaderIndexExclusionPatterns, indexAbstraction.getName()) == false
                && Regex.simpleMatch(leaderIndexPatterns, indexAbstraction.getName());

            if (matches) {
                return true;
            } else {
                final DataStream parentDataStream = indexAbstraction.getParentDataStream();
                return parentDataStream != null
                    && parentDataStream.isSystem() == false
                    && Regex.simpleMatch(leaderIndexExclusionPatterns, indexAbstraction.getParentDataStream().getName()) == false
                    && Regex.simpleMatch(leaderIndexPatterns, indexAbstraction.getParentDataStream().getName());
            }
        }

        public String getRemoteCluster() {
            return remoteCluster;
        }

        public List<String> getLeaderIndexPatterns() {
            return leaderIndexPatterns;
        }

        public List<String> getLeaderIndexExclusionPatterns() {
            return leaderIndexExclusionPatterns;
        }

        public String getFollowIndexPattern() {
            return followIndexPattern;
        }

        public Settings getSettings() {
            return settings;
        }

        public boolean isActive() {
            return active;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(remoteCluster);
            out.writeStringCollection(leaderIndexPatterns);
            out.writeOptionalString(followIndexPattern);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_9_0)) {
                settings.writeTo(out);
            }
            super.writeTo(out);
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_5_0)) {
                out.writeBoolean(active);
            }
            if (out.getTransportVersion().onOrAfter(TransportVersions.V_7_14_0)) {
                out.writeStringCollection(leaderIndexExclusionPatterns);
            }
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(ACTIVE.getPreferredName(), active);
            builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
            builder.array(LEADER_PATTERNS_FIELD.getPreferredName(), leaderIndexPatterns.toArray(new String[0]));
            builder.array(LEADER_EXCLUSION_PATTERNS_FIELD.getPreferredName(), leaderIndexExclusionPatterns.toArray(new String[0]));
            if (followIndexPattern != null) {
                builder.field(FOLLOW_PATTERN_FIELD.getPreferredName(), followIndexPattern);
            }
            if (settings.isEmpty() == false) {
                builder.startObject(SETTINGS_FIELD.getPreferredName());
                {
                    settings.toXContent(builder, params);
                }
                builder.endObject();
            }
            toXContentFragment(builder);
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            AutoFollowPattern pattern = (AutoFollowPattern) o;
            return active == pattern.active
                && remoteCluster.equals(pattern.remoteCluster)
                && leaderIndexPatterns.equals(pattern.leaderIndexPatterns)
                && leaderIndexExclusionPatterns.equals(pattern.leaderIndexExclusionPatterns)
                && followIndexPattern.equals(pattern.followIndexPattern)
                && settings.equals(pattern.settings);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                super.hashCode(),
                remoteCluster,
                leaderIndexPatterns,
                leaderIndexExclusionPatterns,
                followIndexPattern,
                settings,
                active
            );
        }
    }

}
