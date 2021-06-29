/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.xcontent.ParseField;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;
import java.util.stream.Collectors;

public final class GetAutoFollowPatternResponse {

    static final ParseField PATTERNS_FIELD = new ParseField("patterns");
    static final ParseField NAME_FIELD = new ParseField("name");
    static final ParseField PATTERN_FIELD = new ParseField("pattern");

    private static final ConstructingObjectParser<Map.Entry<String, Pattern>, Void> ENTRY_PARSER = new ConstructingObjectParser<>(
        "get_auto_follow_pattern_response", true, args -> new AbstractMap.SimpleEntry<>((String) args[0], (Pattern) args[1]));

    static {
        ENTRY_PARSER.declareString(ConstructingObjectParser.constructorArg(), NAME_FIELD);
        ENTRY_PARSER.declareObject(ConstructingObjectParser.constructorArg(), Pattern.PARSER, PATTERN_FIELD);
    }

    private static final ConstructingObjectParser<GetAutoFollowPatternResponse, Void> PARSER = new ConstructingObjectParser<>(
        "get_auto_follow_pattern_response", true, args -> {
            @SuppressWarnings("unchecked")
            List<Map.Entry<String, Pattern>> entries = (List<Map.Entry<String, Pattern>>) args[0];
            return new GetAutoFollowPatternResponse(new TreeMap<>(entries.stream()
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))));
    });

    static {
        PARSER.declareObjectArray(ConstructingObjectParser.constructorArg(), ENTRY_PARSER, PATTERNS_FIELD);
    }

    public static GetAutoFollowPatternResponse fromXContent(final XContentParser parser) {
        return PARSER.apply(parser, null);
    }

    private final NavigableMap<String, Pattern> patterns;

    GetAutoFollowPatternResponse(NavigableMap<String, Pattern> patterns) {
        this.patterns = Collections.unmodifiableNavigableMap(patterns);
    }

    public NavigableMap<String, Pattern> getPatterns() {
        return patterns;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GetAutoFollowPatternResponse that = (GetAutoFollowPatternResponse) o;
        return Objects.equals(patterns, that.patterns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patterns);
    }

    public static class Pattern extends FollowConfig {

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<Pattern, Void> PARSER = new ConstructingObjectParser<>(
            "pattern",
            true,
            args -> new Pattern((String) args[0],
                                (List<String>) args[1],
                                args[2] == null ? Collections.emptyList() : (List<String>) args[2],
                                (String) args[3])
        );

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PutAutoFollowPatternRequest.LEADER_PATTERNS_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.optionalConstructorArg(),
                                      PutAutoFollowPatternRequest.LEADER_EXCLUSION_PATTERNS_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PutAutoFollowPatternRequest.FOLLOW_PATTERN_FIELD);
            PARSER.declareObject(Pattern::setSettings, (p, c) -> Settings.fromXContent(p), PutAutoFollowPatternRequest.SETTINGS);
            PARSER.declareInt(Pattern::setMaxReadRequestOperationCount, FollowConfig.MAX_READ_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                Pattern::setMaxReadRequestSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_READ_REQUEST_SIZE.getPreferredName()),
                PutFollowRequest.MAX_READ_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt(Pattern::setMaxOutstandingReadRequests, FollowConfig.MAX_OUTSTANDING_READ_REQUESTS);
            PARSER.declareInt(Pattern::setMaxWriteRequestOperationCount, FollowConfig.MAX_WRITE_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                Pattern::setMaxWriteRequestSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
                PutFollowRequest.MAX_WRITE_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt(Pattern::setMaxOutstandingWriteRequests, FollowConfig.MAX_OUTSTANDING_WRITE_REQUESTS);
            PARSER.declareInt(Pattern::setMaxWriteBufferCount, FollowConfig.MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                Pattern::setMaxWriteBufferSize,
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                PutFollowRequest.MAX_WRITE_BUFFER_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(
                Pattern::setMaxRetryDelay,
                (p, c) -> TimeValue.parseTimeValue(p.text(), FollowConfig.MAX_RETRY_DELAY_FIELD.getPreferredName()),
                PutFollowRequest.MAX_RETRY_DELAY_FIELD,
                ObjectParser.ValueType.STRING);
            PARSER.declareField(
                Pattern::setReadPollTimeout,
                (p, c) -> TimeValue.parseTimeValue(p.text(), FollowConfig.READ_POLL_TIMEOUT.getPreferredName()),
                PutFollowRequest.READ_POLL_TIMEOUT,
                ObjectParser.ValueType.STRING);
        }

        private final String remoteCluster;
        private final List<String> leaderIndexPatterns;
        private final List<String> leaderIndexExclusionPatterns;
        private final String followIndexNamePattern;

        Pattern(String remoteCluster,
                List<String> leaderIndexPatterns,
                List<String> leaderIndexExclusionPatterns,
                String followIndexNamePattern) {
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.leaderIndexExclusionPatterns = leaderIndexExclusionPatterns;
            this.followIndexNamePattern = followIndexNamePattern;
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

        public String getFollowIndexNamePattern() {
            return followIndexNamePattern;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (super.equals(o) == false) return false;
            Pattern pattern = (Pattern) o;
            return Objects.equals(remoteCluster, pattern.remoteCluster) &&
                Objects.equals(leaderIndexPatterns, pattern.leaderIndexPatterns) &&
                Objects.equals(leaderIndexExclusionPatterns, pattern.leaderIndexExclusionPatterns) &&
                Objects.equals(followIndexNamePattern, pattern.followIndexNamePattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                super.hashCode(),
                remoteCluster,
                leaderIndexPatterns,
                leaderIndexExclusionPatterns,
                followIndexNamePattern
            );
        }
    }

}
