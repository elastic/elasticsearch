/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ccr;

import org.elasticsearch.ElasticsearchParseException;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.AbstractNamedDiffable;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.regex.Regex;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.core.XPackPlugin;

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
public class AutoFollowMetadata extends AbstractNamedDiffable<MetaData.Custom> implements XPackPlugin.XPackMetaDataCustom {

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
            in.readMap(StreamInput::readString, AutoFollowPattern::new),
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
    public EnumSet<MetaData.XContentContext> context() {
        // No XContentContext.API, because the headers should not be serialized as part of clusters state api
        return EnumSet.of(MetaData.XContentContext.SNAPSHOT, MetaData.XContentContext.GATEWAY);
    }

    @Override
    public String getWriteableName() {
        return TYPE;
    }

    @Override
    public Version getMinimalSupportedVersion() {
        return Version.V_6_5_0.minimumCompatibilityVersion();
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
        return Objects.equals(patterns, that.patterns);
    }

    @Override
    public int hashCode() {
        return Objects.hash(patterns);
    }

    public static class AutoFollowPattern implements Writeable, ToXContentObject {

        public static final ParseField REMOTE_CLUSTER_FIELD = new ParseField("remote_cluster");
        public static final ParseField LEADER_PATTERNS_FIELD = new ParseField("leader_index_patterns");
        public static final ParseField FOLLOW_PATTERN_FIELD = new ParseField("follow_index_pattern");
        public static final ParseField MAX_READ_REQUEST_OPERATION_COUNT = new ParseField("max_read_request_operation_count");
        public static final ParseField MAX_READ_REQUEST_SIZE = new ParseField("max_read_request_size");
        public static final ParseField MAX_OUTSTANDING_READ_REQUESTS = new ParseField("max_outstanding_read_requests");
        public static final ParseField MAX_WRITE_REQUEST_OPERATION_COUNT = new ParseField("max_write_request_operation_count");
        public static final ParseField MAX_WRITE_REQUEST_SIZE = new ParseField("max_write_request_size");
        public static final ParseField MAX_OUTSTANDING_WRITE_REQUESTS = new ParseField("max_outstanding_write_requests");
        public static final ParseField MAX_WRITE_BUFFER_COUNT = new ParseField("max_write_buffer_count");
        public static final ParseField MAX_WRITE_BUFFER_SIZE = new ParseField("max_write_buffer_size");
        public static final ParseField MAX_RETRY_DELAY = new ParseField("max_retry_delay");
        public static final ParseField READ_POLL_TIMEOUT = new ParseField("read_poll_timeout");

        @SuppressWarnings("unchecked")
        private static final ConstructingObjectParser<AutoFollowPattern, Void> PARSER =
            new ConstructingObjectParser<>("auto_follow_pattern",
                args -> new AutoFollowPattern((String) args[0], (List<String>) args[1], (String) args[2], (Integer) args[3],
                    (ByteSizeValue) args[4], (Integer) args[5], (Integer) args[6], (ByteSizeValue) args[7], (Integer) args[8],
                    (Integer) args[9], (ByteSizeValue) args[10], (TimeValue) args[11], (TimeValue) args[12]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), LEADER_PATTERNS_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), FOLLOW_PATTERN_FIELD);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_READ_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_READ_REQUEST_SIZE.getPreferredName()),
                    MAX_READ_REQUEST_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_OUTSTANDING_READ_REQUESTS);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_WRITE_REQUEST_OPERATION_COUNT);
            PARSER.declareField(
                ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_REQUEST_SIZE.getPreferredName()),
                MAX_WRITE_REQUEST_SIZE,
                ObjectParser.ValueType.STRING);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_OUTSTANDING_WRITE_REQUESTS);
            PARSER.declareInt(ConstructingObjectParser.optionalConstructorArg(), MAX_WRITE_BUFFER_COUNT);
            PARSER.declareField(
                    ConstructingObjectParser.optionalConstructorArg(),
                    (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), MAX_WRITE_BUFFER_SIZE.getPreferredName()),
                    MAX_WRITE_BUFFER_SIZE,
                    ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), MAX_RETRY_DELAY.getPreferredName()),
                MAX_RETRY_DELAY, ObjectParser.ValueType.STRING);
            PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
                (p, c) -> TimeValue.parseTimeValue(p.text(), READ_POLL_TIMEOUT.getPreferredName()),
                READ_POLL_TIMEOUT, ObjectParser.ValueType.STRING);
        }

        private final String remoteCluster;
        private final List<String> leaderIndexPatterns;
        private final String followIndexPattern;
        private final Integer maxReadRequestOperationCount;
        private final ByteSizeValue maxReadRequestSize;
        private final Integer maxOutstandingReadRequests;
        private final Integer maxWriteRequestOperationCount;
        private final ByteSizeValue maxWriteRequestSize;
        private final Integer maxOutstandingWriteRequests;
        private final Integer maxWriteBufferCount;
        private final ByteSizeValue maxWriteBufferSize;
        private final TimeValue maxRetryDelay;
        private final TimeValue pollTimeout;

        public AutoFollowPattern(String remoteCluster,
                                 List<String> leaderIndexPatterns,
                                 String followIndexPattern,
                                 Integer maxReadRequestOperationCount,
                                 ByteSizeValue maxReadRequestSize,
                                 Integer maxOutstandingReadRequests,
                                 Integer maxWriteRequestOperationCount,
                                 ByteSizeValue maxWriteRequestSize,
                                 Integer maxOutstandingWriteRequests,
                                 Integer maxWriteBufferCount,
                                 ByteSizeValue maxWriteBufferSize, TimeValue maxRetryDelay,
                                 TimeValue pollTimeout) {
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.followIndexPattern = followIndexPattern;
            this.maxReadRequestOperationCount = maxReadRequestOperationCount;
            this.maxReadRequestSize = maxReadRequestSize;
            this.maxOutstandingReadRequests = maxOutstandingReadRequests;
            this.maxWriteRequestOperationCount = maxWriteRequestOperationCount;
            this.maxWriteRequestSize = maxWriteRequestSize;
            this.maxOutstandingWriteRequests = maxOutstandingWriteRequests;
            this.maxWriteBufferCount = maxWriteBufferCount;
            this.maxWriteBufferSize = maxWriteBufferSize;
            this.maxRetryDelay = maxRetryDelay;
            this.pollTimeout = pollTimeout;
        }

        public AutoFollowPattern(StreamInput in) throws IOException {
            remoteCluster = in.readString();
            leaderIndexPatterns = in.readList(StreamInput::readString);
            followIndexPattern = in.readOptionalString();
            maxReadRequestOperationCount = in.readOptionalVInt();
            maxReadRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxOutstandingReadRequests = in.readOptionalVInt();
            maxWriteRequestOperationCount = in.readOptionalVInt();
            maxWriteRequestSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxOutstandingWriteRequests = in.readOptionalVInt();
            maxWriteBufferCount = in.readOptionalVInt();
            maxWriteBufferSize = in.readOptionalWriteable(ByteSizeValue::new);
            maxRetryDelay = in.readOptionalTimeValue();
            pollTimeout = in.readOptionalTimeValue();
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

        public Integer getMaxReadRequestOperationCount() {
            return maxReadRequestOperationCount;
        }

        public Integer getMaxOutstandingReadRequests() {
            return maxOutstandingReadRequests;
        }

        public ByteSizeValue getMaxReadRequestSize() {
            return maxReadRequestSize;
        }

        public Integer getMaxWriteRequestOperationCount() {
            return maxWriteRequestOperationCount;
        }

        public ByteSizeValue getMaxWriteRequestSize() {
            return maxWriteRequestSize;
        }

        public Integer getMaxOutstandingWriteRequests() {
            return maxOutstandingWriteRequests;
        }

        public Integer getMaxWriteBufferCount() {
            return maxWriteBufferCount;
        }

        public ByteSizeValue getMaxWriteBufferSize() {
            return maxWriteBufferSize;
        }

        public TimeValue getMaxRetryDelay() {
            return maxRetryDelay;
        }

        public TimeValue getPollTimeout() {
            return pollTimeout;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(remoteCluster);
            out.writeStringList(leaderIndexPatterns);
            out.writeOptionalString(followIndexPattern);
            out.writeOptionalVInt(maxReadRequestOperationCount);
            out.writeOptionalWriteable(maxReadRequestSize);
            out.writeOptionalVInt(maxOutstandingReadRequests);
            out.writeOptionalVInt(maxWriteRequestOperationCount);
            out.writeOptionalWriteable(maxWriteRequestSize);
            out.writeOptionalVInt(maxOutstandingWriteRequests);
            out.writeOptionalVInt(maxWriteBufferCount);
            out.writeOptionalWriteable(maxWriteBufferSize);
            out.writeOptionalTimeValue(maxRetryDelay);
            out.writeOptionalTimeValue(pollTimeout);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(REMOTE_CLUSTER_FIELD.getPreferredName(), remoteCluster);
            builder.array(LEADER_PATTERNS_FIELD.getPreferredName(), leaderIndexPatterns.toArray(new String[0]));
            if (followIndexPattern != null) {
                builder.field(FOLLOW_PATTERN_FIELD.getPreferredName(), followIndexPattern);
            }
            if (maxReadRequestOperationCount != null) {
                builder.field(MAX_READ_REQUEST_OPERATION_COUNT.getPreferredName(), maxReadRequestOperationCount);
            }
            if (maxReadRequestSize != null) {
                builder.field(MAX_READ_REQUEST_SIZE.getPreferredName(), maxReadRequestSize.getStringRep());
            }
            if (maxOutstandingReadRequests != null) {
                builder.field(MAX_OUTSTANDING_READ_REQUESTS.getPreferredName(), maxOutstandingReadRequests);
            }
            if (maxWriteRequestOperationCount != null) {
                builder.field(MAX_WRITE_REQUEST_OPERATION_COUNT.getPreferredName(), maxWriteRequestOperationCount);
            }
            if (maxWriteRequestSize != null) {
                builder.field(MAX_WRITE_REQUEST_SIZE.getPreferredName(), maxWriteRequestSize.getStringRep());
            }
            if (maxOutstandingWriteRequests != null) {
                builder.field(MAX_OUTSTANDING_WRITE_REQUESTS.getPreferredName(), maxOutstandingWriteRequests);
            }
            if (maxWriteBufferCount != null){
                builder.field(MAX_WRITE_BUFFER_COUNT.getPreferredName(), maxWriteBufferCount);
            }
            if (maxWriteBufferSize != null) {
                builder.field(MAX_WRITE_BUFFER_SIZE.getPreferredName(), maxWriteBufferSize.getStringRep());
            }
            if (maxRetryDelay != null) {
                builder.field(MAX_RETRY_DELAY.getPreferredName(), maxRetryDelay);
            }
            if (pollTimeout != null) {
                builder.field(READ_POLL_TIMEOUT.getPreferredName(), pollTimeout);
            }
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
            AutoFollowPattern that = (AutoFollowPattern) o;
            return Objects.equals(remoteCluster, that.remoteCluster) &&
                    Objects.equals(leaderIndexPatterns, that.leaderIndexPatterns) &&
                    Objects.equals(followIndexPattern, that.followIndexPattern) &&
                    Objects.equals(maxReadRequestOperationCount, that.maxReadRequestOperationCount) &&
                    Objects.equals(maxReadRequestSize, that.maxReadRequestSize) &&
                    Objects.equals(maxOutstandingReadRequests, that.maxOutstandingReadRequests) &&
                    Objects.equals(maxWriteRequestOperationCount, that.maxWriteRequestOperationCount) &&
                    Objects.equals(maxWriteRequestSize, that.maxWriteRequestSize) &&
                    Objects.equals(maxOutstandingWriteRequests, that.maxOutstandingWriteRequests) &&
                    Objects.equals(maxWriteBufferCount, that.maxWriteBufferCount) &&
                    Objects.equals(maxWriteBufferSize, that.maxWriteBufferSize) &&
                    Objects.equals(maxRetryDelay, that.maxRetryDelay) &&
                    Objects.equals(pollTimeout, that.pollTimeout);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    remoteCluster,
                    leaderIndexPatterns,
                    followIndexPattern,
                    maxReadRequestOperationCount,
                    maxReadRequestSize,
                    maxOutstandingReadRequests,
                    maxWriteRequestOperationCount,
                    maxWriteRequestSize,
                    maxOutstandingWriteRequests,
                    maxWriteBufferCount,
                    maxWriteBufferSize,
                    maxRetryDelay,
                    pollTimeout);
        }
    }

}
