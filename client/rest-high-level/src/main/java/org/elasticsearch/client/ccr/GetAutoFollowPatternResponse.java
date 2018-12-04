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

package org.elasticsearch.client.ccr;

import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentParser.Token;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class GetAutoFollowPatternResponse {

    public static GetAutoFollowPatternResponse fromXContent(final XContentParser parser) throws IOException {
        final Map<String, Pattern> patterns = new HashMap<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                final String name = parser.currentName();
                final Pattern pattern = Pattern.PARSER.parse(parser, null);
                patterns.put(name, pattern);
            }
        }
        return new GetAutoFollowPatternResponse(patterns);
    }

    private final Map<String, Pattern> patterns;

    GetAutoFollowPatternResponse(Map<String, Pattern> patterns) {
        this.patterns = Collections.unmodifiableMap(patterns);
    }

    public Map<String, Pattern> getPatterns() {
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
            "pattern", args -> new Pattern((String) args[0], (List<String>) args[1], (String) args[2]));

        static {
            PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.REMOTE_CLUSTER_FIELD);
            PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PutAutoFollowPatternRequest.LEADER_PATTERNS_FIELD);
            PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), PutAutoFollowPatternRequest.FOLLOW_PATTERN_FIELD);
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
        private final String followIndexNamePattern;

        Pattern(String remoteCluster, List<String> leaderIndexPatterns, String followIndexNamePattern) {
            this.remoteCluster = remoteCluster;
            this.leaderIndexPatterns = leaderIndexPatterns;
            this.followIndexNamePattern = followIndexNamePattern;
        }

        public String getRemoteCluster() {
            return remoteCluster;
        }

        public List<String> getLeaderIndexPatterns() {
            return leaderIndexPatterns;
        }

        public String getFollowIndexNamePattern() {
            return followIndexNamePattern;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            if (!super.equals(o)) return false;
            Pattern pattern = (Pattern) o;
            return Objects.equals(remoteCluster, pattern.remoteCluster) &&
                Objects.equals(leaderIndexPatterns, pattern.leaderIndexPatterns) &&
                Objects.equals(followIndexNamePattern, pattern.followIndexNamePattern);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                super.hashCode(),
                remoteCluster,
                leaderIndexPatterns,
                followIndexNamePattern
            );
        }
    }

}
