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
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class PutAutoFollowPatternRequestTests extends AbstractXContentTestCase<PutAutoFollowPatternRequest> {

    @SuppressWarnings("unchecked")
    private static final ConstructingObjectParser<PutAutoFollowPatternRequest, Void> PARSER = new ConstructingObjectParser<>("test_parser",
        true, (args) -> new PutAutoFollowPatternRequest("name", (String) args[0], (List<String>) args[1]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.REMOTE_CLUSTER_FIELD);
        PARSER.declareStringArray(ConstructingObjectParser.constructorArg(), PutAutoFollowPatternRequest.LEADER_PATTERNS_FIELD);
        PARSER.declareString(PutAutoFollowPatternRequest::setFollowIndexNamePattern, PutAutoFollowPatternRequest.FOLLOW_PATTERN_FIELD);
        PARSER.declareInt(PutAutoFollowPatternRequest::setMaxReadRequestOperationCount, FollowConfig.MAX_READ_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
            PutAutoFollowPatternRequest::setMaxReadRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_READ_REQUEST_SIZE.getPreferredName()),
            PutFollowRequest.MAX_READ_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(PutAutoFollowPatternRequest::setMaxOutstandingReadRequests, FollowConfig.MAX_OUTSTANDING_READ_REQUESTS);
        PARSER.declareInt(PutAutoFollowPatternRequest::setMaxWriteRequestOperationCount, FollowConfig.MAX_WRITE_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
            PutAutoFollowPatternRequest::setMaxWriteRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
            PutFollowRequest.MAX_WRITE_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(PutAutoFollowPatternRequest::setMaxOutstandingWriteRequests, FollowConfig.MAX_OUTSTANDING_WRITE_REQUESTS);
        PARSER.declareInt(PutAutoFollowPatternRequest::setMaxWriteBufferCount, FollowConfig.MAX_WRITE_BUFFER_COUNT);
        PARSER.declareField(
            PutAutoFollowPatternRequest::setMaxWriteBufferSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            PutFollowRequest.MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            PutAutoFollowPatternRequest::setMaxRetryDelay,
            (p, c) -> TimeValue.parseTimeValue(p.text(), FollowConfig.MAX_RETRY_DELAY_FIELD.getPreferredName()),
            PutFollowRequest.MAX_RETRY_DELAY_FIELD,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            PutAutoFollowPatternRequest::setReadPollTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), FollowConfig.READ_POLL_TIMEOUT.getPreferredName()),
            PutFollowRequest.READ_POLL_TIMEOUT,
            ObjectParser.ValueType.STRING);
    }

    @Override
    protected PutAutoFollowPatternRequest doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected PutAutoFollowPatternRequest createTestInstance() {
        // Name isn't serialized, because it specified in url path, so no need to randomly generate it here.
        PutAutoFollowPatternRequest putAutoFollowPatternRequest = new PutAutoFollowPatternRequest("name",
            randomAlphaOfLength(4), Arrays.asList(generateRandomStringArray(4, 4, false)));
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setFollowIndexNamePattern(randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putAutoFollowPatternRequest.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return putAutoFollowPatternRequest;
    }

}
