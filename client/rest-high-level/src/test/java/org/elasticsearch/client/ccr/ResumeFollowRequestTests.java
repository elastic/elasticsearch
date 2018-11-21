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

public class ResumeFollowRequestTests extends AbstractXContentTestCase<ResumeFollowRequest> {

    private static final ConstructingObjectParser<ResumeFollowRequest, Void> PARSER = new ConstructingObjectParser<>("test_parser",
        true, (args) -> new ResumeFollowRequest((String) args[0]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.FOLLOWER_INDEX_FIELD);
        PARSER.declareInt(ResumeFollowRequest::setMaxReadRequestOperationCount, FollowConfig.MAX_READ_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
            ResumeFollowRequest::setMaxReadRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_READ_REQUEST_SIZE.getPreferredName()),
            PutFollowRequest.MAX_READ_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(ResumeFollowRequest::setMaxOutstandingReadRequests, FollowConfig.MAX_OUTSTANDING_READ_REQUESTS);
        PARSER.declareInt(ResumeFollowRequest::setMaxWriteRequestOperationCount, FollowConfig.MAX_WRITE_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
            ResumeFollowRequest::setMaxWriteRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
            PutFollowRequest.MAX_WRITE_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(ResumeFollowRequest::setMaxOutstandingWriteRequests, FollowConfig.MAX_OUTSTANDING_WRITE_REQUESTS);
        PARSER.declareInt(ResumeFollowRequest::setMaxWriteBufferCount, FollowConfig.MAX_WRITE_BUFFER_COUNT);
        PARSER.declareField(
            ResumeFollowRequest::setMaxWriteBufferSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), FollowConfig.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            PutFollowRequest.MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            ResumeFollowRequest::setMaxRetryDelay,
            (p, c) -> TimeValue.parseTimeValue(p.text(), FollowConfig.MAX_RETRY_DELAY_FIELD.getPreferredName()),
            PutFollowRequest.MAX_RETRY_DELAY_FIELD,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            ResumeFollowRequest::setReadPollTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), FollowConfig.READ_POLL_TIMEOUT.getPreferredName()),
            PutFollowRequest.READ_POLL_TIMEOUT,
            ObjectParser.ValueType.STRING);
    }

    @Override
    protected ResumeFollowRequest doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    @Override
    protected ResumeFollowRequest createTestInstance() {
        ResumeFollowRequest resumeFollowRequest = new ResumeFollowRequest(randomAlphaOfLength(4));
        if (randomBoolean()) {
            resumeFollowRequest.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            resumeFollowRequest.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return resumeFollowRequest;
    }

}
