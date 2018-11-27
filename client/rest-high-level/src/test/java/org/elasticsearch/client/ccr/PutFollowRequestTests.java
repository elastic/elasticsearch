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

public class PutFollowRequestTests extends AbstractXContentTestCase<PutFollowRequest> {

    private static final ConstructingObjectParser<PutFollowRequest, Void> PARSER = new ConstructingObjectParser<>("test_parser",
        (args) -> new PutFollowRequest((String) args[0], (String) args[1], (String) args[2]));

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.REMOTE_CLUSTER_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.LEADER_INDEX_FIELD);
        PARSER.declareString(ConstructingObjectParser.constructorArg(), PutFollowRequest.FOLLOWER_INDEX_FIELD);
        PARSER.declareInt(PutFollowRequest::setMaxReadRequestOperationCount, PutFollowRequest.MAX_READ_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
            PutFollowRequest::setMaxReadRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), PutFollowRequest.MAX_READ_REQUEST_SIZE.getPreferredName()),
            PutFollowRequest.MAX_READ_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(PutFollowRequest::setMaxOutstandingReadRequests, PutFollowRequest.MAX_OUTSTANDING_READ_REQUESTS);
        PARSER.declareInt(PutFollowRequest::setMaxWriteRequestOperationCount, PutFollowRequest.MAX_WRITE_REQUEST_OPERATION_COUNT);
        PARSER.declareField(
            PutFollowRequest::setMaxWriteRequestSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), PutFollowRequest.MAX_WRITE_REQUEST_SIZE.getPreferredName()),
            PutFollowRequest.MAX_WRITE_REQUEST_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareInt(PutFollowRequest::setMaxOutstandingWriteRequests, PutFollowRequest.MAX_OUTSTANDING_WRITE_REQUESTS);
        PARSER.declareInt(PutFollowRequest::setMaxWriteBufferCount, PutFollowRequest.MAX_WRITE_BUFFER_COUNT);
        PARSER.declareField(
            PutFollowRequest::setMaxWriteBufferSize,
            (p, c) -> ByteSizeValue.parseBytesSizeValue(p.text(), PutFollowRequest.MAX_WRITE_BUFFER_SIZE.getPreferredName()),
            PutFollowRequest.MAX_WRITE_BUFFER_SIZE,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            PutFollowRequest::setMaxRetryDelay,
            (p, c) -> TimeValue.parseTimeValue(p.text(), PutFollowRequest.MAX_RETRY_DELAY_FIELD.getPreferredName()),
            PutFollowRequest.MAX_RETRY_DELAY_FIELD,
            ObjectParser.ValueType.STRING);
        PARSER.declareField(
            PutFollowRequest::setReadPollTimeout,
            (p, c) -> TimeValue.parseTimeValue(p.text(), PutFollowRequest.READ_POLL_TIMEOUT.getPreferredName()),
            PutFollowRequest.READ_POLL_TIMEOUT,
            ObjectParser.ValueType.STRING);
    }

    @Override
    protected PutFollowRequest doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutFollowRequest createTestInstance() {
        PutFollowRequest putFollowRequest =
            new PutFollowRequest(randomAlphaOfLength(4), randomAlphaOfLength(4), randomAlphaOfLength(4));
        if (randomBoolean()) {
            putFollowRequest.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        }
        if (randomBoolean()) {
            putFollowRequest.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        }
        return putFollowRequest;
    }

}
