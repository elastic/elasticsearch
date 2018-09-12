/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;

import java.io.IOException;
import java.util.Arrays;

public class PutAutoFollowPatternRequestTests extends AbstractStreamableXContentTestCase<PutAutoFollowPatternAction.Request> {

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutAutoFollowPatternAction.Request doParseInstance(XContentParser parser) throws IOException {
        return PutAutoFollowPatternAction.Request.fromXContent(parser, null);
    }

    @Override
    protected PutAutoFollowPatternAction.Request createBlankInstance() {
        return new PutAutoFollowPatternAction.Request();
    }

    @Override
    protected PutAutoFollowPatternAction.Request createTestInstance() {
        PutAutoFollowPatternAction.Request request = new PutAutoFollowPatternAction.Request();
        request.setLeaderClusterAlias(randomAlphaOfLength(4));
        request.setLeaderIndexPatterns(Arrays.asList(generateRandomStringArray(4, 4, false)));
        if (randomBoolean()) {
            request.setFollowIndexNamePattern(randomAlphaOfLength(4));
        }
        if (randomBoolean()) {
            request.setIdleShardRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxRetryDelay(TimeValue.timeValueMillis(500));
        }
        if (randomBoolean()) {
            request.setMaxBatchOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentReadBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxConcurrentWriteBatches(randomIntBetween(0, Integer.MAX_VALUE));
        }
        if (randomBoolean()) {
            request.setMaxOperationSizeInBytes(randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.setMaxWriteBufferSize(randomIntBetween(0, Integer.MAX_VALUE));
        }
        return request;
    }
}
