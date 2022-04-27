/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.ccr.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.AbstractSerializingTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;

import java.io.IOException;

public class FollowParametersTests extends AbstractSerializingTestCase<FollowParameters> {

    static final ObjectParser<FollowParameters, Void> PARSER = new ObjectParser<>("test_parser", FollowParameters::new);
    static {
        FollowParameters.initParser(PARSER);
    }

    @Override
    protected FollowParameters doParseInstance(XContentParser parser) throws IOException {
        return PARSER.apply(parser, null);
    }

    @Override
    protected FollowParameters createTestInstance() {
        return randomInstance();
    }

    @Override
    protected Writeable.Reader<FollowParameters> instanceReader() {
        return FollowParameters::new;
    }

    static FollowParameters randomInstance() {
        FollowParameters followParameters = new FollowParameters();
        followParameters.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxReadRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        followParameters.setMaxWriteRequestSize(new ByteSizeValue(randomNonNegativeLong()));
        followParameters.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxWriteBufferSize(new ByteSizeValue(randomNonNegativeLong()));
        followParameters.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        followParameters.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        return followParameters;
    }
}
