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
import org.elasticsearch.test.AbstractXContentSerializingTestCase;
import org.elasticsearch.xcontent.ObjectParser;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xpack.core.ccr.action.FollowParameters;

import java.io.IOException;

public class FollowParametersTests extends AbstractXContentSerializingTestCase<FollowParameters> {

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

    @Override
    protected FollowParameters mutateInstance(FollowParameters instance) {
        FollowParameters mutated = new FollowParameters(instance);
        switch (between(0, 9)) {
            case 0 -> mutated.setMaxOutstandingReadRequests(
                randomValueOtherThan(instance.getMaxOutstandingReadRequests(), () -> randomIntBetween(0, Integer.MAX_VALUE))
            );
            case 1 -> mutated.setMaxOutstandingWriteRequests(
                randomValueOtherThan(instance.getMaxOutstandingWriteRequests(), () -> randomIntBetween(0, Integer.MAX_VALUE))
            );
            case 2 -> mutated.setMaxReadRequestOperationCount(
                randomValueOtherThan(instance.getMaxReadRequestOperationCount(), () -> randomIntBetween(0, Integer.MAX_VALUE))
            );
            case 3 -> mutated.setMaxWriteRequestOperationCount(
                randomValueOtherThan(instance.getMaxWriteRequestOperationCount(), () -> randomIntBetween(0, Integer.MAX_VALUE))
            );
            case 4 -> mutated.setMaxReadRequestSize(
                randomValueOtherThan(instance.getMaxReadRequestSize(), () -> ByteSizeValue.ofBytes(randomNonNegativeLong()))
            );
            case 5 -> mutated.setMaxWriteRequestSize(
                randomValueOtherThan(instance.getMaxWriteRequestSize(), () -> ByteSizeValue.ofBytes(randomNonNegativeLong()))
            );
            case 6 -> mutated.setMaxWriteBufferCount(
                randomValueOtherThan(instance.getMaxWriteBufferCount(), () -> randomIntBetween(0, Integer.MAX_VALUE))
            );
            case 7 -> mutated.setMaxWriteBufferSize(
                randomValueOtherThan(instance.getMaxWriteBufferSize(), () -> ByteSizeValue.ofBytes(randomNonNegativeLong()))
            );
            case 8 -> mutated.setMaxRetryDelay(
                randomValueOtherThan(instance.getMaxRetryDelay(), () -> new TimeValue(randomNonNegativeLong()))
            );
            case 9 -> mutated.setReadPollTimeout(
                randomValueOtherThan(instance.getReadPollTimeout(), () -> new TimeValue(randomNonNegativeLong()))
            );
            default -> throw new AssertionError("failure, got illegal switch case");
        }
        return mutated;
    }

    static FollowParameters randomInstance() {
        FollowParameters followParameters = new FollowParameters();
        followParameters.setMaxOutstandingReadRequests(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxOutstandingWriteRequests(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxReadRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxWriteRequestOperationCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxReadRequestSize(ByteSizeValue.ofBytes(randomNonNegativeLong()));
        followParameters.setMaxWriteRequestSize(ByteSizeValue.ofBytes(randomNonNegativeLong()));
        followParameters.setMaxWriteBufferCount(randomIntBetween(0, Integer.MAX_VALUE));
        followParameters.setMaxWriteBufferSize(ByteSizeValue.ofBytes(randomNonNegativeLong()));
        followParameters.setMaxRetryDelay(new TimeValue(randomNonNegativeLong()));
        followParameters.setReadPollTimeout(new TimeValue(randomNonNegativeLong()));
        return followParameters;
    }
}
