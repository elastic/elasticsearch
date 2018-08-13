/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;


import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.rollup.PutRollupJobRequest;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.junit.Before;

import java.io.IOException;

public class PutRollupJobRequestTests extends AbstractStreamableXContentTestCase<PutRollupJobRequest> {

    private String jobId;

    @Before
    public void setupJobID() {
        jobId = randomAlphaOfLengthBetween(1,10);
    }

    @Override
    protected PutRollupJobRequest createTestInstance() {
        return new PutRollupJobRequest(ConfigTestHelpers.randomRollupJobConfig(random(), jobId));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutRollupJobRequest createBlankInstance() {
        return new PutRollupJobRequest();
    }

    @Override
    protected PutRollupJobRequest doParseInstance(final XContentParser parser) throws IOException {
        return PutRollupJobRequest.fromXContent(parser, jobId);
    }

}
