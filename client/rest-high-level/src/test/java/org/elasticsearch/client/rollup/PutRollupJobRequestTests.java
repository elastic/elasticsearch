/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.rollup;

import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.client.rollup.job.config.RollupJobConfigTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;
import org.junit.Before;

import java.io.IOException;


public class PutRollupJobRequestTests extends AbstractXContentTestCase<PutRollupJobRequest> {

    private String jobId;

    @Before
    public void setUpOptionalId() {
        jobId = randomAlphaOfLengthBetween(1, 10);
    }

    @Override
    protected PutRollupJobRequest createTestInstance() {
        return new PutRollupJobRequest(RollupJobConfigTests.randomRollupJobConfig(jobId));
    }

    @Override
    protected PutRollupJobRequest doParseInstance(final XContentParser parser) throws IOException {
        final String optionalId = randomBoolean() ? jobId : null;
        return new PutRollupJobRequest(RollupJobConfig.fromXContent(parser, optionalId));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    public void testRequireConfiguration() {
        final NullPointerException e = expectThrows(NullPointerException.class, ()-> new PutRollupJobRequest(null));
        assertEquals("rollup job configuration is required", e.getMessage());
    }
}
