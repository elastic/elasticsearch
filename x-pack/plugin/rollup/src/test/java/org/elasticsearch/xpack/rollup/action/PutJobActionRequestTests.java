/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;


import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.elasticsearch.xpack.core.rollup.ConfigTestHelpers;
import org.elasticsearch.xpack.core.rollup.action.PutRollupJobAction.Request;
import org.junit.Before;

import java.io.IOException;

public class PutJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String jobId;

    @Before
    public void setupJobID() {
        jobId = randomAlphaOfLengthBetween(1,10);
    }

    @Override
    protected Request createTestInstance() {
        return new Request(ConfigTestHelpers.randomRollupJobConfig(random(), jobId));
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request doParseInstance(XContentParser parser) throws IOException {
        return Request.fromXContent(parser, jobId);
    }
}
