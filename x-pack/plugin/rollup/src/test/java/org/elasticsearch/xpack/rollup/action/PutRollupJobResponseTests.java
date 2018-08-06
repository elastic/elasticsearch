/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.rollup.action;


import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.rollup.PutRollupJobResponse;
import org.elasticsearch.test.AbstractStreamableXContentTestCase;
import org.junit.Before;

public class PutRollupJobResponseTests extends AbstractStreamableXContentTestCase<PutRollupJobResponse> {

    private boolean acknowledged;

    @Before
    public void setupJobID() {
        acknowledged = randomBoolean();
    }

    @Override
    protected PutRollupJobResponse createTestInstance() {
        return new PutRollupJobResponse(acknowledged);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }

    @Override
    protected PutRollupJobResponse createBlankInstance() {
        return new PutRollupJobResponse();
    }

    @Override
    protected PutRollupJobResponse doParseInstance(final XContentParser parser) {
        return PutRollupJobResponse.fromXContent(parser);
    }

}
