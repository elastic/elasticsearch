/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;

import java.io.IOException;

public class PutJobResponseTests extends AbstractXContentTestCase<PutJobResponse> {

    @Override
    protected PutJobResponse createTestInstance() {
        return new PutJobResponse(JobTests.createRandomizedJob());
    }

    @Override
    protected PutJobResponse doParseInstance(XContentParser parser) throws IOException {
        return PutJobResponse.fromXContent(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
