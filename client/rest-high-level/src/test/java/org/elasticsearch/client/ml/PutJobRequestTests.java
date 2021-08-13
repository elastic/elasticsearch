/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;


public class PutJobRequestTests extends AbstractXContentTestCase<PutJobRequest> {

    @Override
    protected PutJobRequest createTestInstance() {
        return new PutJobRequest(JobTests.createRandomizedJob());
    }

    @Override
    protected PutJobRequest doParseInstance(XContentParser parser) {
        return new PutJobRequest(Job.PARSER.apply(parser, null).build());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
