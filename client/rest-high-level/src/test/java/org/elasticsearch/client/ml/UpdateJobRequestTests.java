/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.ml;

import org.elasticsearch.client.ml.job.config.JobTests;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.JobUpdateTests;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.AbstractXContentTestCase;


public class UpdateJobRequestTests extends AbstractXContentTestCase<UpdateJobRequest> {

    @Override
    protected UpdateJobRequest createTestInstance() {
        return new UpdateJobRequest(JobUpdateTests.createRandom(JobTests.randomValidJobId()));
    }

    @Override
    protected UpdateJobRequest doParseInstance(XContentParser parser) {
        return new UpdateJobRequest(JobUpdate.PARSER.apply(parser, null).build());
    }

    @Override
    protected boolean supportsUnknownFields() {
        return false;
    }
}
