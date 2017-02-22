/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.PutJobAction.Request;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;

import java.util.Date;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.job.config.JobTests.randomValidJobId;

public class PutJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private final String jobId = randomValidJobId();
    private final Date date = new Date();

    @Override
    protected Request createTestInstance() {
        Job.Builder jobConfiguration = buildJobBuilder(jobId, date);
        return new Request(jobConfiguration.build());
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(jobId, parser);
    }

}
