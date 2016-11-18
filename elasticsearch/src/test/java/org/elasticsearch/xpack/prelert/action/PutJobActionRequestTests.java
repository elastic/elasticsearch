/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.action.PutJobAction.Request;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableXContentTestCase;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.prelert.job.JobTests.randomValidJobId;

public class PutJobActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    @Override
    protected Request createTestInstance() {
        Job.Builder jobConfiguration = buildJobBuilder(randomValidJobId());
        return new Request(jobConfiguration.build(true));
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Request.parseRequest(parser, () -> matcher);
    }

}
