/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.common.ParseFieldMatcher;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.prelert.action.StartJobSchedulerAction.Request;
import org.elasticsearch.xpack.prelert.job.JobSchedulerStatus;
import org.elasticsearch.xpack.prelert.job.SchedulerState;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableXContentTestCase;

public class StartJobSchedulerActionRequestTests extends AbstractStreamableXContentTestCase<StartJobSchedulerAction.Request> {

    @Override
    protected Request createTestInstance() {
        SchedulerState state = new SchedulerState(JobSchedulerStatus.STARTING, randomLong(), randomLong());
        return new Request(randomAsciiOfLengthBetween(1, 20), state);
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser, ParseFieldMatcher matcher) {
        return Request.parseRequest(null, parser, () -> matcher);
    }

}
