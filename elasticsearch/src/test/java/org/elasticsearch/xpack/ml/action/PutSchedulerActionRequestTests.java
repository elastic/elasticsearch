/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.xpack.ml.action.PutSchedulerAction.Request;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfig;
import org.elasticsearch.xpack.ml.scheduler.SchedulerConfigTests;
import org.elasticsearch.xpack.ml.support.AbstractStreamableXContentTestCase;
import org.junit.Before;

import java.util.Arrays;

public class PutSchedulerActionRequestTests extends AbstractStreamableXContentTestCase<Request> {

    private String schedulerId;

    @Before
    public void setUpSchedulerId() {
        schedulerId = SchedulerConfigTests.randomValidSchedulerId();
    }

    @Override
    protected Request createTestInstance() {
        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(schedulerId, randomAsciiOfLength(10));
        schedulerConfig.setIndexes(Arrays.asList(randomAsciiOfLength(10)));
        schedulerConfig.setTypes(Arrays.asList(randomAsciiOfLength(10)));
        return new Request(schedulerConfig.build());
    }

    @Override
    protected Request createBlankInstance() {
        return new Request();
    }

    @Override
    protected Request parseInstance(XContentParser parser) {
        return Request.parseRequest(schedulerId, parser);
    }

}
