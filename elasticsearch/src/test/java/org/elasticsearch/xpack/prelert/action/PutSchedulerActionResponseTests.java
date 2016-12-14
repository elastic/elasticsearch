/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.PutSchedulerAction.Response;
import org.elasticsearch.xpack.prelert.job.SchedulerConfig;
import org.elasticsearch.xpack.prelert.job.SchedulerConfigTests;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import java.util.Arrays;

public class PutSchedulerActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        SchedulerConfig.Builder schedulerConfig = new SchedulerConfig.Builder(
                SchedulerConfigTests.randomValidSchedulerId(), randomAsciiOfLength(10));
        schedulerConfig.setIndexes(Arrays.asList(randomAsciiOfLength(10)));
        schedulerConfig.setTypes(Arrays.asList(randomAsciiOfLength(10)));
        return new Response(randomBoolean(), schedulerConfig.build());
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
