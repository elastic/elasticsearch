/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.PutJobAction.Response;
import org.elasticsearch.xpack.prelert.job.IgnoreDowntime;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.prelert.job.JobTests.randomValidJobId;

public class PutJobActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        Job.Builder builder = buildJobBuilder(randomValidJobId());
        builder.setIgnoreDowntime(IgnoreDowntime.NEVER);
        return new Response(randomBoolean(), builder.build());
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
