/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.xpack.ml.action.PutJobAction.Response;
import org.elasticsearch.xpack.ml.job.config.IgnoreDowntime;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.support.AbstractStreamableTestCase;

import static org.elasticsearch.xpack.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.ml.job.config.JobTests.randomValidJobId;

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
