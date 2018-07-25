/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.test.AbstractStreamableTestCase;
import org.elasticsearch.xpack.core.ml.action.PutJobAction.Response;
import org.elasticsearch.xpack.core.ml.job.config.Job;

import static org.elasticsearch.xpack.core.ml.job.config.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.core.ml.job.config.JobTests.randomValidJobId;

public class PutJobActionResponseTests extends AbstractStreamableTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        Job.Builder builder = buildJobBuilder(randomValidJobId());
        return new Response(builder.build());
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
