/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction.Response;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobTests;

import java.util.ArrayList;
import java.util.List;

public class GetJobsActionResponseTests extends AbstractWireSerializingTestCase<GetJobsAction.Response> {

    @Override
    protected Response createTestInstance() {
        final Response result;

        int listSize = randomInt(10);
        List<Job> jobList = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            jobList.add(JobTests.createRandomizedJob());
        }

        result = new Response(new QueryPage<>(jobList, jobList.size(), Job.RESULTS_FIELD));

        return result;
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
