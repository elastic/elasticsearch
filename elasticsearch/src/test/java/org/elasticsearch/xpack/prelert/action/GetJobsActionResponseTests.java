/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.prelert.action;

import org.elasticsearch.xpack.prelert.action.GetJobsAction.Response;
import org.elasticsearch.xpack.prelert.job.Job;
import org.elasticsearch.xpack.prelert.job.persistence.QueryPage;
import org.elasticsearch.xpack.prelert.support.AbstractStreamableTestCase;

import java.util.ArrayList;
import java.util.List;

import static org.elasticsearch.xpack.prelert.job.JobTests.buildJobBuilder;
import static org.elasticsearch.xpack.prelert.job.JobTests.randomValidJobId;

public class GetJobsActionResponseTests extends AbstractStreamableTestCase<GetJobsAction.Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<Job> hits = new ArrayList<>(listSize);
        for (int j = 0; j < listSize; j++) {
            hits.add(buildJobBuilder(randomValidJobId()).build());
        }
        QueryPage<Job> buckets = new QueryPage<>(hits, listSize);
        return new Response(buckets);
    }

    @Override
    protected Response createBlankInstance() {
        return new Response();
    }

}
