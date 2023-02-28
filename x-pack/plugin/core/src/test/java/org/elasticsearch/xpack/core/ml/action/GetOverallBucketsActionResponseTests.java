/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.core.ml.action;

import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.test.AbstractWireSerializingTestCase;
import org.elasticsearch.xpack.core.action.util.QueryPage;
import org.elasticsearch.xpack.core.ml.action.GetOverallBucketsAction.Response;
import org.elasticsearch.xpack.core.ml.job.results.OverallBucket;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class GetOverallBucketsActionResponseTests extends AbstractWireSerializingTestCase<Response> {

    @Override
    protected Response createTestInstance() {
        int listSize = randomInt(10);
        List<OverallBucket> hits = new ArrayList<>(listSize);
        String jobId = randomAlphaOfLengthBetween(1, 20);
        for (int bucketIndex = 0; bucketIndex < listSize; bucketIndex++) {
            int jobsCount = randomInt(5);
            List<OverallBucket.JobInfo> jobs = new ArrayList<>(jobsCount);
            for (int jobIndex = 0; jobIndex < jobsCount; jobIndex++) {
                jobs.add(new OverallBucket.JobInfo(jobId, randomDouble()));
            }
            hits.add(new OverallBucket(new Date(randomNonNegativeLong()), randomNonNegativeLong(), randomDouble(), jobs, randomBoolean()));
        }
        QueryPage<OverallBucket> snapshots = new QueryPage<>(hits, listSize, OverallBucket.RESULTS_FIELD);
        return new Response(snapshots);
    }

    @Override
    protected Response mutateInstance(Response instance) {
        return null;// TODO implement https://github.com/elastic/elasticsearch/issues/25929
    }

    @Override
    protected Writeable.Reader<Response> instanceReader() {
        return Response::new;
    }
}
