/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.rollup.GetRollupJobRequest;
import org.elasticsearch.client.rollup.PutRollupJobRequest;
import org.elasticsearch.client.rollup.StartRollupJobRequest;
import org.elasticsearch.client.rollup.StopRollupJobRequest;
import org.elasticsearch.client.rollup.job.config.RollupJobConfig;
import org.elasticsearch.client.rollup.job.config.RollupJobConfigTests;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class RollupRequestConvertersTests extends ESTestCase {
    public void testPutJob() throws IOException {
        String job = randomAlphaOfLength(5);

        RollupJobConfig config = RollupJobConfigTests.randomRollupJobConfig(job);
        PutRollupJobRequest put = new PutRollupJobRequest(config);

        Request request = RollupRequestConverters.putJob(put);
        assertThat(request.getEndpoint(), equalTo("/_rollup/job/" + job));
        assertThat(HttpPut.METHOD_NAME, equalTo(request.getMethod()));
        assertThat(request.getParameters().keySet(), empty());
        RequestConvertersTests.assertToXContentBody(put, request.getEntity());
    }

    public void testStartJob() throws IOException {
        String jobId = randomAlphaOfLength(5);

        StartRollupJobRequest startJob = new StartRollupJobRequest(jobId);

        Request request = RollupRequestConverters.startJob(startJob);
        assertThat(request.getEndpoint(), equalTo("/_rollup/job/" + jobId + "/_start"));
        assertThat(HttpPost.METHOD_NAME, equalTo(request.getMethod()));
        assertThat(request.getParameters().keySet(), empty());
        assertThat(request.getEntity(), nullValue());
    }

    public void testStopJob() throws IOException {
        String jobId = randomAlphaOfLength(5);
        StopRollupJobRequest stopJob = new StopRollupJobRequest(jobId);
        String expectedTimeOutString = null;
        String expectedWaitForCompletion = null;
        int expectedParameters = 0;
        if (randomBoolean()) {
            stopJob.timeout(TimeValue.parseTimeValue(randomPositiveTimeValue(), "timeout"));
            expectedTimeOutString = stopJob.timeout().getStringRep();
            expectedParameters++;
        }
        if (randomBoolean()) {
            stopJob.waitForCompletion(randomBoolean());
            expectedWaitForCompletion = stopJob.waitForCompletion().toString();
            expectedParameters++;
        }

        Request request = RollupRequestConverters.stopJob(stopJob);
        assertThat(request.getEndpoint(), equalTo("/_rollup/job/" + jobId + "/_stop"));
        assertThat(HttpPost.METHOD_NAME, equalTo(request.getMethod()));
        assertThat(request.getParameters().keySet().size(), equalTo(expectedParameters));
        assertThat(request.getParameters().get("timeout"), equalTo(expectedTimeOutString));
        assertThat(request.getParameters().get("wait_for_completion"), equalTo(expectedWaitForCompletion));
        assertNull(request.getEntity());
    }

    public void testGetJob() {
        boolean getAll = randomBoolean();
        String job = getAll ? "_all" : RequestConvertersTests.randomIndicesNames(1, 1)[0];
        GetRollupJobRequest get = getAll ? new GetRollupJobRequest() : new GetRollupJobRequest(job);

        Request request = RollupRequestConverters.getJob(get);
        assertThat(request.getEndpoint(), equalTo("/_rollup/job/" + job));
        assertThat(HttpGet.METHOD_NAME, equalTo(request.getMethod()));
        assertThat(request.getParameters().keySet(), empty());
        assertThat(request.getEntity(), nullValue());
    }
}
