/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.client;

import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.JobUpdateTests;
import org.elasticsearch.client.ml.job.util.PageParams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MLRequestConvertersTests extends ESTestCase {

    public void testPutJob() throws IOException {
        Job job = createValidJob("foo");
        PutJobRequest putJobRequest = new PutJobRequest(job);

        Request request = MLRequestConverters.putJob(putJobRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_xpack/ml/anomaly_detectors/foo"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            Job parsedJob = Job.PARSER.apply(parser, null).build();
            assertThat(parsedJob, equalTo(job));
        }
    }

    public void testGetJob() {
        GetJobRequest getJobRequest = new GetJobRequest();

        Request request = MLRequestConverters.getJob(getJobRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("allow_no_jobs"));

        getJobRequest = new GetJobRequest("job1", "jobs*");
        getJobRequest.setAllowNoJobs(true);
        request = MLRequestConverters.getJob(getJobRequest);

        assertEquals("/_xpack/ml/anomaly_detectors/job1,jobs*", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("allow_no_jobs"));
    }

    public void testGetJobStats() {
        GetJobStatsRequest getJobStatsRequestRequest = new GetJobStatsRequest();

        Request request = MLRequestConverters.getJobStats(getJobStatsRequestRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/_stats", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("allow_no_jobs"));

        getJobStatsRequestRequest = new GetJobStatsRequest("job1", "jobs*");
        getJobStatsRequestRequest.setAllowNoJobs(true);
        request = MLRequestConverters.getJobStats(getJobStatsRequestRequest);

        assertEquals("/_xpack/ml/anomaly_detectors/job1,jobs*/_stats", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("allow_no_jobs"));
    }


    public void testOpenJob() throws Exception {
        String jobId = "some-job-id";
        OpenJobRequest openJobRequest = new OpenJobRequest(jobId);
        openJobRequest.setTimeout(TimeValue.timeValueMinutes(10));

        Request request = MLRequestConverters.openJob(openJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_open", request.getEndpoint());
        assertEquals(requestEntityToString(request), "{\"job_id\":\""+ jobId +"\",\"timeout\":\"10m\"}");
    }

    public void testCloseJob() throws Exception {
        String jobId = "somejobid";
        CloseJobRequest closeJobRequest = new CloseJobRequest(jobId);

        Request request = MLRequestConverters.closeJob(closeJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_close", request.getEndpoint());
        assertEquals("{\"job_id\":\"somejobid\"}", requestEntityToString(request));

        closeJobRequest = new CloseJobRequest(jobId, "otherjobs*");
        closeJobRequest.setForce(true);
        closeJobRequest.setAllowNoJobs(false);
        closeJobRequest.setTimeout(TimeValue.timeValueMinutes(10));
        request = MLRequestConverters.closeJob(closeJobRequest);

        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + ",otherjobs*/_close", request.getEndpoint());
        assertEquals("{\"job_id\":\"somejobid,otherjobs*\",\"timeout\":\"10m\",\"force\":true,\"allow_no_jobs\":false}",
            requestEntityToString(request));
    }

    public void testDeleteJob() {
        String jobId = randomAlphaOfLength(10);
        DeleteJobRequest deleteJobRequest = new DeleteJobRequest(jobId);

        Request request = MLRequestConverters.deleteJob(deleteJobRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId, request.getEndpoint());
        assertEquals(Boolean.toString(false), request.getParameters().get("force"));

        deleteJobRequest.setForce(true);
        request = MLRequestConverters.deleteJob(deleteJobRequest);
        assertEquals(Boolean.toString(true), request.getParameters().get("force"));
    }

    public void testFlushJob() throws Exception {
        String jobId = randomAlphaOfLength(10);
        FlushJobRequest flushJobRequest = new FlushJobRequest(jobId);

        Request request = MLRequestConverters.flushJob(flushJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_flush", request.getEndpoint());
        assertEquals("{\"job_id\":\"" + jobId + "\"}", requestEntityToString(request));

        flushJobRequest.setSkipTime("1000");
        flushJobRequest.setStart("105");
        flushJobRequest.setEnd("200");
        flushJobRequest.setAdvanceTime("100");
        flushJobRequest.setCalcInterim(true);
        request = MLRequestConverters.flushJob(flushJobRequest);
        assertEquals(
                "{\"job_id\":\"" + jobId + "\",\"calc_interim\":true,\"start\":\"105\"," +
                        "\"end\":\"200\",\"advance_time\":\"100\",\"skip_time\":\"1000\"}",
                requestEntityToString(request));
    }

    public void testUpdateJob() throws Exception {
        String jobId = randomAlphaOfLength(10);
        JobUpdate updates = JobUpdateTests.createRandom(jobId);
        UpdateJobRequest updateJobRequest = new UpdateJobRequest(updates);

        Request request = MLRequestConverters.updateJob(updateJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_update", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            JobUpdate.Builder parsedRequest = JobUpdate.PARSER.apply(parser, null);
            assertThat(parsedRequest.build(), equalTo(updates));
        }
    }

    public void testGetBuckets() throws IOException {
        String jobId = randomAlphaOfLength(10);
        GetBucketsRequest getBucketsRequest = new GetBucketsRequest(jobId);
        getBucketsRequest.setPageParams(new PageParams(100, 300));
        getBucketsRequest.setAnomalyScore(75.0);
        getBucketsRequest.setSort("anomaly_score");
        getBucketsRequest.setDescending(true);

        Request request = MLRequestConverters.getBuckets(getBucketsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/results/buckets", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetBucketsRequest parsedRequest = GetBucketsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getBucketsRequest));
        }
    }

    public void testGetOverallBuckets() throws IOException {
        String jobId = randomAlphaOfLength(10);
        GetOverallBucketsRequest getOverallBucketsRequest = new GetOverallBucketsRequest(jobId);
        getOverallBucketsRequest.setBucketSpan(TimeValue.timeValueHours(3));
        getOverallBucketsRequest.setTopN(3);
        getOverallBucketsRequest.setStart("2018-08-08T00:00:00Z");
        getOverallBucketsRequest.setEnd("2018-09-08T00:00:00Z");
        getOverallBucketsRequest.setExcludeInterim(true);

        Request request = MLRequestConverters.getOverallBuckets(getOverallBucketsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/results/overall_buckets", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetOverallBucketsRequest parsedRequest = GetOverallBucketsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getOverallBucketsRequest));
        }
    }

    public void testGetRecords() throws IOException {
        String jobId = randomAlphaOfLength(10);
        GetRecordsRequest getRecordsRequest = new GetRecordsRequest(jobId);
        getRecordsRequest.setStart("2018-08-08T00:00:00Z");
        getRecordsRequest.setEnd("2018-09-08T00:00:00Z");
        getRecordsRequest.setPageParams(new PageParams(100, 300));
        getRecordsRequest.setRecordScore(75.0);
        getRecordsRequest.setSort("anomaly_score");
        getRecordsRequest.setDescending(true);
        getRecordsRequest.setExcludeInterim(true);

        Request request = MLRequestConverters.getRecords(getRecordsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/results/records", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetRecordsRequest parsedRequest = GetRecordsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getRecordsRequest));
        }
    }

    public void testPostData() throws Exception {
        String jobId = randomAlphaOfLength(10);
        PostDataRequest.JsonBuilder jsonBuilder = new PostDataRequest.JsonBuilder();
        Map<String, Object> obj = new HashMap<>();
        obj.put("foo", "bar");
        jsonBuilder.addDoc(obj);

        PostDataRequest postDataRequest = new PostDataRequest(jobId, jsonBuilder);
        Request request = MLRequestConverters.postData(postDataRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_data", request.getEndpoint());
        assertEquals("{\"foo\":\"bar\"}", requestEntityToString(request));
        assertEquals(postDataRequest.getXContentType().mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        assertFalse(request.getParameters().containsKey(PostDataRequest.RESET_END.getPreferredName()));
        assertFalse(request.getParameters().containsKey(PostDataRequest.RESET_START.getPreferredName()));

        PostDataRequest postDataRequest2 = new PostDataRequest(jobId, XContentType.SMILE, new byte[0]);
        postDataRequest2.setResetStart("2018-08-08T00:00:00Z");
        postDataRequest2.setResetEnd("2018-09-08T00:00:00Z");

        request = MLRequestConverters.postData(postDataRequest2);

        assertEquals(postDataRequest2.getXContentType().mediaTypeWithoutParameters(), request.getEntity().getContentType().getValue());
        assertEquals("2018-09-08T00:00:00Z", request.getParameters().get(PostDataRequest.RESET_END.getPreferredName()));
        assertEquals("2018-08-08T00:00:00Z", request.getParameters().get(PostDataRequest.RESET_START.getPreferredName()));
    }

    public void testGetInfluencers() throws IOException {
        String jobId = randomAlphaOfLength(10);
        GetInfluencersRequest getInfluencersRequest = new GetInfluencersRequest(jobId);
        getInfluencersRequest.setStart("2018-08-08T00:00:00Z");
        getInfluencersRequest.setEnd("2018-09-08T00:00:00Z");
        getInfluencersRequest.setPageParams(new PageParams(100, 300));
        getInfluencersRequest.setInfluencerScore(75.0);
        getInfluencersRequest.setSort("anomaly_score");
        getInfluencersRequest.setDescending(true);
        getInfluencersRequest.setExcludeInterim(true);

        Request request = MLRequestConverters.getInfluencers(getInfluencersRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/results/influencers", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetInfluencersRequest parsedRequest = GetInfluencersRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getInfluencersRequest));
        }
    }

    private static Job createValidJob(String jobId) {
        AnalysisConfig.Builder analysisConfig = AnalysisConfig.builder(Collections.singletonList(
                Detector.builder().setFunction("count").build()));
        Job.Builder jobBuilder = Job.builder(jobId);
        jobBuilder.setAnalysisConfig(analysisConfig);
        return jobBuilder.build();
    }

    private static String requestEntityToString(Request request) throws Exception {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        request.getEntity().writeTo(bos);
        return bos.toString("UTF-8");
    }
}
