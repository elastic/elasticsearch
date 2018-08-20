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
import org.apache.http.client.methods.HttpPost;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.protocol.xpack.ml.CloseJobRequest;
import org.elasticsearch.protocol.xpack.ml.DeleteJobRequest;
import org.elasticsearch.protocol.xpack.ml.OpenJobRequest;
import org.elasticsearch.protocol.xpack.ml.PutJobRequest;
import org.elasticsearch.protocol.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.protocol.xpack.ml.job.config.Detector;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.equalTo;

public class MLRequestConvertersTests extends ESTestCase {

    public void testPutJob() throws IOException {
        Job job = createValidJob("foo");
        PutJobRequest putJobRequest = new PutJobRequest(job);

        Request request = MLRequestConverters.putJob(putJobRequest);

        assertThat(request.getEndpoint(), equalTo("/_xpack/ml/anomaly_detectors/foo"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            Job parsedJob = Job.PARSER.apply(parser, null).build();
            assertThat(parsedJob, equalTo(job));
        }
    }

    public void testOpenJob() throws Exception {
        String jobId = "some-job-id";
        OpenJobRequest openJobRequest = new OpenJobRequest(jobId);
        openJobRequest.setTimeout(TimeValue.timeValueMinutes(10));

        Request request = MLRequestConverters.openJob(openJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_open", request.getEndpoint());
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        request.getEntity().writeTo(bos);
        assertEquals(bos.toString("UTF-8"), "{\"job_id\":\""+ jobId +"\",\"timeout\":\"10m\"}");
    }

    public void testCloseJob() {
        String jobId = "somejobid";
        CloseJobRequest closeJobRequest = new CloseJobRequest(jobId);

        Request request = MLRequestConverters.closeJob(closeJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + "/_close", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("force"));
        assertFalse(request.getParameters().containsKey("allow_no_jobs"));
        assertFalse(request.getParameters().containsKey("timeout"));

        closeJobRequest = new CloseJobRequest(jobId, "otherjobs*");
        closeJobRequest.setForce(true);
        closeJobRequest.setAllowNoJobs(false);
        closeJobRequest.setTimeout(TimeValue.timeValueMinutes(10));
        request = MLRequestConverters.closeJob(closeJobRequest);

        assertEquals("/_xpack/ml/anomaly_detectors/" + jobId + ",otherjobs*/_close", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("force"));
        assertEquals(Boolean.toString(false), request.getParameters().get("allow_no_jobs"));
        assertEquals("10m", request.getParameters().get("timeout"));
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

    private static Job createValidJob(String jobId) {
        AnalysisConfig.Builder analysisConfig = AnalysisConfig.builder(Collections.singletonList(
                Detector.builder().setFunction("count").build()));
        Job.Builder jobBuilder = Job.builder(jobId);
        jobBuilder.setAnalysisConfig(analysisConfig);
        return jobBuilder.build();
    }
}
