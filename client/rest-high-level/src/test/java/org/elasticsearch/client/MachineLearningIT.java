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

import com.carrotsearch.randomizedtesting.generators.CodepointSetGenerator;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.ml.DeleteJobRequest;
import org.elasticsearch.protocol.xpack.ml.DeleteJobResponse;
import org.elasticsearch.protocol.xpack.ml.GetJobRequest;
import org.elasticsearch.protocol.xpack.ml.GetJobResponse;
import org.elasticsearch.protocol.xpack.ml.OpenJobRequest;
import org.elasticsearch.protocol.xpack.ml.OpenJobResponse;
import org.elasticsearch.protocol.xpack.ml.PutJobRequest;
import org.elasticsearch.protocol.xpack.ml.PutJobResponse;
import org.elasticsearch.protocol.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.protocol.xpack.ml.job.config.DataDescription;
import org.elasticsearch.protocol.xpack.ml.job.config.Detector;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class MachineLearningIT extends ESRestHighLevelClientTestCase {

    public void testPutJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        PutJobResponse putJobResponse = execute(new PutJobRequest(job), machineLearningClient::putJob, machineLearningClient::putJobAsync);
        Job createdJob = putJobResponse.getResponse();

        assertThat(createdJob.getId(), is(jobId));
        assertThat(createdJob.getJobType(), is(Job.ANOMALY_DETECTOR_JOB_TYPE));
    }

    public void testGetJob() throws Exception {
        String jobId1 = randomValidJobId();
        String jobId2 = randomValidJobId();

        Job job1 = buildJob(jobId1);
        Job job2 = buildJob(jobId2);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job1), RequestOptions.DEFAULT);
        machineLearningClient.putJob(new PutJobRequest(job2), RequestOptions.DEFAULT);

        GetJobRequest request = new GetJobRequest(jobId1);
        request.addJobId(jobId2);

        // Test getting specific jobs
        GetJobResponse response = execute(request, machineLearningClient::getJob, machineLearningClient::getJobAsync);

        assertEquals(2, response.getCount());
        assertThat(response.getJobs(), hasSize(2));
        assertThat(response.getJobs().stream().map(Job::getId).collect(Collectors.toList()), containsInAnyOrder(jobId1, jobId2));

        // Test getting all jobs explicitly
        request = GetJobRequest.getAllJobsRequest();
        response = execute(request, machineLearningClient::getJob, machineLearningClient::getJobAsync);

        assertTrue(response.getCount() >= 2L);
        assertTrue(response.getJobs().size() >= 2L);
        assertThat(response.getJobs().stream().map(Job::getId).collect(Collectors.toList()), hasItems(jobId1, jobId2));

        // Test getting all jobs implicitly
        response = execute(new GetJobRequest(), machineLearningClient::getJob, machineLearningClient::getJobAsync);

        assertTrue(response.getCount() >= 2L);
        assertTrue(response.getJobs().size() >= 2L);
        assertThat(response.getJobs().stream().map(Job::getId).collect(Collectors.toList()), hasItems(jobId1, jobId2));
    }

    public void testDeleteJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        DeleteJobResponse response = execute(new DeleteJobRequest(jobId),
            machineLearningClient::deleteJob,
            machineLearningClient::deleteJobAsync);

        assertTrue(response.isAcknowledged());
    }

    public void testOpenJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        OpenJobResponse response = execute(new OpenJobRequest(jobId), machineLearningClient::openJob, machineLearningClient::openJobAsync);

        assertTrue(response.isOpened());
    }

    public static String randomValidJobId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz0123456789".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    public static Job buildJob(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        builder.setDescription(randomAlphaOfLength(10));

        Detector detector = new Detector.Builder()
            .setFieldName("total")
            .setFunction("sum")
            .setDetectorDescription(randomAlphaOfLength(10))
            .build();
        AnalysisConfig.Builder configBuilder = new AnalysisConfig.Builder(Arrays.asList(detector));
        configBuilder.setBucketSpan(new TimeValue(randomIntBetween(1, 10), TimeUnit.SECONDS));
        builder.setAnalysisConfig(configBuilder);

        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat(randomFrom(DataDescription.EPOCH_MS, DataDescription.EPOCH));
        dataDescription.setTimeField(randomAlphaOfLength(10));
        builder.setDataDescription(dataDescription);

        return builder.build();
    }
}
