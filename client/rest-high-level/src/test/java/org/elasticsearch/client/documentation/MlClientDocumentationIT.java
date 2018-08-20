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
package org.elasticsearch.client.documentation;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.LatchedActionListener;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.MachineLearningIT;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.protocol.xpack.ml.CloseJobRequest;
import org.elasticsearch.protocol.xpack.ml.CloseJobResponse;
import org.elasticsearch.protocol.xpack.ml.DeleteJobRequest;
import org.elasticsearch.protocol.xpack.ml.DeleteJobResponse;
import org.elasticsearch.protocol.xpack.ml.OpenJobRequest;
import org.elasticsearch.protocol.xpack.ml.OpenJobResponse;
import org.elasticsearch.protocol.xpack.ml.PutJobRequest;
import org.elasticsearch.protocol.xpack.ml.PutJobResponse;
import org.elasticsearch.protocol.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.protocol.xpack.ml.job.config.DataDescription;
import org.elasticsearch.protocol.xpack.ml.job.config.Detector;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;

import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThan;

public class MlClientDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testCreateJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        //tag::x-pack-ml-put-job-detector
        Detector.Builder detectorBuilder = new Detector.Builder()
            .setFunction("sum")                                    // <1>
            .setFieldName("total")                                 // <2>
            .setDetectorDescription("Sum of total");               // <3>
        //end::x-pack-ml-put-job-detector

        //tag::x-pack-ml-put-job-analysis-config
        List<Detector> detectors = Collections.singletonList(detectorBuilder.build());       // <1>
        AnalysisConfig.Builder analysisConfigBuilder = new AnalysisConfig.Builder(detectors) // <2>
            .setBucketSpan(TimeValue.timeValueMinutes(10));                                  // <3>
        //end::x-pack-ml-put-job-analysis-config

        //tag::x-pack-ml-put-job-data-description
        DataDescription.Builder dataDescriptionBuilder = new DataDescription.Builder()
            .setTimeField("timestamp");  // <1>
        //end::x-pack-ml-put-job-data-description

        {
            String id = "job_1";

            //tag::x-pack-ml-put-job-config
            Job.Builder jobBuilder = new Job.Builder(id)      // <1>
                .setAnalysisConfig(analysisConfigBuilder)     // <2>
                .setDataDescription(dataDescriptionBuilder)   // <3>
                .setDescription("Total sum of requests");     // <4>
            //end::x-pack-ml-put-job-config

            //tag::x-pack-ml-put-job-request
            PutJobRequest request = new PutJobRequest(jobBuilder.build()); // <1>
            //end::x-pack-ml-put-job-request

            //tag::x-pack-ml-put-job-execute
            PutJobResponse response = client.machineLearning().putJob(request, RequestOptions.DEFAULT);
            //end::x-pack-ml-put-job-execute

            //tag::x-pack-ml-put-job-response
            Date createTime = response.getResponse().getCreateTime(); // <1>
            //end::x-pack-ml-put-job-response
            assertThat(createTime.getTime(), greaterThan(0L));
        }
        {
            String id = "job_2";
            Job.Builder jobBuilder = new Job.Builder(id)
                .setAnalysisConfig(analysisConfigBuilder)
                .setDataDescription(dataDescriptionBuilder)
                .setDescription("Total sum of requests");

            PutJobRequest request = new PutJobRequest(jobBuilder.build());
            // tag::x-pack-ml-put-job-execute-listener
            ActionListener<PutJobResponse> listener = new ActionListener<PutJobResponse>() {
                @Override
                public void onResponse(PutJobResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-ml-put-job-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-put-job-execute-async
            client.machineLearning().putJobAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-put-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        String jobId = "my-first-machine-learning-job";

        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("my-second-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);

        {
            //tag::x-pack-delete-ml-job-request
            DeleteJobRequest deleteJobRequest = new DeleteJobRequest("my-first-machine-learning-job");
            deleteJobRequest.setForce(false); //<1>
            DeleteJobResponse deleteJobResponse = client.machineLearning().deleteJob(deleteJobRequest, RequestOptions.DEFAULT);
            //end::x-pack-delete-ml-job-request

            //tag::x-pack-delete-ml-job-response
            boolean isAcknowledged = deleteJobResponse.isAcknowledged(); //<1>
            //end::x-pack-delete-ml-job-response
        }
        {
            //tag::x-pack-delete-ml-job-request-listener
            ActionListener<DeleteJobResponse> listener = new ActionListener<DeleteJobResponse>() {
                @Override
                public void onResponse(DeleteJobResponse deleteJobResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-delete-ml-job-request-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::x-pack-delete-ml-job-request-async
            DeleteJobRequest deleteJobRequest = new DeleteJobRequest("my-second-machine-learning-job");
            client.machineLearning().deleteJobAsync(deleteJobRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::x-pack-delete-ml-job-request-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testOpenJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("opening-my-first-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("opening-my-second-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);

        {
            //tag::x-pack-ml-open-job-request
            OpenJobRequest openJobRequest = new OpenJobRequest("opening-my-first-machine-learning-job"); //<1>
            openJobRequest.setTimeout(TimeValue.timeValueMinutes(10)); //<2>
            //end::x-pack-ml-open-job-request

            //tag::x-pack-ml-open-job-execute
            OpenJobResponse openJobResponse = client.machineLearning().openJob(openJobRequest, RequestOptions.DEFAULT);
            boolean isOpened = openJobResponse.isOpened(); //<1>
            //end::x-pack-ml-open-job-execute

        }
        {
            //tag::x-pack-ml-open-job-listener
            ActionListener<OpenJobResponse> listener = new ActionListener<OpenJobResponse>() {
                @Override
                public void onResponse(OpenJobResponse openJobResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-open-job-listener
            OpenJobRequest openJobRequest = new OpenJobRequest("opening-my-second-machine-learning-job");
            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-open-job-execute-async
            client.machineLearning().openJobAsync(openJobRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-open-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
    
    public void testCloseJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Job job = MachineLearningIT.buildJob("closing-my-first-machine-learning-job");
            client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
            client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

            //tag::x-pack-ml-close-job-request
            CloseJobRequest closeJobRequest = new CloseJobRequest("closing-my-first-machine-learning-job", "otherjobs*"); //<1>
            closeJobRequest.setForce(false); //<2>
            closeJobRequest.setAllowNoJobs(true); //<3>
            closeJobRequest.setTimeout(TimeValue.timeValueMinutes(10)); //<4>
            //end::x-pack-ml-close-job-request

            //tag::x-pack-ml-close-job-execute
            CloseJobResponse closeJobResponse = client.machineLearning().closeJob(closeJobRequest, RequestOptions.DEFAULT);
            boolean isClosed = closeJobResponse.isClosed(); //<1>
            //end::x-pack-ml-close-job-execute

        }
        {
            Job job = MachineLearningIT.buildJob("closing-my-second-machine-learning-job");
            client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
            client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

            //tag::x-pack-ml-close-job-listener
            ActionListener<CloseJobResponse> listener = new ActionListener<CloseJobResponse>() {
                @Override
                public void onResponse(CloseJobResponse closeJobResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-close-job-listener
            CloseJobRequest closeJobRequest = new CloseJobRequest("closing-my-second-machine-learning-job");
            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-close-job-execute-async
            client.machineLearning().closeJobAsync(closeJobRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-close-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
