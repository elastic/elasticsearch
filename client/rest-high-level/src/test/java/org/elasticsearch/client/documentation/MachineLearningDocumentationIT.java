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
import org.elasticsearch.protocol.xpack.ml.DeleteJobRequest;
import org.elasticsearch.protocol.xpack.ml.DeleteJobResponse;
import org.elasticsearch.protocol.xpack.ml.PutJobRequest;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MachineLearningDocumentationIT extends ESRestHighLevelClientTestCase {

    public void testDeleteJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        String jobId = "my-first-machine-learning-job";

        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("my-second-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);

        {
            //tag::x-pack-delete-machine-learning-job-request
            DeleteJobRequest deleteJobRequest = new DeleteJobRequest("my-first-machine-learning-job");
            deleteJobRequest.setForce(false); //<1>
            DeleteJobResponse deleteJobResponse = client.machineLearning().deleteJob(deleteJobRequest, RequestOptions.DEFAULT);
            //end::x-pack-delete-machine-learning-job-request

            //tag::x-pack-delete-machine-learning-job-response
            boolean isAcknowledged = deleteJobResponse.isAcknowledged(); //<1>
            //end::x-pack-delete-machine-learning-job-response
        }
        {
            //tag::x-pack-delete-machine-learning-job-request-listener
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
            //end::x-pack-delete-machine-learning-job-request-listener


            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            //tag::x-pack-delete-machine-learning-job-request-async
            DeleteJobRequest deleteJobRequest = new DeleteJobRequest("my-second-machine-learning-job");
            client.machineLearning().deleteJobAsync(deleteJobRequest, RequestOptions.DEFAULT, listener); // <1>
            //end::x-pack-delete-machine-learning-job-request-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
}
