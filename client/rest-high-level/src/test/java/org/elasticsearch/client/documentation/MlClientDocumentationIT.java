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
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.ESRestHighLevelClientTestCase;
import org.elasticsearch.client.MachineLearningGetResultsIT;
import org.elasticsearch.client.MachineLearningIT;
import org.elasticsearch.client.MlRestTestStateCleaner;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteJobResponse;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.ForecastJobResponse;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetCategoriesResponse;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetInfluencersResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetRecordsResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PostDataResponse;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutDatafeedResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.datafeed.ChunkingConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.AnalysisLimits;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.DetectionRule;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.ModelPlotConfig;
import org.elasticsearch.client.ml.job.config.Operator;
import org.elasticsearch.client.ml.job.config.RuleCondition;
import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.client.ml.job.results.AnomalyRecord;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.client.ml.job.results.Influencer;
import org.elasticsearch.client.ml.job.results.OverallBucket;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.client.ml.job.util.PageParams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.core.Is.is;

public class MlClientDocumentationIT extends ESRestHighLevelClientTestCase {

    @After
    public void cleanUp() throws IOException {
        new MlRestTestStateCleaner(logger, client()).clearMlMetadata();
    }

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

    public void testGetJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("get-machine-learning-job1");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("get-machine-learning-job2");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);

        {
            //tag::x-pack-ml-get-job-request
            GetJobRequest request = new GetJobRequest("get-machine-learning-job1", "get-machine-learning-job*"); //<1>
            request.setAllowNoJobs(true); //<2>
            //end::x-pack-ml-get-job-request

            //tag::x-pack-ml-get-job-execute
            GetJobResponse response = client.machineLearning().getJob(request, RequestOptions.DEFAULT);
            long numberOfJobs = response.count(); //<1>
            List<Job> jobs = response.jobs(); //<2>
            //end::x-pack-ml-get-job-execute

            assertEquals(2, response.count());
            assertThat(response.jobs(), hasSize(2));
            assertThat(response.jobs().stream().map(Job::getId).collect(Collectors.toList()),
                containsInAnyOrder(job.getId(), secondJob.getId()));
        }
        {
            GetJobRequest request = new GetJobRequest("get-machine-learning-job1", "get-machine-learning-job*");

            // tag::x-pack-ml-get-job-listener
            ActionListener<GetJobResponse> listener = new ActionListener<GetJobResponse>() {
                @Override
                public void onResponse(GetJobResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-ml-get-job-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-job-execute-async
            client.machineLearning().getJobAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-job-execute-async

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

    public void testUpdateJob() throws Exception {
        RestHighLevelClient client = highLevelClient();
        String jobId = "test-update-job";
        Job tempJob = MachineLearningIT.buildJob(jobId);
        Job job = new Job.Builder(tempJob)
            .setAnalysisConfig(new AnalysisConfig.Builder(tempJob.getAnalysisConfig())
                .setCategorizationFieldName("categorization-field")
                .setDetector(0,
                    new Detector.Builder().setFieldName("total")
                        .setFunction("sum")
                        .setPartitionFieldName("mlcategory")
                        .setDetectorDescription(randomAlphaOfLength(10))
                        .build()))
            .build();
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        {

            List<DetectionRule> detectionRules = Arrays.asList(
                new DetectionRule.Builder(Arrays.asList(RuleCondition.createTime(Operator.GT, 100L))).build());
            Map<String, Object> customSettings = new HashMap<>();
            customSettings.put("custom-setting-1", "custom-value");

            //tag::x-pack-ml-update-job-detector-options
            JobUpdate.DetectorUpdate detectorUpdate = new JobUpdate.DetectorUpdate(0, //<1>
                "detector description", //<2>
                detectionRules); //<3>
            //end::x-pack-ml-update-job-detector-options

            //tag::x-pack-ml-update-job-options
            JobUpdate update = new JobUpdate.Builder(jobId) //<1>
                .setDescription("My description") //<2>
                .setAnalysisLimits(new AnalysisLimits(1000L, null)) //<3>
                .setBackgroundPersistInterval(TimeValue.timeValueHours(3)) //<4>
                .setCategorizationFilters(Arrays.asList("categorization-filter")) //<5>
                .setDetectorUpdates(Arrays.asList(detectorUpdate)) //<6>
                .setGroups(Arrays.asList("job-group-1")) //<7>
                .setResultsRetentionDays(10L) //<8>
                .setModelPlotConfig(new ModelPlotConfig(true, null)) //<9>
                .setModelSnapshotRetentionDays(7L) //<10>
                .setCustomSettings(customSettings) //<11>
                .setRenormalizationWindowDays(3L) //<12>
                .build();
            //end::x-pack-ml-update-job-options


            //tag::x-pack-ml-update-job-request
            UpdateJobRequest updateJobRequest = new UpdateJobRequest(update); //<1>
            //end::x-pack-ml-update-job-request

            //tag::x-pack-ml-update-job-execute
            PutJobResponse updateJobResponse = client.machineLearning().updateJob(updateJobRequest, RequestOptions.DEFAULT);
            //end::x-pack-ml-update-job-execute
            //tag::x-pack-ml-update-job-response
            Job updatedJob = updateJobResponse.getResponse(); //<1>
            //end::x-pack-ml-update-job-response

            assertEquals(update.getDescription(), updatedJob.getDescription());
        }
        {
            //tag::x-pack-ml-update-job-listener
            ActionListener<PutJobResponse> listener = new ActionListener<PutJobResponse>() {
                @Override
                public void onResponse(PutJobResponse updateJobResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-update-job-listener
            UpdateJobRequest updateJobRequest = new UpdateJobRequest(new JobUpdate.Builder(jobId).build());

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-update-job-execute-async
            client.machineLearning().updateJobAsync(updateJobRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-update-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // We need to create a job for the datafeed request to be valid
            String jobId = "put-datafeed-job-1";
            Job job = MachineLearningIT.buildJob(jobId);
            client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

            String id = "datafeed-1";

            //tag::x-pack-ml-create-datafeed-config
            DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(id, jobId) // <1>
                    .setIndices("index_1", "index_2");  // <2>
            //end::x-pack-ml-create-datafeed-config

            AggregatorFactories.Builder aggs = AggregatorFactories.builder();

            //tag::x-pack-ml-create-datafeed-config-set-aggregations
            datafeedBuilder.setAggregations(aggs); // <1>
            //end::x-pack-ml-create-datafeed-config-set-aggregations

            // Clearing aggregation to avoid complex validation rules
            datafeedBuilder.setAggregations((String) null);

            //tag::x-pack-ml-create-datafeed-config-set-chunking-config
            datafeedBuilder.setChunkingConfig(ChunkingConfig.newAuto()); // <1>
            //end::x-pack-ml-create-datafeed-config-set-chunking-config

            //tag::x-pack-ml-create-datafeed-config-set-frequency
            datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(30)); // <1>
            //end::x-pack-ml-create-datafeed-config-set-frequency

            //tag::x-pack-ml-create-datafeed-config-set-query
            datafeedBuilder.setQuery(QueryBuilders.matchAllQuery()); // <1>
            //end::x-pack-ml-create-datafeed-config-set-query

            //tag::x-pack-ml-create-datafeed-config-set-query-delay
            datafeedBuilder.setQueryDelay(TimeValue.timeValueMinutes(1)); // <1>
            //end::x-pack-ml-create-datafeed-config-set-query-delay

            List<SearchSourceBuilder.ScriptField> scriptFields = Collections.emptyList();
            //tag::x-pack-ml-create-datafeed-config-set-script-fields
            datafeedBuilder.setScriptFields(scriptFields); // <1>
            //end::x-pack-ml-create-datafeed-config-set-script-fields

            //tag::x-pack-ml-create-datafeed-config-set-scroll-size
            datafeedBuilder.setScrollSize(1000); // <1>
            //end::x-pack-ml-create-datafeed-config-set-scroll-size

            //tag::x-pack-ml-put-datafeed-request
            PutDatafeedRequest request = new PutDatafeedRequest(datafeedBuilder.build()); // <1>
            //end::x-pack-ml-put-datafeed-request

            //tag::x-pack-ml-put-datafeed-execute
            PutDatafeedResponse response = client.machineLearning().putDatafeed(request, RequestOptions.DEFAULT);
            //end::x-pack-ml-put-datafeed-execute

            //tag::x-pack-ml-put-datafeed-response
            DatafeedConfig datafeed = response.getResponse(); // <1>
            //end::x-pack-ml-put-datafeed-response
            assertThat(datafeed.getId(), equalTo("datafeed-1"));
        }
        {
            // We need to create a job for the datafeed request to be valid
            String jobId = "put-datafeed-job-2";
            Job job = MachineLearningIT.buildJob(jobId);
            client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

            String id = "datafeed-2";

            DatafeedConfig datafeed = new DatafeedConfig.Builder(id, jobId).setIndices("index_1", "index_2").build();

            PutDatafeedRequest request = new PutDatafeedRequest(datafeed);
            // tag::x-pack-ml-put-datafeed-execute-listener
            ActionListener<PutDatafeedResponse> listener = new ActionListener<PutDatafeedResponse>() {
                @Override
                public void onResponse(PutDatafeedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-ml-put-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-put-datafeed-execute-async
            client.machineLearning().putDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-put-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetBuckets() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-buckets";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a bucket
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "doc");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-buckets\", \"result_type\":\"bucket\", \"timestamp\": 1533081600000," +
                        "\"bucket_span\": 600,\"is_interim\": false, \"anomaly_score\": 80.0}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::x-pack-ml-get-buckets-request
            GetBucketsRequest request = new GetBucketsRequest(jobId); // <1>
            // end::x-pack-ml-get-buckets-request

            // tag::x-pack-ml-get-buckets-timestamp
            request.setTimestamp("2018-08-17T00:00:00Z"); // <1>
            // end::x-pack-ml-get-buckets-timestamp

            // Set timestamp to null as it is incompatible with other args
            request.setTimestamp(null);

            // tag::x-pack-ml-get-buckets-anomaly-score
            request.setAnomalyScore(75.0); // <1>
            // end::x-pack-ml-get-buckets-anomaly-score

            // tag::x-pack-ml-get-buckets-desc
            request.setDescending(true); // <1>
            // end::x-pack-ml-get-buckets-desc

            // tag::x-pack-ml-get-buckets-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::x-pack-ml-get-buckets-end

            // tag::x-pack-ml-get-buckets-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::x-pack-ml-get-buckets-exclude-interim

            // tag::x-pack-ml-get-buckets-expand
            request.setExpand(true); // <1>
            // end::x-pack-ml-get-buckets-expand

            // tag::x-pack-ml-get-buckets-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::x-pack-ml-get-buckets-page

            // Set page params back to null so the response contains the bucket we indexed
            request.setPageParams(null);

            // tag::x-pack-ml-get-buckets-sort
            request.setSort("anomaly_score"); // <1>
            // end::x-pack-ml-get-buckets-sort

            // tag::x-pack-ml-get-buckets-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::x-pack-ml-get-buckets-start

            // tag::x-pack-ml-get-buckets-execute
            GetBucketsResponse response = client.machineLearning().getBuckets(request, RequestOptions.DEFAULT);
            // end::x-pack-ml-get-buckets-execute

            // tag::x-pack-ml-get-buckets-response
            long count = response.count(); // <1>
            List<Bucket> buckets = response.buckets(); // <2>
            // end::x-pack-ml-get-buckets-response
            assertEquals(1, buckets.size());
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(jobId);

            // tag::x-pack-ml-get-buckets-listener
            ActionListener<GetBucketsResponse> listener =
                    new ActionListener<GetBucketsResponse>() {
                        @Override
                        public void onResponse(GetBucketsResponse getBucketsResponse) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::x-pack-ml-get-buckets-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-buckets-execute-async
            client.machineLearning().getBucketsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-buckets-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testFlushJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("flushing-my-first-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("flushing-my-second-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(secondJob.getId()), RequestOptions.DEFAULT);

        {
            //tag::x-pack-ml-flush-job-request
            FlushJobRequest flushJobRequest = new FlushJobRequest("flushing-my-first-machine-learning-job"); //<1>
            //end::x-pack-ml-flush-job-request

            //tag::x-pack-ml-flush-job-request-options
            flushJobRequest.setCalcInterim(true); //<1>
            flushJobRequest.setAdvanceTime("2018-08-31T16:35:07+00:00"); //<2>
            flushJobRequest.setStart("2018-08-31T16:35:17+00:00"); //<3>
            flushJobRequest.setEnd("2018-08-31T16:35:27+00:00"); //<4>
            flushJobRequest.setSkipTime("2018-08-31T16:35:00+00:00"); //<5>
            //end::x-pack-ml-flush-job-request-options

            //tag::x-pack-ml-flush-job-execute
            FlushJobResponse flushJobResponse = client.machineLearning().flushJob(flushJobRequest, RequestOptions.DEFAULT);
            //end::x-pack-ml-flush-job-execute

            //tag::x-pack-ml-flush-job-response
            boolean isFlushed = flushJobResponse.isFlushed(); //<1>
            Date lastFinalizedBucketEnd = flushJobResponse.getLastFinalizedBucketEnd(); //<2>
            //end::x-pack-ml-flush-job-response

        }
        {
            //tag::x-pack-ml-flush-job-listener
            ActionListener<FlushJobResponse> listener = new ActionListener<FlushJobResponse>() {
                @Override
                public void onResponse(FlushJobResponse FlushJobResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-flush-job-listener
            FlushJobRequest flushJobRequest = new FlushJobRequest("flushing-my-second-machine-learning-job");

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-flush-job-execute-async
            client.machineLearning().flushJobAsync(flushJobRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-flush-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
    
    public void testDeleteForecast() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("deleting-forecast-for-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);
        PostDataRequest.JsonBuilder builder = new PostDataRequest.JsonBuilder();
        for(int i = 0; i < 30; i++) {
            Map<String, Object> hashMap = new HashMap<>();
            hashMap.put("total", randomInt(1000));
            hashMap.put("timestamp", (i+1)*1000);
            builder.addDoc(hashMap);
        }

        PostDataRequest postDataRequest = new PostDataRequest(job.getId(), builder);
        client.machineLearning().postData(postDataRequest, RequestOptions.DEFAULT);
        client.machineLearning().flushJob(new FlushJobRequest(job.getId()), RequestOptions.DEFAULT);
        ForecastJobResponse forecastJobResponse = client.machineLearning().
            forecastJob(new ForecastJobRequest(job.getId()), RequestOptions.DEFAULT);
        String forecastId = forecastJobResponse.getForecastId();

        GetRequest request = new GetRequest(".ml-anomalies-" + job.getId());
        request.id(job.getId() + "_model_forecast_request_stats_" + forecastId);
        assertBusy(() -> {
            GetResponse getResponse = highLevelClient().get(request, RequestOptions.DEFAULT);
            assertTrue(getResponse.isExists());
            assertTrue(getResponse.getSourceAsString().contains("finished"));
        }, 30, TimeUnit.SECONDS);

        {
            //tag::x-pack-ml-delete-forecast-request
            DeleteForecastRequest deleteForecastRequest = new DeleteForecastRequest("deleting-forecast-for-job"); //<1>
            //end::x-pack-ml-delete-forecast-request

            //tag::x-pack-ml-delete-forecast-request-options
            deleteForecastRequest.setForecastIds(forecastId); //<1>
            deleteForecastRequest.timeout("30s"); //<2>
            deleteForecastRequest.setAllowNoForecasts(true); //<3>
            //end::x-pack-ml-delete-forecast-request-options

            //tag::x-pack-ml-delete-forecast-execute
            AcknowledgedResponse deleteForecastResponse = client.machineLearning().deleteForecast(deleteForecastRequest,
                RequestOptions.DEFAULT);
            //end::x-pack-ml-delete-forecast-execute

            //tag::x-pack-ml-delete-forecast-response
            boolean isAcknowledged = deleteForecastResponse.isAcknowledged(); //<1>
            //end::x-pack-ml-delete-forecast-response
        }
        {
            //tag::x-pack-ml-delete-forecast-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse DeleteForecastResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-delete-forecast-listener
            DeleteForecastRequest deleteForecastRequest = DeleteForecastRequest.deleteAllForecasts(job.getId());
            deleteForecastRequest.setAllowNoForecasts(true);

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-delete-forecast-execute-async
            client.machineLearning().deleteForecastAsync(deleteForecastRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-delete-forecast-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
    
    public void testGetJobStats() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("get-machine-learning-job-stats1");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("get-machine-learning-job-stats2");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);

        {
            //tag::x-pack-ml-get-job-stats-request
            GetJobStatsRequest request = new GetJobStatsRequest("get-machine-learning-job-stats1", "get-machine-learning-job-*"); //<1>
            request.setAllowNoJobs(true); //<2>
            //end::x-pack-ml-get-job-stats-request

            //tag::x-pack-ml-get-job-stats-execute
            GetJobStatsResponse response = client.machineLearning().getJobStats(request, RequestOptions.DEFAULT);
            //end::x-pack-ml-get-job-stats-execute

            //tag::x-pack-ml-get-job-stats-response
            long numberOfJobStats = response.count(); //<1>
            List<JobStats> jobStats = response.jobStats(); //<2>
            //end::x-pack-ml-get-job-stats-response

            assertEquals(2, response.count());
            assertThat(response.jobStats(), hasSize(2));
            assertThat(response.jobStats().stream().map(JobStats::getJobId).collect(Collectors.toList()),
                containsInAnyOrder(job.getId(), secondJob.getId()));
        }
        {
            GetJobStatsRequest request = new GetJobStatsRequest("get-machine-learning-job-stats1", "get-machine-learning-job-*");

            // tag::x-pack-ml-get-job-stats-listener
            ActionListener<GetJobStatsResponse> listener = new ActionListener<GetJobStatsResponse>() {
                @Override
                public void onResponse(GetJobStatsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::x-pack-ml-get-job-stats-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-job-stats-execute-async
            client.machineLearning().getJobStatsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-job-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testForecastJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("forecasting-my-first-machine-learning-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

        PostDataRequest.JsonBuilder builder = new PostDataRequest.JsonBuilder();
        for(int i = 0; i < 30; i++) {
            Map<String, Object> hashMap = new HashMap<>();
            hashMap.put("total", randomInt(1000));
            hashMap.put("timestamp", (i+1)*1000);
            builder.addDoc(hashMap);
        }
        PostDataRequest postDataRequest = new PostDataRequest(job.getId(), builder);
        client.machineLearning().postData(postDataRequest, RequestOptions.DEFAULT);
        client.machineLearning().flushJob(new FlushJobRequest(job.getId()), RequestOptions.DEFAULT);

        {
            //tag::x-pack-ml-forecast-job-request
            ForecastJobRequest forecastJobRequest = new ForecastJobRequest("forecasting-my-first-machine-learning-job"); //<1>
            //end::x-pack-ml-forecast-job-request

            //tag::x-pack-ml-forecast-job-request-options
            forecastJobRequest.setExpiresIn(TimeValue.timeValueHours(48)); //<1>
            forecastJobRequest.setDuration(TimeValue.timeValueHours(24)); //<2>
            //end::x-pack-ml-forecast-job-request-options

            //tag::x-pack-ml-forecast-job-execute
            ForecastJobResponse forecastJobResponse = client.machineLearning().forecastJob(forecastJobRequest, RequestOptions.DEFAULT);
            //end::x-pack-ml-forecast-job-execute

            //tag::x-pack-ml-forecast-job-response
            boolean isAcknowledged = forecastJobResponse.isAcknowledged(); //<1>
            String forecastId = forecastJobResponse.getForecastId(); //<2>
            //end::x-pack-ml-forecast-job-response
            assertTrue(isAcknowledged);
            assertNotNull(forecastId);
        }
        {
            //tag::x-pack-ml-forecast-job-listener
            ActionListener<ForecastJobResponse> listener = new ActionListener<ForecastJobResponse>() {
                @Override
                public void onResponse(ForecastJobResponse forecastJobResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-forecast-job-listener
            ForecastJobRequest forecastJobRequest = new ForecastJobRequest("forecasting-my-first-machine-learning-job");

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-forecast-job-execute-async
            client.machineLearning().forecastJobAsync(forecastJobRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-forecast-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }
    
    public void testGetOverallBuckets() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId1 = "test-get-overall-buckets-1";
        String jobId2 = "test-get-overall-buckets-2";
        Job job1 = MachineLearningGetResultsIT.buildJob(jobId1);
        Job job2 = MachineLearningGetResultsIT.buildJob(jobId2);
        client.machineLearning().putJob(new PutJobRequest(job1), RequestOptions.DEFAULT);
        client.machineLearning().putJob(new PutJobRequest(job2), RequestOptions.DEFAULT);

        // Let us index some buckets
        BulkRequest bulkRequest = new BulkRequest();
        bulkRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        {
            IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "doc");
            indexRequest.source("{\"job_id\":\"test-get-overall-buckets-1\", \"result_type\":\"bucket\", \"timestamp\": 1533081600000," +
                    "\"bucket_span\": 600,\"is_interim\": false, \"anomaly_score\": 60.0}", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        {
            IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "doc");
            indexRequest.source("{\"job_id\":\"test-get-overall-buckets-2\", \"result_type\":\"bucket\", \"timestamp\": 1533081600000," +
                    "\"bucket_span\": 3600,\"is_interim\": false, \"anomaly_score\": 100.0}", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        client.bulk(bulkRequest, RequestOptions.DEFAULT);

        {
            // tag::x-pack-ml-get-overall-buckets-request
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(jobId1, jobId2); // <1>
            // end::x-pack-ml-get-overall-buckets-request

            // tag::x-pack-ml-get-overall-buckets-bucket-span
            request.setBucketSpan(TimeValue.timeValueHours(24)); // <1>
            // end::x-pack-ml-get-overall-buckets-bucket-span

            // tag::x-pack-ml-get-overall-buckets-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::x-pack-ml-get-overall-buckets-end

            // tag::x-pack-ml-get-overall-buckets-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::x-pack-ml-get-overall-buckets-exclude-interim

            // tag::x-pack-ml-get-overall-buckets-overall-score
            request.setOverallScore(75.0); // <1>
            // end::x-pack-ml-get-overall-buckets-overall-score

            // tag::x-pack-ml-get-overall-buckets-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::x-pack-ml-get-overall-buckets-start

            // tag::x-pack-ml-get-overall-buckets-top-n
            request.setTopN(2); // <1>
            // end::x-pack-ml-get-overall-buckets-top-n

            // tag::x-pack-ml-get-overall-buckets-execute
            GetOverallBucketsResponse response = client.machineLearning().getOverallBuckets(request, RequestOptions.DEFAULT);
            // end::x-pack-ml-get-overall-buckets-execute

            // tag::x-pack-ml-get-overall-buckets-response
            long count = response.count(); // <1>
            List<OverallBucket> overallBuckets = response.overallBuckets(); // <2>
            // end::x-pack-ml-get-overall-buckets-response

            assertEquals(1, overallBuckets.size());
            assertThat(overallBuckets.get(0).getOverallScore(), is(closeTo(80.0, 0.001)));

        }
        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(jobId1, jobId2);

            // tag::x-pack-ml-get-overall-buckets-listener
            ActionListener<GetOverallBucketsResponse> listener =
                    new ActionListener<GetOverallBucketsResponse>() {
                        @Override
                        public void onResponse(GetOverallBucketsResponse getOverallBucketsResponse) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::x-pack-ml-get-overall-buckets-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-overall-buckets-execute-async
            client.machineLearning().getOverallBucketsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-overall-buckets-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetRecords() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-records";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a record
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "doc");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-records\", \"result_type\":\"record\", \"timestamp\": 1533081600000," +
                "\"bucket_span\": 600,\"is_interim\": false, \"record_score\": 80.0}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::x-pack-ml-get-records-request
            GetRecordsRequest request = new GetRecordsRequest(jobId); // <1>
            // end::x-pack-ml-get-records-request

            // tag::x-pack-ml-get-records-desc
            request.setDescending(true); // <1>
            // end::x-pack-ml-get-records-desc

            // tag::x-pack-ml-get-records-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::x-pack-ml-get-records-end

            // tag::x-pack-ml-get-records-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::x-pack-ml-get-records-exclude-interim

            // tag::x-pack-ml-get-records-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::x-pack-ml-get-records-page

            // Set page params back to null so the response contains the record we indexed
            request.setPageParams(null);

            // tag::x-pack-ml-get-records-record-score
            request.setRecordScore(75.0); // <1>
            // end::x-pack-ml-get-records-record-score

            // tag::x-pack-ml-get-records-sort
            request.setSort("probability"); // <1>
            // end::x-pack-ml-get-records-sort

            // tag::x-pack-ml-get-records-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::x-pack-ml-get-records-start

            // tag::x-pack-ml-get-records-execute
            GetRecordsResponse response = client.machineLearning().getRecords(request, RequestOptions.DEFAULT);
            // end::x-pack-ml-get-records-execute

            // tag::x-pack-ml-get-records-response
            long count = response.count(); // <1>
            List<AnomalyRecord> records = response.records(); // <2>
            // end::x-pack-ml-get-records-response
            assertEquals(1, records.size());
        }
        {
            GetRecordsRequest request = new GetRecordsRequest(jobId);

            // tag::x-pack-ml-get-records-listener
            ActionListener<GetRecordsResponse> listener =
                    new ActionListener<GetRecordsResponse>() {
                        @Override
                        public void onResponse(GetRecordsResponse getRecordsResponse) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::x-pack-ml-get-records-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-records-execute-async
            client.machineLearning().getRecordsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-records-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPostData() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("test-post-data");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

        {
            //tag::x-pack-ml-post-data-request
            PostDataRequest.JsonBuilder jsonBuilder = new PostDataRequest.JsonBuilder(); //<1>
            Map<String, Object> mapData = new HashMap<>();
            mapData.put("total", 109);
            jsonBuilder.addDoc(mapData); //<2>
            jsonBuilder.addDoc("{\"total\":1000}"); //<3>
            PostDataRequest postDataRequest = new PostDataRequest("test-post-data", jsonBuilder); //<4>
            //end::x-pack-ml-post-data-request


            //tag::x-pack-ml-post-data-request-options
            postDataRequest.setResetStart("2018-08-31T16:35:07+00:00"); //<1>
            postDataRequest.setResetEnd("2018-08-31T16:35:17+00:00"); //<2>
            //end::x-pack-ml-post-data-request-options
            postDataRequest.setResetEnd(null);
            postDataRequest.setResetStart(null);

            //tag::x-pack-ml-post-data-execute
            PostDataResponse postDataResponse = client.machineLearning().postData(postDataRequest, RequestOptions.DEFAULT);
            //end::x-pack-ml-post-data-execute

            //tag::x-pack-ml-post-data-response
            DataCounts dataCounts = postDataResponse.getDataCounts(); //<1>
            //end::x-pack-ml-post-data-response
            assertEquals(2, dataCounts.getInputRecordCount());

        }
        {
            //tag::x-pack-ml-post-data-listener
            ActionListener<PostDataResponse> listener = new ActionListener<PostDataResponse>() {
                @Override
                public void onResponse(PostDataResponse postDataResponse) {
                    //<1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            //end::x-pack-ml-post-data-listener
            PostDataRequest.JsonBuilder jsonBuilder = new PostDataRequest.JsonBuilder();
            Map<String, Object> mapData = new HashMap<>();
            mapData.put("total", 109);
            jsonBuilder.addDoc(mapData);
            PostDataRequest postDataRequest = new PostDataRequest("test-post-data", jsonBuilder); //<1>

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-post-data-execute-async
            client.machineLearning().postDataAsync(postDataRequest, RequestOptions.DEFAULT, listener); //<1>
            // end::x-pack-ml-post-data-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetInfluencers() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-influencers";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a record
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "doc");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-influencers\", \"result_type\":\"influencer\", \"timestamp\": 1533081600000," +
                "\"bucket_span\": 600,\"is_interim\": false, \"influencer_score\": 80.0, \"influencer_field_name\": \"my_influencer\"," +
                "\"influencer_field_value\":\"foo\"}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::x-pack-ml-get-influencers-request
            GetInfluencersRequest request = new GetInfluencersRequest(jobId); // <1>
            // end::x-pack-ml-get-influencers-request

            // tag::x-pack-ml-get-influencers-desc
            request.setDescending(true); // <1>
            // end::x-pack-ml-get-influencers-desc

            // tag::x-pack-ml-get-influencers-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::x-pack-ml-get-influencers-end

            // tag::x-pack-ml-get-influencers-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::x-pack-ml-get-influencers-exclude-interim

            // tag::x-pack-ml-get-influencers-influencer-score
            request.setInfluencerScore(75.0); // <1>
            // end::x-pack-ml-get-influencers-influencer-score

            // tag::x-pack-ml-get-influencers-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::x-pack-ml-get-influencers-page

            // Set page params back to null so the response contains the influencer we indexed
            request.setPageParams(null);

            // tag::x-pack-ml-get-influencers-sort
            request.setSort("probability"); // <1>
            // end::x-pack-ml-get-influencers-sort

            // tag::x-pack-ml-get-influencers-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::x-pack-ml-get-influencers-start

            // tag::x-pack-ml-get-influencers-execute
            GetInfluencersResponse response = client.machineLearning().getInfluencers(request, RequestOptions.DEFAULT);
            // end::x-pack-ml-get-influencers-execute

            // tag::x-pack-ml-get-influencers-response
            long count = response.count(); // <1>
            List<Influencer> influencers = response.influencers(); // <2>
            // end::x-pack-ml-get-influencers-response
            assertEquals(1, influencers.size());
        }
        {
            GetInfluencersRequest request = new GetInfluencersRequest(jobId);

            // tag::x-pack-ml-get-influencers-listener
            ActionListener<GetInfluencersResponse> listener =
                    new ActionListener<GetInfluencersResponse>() {
                        @Override
                        public void onResponse(GetInfluencersResponse getInfluencersResponse) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::x-pack-ml-get-influencers-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-influencers-execute-async
            client.machineLearning().getInfluencersAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-influencers-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetCategories() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-categories";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a category
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "doc");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\": \"test-get-categories\", \"category_id\": 1, \"terms\": \"AAL\"," +
            " \"regex\": \".*?AAL.*\", \"max_matching_length\": 3, \"examples\": [\"AAL\"]}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::x-pack-ml-get-categories-request
            GetCategoriesRequest request = new GetCategoriesRequest(jobId); // <1>
            // end::x-pack-ml-get-categories-request

            // tag::x-pack-ml-get-categories-category-id
            request.setCategoryId(1L); // <1>
            // end::x-pack-ml-get-categories-category-id

            // tag::x-pack-ml-get-categories-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::x-pack-ml-get-categories-page

            // Set page params back to null so the response contains the category we indexed
            request.setPageParams(null);

            // tag::x-pack-ml-get-categories-execute
            GetCategoriesResponse response = client.machineLearning().getCategories(request, RequestOptions.DEFAULT);
            // end::x-pack-ml-get-categories-execute

            // tag::x-pack-ml-get-categories-response
            long count = response.count(); // <1>
            List<CategoryDefinition> categories = response.categories(); // <2>
            // end::x-pack-ml-get-categories-response
            assertEquals(1, categories.size());
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(jobId);

            // tag::x-pack-ml-get-categories-listener
            ActionListener<GetCategoriesResponse> listener =
                new ActionListener<GetCategoriesResponse>() {
                    @Override
                    public void onResponse(GetCategoriesResponse getcategoriesResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::x-pack-ml-get-categories-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::x-pack-ml-get-categories-execute-async
            client.machineLearning().getCategoriesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::x-pack-ml-get-categories-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
         }
    }
}
