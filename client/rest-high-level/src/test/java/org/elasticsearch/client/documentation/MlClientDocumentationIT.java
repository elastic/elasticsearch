/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */
package org.elasticsearch.client.documentation;

import org.elasticsearch.ElasticsearchException;
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
import org.elasticsearch.client.MlTestStateCleaner;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.DeleteCalendarEventRequest;
import org.elasticsearch.client.ml.DeleteCalendarJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarRequest;
import org.elasticsearch.client.ml.DeleteDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataResponse;
import org.elasticsearch.client.ml.DeleteFilterRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteJobResponse;
import org.elasticsearch.client.ml.DeleteModelSnapshotRequest;
import org.elasticsearch.client.ml.DeleteTrainedModelAliasRequest;
import org.elasticsearch.client.ml.DeleteTrainedModelRequest;
import org.elasticsearch.client.ml.EstimateModelMemoryRequest;
import org.elasticsearch.client.ml.EstimateModelMemoryResponse;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameResponse;
import org.elasticsearch.client.ml.ExplainDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.ExplainDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.ForecastJobResponse;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetBucketsResponse;
import org.elasticsearch.client.ml.GetCalendarEventsRequest;
import org.elasticsearch.client.ml.GetCalendarEventsResponse;
import org.elasticsearch.client.ml.GetCalendarsRequest;
import org.elasticsearch.client.ml.GetCalendarsResponse;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetCategoriesResponse;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsStatsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsStatsResponse;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedResponse;
import org.elasticsearch.client.ml.GetDatafeedStatsRequest;
import org.elasticsearch.client.ml.GetDatafeedStatsResponse;
import org.elasticsearch.client.ml.GetFiltersRequest;
import org.elasticsearch.client.ml.GetFiltersResponse;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetInfluencersResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsResponse;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsResponse;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetRecordsResponse;
import org.elasticsearch.client.ml.GetTrainedModelsRequest;
import org.elasticsearch.client.ml.GetTrainedModelsResponse;
import org.elasticsearch.client.ml.GetTrainedModelsStatsRequest;
import org.elasticsearch.client.ml.GetTrainedModelsStatsResponse;
import org.elasticsearch.client.ml.MlInfoRequest;
import org.elasticsearch.client.ml.MlInfoResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PostCalendarEventRequest;
import org.elasticsearch.client.ml.PostCalendarEventResponse;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PostDataResponse;
import org.elasticsearch.client.ml.PreviewDatafeedRequest;
import org.elasticsearch.client.ml.PreviewDatafeedResponse;
import org.elasticsearch.client.ml.PutCalendarJobRequest;
import org.elasticsearch.client.ml.PutCalendarRequest;
import org.elasticsearch.client.ml.PutCalendarResponse;
import org.elasticsearch.client.ml.PutDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.PutDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutDatafeedResponse;
import org.elasticsearch.client.ml.PutFilterRequest;
import org.elasticsearch.client.ml.PutFilterResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.PutTrainedModelAliasRequest;
import org.elasticsearch.client.ml.PutTrainedModelRequest;
import org.elasticsearch.client.ml.PutTrainedModelResponse;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.RevertModelSnapshotResponse;
import org.elasticsearch.client.ml.SetUpgradeModeRequest;
import org.elasticsearch.client.ml.StartDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StartDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.StartDatafeedRequest;
import org.elasticsearch.client.ml.StartDatafeedResponse;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.StopDatafeedResponse;
import org.elasticsearch.client.ml.UpdateDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.UpdateDatafeedRequest;
import org.elasticsearch.client.ml.UpdateFilterRequest;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.UpdateModelSnapshotRequest;
import org.elasticsearch.client.ml.UpdateModelSnapshotResponse;
import org.elasticsearch.client.ml.UpgradeJobModelSnapshotRequest;
import org.elasticsearch.client.ml.UpgradeJobModelSnapshotResponse;
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.client.ml.calendars.ScheduledEventTests;
import org.elasticsearch.client.ml.datafeed.ChunkingConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedStats;
import org.elasticsearch.client.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.client.ml.datafeed.DelayedDataCheckConfig;
import org.elasticsearch.client.ml.dataframe.Classification;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalysis;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsStats;
import org.elasticsearch.client.ml.dataframe.QueryConfig;
import org.elasticsearch.client.ml.dataframe.Regression;
import org.elasticsearch.client.ml.dataframe.evaluation.Evaluation;
import org.elasticsearch.client.ml.dataframe.evaluation.EvaluationMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AccuracyMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AucRocMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric.ActualClass;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric.PredictedClass;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.common.AucRocResult;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.ConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.ConfusionMatrixMetric.ConfusionMatrix;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.HuberMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredLogarithmicErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetric;
import org.elasticsearch.client.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.client.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.client.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.client.ml.inference.TrainedModelDefinition;
import org.elasticsearch.client.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.client.ml.inference.TrainedModelInput;
import org.elasticsearch.client.ml.inference.TrainedModelStats;
import org.elasticsearch.client.ml.inference.TrainedModelType;
import org.elasticsearch.client.ml.inference.preprocessing.OneHotEncoding;
import org.elasticsearch.client.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.AnalysisLimits;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.DetectionRule;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.client.ml.job.config.ModelPlotConfig;
import org.elasticsearch.client.ml.job.config.Operator;
import org.elasticsearch.client.ml.job.config.RuleCondition;
import org.elasticsearch.client.ml.job.process.DataCounts;
import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.client.ml.job.results.AnomalyRecord;
import org.elasticsearch.client.ml.job.results.Bucket;
import org.elasticsearch.client.ml.job.results.CategoryDefinition;
import org.elasticsearch.client.ml.job.results.Influencer;
import org.elasticsearch.client.ml.job.results.OverallBucket;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.fetch.subphase.FetchSourceContext;
import org.elasticsearch.tasks.TaskId;
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
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.core.Is.is;

public class MlClientDocumentationIT extends ESRestHighLevelClientTestCase {

    private static final RequestOptions POST_DATA_OPTIONS = RequestOptions.DEFAULT.toBuilder()
        .setWarningsHandler(warnings -> Collections.singletonList("Posting data directly to anomaly detection jobs is deprecated, " +
            "in a future major version it will be compulsory to use a datafeed").equals(warnings) == false).build();

    @After
    public void cleanUp() throws IOException {
        new MlTestStateCleaner(logger, highLevelClient()).clearMlMetadata();
    }

    public void testCreateJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        // tag::put-job-detector
        Detector.Builder detectorBuilder = new Detector.Builder()
            .setFunction("sum")                                    // <1>
            .setFieldName("total")                                 // <2>
            .setDetectorDescription("Sum of total");               // <3>
        // end::put-job-detector

        // tag::put-job-analysis-config
        List<Detector> detectors = Collections.singletonList(detectorBuilder.build());       // <1>
        AnalysisConfig.Builder analysisConfigBuilder = new AnalysisConfig.Builder(detectors) // <2>
            .setBucketSpan(TimeValue.timeValueMinutes(10));                                  // <3>
        // end::put-job-analysis-config

        // tag::put-job-data-description
        DataDescription.Builder dataDescriptionBuilder = new DataDescription.Builder()
            .setTimeField("timestamp");  // <1>
        // end::put-job-data-description

        {
            String id = "job_1";

            // tag::put-job-config
            Job.Builder jobBuilder = new Job.Builder(id)      // <1>
                .setAnalysisConfig(analysisConfigBuilder)     // <2>
                .setDataDescription(dataDescriptionBuilder)   // <3>
                .setDescription("Total sum of requests");     // <4>
            // end::put-job-config

            // tag::put-job-request
            PutJobRequest request = new PutJobRequest(jobBuilder.build()); // <1>
            // end::put-job-request

            // tag::put-job-execute
            PutJobResponse response = client.machineLearning().putJob(request, RequestOptions.DEFAULT);
            // end::put-job-execute

            // tag::put-job-response
            Date createTime = response.getResponse().getCreateTime(); // <1>
            // end::put-job-response
            assertThat(createTime.getTime(), greaterThan(0L));
        }
        {
            String id = "job_2";
            Job.Builder jobBuilder = new Job.Builder(id)
                .setAnalysisConfig(analysisConfigBuilder)
                .setDataDescription(dataDescriptionBuilder)
                .setDescription("Total sum of requests");

            PutJobRequest request = new PutJobRequest(jobBuilder.build());
            // tag::put-job-execute-listener
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
            // end::put-job-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-job-execute-async
            client.machineLearning().putJobAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-job-execute-async

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
            // tag::get-job-request
            GetJobRequest request = new GetJobRequest("get-machine-learning-job1", "get-machine-learning-job*"); // <1>
            request.setAllowNoMatch(true); // <2>
            request.setExcludeGenerated(false); // <3>
            // end::get-job-request

            // tag::get-job-execute
            GetJobResponse response = client.machineLearning().getJob(request, RequestOptions.DEFAULT);
            // end::get-job-execute

            // tag::get-job-response
            long numberOfJobs = response.count(); // <1>
            List<Job> jobs = response.jobs(); // <2>
            // end::get-job-response
            assertEquals(2, response.count());
            assertThat(response.jobs(), hasSize(2));
            assertThat(response.jobs().stream().map(Job::getId).collect(Collectors.toList()),
                containsInAnyOrder(job.getId(), secondJob.getId()));
        }
        {
            GetJobRequest request = new GetJobRequest("get-machine-learning-job1", "get-machine-learning-job*");

            // tag::get-job-execute-listener
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
            // end::get-job-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-job-execute-async
            client.machineLearning().getJobAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-job-execute-async

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
            //tag::delete-job-request
            DeleteJobRequest deleteJobRequest = new DeleteJobRequest("my-first-machine-learning-job"); // <1>
            //end::delete-job-request

            //tag::delete-job-request-force
            deleteJobRequest.setForce(false); // <1>
            //end::delete-job-request-force

            //tag::delete-job-request-wait-for-completion
            deleteJobRequest.setWaitForCompletion(true); // <1>
            //end::delete-job-request-wait-for-completion

            //tag::delete-job-execute
            DeleteJobResponse deleteJobResponse = client.machineLearning().deleteJob(deleteJobRequest, RequestOptions.DEFAULT);
            //end::delete-job-execute

            //tag::delete-job-response
            Boolean isAcknowledged = deleteJobResponse.getAcknowledged(); // <1>
            TaskId task = deleteJobResponse.getTask(); // <2>
            //end::delete-job-response

            assertTrue(isAcknowledged);
            assertNull(task);
        }
        {
            //tag::delete-job-execute-listener
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
            // end::delete-job-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            DeleteJobRequest deleteJobRequest = new DeleteJobRequest("my-second-machine-learning-job");
            // tag::delete-job-execute-async
            client.machineLearning().deleteJobAsync(deleteJobRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-job-execute-async

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
            // tag::open-job-request
            OpenJobRequest openJobRequest = new OpenJobRequest("opening-my-first-machine-learning-job"); // <1>
            openJobRequest.setTimeout(TimeValue.timeValueMinutes(10)); // <2>
            // end::open-job-request

            // tag::open-job-execute
            OpenJobResponse openJobResponse = client.machineLearning().openJob(openJobRequest, RequestOptions.DEFAULT);
            // end::open-job-execute

            // tag::open-job-response
            boolean isOpened = openJobResponse.isOpened(); // <1>
            String node = openJobResponse.getNode(); // <2>
            // end::open-job-response

            assertThat(node, notNullValue());
        }
        {
            // tag::open-job-execute-listener
            ActionListener<OpenJobResponse> listener = new ActionListener<OpenJobResponse>() {
                @Override
                public void onResponse(OpenJobResponse openJobResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::open-job-execute-listener
            OpenJobRequest openJobRequest = new OpenJobRequest("opening-my-second-machine-learning-job");
            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::open-job-execute-async
            client.machineLearning().openJobAsync(openJobRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::open-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testCloseJob() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            Job job = MachineLearningIT.buildJob("closing-my-first-machine-learning-job");
            client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
            client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

            // tag::close-job-request
            CloseJobRequest closeJobRequest = new CloseJobRequest("closing-my-first-machine-learning-job", "otherjobs*"); // <1>
            closeJobRequest.setForce(false); // <2>
            closeJobRequest.setAllowNoMatch(true); // <3>
            closeJobRequest.setTimeout(TimeValue.timeValueMinutes(10)); // <4>
            // end::close-job-request

            // tag::close-job-execute
            CloseJobResponse closeJobResponse = client.machineLearning().closeJob(closeJobRequest, RequestOptions.DEFAULT);
            // end::close-job-execute

            // tag::close-job-response
            boolean isClosed = closeJobResponse.isClosed(); // <1>
            // end::close-job-response

        }
        {
            Job job = MachineLearningIT.buildJob("closing-my-second-machine-learning-job");
            client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
            client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

            // tag::close-job-execute-listener
            ActionListener<CloseJobResponse> listener = new ActionListener<CloseJobResponse>() {
                @Override
                public void onResponse(CloseJobResponse closeJobResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::close-job-execute-listener
            CloseJobRequest closeJobRequest = new CloseJobRequest("closing-my-second-machine-learning-job");

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::close-job-execute-async
            client.machineLearning().closeJobAsync(closeJobRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::close-job-execute-async

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

            // tag::update-job-detector-options
            JobUpdate.DetectorUpdate detectorUpdate = new JobUpdate.DetectorUpdate(0, // <1>
                "detector description", // <2>
                detectionRules); // <3>
            // end::update-job-detector-options

            // tag::update-job-options
            JobUpdate update = new JobUpdate.Builder(jobId) // <1>
                .setDescription("My description") // <2>
                .setAnalysisLimits(new AnalysisLimits(1000L, null)) // <3>
                .setBackgroundPersistInterval(TimeValue.timeValueHours(3)) // <4>
                .setDetectorUpdates(Arrays.asList(detectorUpdate)) // <5>
                .setGroups(Arrays.asList("job-group-1")) // <6>
                .setResultsRetentionDays(10L) // <7>
                .setModelPlotConfig(new ModelPlotConfig(true, null, true)) // <8>
                .setModelSnapshotRetentionDays(7L) // <9>
                .setCustomSettings(customSettings) // <10>
                .setRenormalizationWindowDays(3L) // <11>
                .build();
            // end::update-job-options


            // tag::update-job-request
            UpdateJobRequest updateJobRequest = new UpdateJobRequest(update); // <1>
            // end::update-job-request

            // tag::update-job-execute
            PutJobResponse updateJobResponse = client.machineLearning().updateJob(updateJobRequest, RequestOptions.DEFAULT);
            // end::update-job-execute

            // tag::update-job-response
            Job updatedJob = updateJobResponse.getResponse(); // <1>
            // end::update-job-response

            assertEquals(update.getDescription(), updatedJob.getDescription());
        }
        {
            // tag::update-job-execute-listener
            ActionListener<PutJobResponse> listener = new ActionListener<PutJobResponse>() {
                @Override
                public void onResponse(PutJobResponse updateJobResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::update-job-execute-listener
            UpdateJobRequest updateJobRequest = new UpdateJobRequest(new JobUpdate.Builder(jobId).build());

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-job-execute-async
            client.machineLearning().updateJobAsync(updateJobRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::update-job-execute-async

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

            // tag::put-datafeed-config
            DatafeedConfig.Builder datafeedBuilder = new DatafeedConfig.Builder(id, jobId) // <1>
                    .setIndices("index_1", "index_2");  // <2>
            // end::put-datafeed-config

            AggregatorFactories.Builder aggs = AggregatorFactories.builder();

            // tag::put-datafeed-config-set-aggregations
            datafeedBuilder.setAggregations(aggs); // <1>
            // end::put-datafeed-config-set-aggregations

            // Clearing aggregation to avoid complex validation rules
            datafeedBuilder.setAggregations((String) null);

            // tag::put-datafeed-config-set-chunking-config
            datafeedBuilder.setChunkingConfig(ChunkingConfig.newAuto()); // <1>
            // end::put-datafeed-config-set-chunking-config

            // tag::put-datafeed-config-set-frequency
            datafeedBuilder.setFrequency(TimeValue.timeValueSeconds(30)); // <1>
            // end::put-datafeed-config-set-frequency

            // tag::put-datafeed-config-set-query
            datafeedBuilder.setQuery(QueryBuilders.matchAllQuery()); // <1>
            // end::put-datafeed-config-set-query

            // tag::put-datafeed-config-set-query-delay
            datafeedBuilder.setQueryDelay(TimeValue.timeValueMinutes(1)); // <1>
            // end::put-datafeed-config-set-query-delay

            // tag::put-datafeed-config-set-delayed-data-check-config
            datafeedBuilder.setDelayedDataCheckConfig(DelayedDataCheckConfig
                .enabledDelayedDataCheckConfig(TimeValue.timeValueHours(1))); // <1>
            // end::put-datafeed-config-set-delayed-data-check-config

            // no need to accidentally trip internal validations due to job bucket size
            datafeedBuilder.setDelayedDataCheckConfig(null);

            List<SearchSourceBuilder.ScriptField> scriptFields = Collections.emptyList();
            // tag::put-datafeed-config-set-script-fields
            datafeedBuilder.setScriptFields(scriptFields); // <1>
            // end::put-datafeed-config-set-script-fields

            // tag::put-datafeed-config-set-scroll-size
            datafeedBuilder.setScrollSize(1000); // <1>
            // end::put-datafeed-config-set-scroll-size

            // tag::put-datafeed-config-set-runtime-mappings
            Map<String, Object> fieldProperties = new HashMap<>();
            fieldProperties.put("type", "keyword");
            fieldProperties.put("script", "emit(params._source.agent.toLowerCase())");
            Map<String, Object> runtimeMappings = new HashMap<>();
            runtimeMappings.put("agent_lowercase", fieldProperties);

            datafeedBuilder.setRuntimeMappings(runtimeMappings); // <1>
            // end::put-datafeed-config-set-runtime-mappings

            // tag::put-datafeed-request
            PutDatafeedRequest request = new PutDatafeedRequest(datafeedBuilder.build()); // <1>
            // end::put-datafeed-request

            // tag::put-datafeed-execute
            PutDatafeedResponse response = client.machineLearning().putDatafeed(request, RequestOptions.DEFAULT);
            // end::put-datafeed-execute

            // tag::put-datafeed-response
            DatafeedConfig datafeed = response.getResponse(); // <1>
            // end::put-datafeed-response
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
            // tag::put-datafeed-execute-listener
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
            // end::put-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-datafeed-execute-async
            client.machineLearning().putDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpdateDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("update-datafeed-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        String datafeedId = job.getId() + "-feed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, job.getId()).setIndices("foo").build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);

        {
            AggregatorFactories.Builder aggs = AggregatorFactories.builder();
            List<SearchSourceBuilder.ScriptField> scriptFields = Collections.emptyList();
            Map<String, Object> runtimeMappings = Collections.emptyMap();
            // tag::update-datafeed-config
            DatafeedUpdate.Builder datafeedUpdateBuilder = new DatafeedUpdate.Builder(datafeedId) // <1>
                .setAggregations(aggs) // <2>
                .setIndices("index_1", "index_2") // <3>
                .setChunkingConfig(ChunkingConfig.newAuto()) // <4>
                .setFrequency(TimeValue.timeValueSeconds(30)) // <5>
                .setQuery(QueryBuilders.matchAllQuery()) // <6>
                .setQueryDelay(TimeValue.timeValueMinutes(1)) // <7>
                .setScriptFields(scriptFields) // <8>
                .setScrollSize(1000) // <9>
                .setRuntimeMappings(runtimeMappings); // <10>
            // end::update-datafeed-config

            // Clearing aggregation to avoid complex validation rules
            datafeedUpdateBuilder.setAggregations((String) null);

            // tag::update-datafeed-request
            UpdateDatafeedRequest request = new UpdateDatafeedRequest(datafeedUpdateBuilder.build()); // <1>
            // end::update-datafeed-request

            // tag::update-datafeed-execute
            PutDatafeedResponse response = client.machineLearning().updateDatafeed(request, RequestOptions.DEFAULT);
            // end::update-datafeed-execute

            // tag::update-datafeed-response
            DatafeedConfig updatedDatafeed = response.getResponse(); // <1>
            // end::update-datafeed-response
            assertThat(updatedDatafeed.getId(), equalTo(datafeedId));
        }
        {
            DatafeedUpdate datafeedUpdate = new DatafeedUpdate.Builder(datafeedId).setIndices("index_1", "index_2").build();

            UpdateDatafeedRequest request = new UpdateDatafeedRequest(datafeedUpdate);
            // tag::update-datafeed-execute-listener
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
            // end::update-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-datafeed-execute-async
            client.machineLearning().updateDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("get-datafeed-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        String datafeedId = job.getId() + "-feed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, job.getId()).setIndices("foo").build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);

        {
            // tag::get-datafeed-request
            GetDatafeedRequest request = new GetDatafeedRequest(datafeedId); // <1>
            request.setAllowNoMatch(true); // <2>
            request.setExcludeGenerated(false); // <3>
            // end::get-datafeed-request

            // tag::get-datafeed-execute
            GetDatafeedResponse response = client.machineLearning().getDatafeed(request, RequestOptions.DEFAULT);
            // end::get-datafeed-execute

            // tag::get-datafeed-response
            long numberOfDatafeeds = response.count(); // <1>
            List<DatafeedConfig> datafeeds = response.datafeeds(); // <2>
            // end::get-datafeed-response

            assertEquals(1, numberOfDatafeeds);
            assertEquals(1, datafeeds.size());
        }
        {
            GetDatafeedRequest request = new GetDatafeedRequest(datafeedId);

            // tag::get-datafeed-execute-listener
            ActionListener<GetDatafeedResponse> listener = new ActionListener<GetDatafeedResponse>() {
                @Override
                public void onResponse(GetDatafeedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-datafeed-execute-async
            client.machineLearning().getDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-delete-datafeed-job";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        String datafeedId = "test-delete-datafeed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, jobId).setIndices("foo").build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);

        {
            // tag::delete-datafeed-request
            DeleteDatafeedRequest deleteDatafeedRequest = new DeleteDatafeedRequest(datafeedId);
            deleteDatafeedRequest.setForce(false); // <1>
            // end::delete-datafeed-request

            // tag::delete-datafeed-execute
            AcknowledgedResponse deleteDatafeedResponse = client.machineLearning().deleteDatafeed(
                deleteDatafeedRequest, RequestOptions.DEFAULT);
            // end::delete-datafeed-execute

            // tag::delete-datafeed-response
            boolean isAcknowledged = deleteDatafeedResponse.isAcknowledged(); // <1>
            // end::delete-datafeed-response
        }

        // Recreate datafeed to allow second deletion
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);

        {
            // tag::delete-datafeed-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            DeleteDatafeedRequest deleteDatafeedRequest = new DeleteDatafeedRequest(datafeedId);

            // tag::delete-datafeed-execute-async
            client.machineLearning().deleteDatafeedAsync(deleteDatafeedRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPreviewDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("preview-datafeed-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        String datafeedId = job.getId() + "-feed";
        String indexName = "preview_data_2";
        createIndex(indexName);
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, job.getId())
            .setIndices(indexName)
            .build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);
        {
            // tag::preview-datafeed-request
            PreviewDatafeedRequest request = new PreviewDatafeedRequest(datafeedId); // <1>
            // end::preview-datafeed-request

            // tag::preview-datafeed-execute
            PreviewDatafeedResponse response = client.machineLearning().previewDatafeed(request, RequestOptions.DEFAULT);
            // end::preview-datafeed-execute

            // tag::preview-datafeed-response
            BytesReference rawPreview = response.getPreview(); // <1>
            List<Map<String, Object>> semiParsedPreview = response.getDataList(); // <2>
            // end::preview-datafeed-response

            assertTrue(semiParsedPreview.isEmpty());
        }
        {
            PreviewDatafeedRequest request = new PreviewDatafeedRequest(datafeedId);

            // tag::preview-datafeed-execute-listener
            ActionListener<PreviewDatafeedResponse> listener = new ActionListener<PreviewDatafeedResponse>() {
                @Override
                public void onResponse(PreviewDatafeedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::preview-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::preview-datafeed-execute-async
            client.machineLearning().previewDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::preview-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testStartDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("start-datafeed-job");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        String datafeedId = job.getId() + "-feed";
        String indexName = "start_data_2";
        createIndex(indexName);
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, job.getId())
            .setIndices(indexName)
            .build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);
        {
            // tag::start-datafeed-request
            StartDatafeedRequest request = new StartDatafeedRequest(datafeedId); // <1>
            // end::start-datafeed-request

            // tag::start-datafeed-request-options
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            request.setStart("2018-08-20T00:00:00Z"); // <2>
            request.setTimeout(TimeValue.timeValueMinutes(10)); // <3>
            // end::start-datafeed-request-options

            // tag::start-datafeed-execute
            StartDatafeedResponse response = client.machineLearning().startDatafeed(request, RequestOptions.DEFAULT);
            // end::start-datafeed-execute

            // tag::start-datafeed-response
            boolean started = response.isStarted(); // <1>
            String node = response.getNode(); // <2>
            // end::start-datafeed-response

            assertTrue(started);
            assertThat(node, notNullValue());
        }
        {
            StartDatafeedRequest request = new StartDatafeedRequest(datafeedId);

            // tag::start-datafeed-execute-listener
            ActionListener<StartDatafeedResponse> listener = new ActionListener<StartDatafeedResponse>() {
                @Override
                public void onResponse(StartDatafeedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::start-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::start-datafeed-execute-async
            client.machineLearning().startDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::start-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testStopDatafeed() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::stop-datafeed-request
            StopDatafeedRequest request = new StopDatafeedRequest("datafeed_id1", "datafeed_id*"); // <1>
            // end::stop-datafeed-request
            request = StopDatafeedRequest.stopAllDatafeedsRequest();

            // tag::stop-datafeed-request-options
            request.setAllowNoMatch(true); // <1>
            request.setForce(true); // <2>
            request.setTimeout(TimeValue.timeValueMinutes(10)); // <3>
            // end::stop-datafeed-request-options

            // tag::stop-datafeed-execute
            StopDatafeedResponse response = client.machineLearning().stopDatafeed(request, RequestOptions.DEFAULT);
            // end::stop-datafeed-execute
            // tag::stop-datafeed-response
            boolean stopped = response.isStopped(); // <1>
            // end::stop-datafeed-response

            assertTrue(stopped);
        }
        {
            StopDatafeedRequest request = StopDatafeedRequest.stopAllDatafeedsRequest();

            // tag::stop-datafeed-execute-listener
            ActionListener<StopDatafeedResponse> listener = new ActionListener<StopDatafeedResponse>() {
                @Override
                public void onResponse(StopDatafeedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::stop-datafeed-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::stop-datafeed-execute-async
            client.machineLearning().stopDatafeedAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::stop-datafeed-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetDatafeedStats() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("get-machine-learning-datafeed-stats1");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        Job secondJob = MachineLearningIT.buildJob("get-machine-learning-datafeed-stats2");
        client.machineLearning().putJob(new PutJobRequest(secondJob), RequestOptions.DEFAULT);
        String datafeedId1 = job.getId() + "-feed";
        String indexName = "datafeed_stats_data_2";
        createIndex(indexName);
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId1, job.getId())
            .setIndices(indexName)
            .build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);

        String datafeedId2 = secondJob.getId() + "-feed";
        DatafeedConfig secondDatafeed = DatafeedConfig.builder(datafeedId2, secondJob.getId())
            .setIndices(indexName)
            .build();
        client.machineLearning().putDatafeed(new PutDatafeedRequest(secondDatafeed), RequestOptions.DEFAULT);

        {
            //tag::get-datafeed-stats-request
            GetDatafeedStatsRequest request =
                new GetDatafeedStatsRequest("get-machine-learning-datafeed-stats1-feed", "get-machine-learning-datafeed*"); // <1>
            request.setAllowNoMatch(true); // <2>
            //end::get-datafeed-stats-request

            //tag::get-datafeed-stats-execute
            GetDatafeedStatsResponse response = client.machineLearning().getDatafeedStats(request, RequestOptions.DEFAULT);
            //end::get-datafeed-stats-execute

            //tag::get-datafeed-stats-response
            long numberOfDatafeedStats = response.count(); // <1>
            List<DatafeedStats> datafeedStats = response.datafeedStats(); // <2>
            //end::get-datafeed-stats-response

            assertEquals(2, response.count());
            assertThat(response.datafeedStats(), hasSize(2));
            assertThat(response.datafeedStats().stream().map(DatafeedStats::getDatafeedId).collect(Collectors.toList()),
                containsInAnyOrder(datafeed.getId(), secondDatafeed.getId()));
        }
        {
            GetDatafeedStatsRequest request = new GetDatafeedStatsRequest("*");

            // tag::get-datafeed-stats-execute-listener
            ActionListener<GetDatafeedStatsResponse> listener = new ActionListener<GetDatafeedStatsResponse>() {
                @Override
                public void onResponse(GetDatafeedStatsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-datafeed-stats-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-datafeed-stats-execute-async
            client.machineLearning().getDatafeedStatsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-datafeed-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetBuckets() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-buckets";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a bucket
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-buckets\", \"result_type\":\"bucket\", \"timestamp\": 1533081600000," +
                        "\"bucket_span\": 600,\"is_interim\": false, \"anomaly_score\": 80.0}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::get-buckets-request
            GetBucketsRequest request = new GetBucketsRequest(jobId); // <1>
            // end::get-buckets-request

            // tag::get-buckets-timestamp
            request.setTimestamp("2018-08-17T00:00:00Z"); // <1>
            // end::get-buckets-timestamp

            // Set timestamp to null as it is incompatible with other args
            request.setTimestamp(null);

            // tag::get-buckets-anomaly-score
            request.setAnomalyScore(75.0); // <1>
            // end::get-buckets-anomaly-score

            // tag::get-buckets-desc
            request.setDescending(true); // <1>
            // end::get-buckets-desc

            // tag::get-buckets-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::get-buckets-end

            // tag::get-buckets-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::get-buckets-exclude-interim

            // tag::get-buckets-expand
            request.setExpand(true); // <1>
            // end::get-buckets-expand

            // tag::get-buckets-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::get-buckets-page

            // Set page params back to null so the response contains the bucket we indexed
            request.setPageParams(null);

            // tag::get-buckets-sort
            request.setSort("anomaly_score"); // <1>
            // end::get-buckets-sort

            // tag::get-buckets-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::get-buckets-start

            // tag::get-buckets-execute
            GetBucketsResponse response = client.machineLearning().getBuckets(request, RequestOptions.DEFAULT);
            // end::get-buckets-execute

            // tag::get-buckets-response
            long count = response.count(); // <1>
            List<Bucket> buckets = response.buckets(); // <2>
            // end::get-buckets-response
            assertEquals(1, buckets.size());
        }
        {
            GetBucketsRequest request = new GetBucketsRequest(jobId);

            // tag::get-buckets-execute-listener
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
            // end::get-buckets-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-buckets-execute-async
            client.machineLearning().getBucketsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-buckets-execute-async

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
            // tag::flush-job-request
            FlushJobRequest flushJobRequest = new FlushJobRequest("flushing-my-first-machine-learning-job"); // <1>
            // end::flush-job-request

            // tag::flush-job-request-options
            flushJobRequest.setCalcInterim(true); // <1>
            flushJobRequest.setAdvanceTime("2018-08-31T16:35:07+00:00"); // <2>
            flushJobRequest.setStart("2018-08-31T16:35:17+00:00"); // <3>
            flushJobRequest.setEnd("2018-08-31T16:35:27+00:00"); // <4>
            flushJobRequest.setSkipTime("2018-08-31T16:35:00+00:00"); // <5>
            // end::flush-job-request-options

            // tag::flush-job-execute
            FlushJobResponse flushJobResponse = client.machineLearning().flushJob(flushJobRequest, RequestOptions.DEFAULT);
            // end::flush-job-execute

            // tag::flush-job-response
            boolean isFlushed = flushJobResponse.isFlushed(); // <1>
            Date lastFinalizedBucketEnd = flushJobResponse.getLastFinalizedBucketEnd(); // <2>
            // end::flush-job-response

        }
        {
            // tag::flush-job-execute-listener
            ActionListener<FlushJobResponse> listener = new ActionListener<FlushJobResponse>() {
                @Override
                public void onResponse(FlushJobResponse FlushJobResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::flush-job-execute-listener
            FlushJobRequest flushJobRequest = new FlushJobRequest("flushing-my-second-machine-learning-job");

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::flush-job-execute-async
            client.machineLearning().flushJobAsync(flushJobRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::flush-job-execute-async

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
        // Post data is deprecated, so expect a deprecation warning
        client.machineLearning().postData(postDataRequest, POST_DATA_OPTIONS);
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
            // tag::delete-forecast-request
            DeleteForecastRequest deleteForecastRequest = new DeleteForecastRequest("deleting-forecast-for-job"); // <1>
            // end::delete-forecast-request

            // tag::delete-forecast-request-options
            deleteForecastRequest.setForecastIds(forecastId); // <1>
            deleteForecastRequest.timeout("30s"); // <2>
            deleteForecastRequest.setAllowNoForecasts(true); // <3>
            // end::delete-forecast-request-options

            // tag::delete-forecast-execute
            AcknowledgedResponse deleteForecastResponse = client.machineLearning().deleteForecast(deleteForecastRequest,
                RequestOptions.DEFAULT);
            // end::delete-forecast-execute

            // tag::delete-forecast-response
            boolean isAcknowledged = deleteForecastResponse.isAcknowledged(); // <1>
            // end::delete-forecast-response
        }
        {
            // tag::delete-forecast-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse DeleteForecastResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-forecast-execute-listener
            DeleteForecastRequest deleteForecastRequest = DeleteForecastRequest.deleteAllForecasts(job.getId());
            deleteForecastRequest.setAllowNoForecasts(true);

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-forecast-execute-async
            client.machineLearning().deleteForecastAsync(deleteForecastRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-forecast-execute-async

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
            // tag::get-job-stats-request
            GetJobStatsRequest request = new GetJobStatsRequest("get-machine-learning-job-stats1", "get-machine-learning-job-*"); // <1>
            request.setAllowNoMatch(true); // <2>
            // end::get-job-stats-request

            // tag::get-job-stats-execute
            GetJobStatsResponse response = client.machineLearning().getJobStats(request, RequestOptions.DEFAULT);
            // end::get-job-stats-execute

            // tag::get-job-stats-response
            long numberOfJobStats = response.count(); // <1>
            List<JobStats> jobStats = response.jobStats(); // <2>
            // end::get-job-stats-response

            assertEquals(2, response.count());
            assertThat(response.jobStats(), hasSize(2));
            assertThat(response.jobStats().stream().map(JobStats::getJobId).collect(Collectors.toList()),
                containsInAnyOrder(job.getId(), secondJob.getId()));
        }
        {
            GetJobStatsRequest request = new GetJobStatsRequest("get-machine-learning-job-stats1", "get-machine-learning-job-*");

            // tag::get-job-stats-execute-listener
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
            // end::get-job-stats-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-job-stats-execute-async
            client.machineLearning().getJobStatsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-job-stats-execute-async

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
        // Post data is deprecated, so expect a deprecation warning
        client.machineLearning().postData(postDataRequest, POST_DATA_OPTIONS);
        client.machineLearning().flushJob(new FlushJobRequest(job.getId()), RequestOptions.DEFAULT);

        {
            // tag::forecast-job-request
            ForecastJobRequest forecastJobRequest = new ForecastJobRequest("forecasting-my-first-machine-learning-job"); // <1>
            // end::forecast-job-request

            // tag::forecast-job-request-options
            forecastJobRequest.setExpiresIn(TimeValue.timeValueHours(48)); // <1>
            forecastJobRequest.setDuration(TimeValue.timeValueHours(24)); // <2>
            forecastJobRequest.setMaxModelMemory(new ByteSizeValue(30, ByteSizeUnit.MB)); // <3>
            // end::forecast-job-request-options

            // tag::forecast-job-execute
            ForecastJobResponse forecastJobResponse = client.machineLearning().forecastJob(forecastJobRequest, RequestOptions.DEFAULT);
            // end::forecast-job-execute

            // tag::forecast-job-response
            boolean isAcknowledged = forecastJobResponse.isAcknowledged(); // <1>
            String forecastId = forecastJobResponse.getForecastId(); // <2>
            // end::forecast-job-response
            assertTrue(isAcknowledged);
            assertNotNull(forecastId);
        }
        {
            // tag::forecast-job-execute-listener
            ActionListener<ForecastJobResponse> listener = new ActionListener<ForecastJobResponse>() {
                @Override
                public void onResponse(ForecastJobResponse forecastJobResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::forecast-job-execute-listener
            ForecastJobRequest forecastJobRequest = new ForecastJobRequest("forecasting-my-first-machine-learning-job");

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::forecast-job-execute-async
            client.machineLearning().forecastJobAsync(forecastJobRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::forecast-job-execute-async

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
            IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
            indexRequest.source("{\"job_id\":\"test-get-overall-buckets-1\", \"result_type\":\"bucket\", \"timestamp\": 1533081600000," +
                    "\"bucket_span\": 600,\"is_interim\": false, \"anomaly_score\": 60.0}", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }
        {
            IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
            indexRequest.source("{\"job_id\":\"test-get-overall-buckets-2\", \"result_type\":\"bucket\", \"timestamp\": 1533081600000," +
                    "\"bucket_span\": 3600,\"is_interim\": false, \"anomaly_score\": 100.0}", XContentType.JSON);
            bulkRequest.add(indexRequest);
        }

        client.bulk(bulkRequest, RequestOptions.DEFAULT);

        {
            // tag::get-overall-buckets-request
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(jobId1, jobId2); // <1>
            // end::get-overall-buckets-request

            // tag::get-overall-buckets-bucket-span
            request.setBucketSpan(TimeValue.timeValueHours(24)); // <1>
            // end::get-overall-buckets-bucket-span

            // tag::get-overall-buckets-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::get-overall-buckets-end

            // tag::get-overall-buckets-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::get-overall-buckets-exclude-interim

            // tag::get-overall-buckets-overall-score
            request.setOverallScore(75.0); // <1>
            // end::get-overall-buckets-overall-score

            // tag::get-overall-buckets-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::get-overall-buckets-start

            // tag::get-overall-buckets-top-n
            request.setTopN(2); // <1>
            // end::get-overall-buckets-top-n

            // tag::get-overall-buckets-execute
            GetOverallBucketsResponse response = client.machineLearning().getOverallBuckets(request, RequestOptions.DEFAULT);
            // end::get-overall-buckets-execute

            // tag::get-overall-buckets-response
            long count = response.count(); // <1>
            List<OverallBucket> overallBuckets = response.overallBuckets(); // <2>
            // end::get-overall-buckets-response

            assertEquals(1, overallBuckets.size());
            assertThat(overallBuckets.get(0).getOverallScore(), is(closeTo(80.0, 0.001)));

        }
        {
            GetOverallBucketsRequest request = new GetOverallBucketsRequest(jobId1, jobId2);

            // tag::get-overall-buckets-execute-listener
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
            // end::get-overall-buckets-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-overall-buckets-execute-async
            client.machineLearning().getOverallBucketsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-overall-buckets-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetRecords() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-records";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a record
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-records\", \"result_type\":\"record\", \"timestamp\": 1533081600000," +
                "\"bucket_span\": 600,\"is_interim\": false, \"record_score\": 80.0}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::get-records-request
            GetRecordsRequest request = new GetRecordsRequest(jobId); // <1>
            // end::get-records-request

            // tag::get-records-desc
            request.setDescending(true); // <1>
            // end::get-records-desc

            // tag::get-records-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::get-records-end

            // tag::get-records-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::get-records-exclude-interim

            // tag::get-records-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::get-records-page

            // Set page params back to null so the response contains the record we indexed
            request.setPageParams(null);

            // tag::get-records-record-score
            request.setRecordScore(75.0); // <1>
            // end::get-records-record-score

            // tag::get-records-sort
            request.setSort("probability"); // <1>
            // end::get-records-sort

            // tag::get-records-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::get-records-start

            // tag::get-records-execute
            GetRecordsResponse response = client.machineLearning().getRecords(request, RequestOptions.DEFAULT);
            // end::get-records-execute

            // tag::get-records-response
            long count = response.count(); // <1>
            List<AnomalyRecord> records = response.records(); // <2>
            // end::get-records-response
            assertEquals(1, records.size());
        }
        {
            GetRecordsRequest request = new GetRecordsRequest(jobId);

            // tag::get-records-execute-listener
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
            // end::get-records-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-records-execute-async
            client.machineLearning().getRecordsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-records-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPostData() throws Exception {
        RestHighLevelClient client = highLevelClient();

        Job job = MachineLearningIT.buildJob("test-post-data");
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        client.machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);

        {
            // tag::post-data-request
            PostDataRequest.JsonBuilder jsonBuilder = new PostDataRequest.JsonBuilder(); // <1>
            Map<String, Object> mapData = new HashMap<>();
            mapData.put("total", 109);
            jsonBuilder.addDoc(mapData); // <2>
            jsonBuilder.addDoc("{\"total\":1000}"); // <3>
            PostDataRequest postDataRequest = new PostDataRequest("test-post-data", jsonBuilder); // <4>
            // end::post-data-request


            // tag::post-data-request-options
            postDataRequest.setResetStart("2018-08-31T16:35:07+00:00"); // <1>
            postDataRequest.setResetEnd("2018-08-31T16:35:17+00:00"); // <2>
            // end::post-data-request-options
            postDataRequest.setResetEnd(null);
            postDataRequest.setResetStart(null);

            // Post data is deprecated, so expect a deprecation warning
            PostDataResponse postDataResponse = client.machineLearning().postData(postDataRequest, POST_DATA_OPTIONS);
            // The end user can use the default options without it being a fatal error (this is only in the test framework)
            /*
            // tag::post-data-execute
            PostDataResponse postDataResponse = client.machineLearning().postData(postDataRequest, RequestOptions.DEFAULT);
            // end::post-data-execute
            */

            // tag::post-data-response
            DataCounts dataCounts = postDataResponse.getDataCounts(); // <1>
            // end::post-data-response
            assertEquals(2, dataCounts.getInputRecordCount());

        }
        {
            // tag::post-data-execute-listener
            ActionListener<PostDataResponse> listener = new ActionListener<PostDataResponse>() {
                @Override
                public void onResponse(PostDataResponse postDataResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::post-data-execute-listener
            PostDataRequest.JsonBuilder jsonBuilder = new PostDataRequest.JsonBuilder();
            Map<String, Object> mapData = new HashMap<>();
            mapData.put("total", 109);
            jsonBuilder.addDoc(mapData);
            PostDataRequest postDataRequest = new PostDataRequest("test-post-data", jsonBuilder); // <1>

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // Post data is deprecated, so expect a deprecation warning
            client.machineLearning().postDataAsync(postDataRequest, POST_DATA_OPTIONS, listener);
            // The end user can use the default options without it being a fatal error (this is only in the test framework)
            /*
            // tag::post-data-execute-async
            client.machineLearning().postDataAsync(postDataRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::post-data-execute-async
            */

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetInfluencers() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-influencers";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a record
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-influencers\", \"result_type\":\"influencer\", \"timestamp\": 1533081600000," +
                "\"bucket_span\": 600,\"is_interim\": false, \"influencer_score\": 80.0, \"influencer_field_name\": \"my_influencer\"," +
                "\"influencer_field_value\":\"foo\"}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::get-influencers-request
            GetInfluencersRequest request = new GetInfluencersRequest(jobId); // <1>
            // end::get-influencers-request

            // tag::get-influencers-desc
            request.setDescending(true); // <1>
            // end::get-influencers-desc

            // tag::get-influencers-end
            request.setEnd("2018-08-21T00:00:00Z"); // <1>
            // end::get-influencers-end

            // tag::get-influencers-exclude-interim
            request.setExcludeInterim(true); // <1>
            // end::get-influencers-exclude-interim

            // tag::get-influencers-influencer-score
            request.setInfluencerScore(75.0); // <1>
            // end::get-influencers-influencer-score

            // tag::get-influencers-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::get-influencers-page

            // Set page params back to null so the response contains the influencer we indexed
            request.setPageParams(null);

            // tag::get-influencers-sort
            request.setSort("probability"); // <1>
            // end::get-influencers-sort

            // tag::get-influencers-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::get-influencers-start

            // tag::get-influencers-execute
            GetInfluencersResponse response = client.machineLearning().getInfluencers(request, RequestOptions.DEFAULT);
            // end::get-influencers-execute

            // tag::get-influencers-response
            long count = response.count(); // <1>
            List<Influencer> influencers = response.influencers(); // <2>
            // end::get-influencers-response
            assertEquals(1, influencers.size());
        }
        {
            GetInfluencersRequest request = new GetInfluencersRequest(jobId);

            // tag::get-influencers-execute-listener
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
            // end::get-influencers-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-influencers-execute-async
            client.machineLearning().getInfluencersAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-influencers-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetCategories() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-categories";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a category
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\": \"test-get-categories\", \"category_id\": 1, \"terms\": \"AAL\"," +
                " \"regex\": \".*?AAL.*\", \"max_matching_length\": 3, \"examples\": [\"AAL\"]}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::get-categories-request
            GetCategoriesRequest request = new GetCategoriesRequest(jobId); // <1>
            // end::get-categories-request

            // tag::get-categories-category-id
            request.setCategoryId(1L); // <1>
            // end::get-categories-category-id

            // tag::get-categories-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::get-categories-page

            // Set page params back to null so the response contains the category we indexed
            request.setPageParams(null);

            // tag::get-categories-execute
            GetCategoriesResponse response = client.machineLearning().getCategories(request, RequestOptions.DEFAULT);
            // end::get-categories-execute

            // tag::get-categories-response
            long count = response.count(); // <1>
            List<CategoryDefinition> categories = response.categories(); // <2>
            // end::get-categories-response
            assertEquals(1, categories.size());
        }
        {
            GetCategoriesRequest request = new GetCategoriesRequest(jobId);

            // tag::get-categories-execute-listener
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
            // end::get-categories-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-categories-execute-async
            client.machineLearning().getCategoriesAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-categories-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteExpiredData() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-delete-expired-data";
        MachineLearningIT.buildJob(jobId);
       {
            // tag::delete-expired-data-request
            DeleteExpiredDataRequest request = new DeleteExpiredDataRequest( // <1>
                null, // <2>
                1000.0f, // <3>
                TimeValue.timeValueHours(12) // <4>
            );

            // end::delete-expired-data-request

            // tag::delete-expired-data-execute
            DeleteExpiredDataResponse response = client.machineLearning().deleteExpiredData(request, RequestOptions.DEFAULT);
            // end::delete-expired-data-execute

            // tag::delete-expired-data-response
            boolean deleted = response.getDeleted(); // <1>
            // end::delete-expired-data-response

            assertTrue(deleted);
        }
        {
            // tag::delete-expired-data-execute-listener
            ActionListener<DeleteExpiredDataResponse> listener = new ActionListener<DeleteExpiredDataResponse>() {
                @Override
                public void onResponse(DeleteExpiredDataResponse deleteExpiredDataResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-expired-data-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            DeleteExpiredDataRequest deleteExpiredDataRequest = new DeleteExpiredDataRequest();

            // tag::delete-expired-data-execute-async
            client.machineLearning().deleteExpiredDataAsync(deleteExpiredDataRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-expired-data-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }


    public void testDeleteModelSnapshot() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-delete-model-snapshot";
        String snapshotId = "1541587919";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a snapshot
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"" + jobId + "\", \"timestamp\":1541587919000, " +
            "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
            "\"snapshot_id\":\"" + snapshotId + "\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
            "\"job_id\":\"" + jobId + "\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
            "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
            "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
            "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
            "\"latest_result_time_stamp\":1519930800000, \"retain\":false}", XContentType.JSON);
        {
            client.index(indexRequest, RequestOptions.DEFAULT);

            // tag::delete-model-snapshot-request
            DeleteModelSnapshotRequest request = new DeleteModelSnapshotRequest(jobId, snapshotId); // <1>
            // end::delete-model-snapshot-request

            // tag::delete-model-snapshot-execute
            AcknowledgedResponse response = client.machineLearning().deleteModelSnapshot(request, RequestOptions.DEFAULT);
            // end::delete-model-snapshot-execute

            // tag::delete-model-snapshot-response
            boolean isAcknowledged = response.isAcknowledged(); // <1>
            // end::delete-model-snapshot-response

            assertTrue(isAcknowledged);
        }
        {
            client.index(indexRequest, RequestOptions.DEFAULT);

            // tag::delete-model-snapshot-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-model-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            DeleteModelSnapshotRequest deleteModelSnapshotRequest = new DeleteModelSnapshotRequest(jobId, "1541587919");

            // tag::delete-model-snapshot-execute-async
            client.machineLearning().deleteModelSnapshotAsync(deleteModelSnapshotRequest, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-model-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetModelSnapshots() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-get-model-snapshots";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a snapshot
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared");
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-get-model-snapshots\", \"timestamp\":1541587919000, " +
            "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
            "\"snapshot_id\":\"1541587919\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
            "\"job_id\":\"test-get-model-snapshots\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
            "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
            "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
            "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
            "\"latest_result_time_stamp\":1519930800000, \"retain\":false}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::get-model-snapshots-request
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(jobId); // <1>
            // end::get-model-snapshots-request

            // tag::get-model-snapshots-snapshot-id
            request.setSnapshotId("1541587919"); // <1>
            // end::get-model-snapshots-snapshot-id

            // Set snapshot id to null as it is incompatible with other args
            request.setSnapshotId(null);

            // tag::get-model-snapshots-desc
            request.setDesc(true); // <1>
            // end::get-model-snapshots-desc

            // tag::get-model-snapshots-end
            request.setEnd("2018-11-07T21:00:00Z"); // <1>
            // end::get-model-snapshots-end

            // tag::get-model-snapshots-page
            request.setPageParams(new PageParams(100, 200)); // <1>
            // end::get-model-snapshots-page

            // Set page params back to null so the response contains the snapshot we indexed
            request.setPageParams(null);

            // tag::get-model-snapshots-sort
            request.setSort("latest_result_time_stamp"); // <1>
            // end::get-model-snapshots-sort

            // tag::get-model-snapshots-start
            request.setStart("2018-11-07T00:00:00Z"); // <1>
            // end::get-model-snapshots-start

            // tag::get-model-snapshots-execute
            GetModelSnapshotsResponse response = client.machineLearning().getModelSnapshots(request, RequestOptions.DEFAULT);
            // end::get-model-snapshots-execute

            // tag::get-model-snapshots-response
            long count = response.count(); // <1>
            List<ModelSnapshot> modelSnapshots = response.snapshots(); // <2>
            // end::get-model-snapshots-response

            assertEquals(1, modelSnapshots.size());
        }
        {
            GetModelSnapshotsRequest request = new GetModelSnapshotsRequest(jobId);

            // tag::get-model-snapshots-execute-listener
            ActionListener<GetModelSnapshotsResponse> listener =
                new ActionListener<GetModelSnapshotsResponse>() {
                    @Override
                    public void onResponse(GetModelSnapshotsResponse getModelSnapshotsResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-model-snapshots-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-model-snapshots-execute-async
            client.machineLearning().getModelSnapshotsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-model-snapshots-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testRevertModelSnapshot() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-revert-model-snapshot";
        String snapshotId = "1541587919";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a snapshot
        String documentId = jobId + "_model_snapshot_" + snapshotId;
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared").id(documentId);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-revert-model-snapshot\", \"timestamp\":1541587919000, " +
            "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
            "\"snapshot_id\":\"1541587919\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
            "\"job_id\":\"test-revert-model-snapshot\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
            "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
            "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
            "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
            "\"latest_result_time_stamp\":1519930800000, \"retain\":false, " +
            "\"quantiles\":{\"job_id\":\"test-revert-model-snapshot\", \"timestamp\":1541587919000, " +
            "\"quantile_state\":\"state\"}}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::revert-model-snapshot-request
            RevertModelSnapshotRequest request = new RevertModelSnapshotRequest(jobId, snapshotId); // <1>
            // end::revert-model-snapshot-request

            // tag::revert-model-snapshot-delete-intervening-results
            request.setDeleteInterveningResults(true); // <1>
            // end::revert-model-snapshot-delete-intervening-results

            // tag::revert-model-snapshot-execute
            RevertModelSnapshotResponse response = client.machineLearning().revertModelSnapshot(request, RequestOptions.DEFAULT);
            // end::revert-model-snapshot-execute

            // tag::revert-model-snapshot-response
            ModelSnapshot modelSnapshot = response.getModel(); // <1>
            // end::revert-model-snapshot-response

            assertEquals(snapshotId, modelSnapshot.getSnapshotId());
            assertEquals("State persisted due to job close at 2018-11-07T10:51:59+0000", modelSnapshot.getDescription());
            assertEquals(51722, modelSnapshot.getModelSizeStats().getModelBytes());
        }
        {
            RevertModelSnapshotRequest request = new RevertModelSnapshotRequest(jobId, snapshotId);

            // tag::revert-model-snapshot-execute-listener
            ActionListener<RevertModelSnapshotResponse> listener =
                new ActionListener<RevertModelSnapshotResponse>() {
                    @Override
                    public void onResponse(RevertModelSnapshotResponse revertModelSnapshotResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::revert-model-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::revert-model-snapshot-execute-async
            client.machineLearning().revertModelSnapshotAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::revert-model-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpgradeJobSnapshot() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-upgrade-job-model-snapshot";
        String snapshotId = "1541587919";
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a snapshot
        String documentId = jobId + "_model_snapshot_" + snapshotId;
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared").id(documentId);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-upgrade-job-model-snapshot\", \"timestamp\":1541587919000, " +
            "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
            "\"snapshot_id\":\"1541587919\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
            "\"job_id\":\"test-revert-model-snapshot\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
            "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
            "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
            "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
            "\"latest_result_time_stamp\":1519930800000, \"retain\":false, " +
            "\"quantiles\":{\"job_id\":\"test-revert-model-snapshot\", \"timestamp\":1541587919000, " +
            "\"quantile_state\":\"state\"}}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::upgrade-job-model-snapshot-request
            UpgradeJobModelSnapshotRequest request = new UpgradeJobModelSnapshotRequest(
                jobId, // <1>
                snapshotId, // <2>
                TimeValue.timeValueMinutes(30), // <3>
                true); // <4>
            // end::upgrade-job-model-snapshot-request

            try {
                // tag::upgrade-job-model-snapshot-execute
                UpgradeJobModelSnapshotResponse response = client.machineLearning().upgradeJobSnapshot(request, RequestOptions.DEFAULT);
                // end::upgrade-job-model-snapshot-execute
                fail("upgrade model snapshot should not have succeeded.");
            } catch (ElasticsearchException ex) {
                assertThat(ex.getMessage(), containsString("Unexpected state [failed] while waiting for to be assigned to a node"));
            }
            UpgradeJobModelSnapshotResponse response = new UpgradeJobModelSnapshotResponse(true, "");

            // tag::upgrade-job-model-snapshot-response
            boolean completed = response.isCompleted(); // <1>
            String node = response.getNode(); // <2>
            // end::upgrade-job-model-snapshot-response
        }
        {
            UpgradeJobModelSnapshotRequest request = new UpgradeJobModelSnapshotRequest(jobId, snapshotId, null, true);

            // tag::upgrade-job-model-snapshot-execute-listener
            ActionListener<UpgradeJobModelSnapshotResponse> listener =
                new ActionListener<UpgradeJobModelSnapshotResponse>() {
                    @Override
                    public void onResponse(UpgradeJobModelSnapshotResponse revertModelSnapshotResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::upgrade-job-model-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::upgrade-job-model-snapshot-execute-async
            client.machineLearning().upgradeJobSnapshotAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::upgrade-job-model-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpdateModelSnapshot() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        String jobId = "test-update-model-snapshot";
        String snapshotId = "1541587919";
        String documentId = jobId + "_model_snapshot_" + snapshotId;
        Job job = MachineLearningIT.buildJob(jobId);
        client.machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        // Let us index a snapshot
        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared").id(documentId);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"test-update-model-snapshot\", \"timestamp\":1541587919000, " +
            "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
            "\"snapshot_id\":\"1541587919\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
            "\"job_id\":\"test-update-model-snapshot\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
            "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
            "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
            "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
            "\"latest_result_time_stamp\":1519930800000, \"retain\":false}", XContentType.JSON);
        client.index(indexRequest, RequestOptions.DEFAULT);

        {
            // tag::update-model-snapshot-request
            UpdateModelSnapshotRequest request = new UpdateModelSnapshotRequest(jobId, snapshotId); // <1>
            // end::update-model-snapshot-request

            // tag::update-model-snapshot-description
            request.setDescription("My Snapshot"); // <1>
            // end::update-model-snapshot-description

            // tag::update-model-snapshot-retain
            request.setRetain(true); // <1>
            // end::update-model-snapshot-retain

            // tag::update-model-snapshot-execute
            UpdateModelSnapshotResponse response = client.machineLearning().updateModelSnapshot(request, RequestOptions.DEFAULT);
            // end::update-model-snapshot-execute

            // tag::update-model-snapshot-response
            boolean acknowledged = response.getAcknowledged(); // <1>
            ModelSnapshot modelSnapshot = response.getModel(); // <2>
            // end::update-model-snapshot-response

            assertTrue(acknowledged);
            assertEquals("My Snapshot", modelSnapshot.getDescription());        }
        {
            UpdateModelSnapshotRequest request = new UpdateModelSnapshotRequest(jobId, snapshotId);

            // tag::update-model-snapshot-execute-listener
            ActionListener<UpdateModelSnapshotResponse> listener =
                new ActionListener<UpdateModelSnapshotResponse>() {
                    @Override
                    public void onResponse(UpdateModelSnapshotResponse updateModelSnapshotResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::update-model-snapshot-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-model-snapshot-execute-async
            client.machineLearning().updateModelSnapshotAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-model-snapshot-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutCalendar() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        // tag::put-calendar-request
        Calendar calendar = new Calendar("public_holidays", Collections.singletonList("job_1"), "A calendar for public holidays");
        PutCalendarRequest request = new PutCalendarRequest(calendar); // <1>
        // end::put-calendar-request

        // tag::put-calendar-execute
        PutCalendarResponse response = client.machineLearning().putCalendar(request, RequestOptions.DEFAULT);
        // end::put-calendar-execute

        // tag::put-calendar-response
        Calendar newCalendar = response.getCalendar(); // <1>
        // end::put-calendar-response
        assertThat(newCalendar.getId(), equalTo("public_holidays"));

        // tag::put-calendar-execute-listener
        ActionListener<PutCalendarResponse> listener = new ActionListener<PutCalendarResponse>() {
            @Override
            public void onResponse(PutCalendarResponse response) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::put-calendar-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::put-calendar-execute-async
        client.machineLearning().putCalendarAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::put-calendar-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testPutCalendarJob() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays", Collections.singletonList("job_1"), "A calendar for public holidays");
        PutCalendarRequest putRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putRequest, RequestOptions.DEFAULT);
        {
            // tag::put-calendar-job-request
            PutCalendarJobRequest request = new PutCalendarJobRequest("holidays", // <1>
                "job_2", "job_group_1"); // <2>
            // end::put-calendar-job-request

            // tag::put-calendar-job-execute
            PutCalendarResponse response = client.machineLearning().putCalendarJob(request, RequestOptions.DEFAULT);
            // end::put-calendar-job-execute

            // tag::put-calendar-job-response
            Calendar updatedCalendar = response.getCalendar(); // <1>
            // end::put-calendar-job-response

            assertThat(updatedCalendar.getJobIds(), containsInAnyOrder("job_1", "job_2", "job_group_1"));
        }
        {
            PutCalendarJobRequest request = new PutCalendarJobRequest("holidays", "job_4");

            // tag::put-calendar-job-execute-listener
            ActionListener<PutCalendarResponse> listener =
                new ActionListener<PutCalendarResponse>() {
                    @Override
                    public void onResponse(PutCalendarResponse putCalendarsResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::put-calendar-job-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-calendar-job-execute-async
            client.machineLearning().putCalendarJobAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-calendar-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteCalendarJob() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays",
            Arrays.asList("job_1", "job_group_1", "job_2"),
            "A calendar for public holidays");
        PutCalendarRequest putRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putRequest, RequestOptions.DEFAULT);
        {
            // tag::delete-calendar-job-request
            DeleteCalendarJobRequest request = new DeleteCalendarJobRequest("holidays", // <1>
                "job_1", "job_group_1"); // <2>
            // end::delete-calendar-job-request

            // tag::delete-calendar-job-execute
            PutCalendarResponse response = client.machineLearning().deleteCalendarJob(request, RequestOptions.DEFAULT);
            // end::delete-calendar-job-execute

            // tag::delete-calendar-job-response
            Calendar updatedCalendar = response.getCalendar(); // <1>
            // end::delete-calendar-job-response

            assertThat(updatedCalendar.getJobIds(), containsInAnyOrder("job_2"));
        }
        {
            DeleteCalendarJobRequest request = new DeleteCalendarJobRequest("holidays", "job_2");

            // tag::delete-calendar-job-execute-listener
            ActionListener<PutCalendarResponse> listener =
                new ActionListener<PutCalendarResponse>() {
                    @Override
                    public void onResponse(PutCalendarResponse deleteCalendarsResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-calendar-job-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-calendar-job-execute-async
            client.machineLearning().deleteCalendarJobAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-calendar-job-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetCalendar() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays", Collections.singletonList("job_1"), "A calendar for public holidays");
        PutCalendarRequest putRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putRequest, RequestOptions.DEFAULT);
        {
            // tag::get-calendars-request
            GetCalendarsRequest request = new GetCalendarsRequest(); // <1>
            // end::get-calendars-request

            // tag::get-calendars-id
            request.setCalendarId("holidays"); // <1>
            // end::get-calendars-id

            // tag::get-calendars-page
            request.setPageParams(new PageParams(10, 20)); // <1>
            // end::get-calendars-page

            // reset page params
            request.setPageParams(null);

            // tag::get-calendars-execute
            GetCalendarsResponse response = client.machineLearning().getCalendars(request, RequestOptions.DEFAULT);
            // end::get-calendars-execute

            // tag::get-calendars-response
            long count = response.count(); // <1>
            List<Calendar> calendars = response.calendars(); // <2>
            // end::get-calendars-response
            assertEquals(1, calendars.size());
        }
        {
            GetCalendarsRequest request = new GetCalendarsRequest("holidays");

            // tag::get-calendars-execute-listener
            ActionListener<GetCalendarsResponse> listener =
                    new ActionListener<GetCalendarsResponse>() {
                        @Override
                        public void onResponse(GetCalendarsResponse getCalendarsResponse) {
                            // <1>
                        }

                        @Override
                        public void onFailure(Exception e) {
                            // <2>
                        }
                    };
            // end::get-calendars-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-calendars-execute-async
            client.machineLearning().getCalendarsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-calendars-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteCalendar() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays", Collections.singletonList("job_1"), "A calendar for public holidays");
        PutCalendarRequest putCalendarRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putCalendarRequest, RequestOptions.DEFAULT);

        // tag::delete-calendar-request
        DeleteCalendarRequest request = new DeleteCalendarRequest("holidays"); // <1>
        // end::delete-calendar-request

        // tag::delete-calendar-execute
        AcknowledgedResponse response = client.machineLearning().deleteCalendar(request, RequestOptions.DEFAULT);
        // end::delete-calendar-execute

        // tag::delete-calendar-response
        boolean isAcknowledged = response.isAcknowledged(); // <1>
        // end::delete-calendar-response

        assertTrue(isAcknowledged);

        // tag::delete-calendar-execute-listener
        ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
            @Override
            public void onResponse(AcknowledgedResponse response) {
                // <1>
            }

            @Override
            public void onFailure(Exception e) {
                // <2>
            }
        };
        // end::delete-calendar-execute-listener

        // Replace the empty listener by a blocking listener in test
        final CountDownLatch latch = new CountDownLatch(1);
        listener = new LatchedActionListener<>(listener, latch);

        // tag::delete-calendar-execute-async
        client.machineLearning().deleteCalendarAsync(request, RequestOptions.DEFAULT, listener); // <1>
        // end::delete-calendar-execute-async

        assertTrue(latch.await(30L, TimeUnit.SECONDS));
    }

    public void testGetCalendarEvent() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays", Collections.singletonList("job_1"), "A calendar for public holidays");
        PutCalendarRequest putRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putRequest, RequestOptions.DEFAULT);
        List<ScheduledEvent> events = Collections.singletonList(ScheduledEventTests.testInstance(calendar.getId(), null));
        client.machineLearning().postCalendarEvent(new PostCalendarEventRequest("holidays", events), RequestOptions.DEFAULT);
        {
            // tag::get-calendar-events-request
            GetCalendarEventsRequest request = new GetCalendarEventsRequest("holidays"); // <1>
            // end::get-calendar-events-request

            // tag::get-calendar-events-page
            request.setPageParams(new PageParams(10, 20)); // <1>
            // end::get-calendar-events-page

            // tag::get-calendar-events-start
            request.setStart("2018-08-01T00:00:00Z"); // <1>
            // end::get-calendar-events-start

            // tag::get-calendar-events-end
            request.setEnd("2018-08-02T00:00:00Z"); // <1>
            // end::get-calendar-events-end

            // tag::get-calendar-events-jobid
            request.setJobId("job_1"); // <1>
            // end::get-calendar-events-jobid

            // reset params
            request.setPageParams(null);
            request.setJobId(null);
            request.setStart(null);
            request.setEnd(null);

            // tag::get-calendar-events-execute
            GetCalendarEventsResponse response = client.machineLearning().getCalendarEvents(request, RequestOptions.DEFAULT);
            // end::get-calendar-events-execute

            // tag::get-calendar-events-response
            long count = response.count(); // <1>
            List<ScheduledEvent> scheduledEvents = response.events(); // <2>
            // end::get-calendar-events-response
            assertEquals(1, scheduledEvents.size());
        }
        {
            GetCalendarEventsRequest request = new GetCalendarEventsRequest("holidays");

            // tag::get-calendar-events-execute-listener
            ActionListener<GetCalendarEventsResponse> listener =
                new ActionListener<GetCalendarEventsResponse>() {
                    @Override
                    public void onResponse(GetCalendarEventsResponse getCalendarsResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::get-calendar-events-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-calendar-events-execute-async
            client.machineLearning().getCalendarEventsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-calendar-events-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPostCalendarEvent() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays", Collections.singletonList("job_1"), "A calendar for public holidays");
        PutCalendarRequest putRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putRequest, RequestOptions.DEFAULT);
        {
            List<ScheduledEvent> events = Collections.singletonList(ScheduledEventTests.testInstance(calendar.getId(), null));

            // tag::post-calendar-event-request
            PostCalendarEventRequest request = new PostCalendarEventRequest("holidays", // <1>
                events); // <2>
            // end::post-calendar-event-request

            // tag::post-calendar-event-execute
            PostCalendarEventResponse response = client.machineLearning().postCalendarEvent(request, RequestOptions.DEFAULT);
            // end::post-calendar-event-execute

            // tag::post-calendar-event-response
            List<ScheduledEvent> scheduledEvents = response.getScheduledEvents(); // <1>
            // end::post-calendar-event-response

            assertEquals(1, scheduledEvents.size());
        }
        {
            List<ScheduledEvent> events = Collections.singletonList(ScheduledEventTests.testInstance());
            PostCalendarEventRequest request = new PostCalendarEventRequest("holidays", events); // <1>

            // tag::post-calendar-event-execute-listener
            ActionListener<PostCalendarEventResponse> listener =
                new ActionListener<PostCalendarEventResponse>() {
                    @Override
                    public void onResponse(PostCalendarEventResponse postCalendarsResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::post-calendar-event-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::post-calendar-event-execute-async
            client.machineLearning().postCalendarEventAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::post-calendar-event-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteCalendarEvent() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();

        Calendar calendar = new Calendar("holidays",
            Arrays.asList("job_1", "job_group_1", "job_2"),
            "A calendar for public holidays");
        PutCalendarRequest putRequest = new PutCalendarRequest(calendar);
        client.machineLearning().putCalendar(putRequest, RequestOptions.DEFAULT);
        List<ScheduledEvent> events = Arrays.asList(ScheduledEventTests.testInstance(calendar.getId(), null),
            ScheduledEventTests.testInstance(calendar.getId(), null));
        client.machineLearning().postCalendarEvent(new PostCalendarEventRequest("holidays", events), RequestOptions.DEFAULT);
        GetCalendarEventsResponse getCalendarEventsResponse =
            client.machineLearning().getCalendarEvents(new GetCalendarEventsRequest("holidays"), RequestOptions.DEFAULT);
        {

            // tag::delete-calendar-event-request
            DeleteCalendarEventRequest request = new DeleteCalendarEventRequest("holidays", // <1>
                "EventId"); // <2>
            // end::delete-calendar-event-request

            request = new DeleteCalendarEventRequest("holidays", getCalendarEventsResponse.events().get(0).getEventId());

            // tag::delete-calendar-event-execute
            AcknowledgedResponse response = client.machineLearning().deleteCalendarEvent(request, RequestOptions.DEFAULT);
            // end::delete-calendar-event-execute

            // tag::delete-calendar-event-response
            boolean acknowledged = response.isAcknowledged(); // <1>
            // end::delete-calendar-event-response

            assertThat(acknowledged, is(true));
        }
        {
            DeleteCalendarEventRequest request = new DeleteCalendarEventRequest("holidays",
                getCalendarEventsResponse.events().get(1).getEventId());

            // tag::delete-calendar-event-execute-listener
            ActionListener<AcknowledgedResponse> listener =
                new ActionListener<AcknowledgedResponse>() {
                    @Override
                    public void onResponse(AcknowledgedResponse deleteCalendarEventResponse) {
                        // <1>
                    }

                    @Override
                    public void onFailure(Exception e) {
                        // <2>
                    }
                };
            // end::delete-calendar-event-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-calendar-event-execute-async
            client.machineLearning().deleteCalendarEventAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-calendar-event-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetDataFrameAnalytics() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);

        RestHighLevelClient client = highLevelClient();
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            // tag::get-data-frame-analytics-request
            GetDataFrameAnalyticsRequest request = new GetDataFrameAnalyticsRequest("my-analytics-config"); // <1>
            request.setExcludeGenerated(false); // <2>
            // end::get-data-frame-analytics-request

            // tag::get-data-frame-analytics-execute
            GetDataFrameAnalyticsResponse response = client.machineLearning().getDataFrameAnalytics(request, RequestOptions.DEFAULT);
            // end::get-data-frame-analytics-execute

            // tag::get-data-frame-analytics-response
            List<DataFrameAnalyticsConfig> configs = response.getAnalytics();
            // end::get-data-frame-analytics-response

            assertThat(configs, hasSize(1));
        }
        {
            GetDataFrameAnalyticsRequest request = new GetDataFrameAnalyticsRequest("my-analytics-config");

            // tag::get-data-frame-analytics-execute-listener
            ActionListener<GetDataFrameAnalyticsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(GetDataFrameAnalyticsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-data-frame-analytics-execute-async
            client.machineLearning().getDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetDataFrameAnalyticsStats() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);

        RestHighLevelClient client = highLevelClient();
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            // tag::get-data-frame-analytics-stats-request
            GetDataFrameAnalyticsStatsRequest request = new GetDataFrameAnalyticsStatsRequest("my-analytics-config"); // <1>
            // end::get-data-frame-analytics-stats-request

            // tag::get-data-frame-analytics-stats-execute
            GetDataFrameAnalyticsStatsResponse response =
                client.machineLearning().getDataFrameAnalyticsStats(request, RequestOptions.DEFAULT);
            // end::get-data-frame-analytics-stats-execute

            // tag::get-data-frame-analytics-stats-response
            List<DataFrameAnalyticsStats> stats = response.getAnalyticsStats();
            // end::get-data-frame-analytics-stats-response

            assertThat(stats, hasSize(1));
        }
        {
            GetDataFrameAnalyticsStatsRequest request = new GetDataFrameAnalyticsStatsRequest("my-analytics-config");

            // tag::get-data-frame-analytics-stats-execute-listener
            ActionListener<GetDataFrameAnalyticsStatsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(GetDataFrameAnalyticsStatsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-data-frame-analytics-stats-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-data-frame-analytics-stats-execute-async
            client.machineLearning().getDataFrameAnalyticsStatsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-data-frame-analytics-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutDataFrameAnalytics() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);

        RestHighLevelClient client = highLevelClient();
        {
            // tag::put-data-frame-analytics-query-config
            QueryConfig queryConfig = new QueryConfig(new MatchAllQueryBuilder());
            // end::put-data-frame-analytics-query-config

            Map<String, Object> runtimeMappings = Collections.emptyMap();

            // tag::put-data-frame-analytics-source-config
            DataFrameAnalyticsSource sourceConfig = DataFrameAnalyticsSource.builder() // <1>
                .setIndex("put-test-source-index") // <2>
                .setQueryConfig(queryConfig) // <3>
                .setRuntimeMappings(runtimeMappings) // <4>
                .setSourceFiltering(new FetchSourceContext(true,
                    new String[] { "included_field_1", "included_field_2" },
                    new String[] { "excluded_field" })) // <5>
                .build();
            // end::put-data-frame-analytics-source-config

            // tag::put-data-frame-analytics-dest-config
            DataFrameAnalyticsDest destConfig = DataFrameAnalyticsDest.builder() // <1>
                .setIndex("put-test-dest-index") // <2>
                .build();
            // end::put-data-frame-analytics-dest-config

            // tag::put-data-frame-analytics-outlier-detection-default
            DataFrameAnalysis outlierDetection = org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault(); // <1>
            // end::put-data-frame-analytics-outlier-detection-default

            // tag::put-data-frame-analytics-outlier-detection-customized
            DataFrameAnalysis outlierDetectionCustomized = org.elasticsearch.client.ml.dataframe.OutlierDetection.builder() // <1>
                .setMethod(org.elasticsearch.client.ml.dataframe.OutlierDetection.Method.DISTANCE_KNN) // <2>
                .setNNeighbors(5) // <3>
                .setFeatureInfluenceThreshold(0.1) // <4>
                .setComputeFeatureInfluence(true) // <5>
                .setOutlierFraction(0.05) // <6>
                .setStandardizationEnabled(true) // <7>
                .build();
            // end::put-data-frame-analytics-outlier-detection-customized

            // tag::put-data-frame-analytics-classification
            DataFrameAnalysis classification = Classification.builder("my_dependent_variable") // <1>
                .setLambda(1.0) // <2>
                .setGamma(5.5) // <3>
                .setEta(5.5) // <4>
                .setMaxTrees(50) // <5>
                .setFeatureBagFraction(0.4) // <6>
                .setNumTopFeatureImportanceValues(3) // <7>
                .setPredictionFieldName("my_prediction_field_name") // <8>
                .setTrainingPercent(50.0) // <9>
                .setRandomizeSeed(1234L) // <10>
                .setClassAssignmentObjective(Classification.ClassAssignmentObjective.MAXIMIZE_ACCURACY) // <11>
                .setNumTopClasses(1) // <12>
                .setFeatureProcessors(Arrays.asList(OneHotEncoding.builder("categorical_feature") // <13>
                    .addOneHot("cat", "cat_column")
                    .build()))
                .setAlpha(1.0) // <14>
                .setEtaGrowthRatePerTree(1.0) // <15>
                .setSoftTreeDepthLimit(1.0) // <16>
                .setSoftTreeDepthTolerance(1.0) // <17>
                .setDownsampleFactor(0.5) // <18>
                .setMaxOptimizationRoundsPerHyperparameter(3) // <19>
                .setEarlyStoppingEnabled(true) // <20>
                .build();
            // end::put-data-frame-analytics-classification

            // tag::put-data-frame-analytics-regression
            DataFrameAnalysis regression = org.elasticsearch.client.ml.dataframe.Regression.builder("my_dependent_variable") // <1>
                .setLambda(1.0) // <2>
                .setGamma(5.5) // <3>
                .setEta(5.5) // <4>
                .setMaxTrees(50) // <5>
                .setFeatureBagFraction(0.4) // <6>
                .setNumTopFeatureImportanceValues(3) // <7>
                .setPredictionFieldName("my_prediction_field_name") // <8>
                .setTrainingPercent(50.0) // <9>
                .setRandomizeSeed(1234L) // <10>
                .setLossFunction(Regression.LossFunction.MSE) // <11>
                .setLossFunctionParameter(1.0) // <12>
                .setFeatureProcessors(Arrays.asList(OneHotEncoding.builder("categorical_feature") // <13>
                    .addOneHot("cat", "cat_column")
                    .build()))
                .setAlpha(1.0) // <14>
                .setEtaGrowthRatePerTree(1.0) // <15>
                .setSoftTreeDepthLimit(1.0) // <16>
                .setSoftTreeDepthTolerance(1.0) // <17>
                .setDownsampleFactor(0.5) // <18>
                .setMaxOptimizationRoundsPerHyperparameter(3) // <19>
                .setEarlyStoppingEnabled(true) // <20>
                .build();
            // end::put-data-frame-analytics-regression

            // tag::put-data-frame-analytics-analyzed-fields
            FetchSourceContext analyzedFields =
                new FetchSourceContext(
                    true,
                    new String[] { "included_field_1", "included_field_2" },
                    new String[] { "excluded_field" });
            // end::put-data-frame-analytics-analyzed-fields

            // tag::put-data-frame-analytics-config
            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
                .setId("my-analytics-config") // <1>
                .setSource(sourceConfig) // <2>
                .setDest(destConfig) // <3>
                .setAnalysis(outlierDetection) // <4>
                .setAnalyzedFields(analyzedFields) // <5>
                .setModelMemoryLimit(new ByteSizeValue(5, ByteSizeUnit.MB)) // <6>
                .setDescription("this is an example description") // <7>
                .setMaxNumThreads(1) // <8>
                .build();
            // end::put-data-frame-analytics-config

            // tag::put-data-frame-analytics-request
            PutDataFrameAnalyticsRequest request = new PutDataFrameAnalyticsRequest(config); // <1>
            // end::put-data-frame-analytics-request

            // tag::put-data-frame-analytics-execute
            PutDataFrameAnalyticsResponse response = client.machineLearning().putDataFrameAnalytics(request, RequestOptions.DEFAULT);
            // end::put-data-frame-analytics-execute

            // tag::put-data-frame-analytics-response
            DataFrameAnalyticsConfig createdConfig = response.getConfig();
            // end::put-data-frame-analytics-response

            assertThat(createdConfig.getId(), equalTo("my-analytics-config"));
        }
        {
            PutDataFrameAnalyticsRequest request = new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG);
            // tag::put-data-frame-analytics-execute-listener
            ActionListener<PutDataFrameAnalyticsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(PutDataFrameAnalyticsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-data-frame-analytics-execute-async
            client.machineLearning().putDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpdateDataFrameAnalytics() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);

        RestHighLevelClient client = highLevelClient();
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            // tag::update-data-frame-analytics-config-update
            DataFrameAnalyticsConfigUpdate update = DataFrameAnalyticsConfigUpdate.builder()
                .setId("my-analytics-config")  // <1>
                .setDescription("new description")  // <2>
                .setModelMemoryLimit(new ByteSizeValue(128, ByteSizeUnit.MB))  // <3>
                .setMaxNumThreads(4) // <4>
                .build();
            // end::update-data-frame-analytics-config-update

            // tag::update-data-frame-analytics-request
            UpdateDataFrameAnalyticsRequest request = new UpdateDataFrameAnalyticsRequest(update); // <1>
            // end::update-data-frame-analytics-request

            // tag::update-data-frame-analytics-execute
            PutDataFrameAnalyticsResponse response = client.machineLearning().updateDataFrameAnalytics(request, RequestOptions.DEFAULT);
            // end::update-data-frame-analytics-execute

            // tag::update-data-frame-analytics-response
            DataFrameAnalyticsConfig updatedConfig = response.getConfig();
            // end::update-data-frame-analytics-response

            assertThat(updatedConfig.getDescription(), is(equalTo("new description")));
            assertThat(updatedConfig.getModelMemoryLimit(), is(equalTo(new ByteSizeValue(128, ByteSizeUnit.MB))));
        }
        {
            DataFrameAnalyticsConfigUpdate update = DataFrameAnalyticsConfigUpdate.builder()
                .setId("my-analytics-config")
                .build();
            UpdateDataFrameAnalyticsRequest request = new UpdateDataFrameAnalyticsRequest(update);

            // tag::update-data-frame-analytics-execute-listener
            ActionListener<PutDataFrameAnalyticsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(PutDataFrameAnalyticsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::update-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-data-frame-analytics-execute-async
            client.machineLearning().updateDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteDataFrameAnalytics() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);

        RestHighLevelClient client = highLevelClient();
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            // tag::delete-data-frame-analytics-request
            DeleteDataFrameAnalyticsRequest request = new DeleteDataFrameAnalyticsRequest("my-analytics-config"); // <1>
            // end::delete-data-frame-analytics-request

            //tag::delete-data-frame-analytics-request-options
            request.setForce(false); // <1>
            request.setTimeout(TimeValue.timeValueMinutes(1)); // <2>
            //end::delete-data-frame-analytics-request-options

            // tag::delete-data-frame-analytics-execute
            AcknowledgedResponse response = client.machineLearning().deleteDataFrameAnalytics(request, RequestOptions.DEFAULT);
            // end::delete-data-frame-analytics-execute

            // tag::delete-data-frame-analytics-response
            boolean acknowledged = response.isAcknowledged();
            // end::delete-data-frame-analytics-response

            assertThat(acknowledged, is(true));
        }
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            DeleteDataFrameAnalyticsRequest request = new DeleteDataFrameAnalyticsRequest("my-analytics-config");

            // tag::delete-data-frame-analytics-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-data-frame-analytics-execute-async
            client.machineLearning().deleteDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testStartDataFrameAnalytics() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);
        highLevelClient().index(
            new IndexRequest(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]).source(XContentType.JSON, "total", 10000)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        RestHighLevelClient client = highLevelClient();
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            // tag::start-data-frame-analytics-request
            StartDataFrameAnalyticsRequest request = new StartDataFrameAnalyticsRequest("my-analytics-config"); // <1>
            // end::start-data-frame-analytics-request

            // tag::start-data-frame-analytics-execute
            StartDataFrameAnalyticsResponse response = client.machineLearning().startDataFrameAnalytics(request, RequestOptions.DEFAULT);
            // end::start-data-frame-analytics-execute

            // tag::start-data-frame-analytics-response
            boolean acknowledged = response.isAcknowledged();
            String node = response.getNode(); // <1>
            // end::start-data-frame-analytics-response

            assertThat(acknowledged, is(true));
            assertThat(node, notNullValue());
        }
        assertBusy(
            () -> assertThat(getAnalyticsState(DF_ANALYTICS_CONFIG.getId()), equalTo(DataFrameAnalyticsState.STOPPED)),
            30, TimeUnit.SECONDS);
        {
            StartDataFrameAnalyticsRequest request = new StartDataFrameAnalyticsRequest("my-analytics-config");

            // tag::start-data-frame-analytics-execute-listener
            ActionListener<StartDataFrameAnalyticsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(StartDataFrameAnalyticsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::start-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::start-data-frame-analytics-execute-async
            client.machineLearning().startDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::start-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        assertBusy(
            () -> assertThat(getAnalyticsState(DF_ANALYTICS_CONFIG.getId()), equalTo(DataFrameAnalyticsState.STOPPED)),
            30, TimeUnit.SECONDS);
    }

    public void testStopDataFrameAnalytics() throws Exception {
        createIndex(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]);
        highLevelClient().index(
            new IndexRequest(DF_ANALYTICS_CONFIG.getSource().getIndex()[0]).source(XContentType.JSON, "total", 10000)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);
        RestHighLevelClient client = highLevelClient();
        client.machineLearning().putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(DF_ANALYTICS_CONFIG), RequestOptions.DEFAULT);
        {
            // tag::stop-data-frame-analytics-request
            StopDataFrameAnalyticsRequest request = new StopDataFrameAnalyticsRequest("my-analytics-config"); // <1>
            request.setForce(false); // <2>
            // end::stop-data-frame-analytics-request

            // tag::stop-data-frame-analytics-execute
            StopDataFrameAnalyticsResponse response = client.machineLearning().stopDataFrameAnalytics(request, RequestOptions.DEFAULT);
            // end::stop-data-frame-analytics-execute

            // tag::stop-data-frame-analytics-response
            boolean acknowledged = response.isStopped();
            // end::stop-data-frame-analytics-response

            assertThat(acknowledged, is(true));
        }
        assertBusy(
            () -> assertThat(getAnalyticsState(DF_ANALYTICS_CONFIG.getId()), equalTo(DataFrameAnalyticsState.STOPPED)),
            30, TimeUnit.SECONDS);
        {
            StopDataFrameAnalyticsRequest request = new StopDataFrameAnalyticsRequest("my-analytics-config");

            // tag::stop-data-frame-analytics-execute-listener
            ActionListener<StopDataFrameAnalyticsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(StopDataFrameAnalyticsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::stop-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::stop-data-frame-analytics-execute-async
            client.machineLearning().stopDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::stop-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
        assertBusy(
            () -> assertThat(getAnalyticsState(DF_ANALYTICS_CONFIG.getId()), equalTo(DataFrameAnalyticsState.STOPPED)),
            30, TimeUnit.SECONDS);
    }

    public void testEvaluateDataFrame_OutlierDetection() throws Exception {
        String indexName = "evaluate-test-index";
        CreateIndexRequest createIndexRequest =
            new CreateIndexRequest(indexName)
                .mapping(XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("label")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("p")
                            .field("type", "double")
                        .endObject()
                    .endObject()
                .endObject());
        BulkRequest bulkRequest =
            new BulkRequest(indexName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", false, "p", 0.1)) // #0
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", false, "p", 0.2)) // #1
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", false, "p", 0.3)) // #2
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", false, "p", 0.4)) // #3
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", false, "p", 0.7)) // #4
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", true,  "p", 0.2)) // #5
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", true,  "p", 0.3)) // #6
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", true,  "p", 0.4)) // #7
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", true,  "p", 0.8)) // #8
                .add(new IndexRequest().source(XContentType.JSON, "dataset", "blue", "label", true,  "p", 0.9)); // #9
        RestHighLevelClient client = highLevelClient();
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        {
            // tag::evaluate-data-frame-evaluation-outlierdetection
            Evaluation evaluation =
                new OutlierDetection( // <1>
                    "label", // <2>
                    "p", // <3>
                    // Evaluation metrics // <4>
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.at(0.4, 0.5, 0.6), // <5>
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric.at(0.5, 0.7), // <6>
                    ConfusionMatrixMetric.at(0.5), // <7>
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric.withCurve()); // <8>
            // end::evaluate-data-frame-evaluation-outlierdetection

            // tag::evaluate-data-frame-request
            EvaluateDataFrameRequest request =
                new EvaluateDataFrameRequest( // <1>
                    indexName, // <2>
                    new QueryConfig(QueryBuilders.termQuery("dataset", "blue")), // <3>
                    evaluation); // <4>
            // end::evaluate-data-frame-request

            // tag::evaluate-data-frame-execute
            EvaluateDataFrameResponse response = client.machineLearning().evaluateDataFrame(request, RequestOptions.DEFAULT);
            // end::evaluate-data-frame-execute

            // tag::evaluate-data-frame-response
            List<EvaluationMetric.Result> metrics = response.getMetrics(); // <1>
            // end::evaluate-data-frame-response

            // tag::evaluate-data-frame-results-outlierdetection
            org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.Result precisionResult =
                response.getMetricByName(org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.NAME); // <1>
            double precision = precisionResult.getScoreByThreshold("0.4"); // <2>

            ConfusionMatrixMetric.Result confusionMatrixResult = response.getMetricByName(ConfusionMatrixMetric.NAME); // <3>
            ConfusionMatrix confusionMatrix = confusionMatrixResult.getScoreByThreshold("0.5"); // <4>
            // end::evaluate-data-frame-results-outlierdetection

            assertThat(
                metrics.stream().map(EvaluationMetric.Result::getMetricName).collect(Collectors.toList()),
                containsInAnyOrder(
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.NAME,
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric.NAME,
                    ConfusionMatrixMetric.NAME,
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric.NAME));
            assertThat(precision, closeTo(0.6, 1e-9));
            assertThat(confusionMatrix.getTruePositives(), equalTo(2L));  // docs #8 and #9
            assertThat(confusionMatrix.getFalsePositives(), equalTo(1L));  // doc #4
            assertThat(confusionMatrix.getTrueNegatives(), equalTo(4L));  // docs #0, #1, #2 and #3
            assertThat(confusionMatrix.getFalseNegatives(), equalTo(3L));  // docs #5, #6 and #7
        }
        {
            EvaluateDataFrameRequest request = new EvaluateDataFrameRequest(
                indexName,
                new QueryConfig(QueryBuilders.termQuery("dataset", "blue")),
                new OutlierDetection(
                    "label",
                    "p",
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric.at(0.4, 0.5, 0.6),
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric.at(0.5, 0.7),
                    ConfusionMatrixMetric.at(0.5),
                    org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric.withCurve()));

            // tag::evaluate-data-frame-execute-listener
            ActionListener<EvaluateDataFrameResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(EvaluateDataFrameResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::evaluate-data-frame-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::evaluate-data-frame-execute-async
            client.machineLearning().evaluateDataFrameAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::evaluate-data-frame-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testEvaluateDataFrame_Classification() throws Exception {
        String indexName = "evaluate-classification-test-index";
        CreateIndexRequest createIndexRequest =
            new CreateIndexRequest(indexName)
                .mapping(XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("actual_class")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("predicted_class")
                            .field("type", "keyword")
                        .endObject()
                        .startObject("ml.top_classes")
                            .field("type", "nested")
                        .endObject()
                    .endObject()
                    .endObject());
        BiFunction<String, String[], IndexRequest> indexRequest = (actualClass, topPredictedClasses) -> {
            assert topPredictedClasses.length > 0;
            return new IndexRequest()
                .source(XContentType.JSON,
                    "actual_class", actualClass,
                    "predicted_class", topPredictedClasses[0],
                    "ml.top_classes", IntStream.range(0, topPredictedClasses.length)
                        // Consecutive assigned probabilities are: 0.5, 0.25, 0.125, etc.
                        .mapToObj(i -> Map.of("class_name", topPredictedClasses[i], "class_probability", 1.0 / (2 << i)))
                        .collect(toList()));
        };
        BulkRequest bulkRequest =
            new BulkRequest(indexName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .add(indexRequest.apply("cat", new String[]{"cat", "dog", "ant"})) // #0
                .add(indexRequest.apply("cat", new String[]{"cat", "dog", "ant"})) // #1
                .add(indexRequest.apply("cat", new String[]{"cat", "horse", "dog"})) // #2
                .add(indexRequest.apply("cat", new String[]{"dog", "cat", "mule"})) // #3
                .add(indexRequest.apply("cat", new String[]{"fox", "cat", "dog"})) // #4
                .add(indexRequest.apply("dog", new String[]{"cat", "dog", "mule"})) // #5
                .add(indexRequest.apply("dog", new String[]{"dog", "cat", "ant"})) // #6
                .add(indexRequest.apply("dog", new String[]{"dog", "cat", "ant"})) // #7
                .add(indexRequest.apply("dog", new String[]{"dog", "cat", "ant"})) // #8
                .add(indexRequest.apply("ant", new String[]{"cat", "ant", "wasp"})); // #9
        RestHighLevelClient client = highLevelClient();
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        {
            // tag::evaluate-data-frame-evaluation-classification
            Evaluation evaluation =
                new org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification( // <1>
                    "actual_class", // <2>
                    "predicted_class", // <3>
                    "ml.top_classes", // <4>
                    // Evaluation metrics // <5>
                    new AccuracyMetric(), // <6>
                    new PrecisionMetric(), // <7>
                    new RecallMetric(), // <8>
                    new MulticlassConfusionMatrixMetric(3), // <9>
                    AucRocMetric.forClass("cat")); // <10>
            // end::evaluate-data-frame-evaluation-classification

            EvaluateDataFrameRequest request = new EvaluateDataFrameRequest(indexName, null, evaluation);
            EvaluateDataFrameResponse response = client.machineLearning().evaluateDataFrame(request, RequestOptions.DEFAULT);

            // tag::evaluate-data-frame-results-classification
            AccuracyMetric.Result accuracyResult = response.getMetricByName(AccuracyMetric.NAME); // <1>
            double accuracy = accuracyResult.getOverallAccuracy(); // <2>

            PrecisionMetric.Result precisionResult = response.getMetricByName(PrecisionMetric.NAME); // <3>
            double precision = precisionResult.getAvgPrecision(); // <4>

            RecallMetric.Result recallResult = response.getMetricByName(RecallMetric.NAME); // <5>
            double recall = recallResult.getAvgRecall(); // <6>

            MulticlassConfusionMatrixMetric.Result multiclassConfusionMatrix =
                response.getMetricByName(MulticlassConfusionMatrixMetric.NAME); // <7>

            List<ActualClass> confusionMatrix = multiclassConfusionMatrix.getConfusionMatrix(); // <8>
            long otherClassesCount = multiclassConfusionMatrix.getOtherActualClassCount(); // <9>

            AucRocResult aucRocResult = response.getMetricByName(AucRocMetric.NAME); // <10>
            double aucRocScore = aucRocResult.getValue(); // <11>
            // end::evaluate-data-frame-results-classification

            assertThat(accuracyResult.getMetricName(), equalTo(AccuracyMetric.NAME));
            assertThat(accuracy, equalTo(0.6));

            assertThat(precisionResult.getMetricName(), equalTo(PrecisionMetric.NAME));
            assertThat(precision, equalTo(0.675));

            assertThat(recallResult.getMetricName(), equalTo(RecallMetric.NAME));
            assertThat(recall, equalTo(0.45));

            assertThat(multiclassConfusionMatrix.getMetricName(), equalTo(MulticlassConfusionMatrixMetric.NAME));
            assertThat(
                confusionMatrix,
                equalTo(
                    List.of(
                        new ActualClass(
                            "ant",
                            1L,
                            List.of(new PredictedClass("ant", 0L), new PredictedClass("cat", 1L), new PredictedClass("dog", 0L)),
                            0L),
                        new ActualClass(
                            "cat",
                            5L,
                            List.of(new PredictedClass("ant", 0L), new PredictedClass("cat", 3L), new PredictedClass("dog", 1L)),
                            1L),
                        new ActualClass(
                            "dog",
                            4L,
                            List.of(new PredictedClass("ant", 0L), new PredictedClass("cat", 1L), new PredictedClass("dog", 3L)),
                            0L))));
            assertThat(otherClassesCount, equalTo(0L));

            assertThat(aucRocResult.getMetricName(), equalTo(AucRocMetric.NAME));
            assertThat(aucRocScore, closeTo(0.619, 1e-3));
        }
    }

    public void testEvaluateDataFrame_Regression() throws Exception {
        String indexName = "evaluate-classification-test-index";
        CreateIndexRequest createIndexRequest =
            new CreateIndexRequest(indexName)
                .mapping(XContentFactory.jsonBuilder().startObject()
                    .startObject("properties")
                        .startObject("actual_value")
                            .field("type", "double")
                        .endObject()
                        .startObject("predicted_value")
                            .field("type", "double")
                        .endObject()
                    .endObject()
                    .endObject());
        BulkRequest bulkRequest =
            new BulkRequest(indexName)
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 1.0, "predicted_value", 1.0)) // #0
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 1.0, "predicted_value", 0.9)) // #1
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 2.0, "predicted_value", 2.0)) // #2
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 1.5, "predicted_value", 1.4)) // #3
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 1.2, "predicted_value", 1.3)) // #4
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 1.7, "predicted_value", 2.0)) // #5
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 2.1, "predicted_value", 2.1)) // #6
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 2.5, "predicted_value", 2.7)) // #7
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 0.8, "predicted_value", 1.0)) // #8
                .add(new IndexRequest().source(XContentType.JSON, "actual_value", 2.5, "predicted_value", 2.4)); // #9
        RestHighLevelClient client = highLevelClient();
        client.indices().create(createIndexRequest, RequestOptions.DEFAULT);
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        {
            // tag::evaluate-data-frame-evaluation-regression
            Evaluation evaluation =
                new org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression( // <1>
                    "actual_value", // <2>
                    "predicted_value", // <3>
                    // Evaluation metrics // <4>
                    new MeanSquaredErrorMetric(), // <5>
                    new MeanSquaredLogarithmicErrorMetric(1.0), // <6>
                    new HuberMetric(1.0), // <7>
                    new RSquaredMetric()); // <8>
            // end::evaluate-data-frame-evaluation-regression

            EvaluateDataFrameRequest request = new EvaluateDataFrameRequest(indexName, null, evaluation);
            EvaluateDataFrameResponse response = client.machineLearning().evaluateDataFrame(request, RequestOptions.DEFAULT);

            // tag::evaluate-data-frame-results-regression
            MeanSquaredErrorMetric.Result meanSquaredErrorResult = response.getMetricByName(MeanSquaredErrorMetric.NAME); // <1>
            double meanSquaredError = meanSquaredErrorResult.getValue(); // <2>

            MeanSquaredLogarithmicErrorMetric.Result meanSquaredLogarithmicErrorResult =
                response.getMetricByName(MeanSquaredLogarithmicErrorMetric.NAME); // <3>
            double meanSquaredLogarithmicError = meanSquaredLogarithmicErrorResult.getValue(); // <4>

            HuberMetric.Result huberResult = response.getMetricByName(HuberMetric.NAME); // <5>
            double huber = huberResult.getValue(); // <6>

            RSquaredMetric.Result rSquaredResult = response.getMetricByName(RSquaredMetric.NAME); // <7>
            double rSquared = rSquaredResult.getValue(); // <8>
            // end::evaluate-data-frame-results-regression

            assertThat(meanSquaredError, closeTo(0.021, 1e-3));
            assertThat(meanSquaredLogarithmicError, closeTo(0.003, 1e-3));
            assertThat(huber, closeTo(0.01, 1e-3));
            assertThat(rSquared, closeTo(0.941, 1e-3));
        }
    }

    public void testExplainDataFrameAnalytics() throws Exception {
        createIndex("explain-df-test-source-index");
        BulkRequest bulkRequest =
            new BulkRequest("explain-df-test-source-index")
                .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 10; ++i) {
            bulkRequest.add(new IndexRequest().source(XContentType.JSON, "timestamp", 123456789L, "total", 10L));
        }
        RestHighLevelClient client = highLevelClient();
        client.bulk(bulkRequest, RequestOptions.DEFAULT);
        {
            // tag::explain-data-frame-analytics-id-request
            ExplainDataFrameAnalyticsRequest request = new ExplainDataFrameAnalyticsRequest("existing_job_id"); // <1>
            // end::explain-data-frame-analytics-id-request

            // tag::explain-data-frame-analytics-config-request
            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
                .setSource(DataFrameAnalyticsSource.builder().setIndex("explain-df-test-source-index").build())
                .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
                .build();
            request = new ExplainDataFrameAnalyticsRequest(config); // <1>
            // end::explain-data-frame-analytics-config-request

            // tag::explain-data-frame-analytics-execute
            ExplainDataFrameAnalyticsResponse response = client.machineLearning().explainDataFrameAnalytics(request,
                RequestOptions.DEFAULT);
            // end::explain-data-frame-analytics-execute

            // tag::explain-data-frame-analytics-response
            List<FieldSelection> fieldSelection = response.getFieldSelection(); // <1>
            MemoryEstimation memoryEstimation = response.getMemoryEstimation(); // <2>
            // end::explain-data-frame-analytics-response

            assertThat(fieldSelection.size(), equalTo(2));
            assertThat(fieldSelection.stream().map(FieldSelection::getName).collect(Collectors.toList()), contains("timestamp", "total"));

            ByteSizeValue expectedMemoryWithoutDisk = memoryEstimation.getExpectedMemoryWithoutDisk(); // <1>
            ByteSizeValue expectedMemoryWithDisk = memoryEstimation.getExpectedMemoryWithDisk(); // <2>

            // We are pretty liberal here as this test does not aim at verifying concrete numbers but rather end-to-end user workflow.
            ByteSizeValue lowerBound = new ByteSizeValue(1, ByteSizeUnit.KB);
            ByteSizeValue upperBound = new ByteSizeValue(1, ByteSizeUnit.GB);
            assertThat(expectedMemoryWithoutDisk, allOf(greaterThan(lowerBound), lessThan(upperBound)));
            assertThat(expectedMemoryWithDisk, allOf(greaterThan(lowerBound), lessThan(upperBound)));
        }
        {
            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
                .setSource(DataFrameAnalyticsSource.builder().setIndex("explain-df-test-source-index").build())
                .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
                .build();
            ExplainDataFrameAnalyticsRequest request = new ExplainDataFrameAnalyticsRequest(config);
            // tag::explain-data-frame-analytics-execute-listener
            ActionListener<ExplainDataFrameAnalyticsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(ExplainDataFrameAnalyticsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::explain-data-frame-analytics-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::explain-data-frame-analytics-execute-async
            client.machineLearning().explainDataFrameAnalyticsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::explain-data-frame-analytics-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetTrainedModels() throws Exception {
        putTrainedModel("my-trained-model");
        RestHighLevelClient client = highLevelClient();
        {
            // tag::get-trained-models-request
            GetTrainedModelsRequest request = new GetTrainedModelsRequest("my-trained-model") // <1>
                .setPageParams(new PageParams(0, 1)) // <2>
                .includeDefinition() // <3>
                .includeTotalFeatureImportance() // <4>
                .includeFeatureImportanceBaseline() // <5>
                .setDecompressDefinition(false) // <6>
                .setAllowNoMatch(true) // <7>
                .setTags("regression") // <8>
                .setExcludeGenerated(false); // <9>
            // end::get-trained-models-request
            request.setTags((List<String>)null);

            // tag::get-trained-models-execute
            GetTrainedModelsResponse response = client.machineLearning().getTrainedModels(request, RequestOptions.DEFAULT);
            // end::get-trained-models-execute

            // tag::get-trained-models-response
            List<TrainedModelConfig> models = response.getTrainedModels();
            // end::get-trained-models-response

            assertThat(models, hasSize(1));
        }
        {
            GetTrainedModelsRequest request = new GetTrainedModelsRequest("my-trained-model");

            // tag::get-trained-models-execute-listener
            ActionListener<GetTrainedModelsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(GetTrainedModelsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-trained-models-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-trained-models-execute-async
            client.machineLearning().getTrainedModelsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-trained-models-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testPutTrainedModel() throws Exception {
        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION).build();
        // tag::put-trained-model-config
        TrainedModelConfig trainedModelConfig = TrainedModelConfig.builder()
            .setDefinition(definition) // <1>
            .setCompressedDefinition(InferenceToXContentCompressor.deflate(definition)) // <2>
            .setModelId("my-new-trained-model") // <3>
            .setModelType(TrainedModelType.TREE_ENSEMBLE) // <4>
            .setInput(new TrainedModelInput("col1", "col2", "col3", "col4")) // <5>
            .setDescription("test model") // <6>
            .setMetadata(new HashMap<>()) // <7>
            .setTags("my_regression_models") // <8>
            .setInferenceConfig(new RegressionConfig("value", 0)) // <9>
            .build();
        // end::put-trained-model-config

        trainedModelConfig = TrainedModelConfig.builder()
            .setDefinition(definition)
            .setInferenceConfig(new RegressionConfig(null, null))
            .setModelId("my-new-trained-model")
            .setInput(new TrainedModelInput("col1", "col2", "col3", "col4"))
            .setDescription("test model")
            .setMetadata(new HashMap<>())
            .setTags("my_regression_models")
            .build();

        RestHighLevelClient client = highLevelClient();
        {
            // tag::put-trained-model-request
            PutTrainedModelRequest request = new PutTrainedModelRequest(trainedModelConfig); // <1>
            // end::put-trained-model-request

            // tag::put-trained-model-execute
            PutTrainedModelResponse response = client.machineLearning().putTrainedModel(request, RequestOptions.DEFAULT);
            // end::put-trained-model-execute

            // tag::put-trained-model-response
            TrainedModelConfig model = response.getResponse();
            // end::put-trained-model-response

            assertThat(model.getModelId(), equalTo(trainedModelConfig.getModelId()));
            highLevelClient().machineLearning()
                .deleteTrainedModel(new DeleteTrainedModelRequest("my-new-trained-model"), RequestOptions.DEFAULT);
        }
        {
            PutTrainedModelRequest request = new PutTrainedModelRequest(trainedModelConfig);

            // tag::put-trained-model-execute-listener
            ActionListener<PutTrainedModelResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(PutTrainedModelResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-trained-model-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-trained-model-execute-async
            client.machineLearning().putTrainedModelAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-trained-model-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

            highLevelClient().machineLearning()
                .deleteTrainedModel(new DeleteTrainedModelRequest("my-new-trained-model"), RequestOptions.DEFAULT);
        }
    }

    public void testPutTrainedModelAlias() throws Exception {
        putTrainedModel("my-trained-model-with-alias");
        RestHighLevelClient client = highLevelClient();
        {
            // tag::put-trained-model-alias-request
            PutTrainedModelAliasRequest request = new PutTrainedModelAliasRequest(
                "my-alias", // <1>
                "my-trained-model-with-alias", // <2>
                false // <3>
            );
            // end::put-trained-model-alias-request

            // tag::put-trained-model-alias-execute
            AcknowledgedResponse response =
                client.machineLearning().putTrainedModelAlias(request, RequestOptions.DEFAULT);
            // end::put-trained-model-alias-execute

            // tag::put-trained-model-alias-response
            boolean acknowledged = response.isAcknowledged();
            // end::put-trained-model-alias-response

            assertThat(acknowledged, is(true));
        }
        {
            PutTrainedModelAliasRequest request = new PutTrainedModelAliasRequest(
                "my-second-alias",
                "my-trained-model-with-alias",
                false
            );
            // tag::put-trained-model-alias-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-trained-model-alias-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-trained-model-alias-execute-async
            client.machineLearning()
                .putTrainedModelAliasAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-trained-model-alias-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteTrainedModelAlias() throws Exception {
        putTrainedModel("my-trained-model-with-delete-alias");
        RestHighLevelClient client = highLevelClient();
        {
            client.machineLearning()
                .putTrainedModelAlias(
                    new PutTrainedModelAliasRequest("my-alias-to-delete", "my-trained-model-with-delete-alias", false),
                    RequestOptions.DEFAULT
                );

            // tag::delete-trained-model-alias-request
            DeleteTrainedModelAliasRequest request = new DeleteTrainedModelAliasRequest(
                "my-alias-to-delete", // <1>
                "my-trained-model-with-delete-alias" // <2>
            );
            // end::delete-trained-model-alias-request

            // tag::delete-trained-model-alias-execute
            AcknowledgedResponse response =
                client.machineLearning().deleteTrainedModelAlias(request, RequestOptions.DEFAULT);
            // end::delete-trained-model-alias-execute

            // tag::delete-trained-model-alias-response
            boolean acknowledged = response.isAcknowledged();
            // end::delete-trained-model-alias-response

            assertThat(acknowledged, is(true));
        }
        {
            client.machineLearning()
                .putTrainedModelAlias(
                    new PutTrainedModelAliasRequest("my-alias-to-delete", "my-trained-model-with-delete-alias", false),
                    RequestOptions.DEFAULT
                );

            DeleteTrainedModelAliasRequest request = new DeleteTrainedModelAliasRequest(
                "my-alias-to-delete",
                "my-trained-model-with-delete-alias"
            );
            // tag::delete-trained-model-alias-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-trained-model-alias-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-trained-model-alias-execute-async
            client.machineLearning()
                .deleteTrainedModelAliasAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-trained-model-alias-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }


    public void testGetTrainedModelsStats() throws Exception {
        putTrainedModel("my-trained-model");
        RestHighLevelClient client = highLevelClient();
        {
            // tag::get-trained-models-stats-request
            GetTrainedModelsStatsRequest request =
                new GetTrainedModelsStatsRequest("my-trained-model") // <1>
                    .setPageParams(new PageParams(0, 1)) // <2>
                    .setAllowNoMatch(true); // <3>
            // end::get-trained-models-stats-request

            // tag::get-trained-models-stats-execute
            GetTrainedModelsStatsResponse response =
                client.machineLearning().getTrainedModelsStats(request, RequestOptions.DEFAULT);
            // end::get-trained-models-stats-execute

            // tag::get-trained-models-stats-response
            List<TrainedModelStats> models = response.getTrainedModelStats();
            // end::get-trained-models-stats-response

            assertThat(models, hasSize(1));
        }
        {
            GetTrainedModelsStatsRequest request = new GetTrainedModelsStatsRequest("my-trained-model");

            // tag::get-trained-models-stats-execute-listener
            ActionListener<GetTrainedModelsStatsResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(GetTrainedModelsStatsResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-trained-models-stats-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-trained-models-stats-execute-async
            client.machineLearning()
                .getTrainedModelsStatsAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-trained-models-stats-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteTrainedModel() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            putTrainedModel("my-trained-model");
            // tag::delete-trained-models-request
            DeleteTrainedModelRequest request = new DeleteTrainedModelRequest("my-trained-model"); // <1>
            // end::delete-trained-models-request

            // tag::delete-trained-models-execute
            AcknowledgedResponse response = client.machineLearning().deleteTrainedModel(request, RequestOptions.DEFAULT);
            // end::delete-trained-models-execute

            // tag::delete-trained-models-response
            boolean deleted = response.isAcknowledged();
            // end::delete-trained-models-response

            assertThat(deleted, is(true));
        }
        {
            putTrainedModel("my-trained-model");
            DeleteTrainedModelRequest request = new DeleteTrainedModelRequest("my-trained-model");

            // tag::delete-trained-models-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-trained-models-execute-listener

            // Replace the empty listener by a blocking listener in test
            CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-trained-models-execute-async
            client.machineLearning().deleteTrainedModelAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::delete-trained-models-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testCreateFilter() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::put-filter-config
            MlFilter.Builder filterBuilder = MlFilter.builder("my_safe_domains") // <1>
                .setDescription("A list of safe domains")   // <2>
                .setItems("*.google.com", "wikipedia.org"); // <3>
            // end::put-filter-config

            // tag::put-filter-request
            PutFilterRequest request = new PutFilterRequest(filterBuilder.build()); // <1>
            // end::put-filter-request

            // tag::put-filter-execute
            PutFilterResponse response = client.machineLearning().putFilter(request, RequestOptions.DEFAULT);
            // end::put-filter-execute

            // tag::put-filter-response
            MlFilter createdFilter = response.getResponse(); // <1>
            // end::put-filter-response
            assertThat(createdFilter.getId(), equalTo("my_safe_domains"));
        }
        {
            MlFilter.Builder filterBuilder = MlFilter.builder("safe_domains_async")
                .setDescription("A list of safe domains")
                .setItems("*.google.com", "wikipedia.org");

            PutFilterRequest request = new PutFilterRequest(filterBuilder.build());
            // tag::put-filter-execute-listener
            ActionListener<PutFilterResponse> listener = new ActionListener<PutFilterResponse>() {
                @Override
                public void onResponse(PutFilterResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::put-filter-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::put-filter-execute-async
            client.machineLearning().putFilterAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::put-filter-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetFilters() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();
        String filterId = "get-filter-doc-test";
        MlFilter.Builder filterBuilder = MlFilter.builder(filterId).setDescription("test").setItems("*.google.com", "wikipedia.org");

        client.machineLearning().putFilter(new PutFilterRequest(filterBuilder.build()), RequestOptions.DEFAULT);

        {
            // tag::get-filters-request
            GetFiltersRequest request = new GetFiltersRequest(); // <1>
            // end::get-filters-request

            // tag::get-filters-filter-id
            request.setFilterId("get-filter-doc-test"); // <1>
            // end::get-filters-filter-id

            // tag::get-filters-page-params
            request.setFrom(100); // <1>
            request.setSize(200); // <2>
            // end::get-filters-page-params

            request.setFrom(null);
            request.setSize(null);

            // tag::get-filters-execute
            GetFiltersResponse response = client.machineLearning().getFilter(request, RequestOptions.DEFAULT);
            // end::get-filters-execute

            // tag::get-filters-response
            long count = response.count(); // <1>
            List<MlFilter> filters = response.filters(); // <2>
            // end::get-filters-response
            assertEquals(1, filters.size());
        }
        {
            GetFiltersRequest request = new GetFiltersRequest();
            request.setFilterId(filterId);

            // tag::get-filters-execute-listener
            ActionListener<GetFiltersResponse> listener = new ActionListener<GetFiltersResponse>() {
                @Override
                public void onResponse(GetFiltersResponse getfiltersResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-filters-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-filters-execute-async
            client.machineLearning().getFilterAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-filters-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testUpdateFilter() throws IOException, InterruptedException {
        RestHighLevelClient client = highLevelClient();
        String filterId = "update-filter-doc-test";
        MlFilter.Builder filterBuilder = MlFilter.builder(filterId).setDescription("test").setItems("*.google.com", "wikipedia.org");

        client.machineLearning().putFilter(new PutFilterRequest(filterBuilder.build()), RequestOptions.DEFAULT);

        {
            // tag::update-filter-request
            UpdateFilterRequest request = new UpdateFilterRequest(filterId); // <1>
            // end::update-filter-request

            // tag::update-filter-description
            request.setDescription("my new description"); // <1>
            // end::update-filter-description

            // tag::update-filter-add-items
            request.setAddItems(Arrays.asList("*.bing.com", "*.elastic.co")); // <1>
            // end::update-filter-add-items

            // tag::update-filter-remove-items
            request.setRemoveItems(Arrays.asList("*.google.com")); // <1>
            // end::update-filter-remove-items

            // tag::update-filter-execute
            PutFilterResponse response = client.machineLearning().updateFilter(request, RequestOptions.DEFAULT);
            // end::update-filter-execute

            // tag::update-filter-response
            MlFilter updatedFilter = response.getResponse(); // <1>
            // end::update-filter-response
            assertEquals(request.getDescription(), updatedFilter.getDescription());
        }
        {
            UpdateFilterRequest request = new UpdateFilterRequest(filterId);

            // tag::update-filter-execute-listener
            ActionListener<PutFilterResponse> listener = new ActionListener<PutFilterResponse>() {
                @Override
                public void onResponse(PutFilterResponse putFilterResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::update-filter-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::update-filter-execute-async
            client.machineLearning().updateFilterAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::update-filter-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testDeleteFilter() throws Exception {
        RestHighLevelClient client = highLevelClient();
        String filterId = createFilter(client);

        {
            // tag::delete-filter-request
            DeleteFilterRequest request = new DeleteFilterRequest(filterId); // <1>
            // end::delete-filter-request

            // tag::delete-filter-execute
            AcknowledgedResponse response = client.machineLearning().deleteFilter(request, RequestOptions.DEFAULT);
            // end::delete-filter-execute

            // tag::delete-filter-response
            boolean isAcknowledged = response.isAcknowledged(); // <1>
            // end::delete-filter-response
            assertTrue(isAcknowledged);
        }
        filterId = createFilter(client);
        {
            DeleteFilterRequest request = new DeleteFilterRequest(filterId);
            // tag::delete-filter-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::delete-filter-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::delete-filter-execute-async
            client.machineLearning().deleteFilterAsync(request, RequestOptions.DEFAULT, listener); //<1>
            // end::delete-filter-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testGetMlInfo() throws Exception {
        RestHighLevelClient client = highLevelClient();

        {
            // tag::get-ml-info-request
            MlInfoRequest request = new MlInfoRequest(); // <1>
            // end::get-ml-info-request

            // tag::get-ml-info-execute
            MlInfoResponse response = client.machineLearning()
                .getMlInfo(request, RequestOptions.DEFAULT);
            // end::get-ml-info-execute

            // tag::get-ml-info-response
            final Map<String, Object> info = response.getInfo();// <1>
            // end::get-ml-info-response
            assertTrue(info.containsKey("defaults"));
            assertTrue(info.containsKey("limits"));
        }
        {
            MlInfoRequest request = new MlInfoRequest();

            // tag::get-ml-info-execute-listener
            ActionListener<MlInfoResponse> listener = new ActionListener<MlInfoResponse>() {
                @Override
                public void onResponse(MlInfoResponse response) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::get-ml-info-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::get-ml-info-execute-async
            client.machineLearning()
                .getMlInfoAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::get-ml-info-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    public void testSetUpgradeMode() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::set-upgrade-mode-request
            SetUpgradeModeRequest request = new SetUpgradeModeRequest(true); // <1>
            request.setTimeout(TimeValue.timeValueMinutes(10)); // <2>
            // end::set-upgrade-mode-request

            // Set to false so that the cluster setting does not have to be unset at the end of the test.
            request.setEnabled(false);

            // tag::set-upgrade-mode-execute
            AcknowledgedResponse acknowledgedResponse = client.machineLearning().setUpgradeMode(request, RequestOptions.DEFAULT);
            // end::set-upgrade-mode-execute

            // tag::set-upgrade-mode-response
            boolean acknowledged = acknowledgedResponse.isAcknowledged(); // <1>
            // end::set-upgrade-mode-response
            assertThat(acknowledged, is(true));
        }
        {
            SetUpgradeModeRequest request = new SetUpgradeModeRequest(false);

            // tag::set-upgrade-mode-execute-listener
            ActionListener<AcknowledgedResponse> listener = new ActionListener<AcknowledgedResponse>() {
                @Override
                public void onResponse(AcknowledgedResponse acknowledgedResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::set-upgrade-mode-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::set-upgrade-mode-execute-async
            client.machineLearning()
                .setUpgradeModeAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::set-upgrade-mode-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));

        }
    }

    public void testEstimateModelMemory() throws Exception {
        RestHighLevelClient client = highLevelClient();
        {
            // tag::estimate-model-memory-request
            Detector.Builder detectorBuilder = new Detector.Builder()
                .setFunction("count")
                .setPartitionFieldName("status");
            AnalysisConfig.Builder analysisConfigBuilder =
                new AnalysisConfig.Builder(Collections.singletonList(detectorBuilder.build()))
                .setBucketSpan(TimeValue.timeValueMinutes(10))
                .setInfluencers(Collections.singletonList("src_ip"));
            EstimateModelMemoryRequest request = new EstimateModelMemoryRequest(analysisConfigBuilder.build()); // <1>
            request.setOverallCardinality(Collections.singletonMap("status", 50L));                             // <2>
            request.setMaxBucketCardinality(Collections.singletonMap("src_ip", 30L));                           // <3>
            // end::estimate-model-memory-request

            // tag::estimate-model-memory-execute
            EstimateModelMemoryResponse estimateModelMemoryResponse =
                client.machineLearning().estimateModelMemory(request, RequestOptions.DEFAULT);
            // end::estimate-model-memory-execute

            // tag::estimate-model-memory-response
            ByteSizeValue modelMemoryEstimate = estimateModelMemoryResponse.getModelMemoryEstimate(); // <1>
            long estimateInBytes = modelMemoryEstimate.getBytes();
            // end::estimate-model-memory-response
            assertThat(estimateInBytes, greaterThan(10000000L));
        }
        {
            AnalysisConfig analysisConfig =
                AnalysisConfig.builder(Collections.singletonList(Detector.builder().setFunction("count").build())).build();
            EstimateModelMemoryRequest request = new EstimateModelMemoryRequest(analysisConfig);

            // tag::estimate-model-memory-execute-listener
            ActionListener<EstimateModelMemoryResponse> listener = new ActionListener<EstimateModelMemoryResponse>() {
                @Override
                public void onResponse(EstimateModelMemoryResponse estimateModelMemoryResponse) {
                    // <1>
                }

                @Override
                public void onFailure(Exception e) {
                    // <2>
                }
            };
            // end::estimate-model-memory-execute-listener

            // Replace the empty listener by a blocking listener in test
            final CountDownLatch latch = new CountDownLatch(1);
            listener = new LatchedActionListener<>(listener, latch);

            // tag::estimate-model-memory-execute-async
            client.machineLearning()
                .estimateModelMemoryAsync(request, RequestOptions.DEFAULT, listener); // <1>
            // end::estimate-model-memory-execute-async

            assertTrue(latch.await(30L, TimeUnit.SECONDS));
        }
    }

    private String createFilter(RestHighLevelClient client) throws IOException {
        MlFilter.Builder filterBuilder = MlFilter.builder("my_safe_domains")
            .setDescription("A list of safe domains")
            .setItems("*.google.com", "wikipedia.org");
        PutFilterRequest putFilterRequest = new PutFilterRequest(filterBuilder.build());
        PutFilterResponse putFilterResponse = client.machineLearning().putFilter(putFilterRequest, RequestOptions.DEFAULT);
        MlFilter createdFilter = putFilterResponse.getResponse();
        assertThat(createdFilter.getId(), equalTo("my_safe_domains"));
        return createdFilter.getId();
    }

    private void createIndex(String indexName) throws IOException {
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping(XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject("timestamp")
                    .field("type", "date")
                .endObject()
                .startObject("total")
                    .field("type", "long")
                .endObject()
            .endObject()
        .endObject());
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
    }

    private DataFrameAnalyticsState getAnalyticsState(String configId) throws IOException {
        GetDataFrameAnalyticsStatsResponse statsResponse =
            highLevelClient().machineLearning().getDataFrameAnalyticsStats(
                new GetDataFrameAnalyticsStatsRequest(configId), RequestOptions.DEFAULT);
        assertThat(statsResponse.getAnalyticsStats(), hasSize(1));
        DataFrameAnalyticsStats stats = statsResponse.getAnalyticsStats().get(0);
        return stats.getState();
    }

    private void putTrainedModel(String modelId) throws IOException {
        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION).build();
        TrainedModelConfig trainedModelConfig = TrainedModelConfig.builder()
            .setDefinition(definition)
            .setModelId(modelId)
            .setInferenceConfig(new RegressionConfig("value", 0))
            .setInput(new TrainedModelInput(Arrays.asList("col1", "col2", "col3", "col4")))
            .setDescription("test model")
            .build();
        highLevelClient().machineLearning().putTrainedModel(new PutTrainedModelRequest(trainedModelConfig), RequestOptions.DEFAULT);
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }

    private static final DataFrameAnalyticsConfig DF_ANALYTICS_CONFIG =
        DataFrameAnalyticsConfig.builder()
            .setId("my-analytics-config")
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex("put-test-source-index")
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex("put-test-dest-index")
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .build();
}
