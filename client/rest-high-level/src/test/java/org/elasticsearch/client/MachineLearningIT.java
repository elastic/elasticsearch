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
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.ingest.PutPipelineRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.indices.CreateIndexRequest;
import org.elasticsearch.client.indices.GetIndexRequest;
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
import org.elasticsearch.client.ml.DeleteTrainedModelRequest;
import org.elasticsearch.client.ml.EstimateModelMemoryRequest;
import org.elasticsearch.client.ml.EstimateModelMemoryResponse;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameResponse;
import org.elasticsearch.client.ml.ExplainDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.ExplainDataFrameAnalyticsResponse;
import org.elasticsearch.client.ml.FindFileStructureRequest;
import org.elasticsearch.client.ml.FindFileStructureResponse;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.ForecastJobResponse;
import org.elasticsearch.client.ml.GetCalendarEventsRequest;
import org.elasticsearch.client.ml.GetCalendarEventsResponse;
import org.elasticsearch.client.ml.GetCalendarsRequest;
import org.elasticsearch.client.ml.GetCalendarsResponse;
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
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsResponse;
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
import org.elasticsearch.client.ml.PutTrainedModelRequest;
import org.elasticsearch.client.ml.PutTrainedModelResponse;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.RevertModelSnapshotResponse;
import org.elasticsearch.client.ml.SetUpgradeModeRequest;
import org.elasticsearch.client.ml.StartDataFrameAnalyticsRequest;
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
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.client.ml.calendars.CalendarTests;
import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.client.ml.calendars.ScheduledEventTests;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedState;
import org.elasticsearch.client.ml.datafeed.DatafeedStats;
import org.elasticsearch.client.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsDest;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsSource;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsState;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsStats;
import org.elasticsearch.client.ml.dataframe.PhaseProgress;
import org.elasticsearch.client.ml.dataframe.QueryConfig;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.AccuracyMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.Classification;
import org.elasticsearch.client.ml.dataframe.evaluation.classification.MulticlassConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.AucRocMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.ConfusionMatrixMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.OutlierDetection;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.PrecisionMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.outlierdetection.RecallMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.HuberMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.MeanSquaredLogarithmicErrorMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.RSquaredMetric;
import org.elasticsearch.client.ml.dataframe.evaluation.regression.Regression;
import org.elasticsearch.client.ml.dataframe.explain.FieldSelection;
import org.elasticsearch.client.ml.dataframe.explain.MemoryEstimation;
import org.elasticsearch.client.ml.dataframe.stats.common.DataCounts;
import org.elasticsearch.client.ml.dataframe.stats.common.MemoryUsage;
import org.elasticsearch.client.ml.filestructurefinder.FileStructure;
import org.elasticsearch.client.ml.inference.InferenceToXContentCompressor;
import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.client.ml.inference.TrainedModelDefinition;
import org.elasticsearch.client.ml.inference.TrainedModelDefinitionTests;
import org.elasticsearch.client.ml.inference.TrainedModelInput;
import org.elasticsearch.client.ml.inference.TrainedModelStats;
import org.elasticsearch.client.ml.inference.trainedmodel.RegressionConfig;
import org.elasticsearch.client.ml.inference.trainedmodel.TargetType;
import org.elasticsearch.client.ml.inference.trainedmodel.langident.LangIdentNeuralNetwork;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.AnalysisLimits;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobState;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHit;
import org.junit.After;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

public class MachineLearningIT extends ESRestHighLevelClientTestCase {

    @After
    public void cleanUp() throws IOException {
        new MlTestStateCleaner(logger, highLevelClient().machineLearning()).clearMlMetadata();
    }

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

        GetJobRequest request = new GetJobRequest(jobId1, jobId2);

        // Test getting specific jobs
        GetJobResponse response = execute(request, machineLearningClient::getJob, machineLearningClient::getJobAsync);

        assertEquals(2, response.count());
        assertThat(response.jobs(), hasSize(2));
        assertThat(response.jobs().stream().map(Job::getId).collect(Collectors.toList()), containsInAnyOrder(jobId1, jobId2));

        // Test getting all jobs explicitly
        request = GetJobRequest.getAllJobsRequest();
        response = execute(request, machineLearningClient::getJob, machineLearningClient::getJobAsync);

        assertTrue(response.count() >= 2L);
        assertTrue(response.jobs().size() >= 2L);
        assertThat(response.jobs().stream().map(Job::getId).collect(Collectors.toList()), hasItems(jobId1, jobId2));

        // Test getting all jobs implicitly
        response = execute(new GetJobRequest(), machineLearningClient::getJob, machineLearningClient::getJobAsync);

        assertTrue(response.count() >= 2L);
        assertTrue(response.jobs().size() >= 2L);
        assertThat(response.jobs().stream().map(Job::getId).collect(Collectors.toList()), hasItems(jobId1, jobId2));
    }

    public void testDeleteJob_GivenWaitForCompletionIsTrue() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        DeleteJobResponse response = execute(new DeleteJobRequest(jobId),
            machineLearningClient::deleteJob,
            machineLearningClient::deleteJobAsync);

        assertTrue(response.getAcknowledged());
        assertNull(response.getTask());
    }

    public void testDeleteJob_GivenWaitForCompletionIsFalse() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        DeleteJobRequest deleteJobRequest = new DeleteJobRequest(jobId);
        deleteJobRequest.setWaitForCompletion(false);

        DeleteJobResponse response = execute(deleteJobRequest, machineLearningClient::deleteJob, machineLearningClient::deleteJobAsync);

        assertNull(response.getAcknowledged());
        assertNotNull(response.getTask());
    }

    public void testOpenJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        OpenJobResponse response = execute(new OpenJobRequest(jobId), machineLearningClient::openJob, machineLearningClient::openJobAsync);

        assertTrue(response.isOpened());
    }

    public void testCloseJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);

        CloseJobResponse response = execute(new CloseJobRequest(jobId),
            machineLearningClient::closeJob,
            machineLearningClient::closeJobAsync);
        assertTrue(response.isClosed());
    }

    public void testFlushJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);

        FlushJobResponse response = execute(new FlushJobRequest(jobId),
            machineLearningClient::flushJob,
            machineLearningClient::flushJobAsync);
        assertTrue(response.isFlushed());
    }

    public void testGetJobStats() throws Exception {
        String jobId1 = "ml-get-job-stats-test-id-1";
        String jobId2 = "ml-get-job-stats-test-id-2";

        Job job1 = buildJob(jobId1);
        Job job2 = buildJob(jobId2);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job1), RequestOptions.DEFAULT);
        machineLearningClient.putJob(new PutJobRequest(job2), RequestOptions.DEFAULT);

        machineLearningClient.openJob(new OpenJobRequest(jobId1), RequestOptions.DEFAULT);

        GetJobStatsRequest request = new GetJobStatsRequest(jobId1, jobId2);

        // Test getting specific
        GetJobStatsResponse response = execute(request, machineLearningClient::getJobStats, machineLearningClient::getJobStatsAsync);

        assertEquals(2, response.count());
        assertThat(response.jobStats(), hasSize(2));
        assertThat(response.jobStats().stream().map(JobStats::getJobId).collect(Collectors.toList()), containsInAnyOrder(jobId1, jobId2));
        for (JobStats stats : response.jobStats()) {
            if (stats.getJobId().equals(jobId1)) {
                assertEquals(JobState.OPENED, stats.getState());
            } else {
                assertEquals(JobState.CLOSED, stats.getState());
            }
        }

        // Test getting all explicitly
        request = GetJobStatsRequest.getAllJobStatsRequest();
        response = execute(request, machineLearningClient::getJobStats, machineLearningClient::getJobStatsAsync);

        assertTrue(response.count() >= 2L);
        assertTrue(response.jobStats().size() >= 2L);
        assertThat(response.jobStats().stream().map(JobStats::getJobId).collect(Collectors.toList()), hasItems(jobId1, jobId2));

        // Test getting all implicitly
        response = execute(new GetJobStatsRequest(), machineLearningClient::getJobStats, machineLearningClient::getJobStatsAsync);

        assertTrue(response.count() >= 2L);
        assertTrue(response.jobStats().size() >= 2L);
        assertThat(response.jobStats().stream().map(JobStats::getJobId).collect(Collectors.toList()), hasItems(jobId1, jobId2));

        // Test getting all with wildcard
        request = new GetJobStatsRequest("ml-get-job-stats-test-id-*");
        response = execute(request, machineLearningClient::getJobStats, machineLearningClient::getJobStatsAsync);
        assertTrue(response.count() >= 2L);
        assertTrue(response.jobStats().size() >= 2L);
        assertThat(response.jobStats().stream().map(JobStats::getJobId).collect(Collectors.toList()), hasItems(jobId1, jobId2));

        // Test when allow_no_match is false
        final GetJobStatsRequest erroredRequest = new GetJobStatsRequest("jobs-that-do-not-exist*");
        erroredRequest.setAllowNoMatch(false);
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(erroredRequest, machineLearningClient::getJobStats, machineLearningClient::getJobStatsAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testForecastJob() throws Exception {
        String jobId = "ml-forecast-job-test";
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);

        PostDataRequest.JsonBuilder builder = new PostDataRequest.JsonBuilder();
        for(int i = 0; i < 30; i++) {
            Map<String, Object> hashMap = new HashMap<>();
            hashMap.put("total", randomInt(1000));
            hashMap.put("timestamp", (i+1)*1000);
            builder.addDoc(hashMap);
        }
        PostDataRequest postDataRequest = new PostDataRequest(jobId, builder);
        machineLearningClient.postData(postDataRequest, RequestOptions.DEFAULT);
        machineLearningClient.flushJob(new FlushJobRequest(jobId), RequestOptions.DEFAULT);

        ForecastJobRequest request = new ForecastJobRequest(jobId);
        ForecastJobResponse response = execute(request, machineLearningClient::forecastJob, machineLearningClient::forecastJobAsync);

        assertTrue(response.isAcknowledged());
        assertNotNull(response.getForecastId());
    }

    public void testPostData() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);

        PostDataRequest.JsonBuilder builder = new PostDataRequest.JsonBuilder();
        for(int i = 0; i < 10; i++) {
            Map<String, Object> hashMap = new HashMap<>();
            hashMap.put("total", randomInt(1000));
            hashMap.put("timestamp", (i+1)*1000);
            builder.addDoc(hashMap);
        }
        PostDataRequest postDataRequest = new PostDataRequest(jobId, builder);

        PostDataResponse response = execute(postDataRequest, machineLearningClient::postData, machineLearningClient::postDataAsync);
        assertEquals(10, response.getDataCounts().getInputRecordCount());
        assertEquals(0, response.getDataCounts().getOutOfOrderTimeStampCount());
    }

    public void testUpdateJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        UpdateJobRequest request = new UpdateJobRequest(new JobUpdate.Builder(jobId).setDescription("Updated description").build());

        PutJobResponse response = execute(request, machineLearningClient::updateJob, machineLearningClient::updateJobAsync);

        assertEquals("Updated description", response.getResponse().getDescription());

        GetJobRequest getRequest = new GetJobRequest(jobId);
        GetJobResponse getResponse = machineLearningClient.getJob(getRequest, RequestOptions.DEFAULT);
        assertEquals("Updated description", getResponse.jobs().get(0).getDescription());
    }

    public void testPutDatafeed() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        execute(new PutJobRequest(job), machineLearningClient::putJob, machineLearningClient::putJobAsync);

        String datafeedId = "datafeed-" + jobId;
        DatafeedConfig datafeedConfig = DatafeedConfig.builder(datafeedId, jobId).setIndices("some_data_index").build();

        PutDatafeedResponse response = execute(new PutDatafeedRequest(datafeedConfig), machineLearningClient::putDatafeed,
                machineLearningClient::putDatafeedAsync);

        DatafeedConfig createdDatafeed = response.getResponse();
        assertThat(createdDatafeed.getId(), equalTo(datafeedId));
        assertThat(createdDatafeed.getIndices(), equalTo(datafeedConfig.getIndices()));
    }

    public void testUpdateDatafeed() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        execute(new PutJobRequest(job), machineLearningClient::putJob, machineLearningClient::putJobAsync);

        String datafeedId = "datafeed-" + jobId;
        DatafeedConfig datafeedConfig = DatafeedConfig.builder(datafeedId, jobId).setIndices("some_data_index").build();

        PutDatafeedResponse response = machineLearningClient.putDatafeed(new PutDatafeedRequest(datafeedConfig), RequestOptions.DEFAULT);

        DatafeedConfig createdDatafeed = response.getResponse();
        assertThat(createdDatafeed.getId(), equalTo(datafeedId));
        assertThat(createdDatafeed.getIndices(), equalTo(datafeedConfig.getIndices()));

        DatafeedUpdate datafeedUpdate = DatafeedUpdate.builder(datafeedId).setIndices("some_other_data_index").setScrollSize(10).build();

        response = execute(new UpdateDatafeedRequest(datafeedUpdate),
            machineLearningClient::updateDatafeed,
            machineLearningClient::updateDatafeedAsync);

        DatafeedConfig updatedDatafeed = response.getResponse();
        assertThat(datafeedUpdate.getId(), equalTo(updatedDatafeed.getId()));
        assertThat(datafeedUpdate.getIndices(), equalTo(updatedDatafeed.getIndices()));
        assertThat(datafeedUpdate.getScrollSize(), equalTo(updatedDatafeed.getScrollSize()));
    }

    public void testGetDatafeed() throws Exception {
        String jobId1 = "test-get-datafeed-job-1";
        String jobId2 = "test-get-datafeed-job-2";
        Job job1 = buildJob(jobId1);
        Job job2 = buildJob(jobId2);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job1), RequestOptions.DEFAULT);
        machineLearningClient.putJob(new PutJobRequest(job2), RequestOptions.DEFAULT);

        String datafeedId1 = jobId1 + "-feed";
        String datafeedId2 = jobId2 + "-feed";
        DatafeedConfig datafeed1 = DatafeedConfig.builder(datafeedId1, jobId1).setIndices("data_1").build();
        DatafeedConfig datafeed2 = DatafeedConfig.builder(datafeedId2, jobId2).setIndices("data_2").build();
        machineLearningClient.putDatafeed(new PutDatafeedRequest(datafeed1), RequestOptions.DEFAULT);
        machineLearningClient.putDatafeed(new PutDatafeedRequest(datafeed2), RequestOptions.DEFAULT);

        // Test getting specific datafeeds
        {
            GetDatafeedRequest request = new GetDatafeedRequest(datafeedId1, datafeedId2);
            GetDatafeedResponse response = execute(request, machineLearningClient::getDatafeed, machineLearningClient::getDatafeedAsync);

            assertEquals(2, response.count());
            assertThat(response.datafeeds(), hasSize(2));
            assertThat(response.datafeeds().stream().map(DatafeedConfig::getId).collect(Collectors.toList()),
                    containsInAnyOrder(datafeedId1, datafeedId2));
        }

        // Test getting a single one
        {
            GetDatafeedRequest request = new GetDatafeedRequest(datafeedId1);
            GetDatafeedResponse response = execute(request, machineLearningClient::getDatafeed, machineLearningClient::getDatafeedAsync);

            assertTrue(response.count() == 1L);
            assertThat(response.datafeeds().get(0).getId(), equalTo(datafeedId1));
        }

        // Test getting all datafeeds explicitly
        {
            GetDatafeedRequest request = GetDatafeedRequest.getAllDatafeedsRequest();
            GetDatafeedResponse response = execute(request, machineLearningClient::getDatafeed, machineLearningClient::getDatafeedAsync);

            assertTrue(response.count() == 2L);
            assertTrue(response.datafeeds().size() == 2L);
            assertThat(response.datafeeds().stream().map(DatafeedConfig::getId).collect(Collectors.toList()),
                    hasItems(datafeedId1, datafeedId2));
        }

        // Test getting all datafeeds implicitly
        {
            GetDatafeedResponse response = execute(new GetDatafeedRequest(), machineLearningClient::getDatafeed,
                    machineLearningClient::getDatafeedAsync);

            assertTrue(response.count() >= 2L);
            assertTrue(response.datafeeds().size() >= 2L);
            assertThat(response.datafeeds().stream().map(DatafeedConfig::getId).collect(Collectors.toList()),
                    hasItems(datafeedId1, datafeedId2));
        }

        // Test get missing pattern with allow_no_match set to true
        {
            GetDatafeedRequest request = new GetDatafeedRequest("missing-*");

            GetDatafeedResponse response = execute(request, machineLearningClient::getDatafeed, machineLearningClient::getDatafeedAsync);

            assertThat(response.count(), equalTo(0L));
        }

        // Test get missing pattern with allow_no_match set to false
        {
            GetDatafeedRequest request = new GetDatafeedRequest("missing-*");
            request.setAllowNoMatch(false);

            ElasticsearchStatusException e = expectThrows(ElasticsearchStatusException.class,
                    () -> execute(request, machineLearningClient::getDatafeed, machineLearningClient::getDatafeedAsync));
            assertThat(e.status(), equalTo(RestStatus.NOT_FOUND));
        }
    }

    public void testDeleteDatafeed() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        String datafeedId = "datafeed-" + jobId;
        DatafeedConfig datafeedConfig = DatafeedConfig.builder(datafeedId, jobId).setIndices("some_data_index").build();
        execute(new PutDatafeedRequest(datafeedConfig), machineLearningClient::putDatafeed, machineLearningClient::putDatafeedAsync);

        AcknowledgedResponse response = execute(new DeleteDatafeedRequest(datafeedId), machineLearningClient::deleteDatafeed,
                machineLearningClient::deleteDatafeedAsync);

        assertTrue(response.isAcknowledged());
    }

    public void testStartDatafeed() throws Exception {
        String jobId = "test-start-datafeed";
        String indexName = "start_data_1";

        // Set up the index and docs
        createIndex(indexName, defaultMappingForTest());
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        long now = (System.currentTimeMillis()/1000)*1000;
        long thePast = now - 60000;
        int i = 0;
        long pastCopy = thePast;
        while(pastCopy < now) {
            IndexRequest doc = new IndexRequest();
            doc.index(indexName);
            doc.id("id" + i);
            doc.source("{\"total\":" +randomInt(1000) + ",\"timestamp\":"+ pastCopy +"}", XContentType.JSON);
            bulk.add(doc);
            pastCopy += 1000;
            i++;
        }
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);
        final long totalDocCount = i;

        // create the job and the datafeed
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);

        String datafeedId = jobId + "-feed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, jobId)
            .setIndices(indexName)
            .setQueryDelay(TimeValue.timeValueSeconds(1))
            .setFrequency(TimeValue.timeValueSeconds(1)).build();
        machineLearningClient.putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);


        StartDatafeedRequest startDatafeedRequest = new StartDatafeedRequest(datafeedId);
        startDatafeedRequest.setStart(String.valueOf(thePast));
        // Should only process two documents
        startDatafeedRequest.setEnd(String.valueOf(thePast + 2000));
        StartDatafeedResponse response = execute(startDatafeedRequest,
            machineLearningClient::startDatafeed,
            machineLearningClient::startDatafeedAsync);

        assertTrue(response.isStarted());

        assertBusy(() -> {
            JobStats stats = machineLearningClient.getJobStats(new GetJobStatsRequest(jobId), RequestOptions.DEFAULT).jobStats().get(0);
            assertEquals(2L, stats.getDataCounts().getInputRecordCount());
            assertEquals(JobState.CLOSED, stats.getState());
        }, 30, TimeUnit.SECONDS);

        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);
        StartDatafeedRequest wholeDataFeed = new StartDatafeedRequest(datafeedId);
        // Process all documents and end the stream
        wholeDataFeed.setEnd(String.valueOf(now));
        StartDatafeedResponse wholeResponse = execute(wholeDataFeed,
            machineLearningClient::startDatafeed,
            machineLearningClient::startDatafeedAsync);
        assertTrue(wholeResponse.isStarted());

        assertBusy(() -> {
            JobStats stats = machineLearningClient.getJobStats(new GetJobStatsRequest(jobId), RequestOptions.DEFAULT).jobStats().get(0);
            assertEquals(totalDocCount, stats.getDataCounts().getInputRecordCount());
            assertEquals(JobState.CLOSED, stats.getState());
        }, 30, TimeUnit.SECONDS);
    }

    public void testStopDatafeed() throws Exception {
        String jobId1 = "test-stop-datafeed1";
        String jobId2 = "test-stop-datafeed2";
        String jobId3 = "test-stop-datafeed3";
        String indexName = "stop_data_1";

        // Set up the index
        createIndex(indexName, defaultMappingForTest());

        // create the job and the datafeed
        Job job1 = buildJob(jobId1);
        putJob(job1);
        openJob(job1);

        Job job2 = buildJob(jobId2);
        putJob(job2);
        openJob(job2);

        Job job3 = buildJob(jobId3);
        putJob(job3);
        openJob(job3);

        String datafeedId1 = createAndPutDatafeed(jobId1, indexName);
        String datafeedId2 = createAndPutDatafeed(jobId2, indexName);
        String datafeedId3 = createAndPutDatafeed(jobId3, indexName);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        machineLearningClient.startDatafeed(new StartDatafeedRequest(datafeedId1), RequestOptions.DEFAULT);
        machineLearningClient.startDatafeed(new StartDatafeedRequest(datafeedId2), RequestOptions.DEFAULT);
        machineLearningClient.startDatafeed(new StartDatafeedRequest(datafeedId3), RequestOptions.DEFAULT);

        {
            StopDatafeedRequest request = new StopDatafeedRequest(datafeedId1);
            request.setAllowNoMatch(false);
            StopDatafeedResponse stopDatafeedResponse = execute(request,
                machineLearningClient::stopDatafeed,
                machineLearningClient::stopDatafeedAsync);
            assertTrue(stopDatafeedResponse.isStopped());
        }
        {
            StopDatafeedRequest request = new StopDatafeedRequest(datafeedId2, datafeedId3);
            request.setAllowNoMatch(false);
            StopDatafeedResponse stopDatafeedResponse = execute(request,
                machineLearningClient::stopDatafeed,
                machineLearningClient::stopDatafeedAsync);
            assertTrue(stopDatafeedResponse.isStopped());
        }
        {
            StopDatafeedResponse stopDatafeedResponse = execute(new StopDatafeedRequest("datafeed_that_doesnot_exist*"),
                machineLearningClient::stopDatafeed,
                machineLearningClient::stopDatafeedAsync);
            assertTrue(stopDatafeedResponse.isStopped());
        }
        {
            StopDatafeedRequest request = new StopDatafeedRequest("datafeed_that_doesnot_exist*");
            request.setAllowNoMatch(false);
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
                () -> execute(request, machineLearningClient::stopDatafeed, machineLearningClient::stopDatafeedAsync));
            assertThat(exception.status().getStatus(), equalTo(404));
        }
    }

    public void testGetDatafeedStats() throws Exception {
        String jobId1 = "ml-get-datafeed-stats-test-id-1";
        String jobId2 = "ml-get-datafeed-stats-test-id-2";
        String indexName = "datafeed_stats_data_1";

        // Set up the index
        createIndex(indexName, defaultMappingForTest());

        // create the job and the datafeed
        Job job1 = buildJob(jobId1);
        putJob(job1);
        openJob(job1);

        Job job2 = buildJob(jobId2);
        putJob(job2);

        String datafeedId1 = createAndPutDatafeed(jobId1, indexName);
        String datafeedId2 = createAndPutDatafeed(jobId2, indexName);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        machineLearningClient.startDatafeed(new StartDatafeedRequest(datafeedId1), RequestOptions.DEFAULT);

        GetDatafeedStatsRequest request = new GetDatafeedStatsRequest(datafeedId1);

        // Test getting specific
        GetDatafeedStatsResponse response =
            execute(request, machineLearningClient::getDatafeedStats, machineLearningClient::getDatafeedStatsAsync);

        assertEquals(1, response.count());
        assertThat(response.datafeedStats(), hasSize(1));
        assertThat(response.datafeedStats().get(0).getDatafeedId(), equalTo(datafeedId1));
        assertThat(response.datafeedStats().get(0).getDatafeedState().toString(), equalTo(DatafeedState.STARTED.toString()));

        // Test getting all explicitly
        request = GetDatafeedStatsRequest.getAllDatafeedStatsRequest();
        response = execute(request, machineLearningClient::getDatafeedStats, machineLearningClient::getDatafeedStatsAsync);

        assertTrue(response.count() >= 2L);
        assertTrue(response.datafeedStats().size() >= 2L);
        assertThat(response.datafeedStats().stream().map(DatafeedStats::getDatafeedId).collect(Collectors.toList()),
            hasItems(datafeedId1, datafeedId2));

        // Test getting all implicitly
        response =
            execute(new GetDatafeedStatsRequest(), machineLearningClient::getDatafeedStats, machineLearningClient::getDatafeedStatsAsync);

        assertTrue(response.count() >= 2L);
        assertTrue(response.datafeedStats().size() >= 2L);
        assertThat(response.datafeedStats().stream().map(DatafeedStats::getDatafeedId).collect(Collectors.toList()),
            hasItems(datafeedId1, datafeedId2));

        // Test getting all with wildcard
        request = new GetDatafeedStatsRequest("ml-get-datafeed-stats-test-id-*");
        response = execute(request, machineLearningClient::getDatafeedStats, machineLearningClient::getDatafeedStatsAsync);
        assertEquals(2L, response.count());
        assertThat(response.datafeedStats(), hasSize(2));
        assertThat(response.datafeedStats().stream().map(DatafeedStats::getDatafeedId).collect(Collectors.toList()),
            hasItems(datafeedId1, datafeedId2));

        // Test when allow_no_match is false
        final GetDatafeedStatsRequest erroredRequest = new GetDatafeedStatsRequest("datafeeds-that-do-not-exist*");
        erroredRequest.setAllowNoMatch(false);
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(erroredRequest, machineLearningClient::getDatafeedStats, machineLearningClient::getDatafeedStatsAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testPreviewDatafeed() throws Exception {
        String jobId = "test-preview-datafeed";
        String indexName = "preview_data_1";

        // Set up the index and docs
        createIndex(indexName, defaultMappingForTest());
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        long now = (System.currentTimeMillis()/1000)*1000;
        long thePast = now - 60000;
        int i = 0;
        List<Integer> totalTotals = new ArrayList<>(60);
        while(thePast < now) {
            Integer total = randomInt(1000);
            IndexRequest doc = new IndexRequest();
            doc.index(indexName);
            doc.id("id" + i);
            doc.source("{\"total\":" + total + ",\"timestamp\":"+ thePast +"}", XContentType.JSON);
            bulk.add(doc);
            thePast += 1000;
            i++;
            totalTotals.add(total);
        }
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        // create the job and the datafeed
        Job job = buildJob(jobId);
        putJob(job);
        openJob(job);

        String datafeedId = jobId + "-feed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, jobId)
            .setIndices(indexName)
            .setQueryDelay(TimeValue.timeValueSeconds(1))
            .setFrequency(TimeValue.timeValueSeconds(1)).build();
        machineLearningClient.putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);

        PreviewDatafeedResponse response = execute(new PreviewDatafeedRequest(datafeedId),
            machineLearningClient::previewDatafeed,
            machineLearningClient::previewDatafeedAsync);

        Integer[] totals = response.getDataList().stream().map(map -> (Integer)map.get("total")).toArray(Integer[]::new);
        assertThat(totalTotals, containsInAnyOrder(totals));
    }

    public void testDeleteExpiredDataGivenNothingToDelete() throws Exception {
        // Tests that nothing goes wrong when there's nothing to delete
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        DeleteExpiredDataResponse response = execute(new DeleteExpiredDataRequest(),
            machineLearningClient::deleteExpiredData,
            machineLearningClient::deleteExpiredDataAsync);

        assertTrue(response.getDeleted());
    }

    private  String createExpiredData(String jobId) throws Exception {
        String indexName = jobId + "-data";
        // Set up the index and docs
        createIndex(indexName, defaultMappingForTest());
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);

        long nowMillis = System.currentTimeMillis();
        int totalBuckets = 2 * 24;
        int normalRate = 10;
        int anomalousRate = 100;
        int anomalousBucket = 30;
        for (int bucket = 0; bucket < totalBuckets; bucket++) {
            long timestamp = nowMillis - TimeValue.timeValueHours(totalBuckets - bucket).getMillis();
            int bucketRate = bucket == anomalousBucket ? anomalousRate : normalRate;
            for (int point = 0; point < bucketRate; point++) {
                IndexRequest indexRequest = new IndexRequest(indexName);
                indexRequest.source(XContentType.JSON, "timestamp", timestamp, "total", randomInt(1000));
                bulk.add(indexRequest);
            }
        }
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);

        {
            // Index a randomly named unused state document
            String docId = "non_existing_job_" + randomFrom("model_state_1234567#1", "quantiles", "categorizer_state#1");
            IndexRequest indexRequest = new IndexRequest(".ml-state-000001").id(docId);
            indexRequest.source(Collections.emptyMap(), XContentType.JSON);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            highLevelClient().index(indexRequest, RequestOptions.DEFAULT);
        }

        Job job = buildJobForExpiredDataTests(jobId);
        putJob(job);
        openJob(job);
        String datafeedId = createAndPutDatafeed(jobId, indexName);

        startDatafeed(datafeedId, String.valueOf(0), String.valueOf(nowMillis - TimeValue.timeValueHours(24).getMillis()));

        waitForJobToClose(jobId);

        long prevJobTimeStamp = System.currentTimeMillis() / 1000;

        // Check that the current timestamp component, in seconds, differs from previously.
        // Note that we used to use an 'awaitBusy(() -> false, 1, TimeUnit.SECONDS);'
        // for the same purpose but the new approach...
        // a) explicitly checks that the timestamps, in seconds, are actually different and
        // b) is slightly more efficient since we may not need to wait an entire second for the timestamp to increment
        assertBusy(() -> {
            long timeNow = System.currentTimeMillis() / 1000;
            assertThat(prevJobTimeStamp, lessThan(timeNow));
        });

        // Update snapshot timestamp to force it out of snapshot retention window
        long oneDayAgo = nowMillis - TimeValue.timeValueHours(24).getMillis() - 1;
        updateModelSnapshotTimestamp(jobId, String.valueOf(oneDayAgo));

        openJob(job);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        ForecastJobRequest forecastJobRequest = new ForecastJobRequest(jobId);
        forecastJobRequest.setDuration(TimeValue.timeValueHours(3));
        forecastJobRequest.setExpiresIn(TimeValue.timeValueSeconds(1));
        ForecastJobResponse forecastJobResponse = machineLearningClient.forecastJob(forecastJobRequest, RequestOptions.DEFAULT);

        waitForForecastToComplete(jobId, forecastJobResponse.getForecastId());

        // Wait for the forecast to expire
        // FIXME: We should wait for something specific to change, rather than waiting for time to pass.
        waitUntil(() -> false, 1, TimeUnit.SECONDS);

        // Run up to now
        startDatafeed(datafeedId, String.valueOf(0), String.valueOf(nowMillis));

        waitForJobToClose(jobId);

        return forecastJobResponse.getForecastId();
    }

    public void testDeleteExpiredData() throws Exception {

        String jobId = "test-delete-expired-data";

        String forecastId = createExpiredData(jobId);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        GetModelSnapshotsRequest getModelSnapshotsRequest = new GetModelSnapshotsRequest(jobId);
        GetModelSnapshotsResponse getModelSnapshotsResponse = execute(getModelSnapshotsRequest, machineLearningClient::getModelSnapshots,
            machineLearningClient::getModelSnapshotsAsync);

        assertEquals(2L, getModelSnapshotsResponse.count());

        assertTrue(forecastExists(jobId, forecastId));

        {
            // Verify .ml-state* contains the expected unused state document
            Iterable<SearchHit> hits = searchAll(".ml-state*");
            List<SearchHit> target = new ArrayList<>();
            hits.forEach(target::add);
            long numMatches = target.stream()
                .filter(c -> c.getId().startsWith("non_existing_job"))
                .count();

            assertThat(numMatches, equalTo(1L));
        }

        DeleteExpiredDataRequest request = new DeleteExpiredDataRequest();
        DeleteExpiredDataResponse response = execute(request, machineLearningClient::deleteExpiredData,
            machineLearningClient::deleteExpiredDataAsync);

        assertTrue(response.getDeleted());

        // Wait for the forecast to expire
        // FIXME: We should wait for something specific to change, rather than waiting for time to pass.
        waitUntil(() -> false, 1, TimeUnit.SECONDS);

        GetModelSnapshotsRequest getModelSnapshotsRequest1 = new GetModelSnapshotsRequest(jobId);
        GetModelSnapshotsResponse getModelSnapshotsResponse1 = execute(getModelSnapshotsRequest1, machineLearningClient::getModelSnapshots,
            machineLearningClient::getModelSnapshotsAsync);

        assertEquals(1L, getModelSnapshotsResponse1.count());

        assertFalse(forecastExists(jobId, forecastId));

        {
            // Verify .ml-state* doesn't contain unused state documents
            Iterable<SearchHit> hits = searchAll(".ml-state*");
            List<SearchHit> hitList = new ArrayList<>();
            hits.forEach(hitList::add);
            long numMatches = hitList.stream()
                .filter(c -> c.getId().startsWith("non_existing_job"))
                .count();

            assertThat(numMatches, equalTo(0L));
        }
    }

    public void testDeleteForecast() throws Exception {
        String jobId = "test-delete-forecast";

        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
        machineLearningClient.openJob(new OpenJobRequest(jobId), RequestOptions.DEFAULT);

        Job noForecastsJob = buildJob("test-delete-forecast-none");
        machineLearningClient.putJob(new PutJobRequest(noForecastsJob), RequestOptions.DEFAULT);

        PostDataRequest.JsonBuilder builder = new PostDataRequest.JsonBuilder();
        for(int i = 0; i < 30; i++) {
            Map<String, Object> hashMap = new HashMap<>();
            hashMap.put("total", randomInt(1000));
            hashMap.put("timestamp", (i+1)*1000);
            builder.addDoc(hashMap);
        }

        PostDataRequest postDataRequest = new PostDataRequest(jobId, builder);
        machineLearningClient.postData(postDataRequest, RequestOptions.DEFAULT);
        machineLearningClient.flushJob(new FlushJobRequest(jobId), RequestOptions.DEFAULT);
        ForecastJobResponse forecastJobResponse1 = machineLearningClient.forecastJob(new ForecastJobRequest(jobId), RequestOptions.DEFAULT);
        ForecastJobResponse forecastJobResponse2 = machineLearningClient.forecastJob(new ForecastJobRequest(jobId), RequestOptions.DEFAULT);
        waitForForecastToComplete(jobId, forecastJobResponse1.getForecastId());
        waitForForecastToComplete(jobId, forecastJobResponse2.getForecastId());

        {
            DeleteForecastRequest request = new DeleteForecastRequest(jobId);
            request.setForecastIds(forecastJobResponse1.getForecastId(), forecastJobResponse2.getForecastId());
            AcknowledgedResponse response = execute(request, machineLearningClient::deleteForecast,
                machineLearningClient::deleteForecastAsync);
            assertTrue(response.isAcknowledged());
            assertFalse(forecastExists(jobId, forecastJobResponse1.getForecastId()));
            assertFalse(forecastExists(jobId, forecastJobResponse2.getForecastId()));
        }
        {
            DeleteForecastRequest request = DeleteForecastRequest.deleteAllForecasts(noForecastsJob.getId());
            request.setAllowNoForecasts(true);
            AcknowledgedResponse response = execute(request, machineLearningClient::deleteForecast,
                machineLearningClient::deleteForecastAsync);
            assertTrue(response.isAcknowledged());
        }
        {
            DeleteForecastRequest request = DeleteForecastRequest.deleteAllForecasts(noForecastsJob.getId());
            request.setAllowNoForecasts(false);
            ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
                () -> execute(request, machineLearningClient::deleteForecast, machineLearningClient::deleteForecastAsync));
            assertThat(exception.status().getStatus(), equalTo(404));
        }
    }

    private void waitForForecastToComplete(String jobId, String forecastId) throws Exception {
        GetRequest request = new GetRequest(".ml-anomalies-" + jobId);
        request.id(jobId + "_model_forecast_request_stats_" + forecastId);
        assertBusy(() -> {
            GetResponse getResponse = highLevelClient().get(request, RequestOptions.DEFAULT);
            assertTrue(getResponse.isExists());
            assertTrue(getResponse.getSourceAsString().contains("finished"));
        }, 30, TimeUnit.SECONDS);
    }

    private boolean forecastExists(String jobId, String forecastId) throws Exception {
        GetRequest getRequest = new GetRequest(".ml-anomalies-" + jobId);
        getRequest.id(jobId + "_model_forecast_request_stats_" + forecastId);
        GetResponse getResponse = highLevelClient().get(getRequest, RequestOptions.DEFAULT);
        return getResponse.isExists();
    }

    public void testPutCalendar() throws IOException {
        Calendar calendar = CalendarTests.testInstance();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        PutCalendarResponse putCalendarResponse = execute(new PutCalendarRequest(calendar), machineLearningClient::putCalendar,
                machineLearningClient::putCalendarAsync);

        assertThat(putCalendarResponse.getCalendar(), equalTo(calendar));
    }

    public void testPutCalendarJob() throws IOException {
        Calendar calendar = new Calendar("put-calendar-job-id", Collections.singletonList("put-calendar-job-0"), null);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        PutCalendarResponse putCalendarResponse =
            machineLearningClient.putCalendar(new PutCalendarRequest(calendar), RequestOptions.DEFAULT);

        assertThat(putCalendarResponse.getCalendar().getJobIds(), containsInAnyOrder( "put-calendar-job-0"));

        String jobId1 = "put-calendar-job-1";
        String jobId2 = "put-calendar-job-2";

        PutCalendarJobRequest putCalendarJobRequest = new PutCalendarJobRequest(calendar.getId(), jobId1, jobId2);

        putCalendarResponse = execute(putCalendarJobRequest,
            machineLearningClient::putCalendarJob,
            machineLearningClient::putCalendarJobAsync);

        assertThat(putCalendarResponse.getCalendar().getJobIds(), containsInAnyOrder(jobId1, jobId2, "put-calendar-job-0"));
    }

    public void testDeleteCalendarJob() throws IOException {
        Calendar calendar = new Calendar("del-calendar-job-id",
            Arrays.asList("del-calendar-job-0", "del-calendar-job-1", "del-calendar-job-2"),
            null);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        PutCalendarResponse putCalendarResponse =
            machineLearningClient.putCalendar(new PutCalendarRequest(calendar), RequestOptions.DEFAULT);

        assertThat(putCalendarResponse.getCalendar().getJobIds(),
            containsInAnyOrder("del-calendar-job-0", "del-calendar-job-1", "del-calendar-job-2"));

        String jobId1 = "del-calendar-job-0";
        String jobId2 = "del-calendar-job-2";

        DeleteCalendarJobRequest deleteCalendarJobRequest = new DeleteCalendarJobRequest(calendar.getId(), jobId1, jobId2);

        putCalendarResponse = execute(deleteCalendarJobRequest,
            machineLearningClient::deleteCalendarJob,
            machineLearningClient::deleteCalendarJobAsync);

        assertThat(putCalendarResponse.getCalendar().getJobIds(), containsInAnyOrder("del-calendar-job-1"));
    }

    public void testGetCalendars() throws Exception {
        Calendar calendar1 = CalendarTests.testInstance();
        Calendar calendar2 = CalendarTests.testInstance();

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putCalendar(new PutCalendarRequest(calendar1), RequestOptions.DEFAULT);
        machineLearningClient.putCalendar(new PutCalendarRequest(calendar2), RequestOptions.DEFAULT);

        GetCalendarsRequest getCalendarsRequest = new GetCalendarsRequest();
        getCalendarsRequest.setCalendarId("_all");
        GetCalendarsResponse getCalendarsResponse = execute(getCalendarsRequest, machineLearningClient::getCalendars,
                machineLearningClient::getCalendarsAsync);
        assertEquals(2, getCalendarsResponse.count());
        assertEquals(2, getCalendarsResponse.calendars().size());
        assertThat(getCalendarsResponse.calendars().stream().map(Calendar::getId).collect(Collectors.toList()),
                hasItems(calendar1.getId(), calendar1.getId()));

        getCalendarsRequest.setCalendarId(calendar1.getId());
        getCalendarsResponse = execute(getCalendarsRequest, machineLearningClient::getCalendars,
                machineLearningClient::getCalendarsAsync);
        assertEquals(1, getCalendarsResponse.count());
        assertEquals(calendar1, getCalendarsResponse.calendars().get(0));
    }

    public void testDeleteCalendar() throws IOException {
        Calendar calendar = CalendarTests.testInstance();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        execute(new PutCalendarRequest(calendar), machineLearningClient::putCalendar,
                machineLearningClient::putCalendarAsync);

        AcknowledgedResponse response = execute(new DeleteCalendarRequest(calendar.getId()),
                machineLearningClient::deleteCalendar,
                machineLearningClient::deleteCalendarAsync);
        assertTrue(response.isAcknowledged());

        // calendar is missing
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
                () -> execute(new DeleteCalendarRequest(calendar.getId()), machineLearningClient::deleteCalendar,
                        machineLearningClient::deleteCalendarAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testGetCalendarEvent() throws Exception {
        Calendar calendar = new Calendar("get-calendar-event-id", Collections.singletonList("get-calendar-event-job"), null);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putCalendar(new PutCalendarRequest(calendar), RequestOptions.DEFAULT);

        List<ScheduledEvent> events = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            events.add(ScheduledEventTests.testInstance(calendar.getId(), null));
        }
        machineLearningClient.postCalendarEvent(new PostCalendarEventRequest(calendar.getId(), events), RequestOptions.DEFAULT);

        {
            GetCalendarEventsRequest getCalendarEventsRequest = new GetCalendarEventsRequest(calendar.getId());

            GetCalendarEventsResponse getCalendarEventsResponse = execute(getCalendarEventsRequest,
                machineLearningClient::getCalendarEvents,
                machineLearningClient::getCalendarEventsAsync);
            assertThat(getCalendarEventsResponse.events().size(), equalTo(3));
            assertThat(getCalendarEventsResponse.count(), equalTo(3L));
        }
        {
            GetCalendarEventsRequest getCalendarEventsRequest = new GetCalendarEventsRequest(calendar.getId());
            getCalendarEventsRequest.setPageParams(new PageParams(1, 2));
            GetCalendarEventsResponse getCalendarEventsResponse = execute(getCalendarEventsRequest,
                machineLearningClient::getCalendarEvents,
                machineLearningClient::getCalendarEventsAsync);
            assertThat(getCalendarEventsResponse.events().size(), equalTo(2));
            assertThat(getCalendarEventsResponse.count(), equalTo(3L));
        }
        {
            machineLearningClient.putJob(new PutJobRequest(buildJob("get-calendar-event-job")), RequestOptions.DEFAULT);
            GetCalendarEventsRequest getCalendarEventsRequest = new GetCalendarEventsRequest("_all");
            getCalendarEventsRequest.setJobId("get-calendar-event-job");
            GetCalendarEventsResponse getCalendarEventsResponse = execute(getCalendarEventsRequest,
                machineLearningClient::getCalendarEvents,
                machineLearningClient::getCalendarEventsAsync);
            assertThat(getCalendarEventsResponse.events().size(), equalTo(3));
            assertThat(getCalendarEventsResponse.count(), equalTo(3L));
        }
    }

    public void testPostCalendarEvent() throws Exception {
        Calendar calendar = CalendarTests.testInstance();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putCalendar(new PutCalendarRequest(calendar), RequestOptions.DEFAULT);

        List<ScheduledEvent> events = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            events.add(ScheduledEventTests.testInstance(calendar.getId(), null));
        }

        PostCalendarEventRequest postCalendarEventRequest = new PostCalendarEventRequest(calendar.getId(), events);

        PostCalendarEventResponse postCalendarEventResponse = execute(postCalendarEventRequest,
            machineLearningClient::postCalendarEvent,
            machineLearningClient::postCalendarEventAsync);
        assertThat(postCalendarEventResponse.getScheduledEvents(), containsInAnyOrder(events.toArray()));
    }

    public void testDeleteCalendarEvent() throws IOException {
        Calendar calendar = CalendarTests.testInstance();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putCalendar(new PutCalendarRequest(calendar), RequestOptions.DEFAULT);

        List<ScheduledEvent> events = new ArrayList<>(3);
        for (int i = 0; i < 3; i++) {
            events.add(ScheduledEventTests.testInstance(calendar.getId(), null));
        }

        machineLearningClient.postCalendarEvent(new PostCalendarEventRequest(calendar.getId(), events), RequestOptions.DEFAULT);
        GetCalendarEventsResponse getCalendarEventsResponse =
            machineLearningClient.getCalendarEvents(new GetCalendarEventsRequest(calendar.getId()), RequestOptions.DEFAULT);

        assertThat(getCalendarEventsResponse.events().size(), equalTo(3));
        String deletedEvent = getCalendarEventsResponse.events().get(0).getEventId();

        DeleteCalendarEventRequest deleteCalendarEventRequest = new DeleteCalendarEventRequest(calendar.getId(), deletedEvent);

        AcknowledgedResponse response = execute(deleteCalendarEventRequest,
            machineLearningClient::deleteCalendarEvent,
            machineLearningClient::deleteCalendarEventAsync);

        assertThat(response.isAcknowledged(), is(true));

        getCalendarEventsResponse =
            machineLearningClient.getCalendarEvents(new GetCalendarEventsRequest(calendar.getId()), RequestOptions.DEFAULT);
        List<String> remainingIds = getCalendarEventsResponse.events()
            .stream()
            .map(ScheduledEvent::getEventId)
            .collect(Collectors.toList());

        assertThat(remainingIds.size(), equalTo(2));
        assertThat(remainingIds, not(hasItem(deletedEvent)));
    }

    public void testEstimateModelMemory() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        String byFieldName = randomAlphaOfLength(10);
        String influencerFieldName = randomAlphaOfLength(10);
        AnalysisConfig analysisConfig = AnalysisConfig.builder(
            Collections.singletonList(
                Detector.builder().setFunction("count").setByFieldName(byFieldName).build()
            )).setInfluencers(Collections.singletonList(influencerFieldName)).build();
        EstimateModelMemoryRequest estimateModelMemoryRequest = new EstimateModelMemoryRequest(analysisConfig);
        estimateModelMemoryRequest.setOverallCardinality(Collections.singletonMap(byFieldName, randomNonNegativeLong()));
        estimateModelMemoryRequest.setMaxBucketCardinality(Collections.singletonMap(influencerFieldName, randomNonNegativeLong()));

        EstimateModelMemoryResponse estimateModelMemoryResponse = execute(
            estimateModelMemoryRequest,
            machineLearningClient::estimateModelMemory, machineLearningClient::estimateModelMemoryAsync);

        ByteSizeValue modelMemoryEstimate = estimateModelMemoryResponse.getModelMemoryEstimate();
        assertThat(modelMemoryEstimate.getBytes(), greaterThanOrEqualTo(10000000L));
    }

    public void testPutDataFrameAnalyticsConfig_GivenOutlierDetectionAnalysis() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "test-put-df-analytics-outlier-detection";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex("put-test-source-index")
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex("put-test-dest-index")
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .setDescription("some description")
            .build();

        createIndex("put-test-source-index", defaultMappingForTest());

        PutDataFrameAnalyticsResponse putDataFrameAnalyticsResponse = execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
        DataFrameAnalyticsConfig createdConfig = putDataFrameAnalyticsResponse.getConfig();
        assertThat(createdConfig.getId(), equalTo(config.getId()));
        assertThat(createdConfig.getSource().getIndex(), equalTo(config.getSource().getIndex()));
        assertThat(createdConfig.getSource().getQueryConfig(), equalTo(new QueryConfig(new MatchAllQueryBuilder())));  // default value
        assertThat(createdConfig.getDest().getIndex(), equalTo(config.getDest().getIndex()));
        assertThat(createdConfig.getDest().getResultsField(), equalTo("ml"));  // default value
        assertThat(createdConfig.getAnalysis(), equalTo(org.elasticsearch.client.ml.dataframe.OutlierDetection.builder()
            .setComputeFeatureInfluence(true)
            .setOutlierFraction(0.05)
            .setStandardizationEnabled(true).build()));
        assertThat(createdConfig.getAnalyzedFields(), equalTo(config.getAnalyzedFields()));
        assertThat(createdConfig.getModelMemoryLimit(), equalTo(ByteSizeValue.parseBytesSizeValue("1gb", "")));  // default value
        assertThat(createdConfig.getDescription(), equalTo("some description"));
        assertThat(createdConfig.getMaxNumThreads(), equalTo(1));
    }

    public void testPutDataFrameAnalyticsConfig_GivenRegression() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "test-put-df-analytics-regression";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex("put-test-source-index")
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex("put-test-dest-index")
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.Regression.builder("my_dependent_variable")
                .setPredictionFieldName("my_dependent_variable_prediction")
                .setTrainingPercent(80.0)
                .setRandomizeSeed(42L)
                .setLambda(1.0)
                .setGamma(1.0)
                .setEta(1.0)
                .setMaxTrees(10)
                .setFeatureBagFraction(0.5)
                .setNumTopFeatureImportanceValues(3)
                .setLossFunction(org.elasticsearch.client.ml.dataframe.Regression.LossFunction.MSLE)
                .setLossFunctionParameter(1.0)
                .build())
            .setDescription("this is a regression")
            .build();

        createIndex("put-test-source-index", defaultMappingForTest());

        PutDataFrameAnalyticsResponse putDataFrameAnalyticsResponse = execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
        DataFrameAnalyticsConfig createdConfig = putDataFrameAnalyticsResponse.getConfig();
        assertThat(createdConfig.getId(), equalTo(config.getId()));
        assertThat(createdConfig.getSource().getIndex(), equalTo(config.getSource().getIndex()));
        assertThat(createdConfig.getSource().getQueryConfig(), equalTo(new QueryConfig(new MatchAllQueryBuilder())));  // default value
        assertThat(createdConfig.getDest().getIndex(), equalTo(config.getDest().getIndex()));
        assertThat(createdConfig.getDest().getResultsField(), equalTo("ml"));  // default value
        assertThat(createdConfig.getAnalysis(), equalTo(config.getAnalysis()));
        assertThat(createdConfig.getAnalyzedFields(), equalTo(config.getAnalyzedFields()));
        assertThat(createdConfig.getModelMemoryLimit(), equalTo(ByteSizeValue.parseBytesSizeValue("1gb", "")));  // default value
        assertThat(createdConfig.getDescription(), equalTo("this is a regression"));
    }

    public void testPutDataFrameAnalyticsConfig_GivenClassification() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "test-put-df-analytics-classification";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex("put-test-source-index")
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex("put-test-dest-index")
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.Classification.builder("my_dependent_variable")
                .setPredictionFieldName("my_dependent_variable_prediction")
                .setTrainingPercent(80.0)
                .setRandomizeSeed(42L)
                .setClassAssignmentObjective(
                    org.elasticsearch.client.ml.dataframe.Classification.ClassAssignmentObjective.MAXIMIZE_ACCURACY)
                .setNumTopClasses(1)
                .setLambda(1.0)
                .setGamma(1.0)
                .setEta(1.0)
                .setMaxTrees(10)
                .setFeatureBagFraction(0.5)
                .setNumTopFeatureImportanceValues(3)
                .build())
            .setDescription("this is a classification")
            .build();

        createIndex("put-test-source-index", defaultMappingForTest());

        PutDataFrameAnalyticsResponse putDataFrameAnalyticsResponse = execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
        DataFrameAnalyticsConfig createdConfig = putDataFrameAnalyticsResponse.getConfig();
        assertThat(createdConfig.getId(), equalTo(config.getId()));
        assertThat(createdConfig.getSource().getIndex(), equalTo(config.getSource().getIndex()));
        assertThat(createdConfig.getSource().getQueryConfig(), equalTo(new QueryConfig(new MatchAllQueryBuilder())));  // default value
        assertThat(createdConfig.getDest().getIndex(), equalTo(config.getDest().getIndex()));
        assertThat(createdConfig.getDest().getResultsField(), equalTo("ml"));  // default value
        assertThat(createdConfig.getAnalysis(), equalTo(config.getAnalysis()));
        assertThat(createdConfig.getAnalyzedFields(), equalTo(config.getAnalyzedFields()));
        assertThat(createdConfig.getModelMemoryLimit(), equalTo(ByteSizeValue.parseBytesSizeValue("1gb", "")));  // default value
        assertThat(createdConfig.getDescription(), equalTo("this is a classification"));
    }

    public void testUpdateDataFrameAnalytics() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "test-update-df-analytics-classification";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder().setIndex("update-test-source-index").build())
            .setDest(DataFrameAnalyticsDest.builder().setIndex("update-test-dest-index").build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.Classification.builder("my_dependent_variable").build())
            .setDescription("this is a classification")
            .build();

        createIndex("update-test-source-index", defaultMappingForTest());

        machineLearningClient.putDataFrameAnalytics(new PutDataFrameAnalyticsRequest(config), RequestOptions.DEFAULT);

        UpdateDataFrameAnalyticsRequest request =
            new UpdateDataFrameAnalyticsRequest(
                DataFrameAnalyticsConfigUpdate.builder().setId(config.getId()).setDescription("Updated description").build());
        PutDataFrameAnalyticsResponse response =
            execute(request, machineLearningClient::updateDataFrameAnalytics, machineLearningClient::updateDataFrameAnalyticsAsync);
        assertThat(response.getConfig().getDescription(), equalTo("Updated description"));

        GetDataFrameAnalyticsRequest getRequest = new GetDataFrameAnalyticsRequest(config.getId());
        GetDataFrameAnalyticsResponse getResponse = machineLearningClient.getDataFrameAnalytics(getRequest, RequestOptions.DEFAULT);
        assertThat(getResponse.getAnalytics().get(0).getDescription(), equalTo("Updated description"));
    }

    public void testGetDataFrameAnalyticsConfig_SingleConfig() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "get-test-config";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex("get-test-source-index")
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex("get-test-dest-index")
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .build();

        createIndex("get-test-source-index", defaultMappingForTest());

        PutDataFrameAnalyticsResponse putDataFrameAnalyticsResponse = execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
        DataFrameAnalyticsConfig createdConfig = putDataFrameAnalyticsResponse.getConfig();

        GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse = execute(
            new GetDataFrameAnalyticsRequest(configId),
            machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
        assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(1));
        assertThat(getDataFrameAnalyticsResponse.getAnalytics(), contains(createdConfig));
    }

    public void testGetDataFrameAnalyticsConfig_MultipleConfigs() throws Exception {
        createIndex("get-test-source-index", defaultMappingForTest());

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configIdPrefix = "get-test-config-";
        int numberOfConfigs = 10;
        List<DataFrameAnalyticsConfig> createdConfigs = new ArrayList<>();
        for (int i = 0; i < numberOfConfigs; ++i) {
            String configId = configIdPrefix + i;
            DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
                .setId(configId)
                .setSource(DataFrameAnalyticsSource.builder()
                    .setIndex("get-test-source-index")
                    .build())
                .setDest(DataFrameAnalyticsDest.builder()
                    .setIndex("get-test-dest-index")
                    .build())
                .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
                .build();

            PutDataFrameAnalyticsResponse putDataFrameAnalyticsResponse = execute(
                new PutDataFrameAnalyticsRequest(config),
                machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
            DataFrameAnalyticsConfig createdConfig = putDataFrameAnalyticsResponse.getConfig();
            createdConfigs.add(createdConfig);
        }

        {
            GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse = execute(
                GetDataFrameAnalyticsRequest.getAllDataFrameAnalyticsRequest(),
                machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
            assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(numberOfConfigs));
            assertThat(getDataFrameAnalyticsResponse.getAnalytics(), containsInAnyOrder(createdConfigs.toArray()));
        }
        {
            GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse = execute(
                new GetDataFrameAnalyticsRequest(configIdPrefix + "*"),
                machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
            assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(numberOfConfigs));
            assertThat(getDataFrameAnalyticsResponse.getAnalytics(), containsInAnyOrder(createdConfigs.toArray()));
        }
        {
            GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse = execute(
                new GetDataFrameAnalyticsRequest(configIdPrefix + "9", configIdPrefix + "1", configIdPrefix + "4"),
                machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
            assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(3));
            assertThat(
                getDataFrameAnalyticsResponse.getAnalytics(),
                containsInAnyOrder(createdConfigs.get(1), createdConfigs.get(4), createdConfigs.get(9)));
        }
        {
            GetDataFrameAnalyticsRequest getDataFrameAnalyticsRequest = new GetDataFrameAnalyticsRequest(configIdPrefix + "*");
            getDataFrameAnalyticsRequest.setPageParams(new PageParams(3, 4));
            GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse = execute(
                getDataFrameAnalyticsRequest,
                machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
            assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(4));
            assertThat(
                getDataFrameAnalyticsResponse.getAnalytics(),
                containsInAnyOrder(createdConfigs.get(3), createdConfigs.get(4), createdConfigs.get(5), createdConfigs.get(6)));
        }
    }

    public void testGetDataFrameAnalyticsConfig_ConfigNotFound() {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        GetDataFrameAnalyticsRequest request = new GetDataFrameAnalyticsRequest("config_that_does_not_exist");
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(request, machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testGetDataFrameAnalyticsStats() throws Exception {
        String sourceIndex = "get-stats-test-source-index";
        String destIndex = "get-stats-test-dest-index";
        createIndex(sourceIndex, defaultMappingForTest());
        highLevelClient().index(new IndexRequest(sourceIndex).source(XContentType.JSON, "total", 10000), RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "get-stats-test-config";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex(sourceIndex)
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex(destIndex)
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .build();

        execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);

        GetDataFrameAnalyticsStatsResponse statsResponse = execute(
            new GetDataFrameAnalyticsStatsRequest(configId),
            machineLearningClient::getDataFrameAnalyticsStats, machineLearningClient::getDataFrameAnalyticsStatsAsync);

        assertThat(statsResponse.getAnalyticsStats(), hasSize(1));
        DataFrameAnalyticsStats stats = statsResponse.getAnalyticsStats().get(0);
        assertThat(stats.getId(), equalTo(configId));
        assertThat(stats.getState(), equalTo(DataFrameAnalyticsState.STOPPED));
        assertNull(stats.getFailureReason());
        assertNull(stats.getNode());
        assertNull(stats.getAssignmentExplanation());
        assertThat(statsResponse.getNodeFailures(), hasSize(0));
        assertThat(statsResponse.getTaskFailures(), hasSize(0));
        List<PhaseProgress> progress = stats.getProgress();
        assertThat(progress, is(notNullValue()));
        assertThat(progress.size(), equalTo(4));
        assertThat(progress.get(0), equalTo(new PhaseProgress("reindexing", 0)));
        assertThat(progress.get(1), equalTo(new PhaseProgress("loading_data", 0)));
        assertThat(progress.get(2), equalTo(new PhaseProgress("computing_outliers", 0)));
        assertThat(progress.get(3), equalTo(new PhaseProgress("writing_results", 0)));
        assertThat(stats.getMemoryUsage().getPeakUsageBytes(), equalTo(0L));
        assertThat(stats.getMemoryUsage().getStatus(), equalTo(MemoryUsage.Status.OK));
        assertThat(stats.getMemoryUsage().getMemoryReestimateBytes(), is(nullValue()));
        assertThat(stats.getDataCounts(), equalTo(new DataCounts(0, 0, 0)));
    }

    public void testStartDataFrameAnalyticsConfig() throws Exception {
        String sourceIndex = "start-test-source-index";
        String destIndex = "start-test-dest-index";
        createIndex(sourceIndex, defaultMappingForTest());
        highLevelClient().index(new IndexRequest(sourceIndex).source(XContentType.JSON, "total", 10000)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        // Verify that the destination index does not exist. Otherwise, analytics' reindexing step would fail.
        assertFalse(highLevelClient().indices().exists(new GetIndexRequest(destIndex), RequestOptions.DEFAULT));

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "start-test-config";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex(sourceIndex)
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex(destIndex)
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .build();

        execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
        assertThat(getAnalyticsState(configId), equalTo(DataFrameAnalyticsState.STOPPED));

        AcknowledgedResponse startDataFrameAnalyticsResponse = execute(
            new StartDataFrameAnalyticsRequest(configId),
            machineLearningClient::startDataFrameAnalytics, machineLearningClient::startDataFrameAnalyticsAsync);
        assertTrue(startDataFrameAnalyticsResponse.isAcknowledged());

        // Wait for the analytics to stop.
        assertBusy(() -> assertThat(getAnalyticsState(configId), equalTo(DataFrameAnalyticsState.STOPPED)), 30, TimeUnit.SECONDS);

        // Verify that the destination index got created.
        assertTrue(highLevelClient().indices().exists(new GetIndexRequest(destIndex), RequestOptions.DEFAULT));
    }

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/43924")
    public void testStopDataFrameAnalyticsConfig() throws Exception {
        String sourceIndex = "stop-test-source-index";
        String destIndex = "stop-test-dest-index";
        createIndex(sourceIndex, defaultMappingForTest());
        highLevelClient().index(new IndexRequest(sourceIndex).source(XContentType.JSON, "total", 10000)
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE), RequestOptions.DEFAULT);

        // Verify that the destination index does not exist. Otherwise, analytics' reindexing step would fail.
        assertFalse(highLevelClient().indices().exists(new GetIndexRequest(destIndex), RequestOptions.DEFAULT));

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "stop-test-config";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex(sourceIndex)
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex(destIndex)
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .build();

        execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);
        assertThat(getAnalyticsState(configId), equalTo(DataFrameAnalyticsState.STOPPED));

        AcknowledgedResponse startDataFrameAnalyticsResponse = execute(
            new StartDataFrameAnalyticsRequest(configId),
            machineLearningClient::startDataFrameAnalytics, machineLearningClient::startDataFrameAnalyticsAsync);
        assertTrue(startDataFrameAnalyticsResponse.isAcknowledged());
        assertThat(getAnalyticsState(configId), anyOf(equalTo(DataFrameAnalyticsState.STARTED),
            equalTo(DataFrameAnalyticsState.REINDEXING), equalTo(DataFrameAnalyticsState.ANALYZING)));

        StopDataFrameAnalyticsResponse stopDataFrameAnalyticsResponse = execute(
            new StopDataFrameAnalyticsRequest(configId),
            machineLearningClient::stopDataFrameAnalytics, machineLearningClient::stopDataFrameAnalyticsAsync);
        assertTrue(stopDataFrameAnalyticsResponse.isStopped());
        assertThat(getAnalyticsState(configId), equalTo(DataFrameAnalyticsState.STOPPED));
    }

    private DataFrameAnalyticsState getAnalyticsState(String configId) throws IOException {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        GetDataFrameAnalyticsStatsResponse statsResponse =
            machineLearningClient.getDataFrameAnalyticsStats(new GetDataFrameAnalyticsStatsRequest(configId), RequestOptions.DEFAULT);
        assertThat(statsResponse.getAnalyticsStats(), hasSize(1));
        DataFrameAnalyticsStats stats = statsResponse.getAnalyticsStats().get(0);
        return stats.getState();
    }

    public void testDeleteDataFrameAnalyticsConfig() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String configId = "delete-test-config";
        DataFrameAnalyticsConfig config = DataFrameAnalyticsConfig.builder()
            .setId(configId)
            .setSource(DataFrameAnalyticsSource.builder()
                .setIndex("delete-test-source-index")
                .build())
            .setDest(DataFrameAnalyticsDest.builder()
                .setIndex("delete-test-dest-index")
                .build())
            .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
            .build();

        createIndex("delete-test-source-index", defaultMappingForTest());

        GetDataFrameAnalyticsResponse getDataFrameAnalyticsResponse = execute(
            new GetDataFrameAnalyticsRequest(configId + "*"),
            machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
        assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(0));

        execute(
            new PutDataFrameAnalyticsRequest(config),
            machineLearningClient::putDataFrameAnalytics, machineLearningClient::putDataFrameAnalyticsAsync);

        getDataFrameAnalyticsResponse = execute(
            new GetDataFrameAnalyticsRequest(configId + "*"),
            machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
        assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(1));

        DeleteDataFrameAnalyticsRequest deleteRequest = new DeleteDataFrameAnalyticsRequest(configId);
        if (randomBoolean()) {
            deleteRequest.setForce(randomBoolean());
        }
        AcknowledgedResponse deleteDataFrameAnalyticsResponse = execute(deleteRequest,
            machineLearningClient::deleteDataFrameAnalytics, machineLearningClient::deleteDataFrameAnalyticsAsync);
        assertTrue(deleteDataFrameAnalyticsResponse.isAcknowledged());

        getDataFrameAnalyticsResponse = execute(
            new GetDataFrameAnalyticsRequest(configId + "*"),
            machineLearningClient::getDataFrameAnalytics, machineLearningClient::getDataFrameAnalyticsAsync);
        assertThat(getDataFrameAnalyticsResponse.getAnalytics(), hasSize(0));
    }

    public void testDeleteDataFrameAnalyticsConfig_ConfigNotFound() {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        DeleteDataFrameAnalyticsRequest request = new DeleteDataFrameAnalyticsRequest("config_that_does_not_exist");
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(
                request, machineLearningClient::deleteDataFrameAnalytics, machineLearningClient::deleteDataFrameAnalyticsAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testEvaluateDataFrame_OutlierDetection() throws IOException {
        String indexName = "evaluate-test-index";
        createIndex(indexName, mappingForOutlierDetection());
        BulkRequest bulk = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(docForOutlierDetection(indexName, "blue", false, 0.1))  // #0
            .add(docForOutlierDetection(indexName, "blue", false, 0.2))  // #1
            .add(docForOutlierDetection(indexName, "blue", false, 0.3))  // #2
            .add(docForOutlierDetection(indexName, "blue", false, 0.4))  // #3
            .add(docForOutlierDetection(indexName, "blue", false, 0.7))  // #4
            .add(docForOutlierDetection(indexName, "blue", true, 0.2))  // #5
            .add(docForOutlierDetection(indexName, "green", true, 0.3))  // #6
            .add(docForOutlierDetection(indexName, "green", true, 0.4))  // #7
            .add(docForOutlierDetection(indexName, "green", true, 0.8))  // #8
            .add(docForOutlierDetection(indexName, "green", true, 0.9));  // #9
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        EvaluateDataFrameRequest evaluateDataFrameRequest =
            new EvaluateDataFrameRequest(
                indexName,
                null,
                new OutlierDetection(
                    actualField,
                    probabilityField,
                    PrecisionMetric.at(0.4, 0.5, 0.6), RecallMetric.at(0.5, 0.7), ConfusionMatrixMetric.at(0.5), AucRocMetric.withCurve()));

        EvaluateDataFrameResponse evaluateDataFrameResponse =
            execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(OutlierDetection.NAME));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(4));

        PrecisionMetric.Result precisionResult = evaluateDataFrameResponse.getMetricByName(PrecisionMetric.NAME);
        assertThat(precisionResult.getMetricName(), equalTo(PrecisionMetric.NAME));
        // Precision is 3/5=0.6 as there were 3 true examples (#7, #8, #9) among the 5 positive examples (#3, #4, #7, #8, #9)
        assertThat(precisionResult.getScoreByThreshold("0.4"), closeTo(0.6, 1e-9));
        // Precision is 2/3=0.(6) as there were 2 true examples (#8, #9) among the 3 positive examples (#4, #8, #9)
        assertThat(precisionResult.getScoreByThreshold("0.5"), closeTo(0.666666666, 1e-9));
        // Precision is 2/3=0.(6) as there were 2 true examples (#8, #9) among the 3 positive examples (#4, #8, #9)
        assertThat(precisionResult.getScoreByThreshold("0.6"), closeTo(0.666666666, 1e-9));
        assertNull(precisionResult.getScoreByThreshold("0.1"));

        RecallMetric.Result recallResult = evaluateDataFrameResponse.getMetricByName(RecallMetric.NAME);
        assertThat(recallResult.getMetricName(), equalTo(RecallMetric.NAME));
        // Recall is 2/5=0.4 as there were 2 true positive examples (#8, #9) among the 5 true examples (#5, #6, #7, #8, #9)
        assertThat(recallResult.getScoreByThreshold("0.5"), closeTo(0.4, 1e-9));
        // Recall is 2/5=0.4 as there were 2 true positive examples (#8, #9) among the 5 true examples (#5, #6, #7, #8, #9)
        assertThat(recallResult.getScoreByThreshold("0.7"), closeTo(0.4, 1e-9));
        assertNull(recallResult.getScoreByThreshold("0.1"));

        ConfusionMatrixMetric.Result confusionMatrixResult = evaluateDataFrameResponse.getMetricByName(ConfusionMatrixMetric.NAME);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(ConfusionMatrixMetric.NAME));
        ConfusionMatrixMetric.ConfusionMatrix confusionMatrix = confusionMatrixResult.getScoreByThreshold("0.5");
        assertThat(confusionMatrix.getTruePositives(), equalTo(2L));  // docs #8 and #9
        assertThat(confusionMatrix.getFalsePositives(), equalTo(1L));  // doc #4
        assertThat(confusionMatrix.getTrueNegatives(), equalTo(4L));  // docs #0, #1, #2 and #3
        assertThat(confusionMatrix.getFalseNegatives(), equalTo(3L));  // docs #5, #6 and #7
        assertNull(confusionMatrixResult.getScoreByThreshold("0.1"));

        AucRocMetric.Result aucRocResult = evaluateDataFrameResponse.getMetricByName(AucRocMetric.NAME);
        assertThat(aucRocResult.getMetricName(), equalTo(AucRocMetric.NAME));
        assertThat(aucRocResult.getScore(), closeTo(0.70025, 1e-9));
        assertNotNull(aucRocResult.getCurve());
        List<AucRocMetric.AucRocPoint> curve = aucRocResult.getCurve();
        AucRocMetric.AucRocPoint curvePointAtThreshold0 = curve.stream().filter(p -> p.getThreshold() == 0.0).findFirst().get();
        assertThat(curvePointAtThreshold0.getTruePositiveRate(), equalTo(1.0));
        assertThat(curvePointAtThreshold0.getFalsePositiveRate(), equalTo(1.0));
        assertThat(curvePointAtThreshold0.getThreshold(), equalTo(0.0));
        AucRocMetric.AucRocPoint curvePointAtThreshold1 = curve.stream().filter(p -> p.getThreshold() == 1.0).findFirst().get();
        assertThat(curvePointAtThreshold1.getTruePositiveRate(), equalTo(0.0));
        assertThat(curvePointAtThreshold1.getFalsePositiveRate(), equalTo(0.0));
        assertThat(curvePointAtThreshold1.getThreshold(), equalTo(1.0));
    }

    public void testEvaluateDataFrame_OutlierDetection_WithQuery() throws IOException {
        String indexName = "evaluate-with-query-test-index";
        createIndex(indexName, mappingForOutlierDetection());
        BulkRequest bulk = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(docForOutlierDetection(indexName, "blue", true, 1.0))  // #0
            .add(docForOutlierDetection(indexName, "blue", true, 1.0))  // #1
            .add(docForOutlierDetection(indexName, "blue", true, 1.0))  // #2
            .add(docForOutlierDetection(indexName, "blue", true, 1.0))  // #3
            .add(docForOutlierDetection(indexName, "blue", true, 0.0))  // #4
            .add(docForOutlierDetection(indexName, "blue", true, 0.0))  // #5
            .add(docForOutlierDetection(indexName, "green", true, 0.0))  // #6
            .add(docForOutlierDetection(indexName, "green", true, 0.0))  // #7
            .add(docForOutlierDetection(indexName, "green", true, 0.0))  // #8
            .add(docForOutlierDetection(indexName, "green", true, 1.0));  // #9
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        EvaluateDataFrameRequest evaluateDataFrameRequest =
            new EvaluateDataFrameRequest(
                indexName,
                // Request only "blue" subset to be evaluated
                new QueryConfig(QueryBuilders.termQuery(datasetField, "blue")),
                new OutlierDetection(actualField, probabilityField, ConfusionMatrixMetric.at(0.5)));

        EvaluateDataFrameResponse evaluateDataFrameResponse =
            execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(OutlierDetection.NAME));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));

        ConfusionMatrixMetric.Result confusionMatrixResult = evaluateDataFrameResponse.getMetricByName(ConfusionMatrixMetric.NAME);
        assertThat(confusionMatrixResult.getMetricName(), equalTo(ConfusionMatrixMetric.NAME));
        ConfusionMatrixMetric.ConfusionMatrix confusionMatrix = confusionMatrixResult.getScoreByThreshold("0.5");
        assertThat(confusionMatrix.getTruePositives(), equalTo(4L));  // docs #0, #1, #2 and #3
        assertThat(confusionMatrix.getFalsePositives(), equalTo(0L));
        assertThat(confusionMatrix.getTrueNegatives(), equalTo(0L));
        assertThat(confusionMatrix.getFalseNegatives(), equalTo(2L));  // docs #4 and #5
    }

    public void testEvaluateDataFrame_Regression() throws IOException {
        String regressionIndex = "evaluate-regression-test-index";
        createIndex(regressionIndex, mappingForRegression());
        BulkRequest regressionBulk = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(docForRegression(regressionIndex, 0.3, 0.1))  // #0
            .add(docForRegression(regressionIndex, 0.3, 0.2))  // #1
            .add(docForRegression(regressionIndex, 0.3, 0.3))  // #2
            .add(docForRegression(regressionIndex, 0.3, 0.4))  // #3
            .add(docForRegression(regressionIndex, 0.3, 0.7))  // #4
            .add(docForRegression(regressionIndex, 0.5, 0.2))  // #5
            .add(docForRegression(regressionIndex, 0.5, 0.3))  // #6
            .add(docForRegression(regressionIndex, 0.5, 0.4))  // #7
            .add(docForRegression(regressionIndex, 0.5, 0.8))  // #8
            .add(docForRegression(regressionIndex, 0.5, 0.9));  // #9
        highLevelClient().bulk(regressionBulk, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        EvaluateDataFrameRequest evaluateDataFrameRequest =
            new EvaluateDataFrameRequest(
                regressionIndex,
                null,
                new Regression(
                    actualRegression,
                    predictedRegression,
                    new MeanSquaredErrorMetric(),
                    new MeanSquaredLogarithmicErrorMetric(1.0),
                    new HuberMetric(1.0),
                    new RSquaredMetric()));

        EvaluateDataFrameResponse evaluateDataFrameResponse =
            execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
        assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Regression.NAME));
        assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(4));

        MeanSquaredErrorMetric.Result mseResult = evaluateDataFrameResponse.getMetricByName(MeanSquaredErrorMetric.NAME);
        assertThat(mseResult.getMetricName(), equalTo(MeanSquaredErrorMetric.NAME));
        assertThat(mseResult.getValue(), closeTo(0.061000000, 1e-9));

        MeanSquaredLogarithmicErrorMetric.Result msleResult =
            evaluateDataFrameResponse.getMetricByName(MeanSquaredLogarithmicErrorMetric.NAME);
        assertThat(msleResult.getMetricName(), equalTo(MeanSquaredLogarithmicErrorMetric.NAME));
        assertThat(msleResult.getValue(), closeTo(0.02759231770210426, 1e-9));

        HuberMetric.Result huberResult = evaluateDataFrameResponse.getMetricByName(HuberMetric.NAME);
        assertThat(huberResult.getMetricName(), equalTo(HuberMetric.NAME));
        assertThat(huberResult.getValue(), closeTo(0.029669771640929276, 1e-9));

        RSquaredMetric.Result rSquaredResult = evaluateDataFrameResponse.getMetricByName(RSquaredMetric.NAME);
        assertThat(rSquaredResult.getMetricName(), equalTo(RSquaredMetric.NAME));
        assertThat(rSquaredResult.getValue(), closeTo(-5.1000000000000005, 1e-9));
    }

    public void testEvaluateDataFrame_Classification() throws IOException {
        String indexName = "evaluate-classification-test-index";
        createIndex(indexName, mappingForClassification());
        BulkRequest regressionBulk = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE)
            .add(docForClassification(indexName, "cat", "cat"))
            .add(docForClassification(indexName, "cat", "cat"))
            .add(docForClassification(indexName, "cat", "cat"))
            .add(docForClassification(indexName, "cat", "dog"))
            .add(docForClassification(indexName, "cat", "fish"))
            .add(docForClassification(indexName, "dog", "cat"))
            .add(docForClassification(indexName, "dog", "dog"))
            .add(docForClassification(indexName, "dog", "dog"))
            .add(docForClassification(indexName, "dog", "dog"))
            .add(docForClassification(indexName, "ant", "cat"));
        highLevelClient().bulk(regressionBulk, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        {  // Accuracy
            EvaluateDataFrameRequest evaluateDataFrameRequest =
                new EvaluateDataFrameRequest(
                    indexName, null, new Classification(actualClassField, predictedClassField, new AccuracyMetric()));

            EvaluateDataFrameResponse evaluateDataFrameResponse =
                execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
            assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME));
            assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));

            AccuracyMetric.Result accuracyResult = evaluateDataFrameResponse.getMetricByName(AccuracyMetric.NAME);
            assertThat(accuracyResult.getMetricName(), equalTo(AccuracyMetric.NAME));
            assertThat(
                accuracyResult.getClasses(),
                equalTo(
                    List.of(
                        // 9 out of 10 examples were classified correctly
                        new AccuracyMetric.PerClassResult("ant", 0.9),
                        // 6 out of 10 examples were classified correctly
                        new AccuracyMetric.PerClassResult("cat", 0.6),
                        // 8 out of 10 examples were classified correctly
                        new AccuracyMetric.PerClassResult("dog", 0.8))));
            assertThat(accuracyResult.getOverallAccuracy(), equalTo(0.6));  // 6 out of 10 examples were classified correctly
        }
        {  // Precision
            EvaluateDataFrameRequest evaluateDataFrameRequest =
                new EvaluateDataFrameRequest(
                    indexName,
                    null,
                    new Classification(
                        actualClassField,
                        predictedClassField,
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric()));

            EvaluateDataFrameResponse evaluateDataFrameResponse =
                execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
            assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME));
            assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));

            org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.Result precisionResult =
                evaluateDataFrameResponse.getMetricByName(
                    org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.NAME);
            assertThat(
                precisionResult.getMetricName(),
                equalTo(org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.NAME));
            assertThat(
                precisionResult.getClasses(),
                equalTo(
                    List.of(
                        // 3 out of 5 examples labeled as "cat" were classified correctly
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.PerClassResult("cat", 0.6),
                        // 3 out of 4 examples labeled as "dog" were classified correctly
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.PrecisionMetric.PerClassResult("dog", 0.75))));
            assertThat(precisionResult.getAvgPrecision(), equalTo(0.675));
        }
        {  // Recall
            EvaluateDataFrameRequest evaluateDataFrameRequest =
                new EvaluateDataFrameRequest(
                    indexName,
                    null,
                    new Classification(
                        actualClassField,
                        predictedClassField,
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric()));

            EvaluateDataFrameResponse evaluateDataFrameResponse =
                execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
            assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME));
            assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));

            org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.Result recallResult =
                evaluateDataFrameResponse.getMetricByName(
                    org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.NAME);
            assertThat(
                recallResult.getMetricName(),
                equalTo(org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.NAME));
            assertThat(
                recallResult.getClasses(),
                equalTo(
                    List.of(
                        // 3 out of 5 examples labeled as "cat" were classified correctly
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.PerClassResult("cat", 0.6),
                        // 3 out of 4 examples labeled as "dog" were classified correctly
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.PerClassResult("dog", 0.75),
                        // no examples labeled as "ant" were classified correctly
                        new org.elasticsearch.client.ml.dataframe.evaluation.classification.RecallMetric.PerClassResult("ant", 0.0))));
            assertThat(recallResult.getAvgRecall(), equalTo(0.45));
        }
        {  // No size provided for MulticlassConfusionMatrixMetric, default used instead
            EvaluateDataFrameRequest evaluateDataFrameRequest =
                new EvaluateDataFrameRequest(
                    indexName,
                    null,
                    new Classification(actualClassField, predictedClassField, new MulticlassConfusionMatrixMetric()));

            EvaluateDataFrameResponse evaluateDataFrameResponse =
                execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
            assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME));
            assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));

            MulticlassConfusionMatrixMetric.Result mcmResult =
                evaluateDataFrameResponse.getMetricByName(MulticlassConfusionMatrixMetric.NAME);
            assertThat(mcmResult.getMetricName(), equalTo(MulticlassConfusionMatrixMetric.NAME));
            assertThat(
                mcmResult.getConfusionMatrix(),
                equalTo(
                    List.of(
                        new MulticlassConfusionMatrixMetric.ActualClass(
                            "ant",
                            1L,
                            List.of(
                                new MulticlassConfusionMatrixMetric.PredictedClass("ant", 0L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("cat", 1L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("dog", 0L)),
                            0L),
                        new MulticlassConfusionMatrixMetric.ActualClass(
                            "cat",
                            5L,
                            List.of(
                                new MulticlassConfusionMatrixMetric.PredictedClass("ant", 0L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("cat", 3L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("dog", 1L)),
                            1L),
                        new MulticlassConfusionMatrixMetric.ActualClass(
                            "dog",
                            4L,
                            List.of(
                                new MulticlassConfusionMatrixMetric.PredictedClass("ant", 0L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("cat", 1L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("dog", 3L)),
                            0L))));
            assertThat(mcmResult.getOtherActualClassCount(), equalTo(0L));
        }
        {  // Explicit size provided for MulticlassConfusionMatrixMetric metric
            EvaluateDataFrameRequest evaluateDataFrameRequest =
                new EvaluateDataFrameRequest(
                    indexName,
                    null,
                    new Classification(actualClassField, predictedClassField, new MulticlassConfusionMatrixMetric(2)));

            EvaluateDataFrameResponse evaluateDataFrameResponse =
                execute(evaluateDataFrameRequest, machineLearningClient::evaluateDataFrame, machineLearningClient::evaluateDataFrameAsync);
            assertThat(evaluateDataFrameResponse.getEvaluationName(), equalTo(Classification.NAME));
            assertThat(evaluateDataFrameResponse.getMetrics().size(), equalTo(1));

            MulticlassConfusionMatrixMetric.Result mcmResult =
                evaluateDataFrameResponse.getMetricByName(MulticlassConfusionMatrixMetric.NAME);
            assertThat(mcmResult.getMetricName(), equalTo(MulticlassConfusionMatrixMetric.NAME));
            assertThat(
                mcmResult.getConfusionMatrix(),
                equalTo(
                    List.of(
                        new MulticlassConfusionMatrixMetric.ActualClass(
                            "cat",
                            5L,
                            List.of(
                                new MulticlassConfusionMatrixMetric.PredictedClass("cat", 3L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("dog", 1L)),
                            1L),
                        new MulticlassConfusionMatrixMetric.ActualClass(
                            "dog",
                            4L,
                            List.of(
                                new MulticlassConfusionMatrixMetric.PredictedClass("cat", 1L),
                                new MulticlassConfusionMatrixMetric.PredictedClass("dog", 3L)),
                            0L)
                    )));
            assertThat(mcmResult.getOtherActualClassCount(), equalTo(1L));
        }
    }

    private static XContentBuilder defaultMappingForTest() throws IOException {
        return XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
               .startObject("timestamp")
                    .field("type", "date")
                .endObject()
                .startObject("total")
                    .field("type", "long")
                .endObject()
            .endObject()
        .endObject();
    }

    private static final String datasetField = "dataset";
    private static final String actualField = "label";
    private static final String probabilityField = "p";

    private static XContentBuilder mappingForOutlierDetection() throws IOException {
        return XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject(datasetField)
                    .field("type", "keyword")
                .endObject()
                .startObject(actualField)
                    .field("type", "keyword")
                .endObject()
                .startObject(probabilityField)
                    .field("type", "double")
                .endObject()
            .endObject()
        .endObject();
    }

    private static IndexRequest docForOutlierDetection(String indexName, String dataset, boolean isTrue, double p) {
        return new IndexRequest()
            .index(indexName)
            .source(XContentType.JSON, datasetField, dataset, actualField, Boolean.toString(isTrue), probabilityField, p);
    }

    private static final String actualClassField = "actual_class";
    private static final String predictedClassField = "predicted_class";

    private static XContentBuilder mappingForClassification() throws IOException {
        return XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject(actualClassField)
                    .field("type", "keyword")
                .endObject()
                .startObject(predictedClassField)
                    .field("type", "keyword")
                .endObject()
            .endObject()
        .endObject();
    }

    private static IndexRequest docForClassification(String indexName, String actualClass, String predictedClass) {
        return new IndexRequest()
            .index(indexName)
            .source(XContentType.JSON, actualClassField, actualClass, predictedClassField, predictedClass);
    }

    private static final String actualRegression = "regression_actual";
    private static final String predictedRegression = "regression_predicted";

    private static XContentBuilder mappingForRegression() throws IOException {
        return XContentFactory.jsonBuilder().startObject()
            .startObject("properties")
                .startObject(actualRegression)
                    .field("type", "double")
                .endObject()
                .startObject(predictedRegression)
                    .field("type", "double")
                .endObject()
            .endObject()
        .endObject();
    }

    private static IndexRequest docForRegression(String indexName, double actualValue, double predictedValue) {
        return new IndexRequest()
            .index(indexName)
            .source(XContentType.JSON, actualRegression, actualValue, predictedRegression, predictedValue);
    }

    private void createIndex(String indexName, XContentBuilder mapping) throws IOException {
        highLevelClient().indices().create(new CreateIndexRequest(indexName).mapping(mapping), RequestOptions.DEFAULT);
    }

    public void testExplainDataFrameAnalytics() throws IOException {
        String indexName = "explain-df-test-index";
        createIndex(indexName, mappingForOutlierDetection());
        BulkRequest bulk1 = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 0; i < 10; ++i) {
            bulk1.add(docForOutlierDetection(indexName, randomAlphaOfLength(10), randomBoolean(), randomDoubleBetween(0.0, 1.0, true)));
        }
        highLevelClient().bulk(bulk1, RequestOptions.DEFAULT);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        ExplainDataFrameAnalyticsRequest explainRequest =
            new ExplainDataFrameAnalyticsRequest(
                DataFrameAnalyticsConfig.builder()
                    .setSource(DataFrameAnalyticsSource.builder().setIndex(indexName).build())
                    .setAnalysis(org.elasticsearch.client.ml.dataframe.OutlierDetection.createDefault())
                    .build());

        // We are pretty liberal here as this test does not aim at verifying concrete numbers but rather end-to-end user workflow.
        ByteSizeValue lowerBound = new ByteSizeValue(1, ByteSizeUnit.KB);
        ByteSizeValue upperBound = new ByteSizeValue(1, ByteSizeUnit.GB);

        // Data Frame has 10 rows, expect that the returned estimates fall within (1kB, 1GB) range.
        ExplainDataFrameAnalyticsResponse response1 = execute(explainRequest, machineLearningClient::explainDataFrameAnalytics,
            machineLearningClient::explainDataFrameAnalyticsAsync);

        MemoryEstimation memoryEstimation1 = response1.getMemoryEstimation();
        assertThat(memoryEstimation1.getExpectedMemoryWithoutDisk(), allOf(greaterThanOrEqualTo(lowerBound), lessThan(upperBound)));
        assertThat(memoryEstimation1.getExpectedMemoryWithDisk(), allOf(greaterThanOrEqualTo(lowerBound), lessThan(upperBound)));

        List<FieldSelection> fieldSelection = response1.getFieldSelection();
        assertThat(fieldSelection.size(), equalTo(3));
        assertThat(fieldSelection.stream().map(FieldSelection::getName).collect(Collectors.toList()), contains("dataset", "label", "p"));

        BulkRequest bulk2 = new BulkRequest()
            .setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        for (int i = 10; i < 100; ++i) {
            bulk2.add(docForOutlierDetection(indexName, randomAlphaOfLength(10), randomBoolean(), randomDoubleBetween(0.0, 1.0, true)));
        }
        highLevelClient().bulk(bulk2, RequestOptions.DEFAULT);

        // Data Frame now has 100 rows, expect that the returned estimates will be greater than or equal to the previous ones.
        ExplainDataFrameAnalyticsResponse response2 =
            execute(
                explainRequest, machineLearningClient::explainDataFrameAnalytics, machineLearningClient::explainDataFrameAnalyticsAsync);
        MemoryEstimation memoryEstimation2 = response2.getMemoryEstimation();
        assertThat(
            memoryEstimation2.getExpectedMemoryWithoutDisk(),
            allOf(greaterThanOrEqualTo(memoryEstimation1.getExpectedMemoryWithoutDisk()), lessThan(upperBound)));
        assertThat(
            memoryEstimation2.getExpectedMemoryWithDisk(),
            allOf(greaterThanOrEqualTo(memoryEstimation1.getExpectedMemoryWithDisk()), lessThan(upperBound)));
    }

    public void testGetTrainedModels() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String modelIdPrefix = "get-trained-model-";
        int numberOfModels = 5;
        for (int i = 0; i < numberOfModels; ++i) {
            String modelId = modelIdPrefix + i;
            putTrainedModel(modelId);
        }

        {
            GetTrainedModelsResponse getTrainedModelsResponse = execute(
                new GetTrainedModelsRequest(modelIdPrefix + 0).setDecompressDefinition(true).setIncludeDefinition(true),
                machineLearningClient::getTrainedModels,
                machineLearningClient::getTrainedModelsAsync);

            assertThat(getTrainedModelsResponse.getCount(), equalTo(1L));
            assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(1));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getCompressedDefinition(), is(nullValue()));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getDefinition(), is(not(nullValue())));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getModelId(), equalTo(modelIdPrefix + 0));

            getTrainedModelsResponse = execute(
                new GetTrainedModelsRequest(modelIdPrefix + 0).setDecompressDefinition(false).setIncludeDefinition(true),
                machineLearningClient::getTrainedModels,
                machineLearningClient::getTrainedModelsAsync);

            assertThat(getTrainedModelsResponse.getCount(), equalTo(1L));
            assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(1));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getCompressedDefinition(), is(not(nullValue())));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getDefinition(), is(nullValue()));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getModelId(), equalTo(modelIdPrefix + 0));

            getTrainedModelsResponse = execute(
                new GetTrainedModelsRequest(modelIdPrefix + 0).setDecompressDefinition(false).setIncludeDefinition(false),
                machineLearningClient::getTrainedModels,
                machineLearningClient::getTrainedModelsAsync);
            assertThat(getTrainedModelsResponse.getCount(), equalTo(1L));
            assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(1));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getCompressedDefinition(), is(nullValue()));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getDefinition(), is(nullValue()));
            assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getModelId(), equalTo(modelIdPrefix + 0));

        }
        {
            GetTrainedModelsResponse getTrainedModelsResponse = execute(
                GetTrainedModelsRequest.getAllTrainedModelConfigsRequest(),
                machineLearningClient::getTrainedModels, machineLearningClient::getTrainedModelsAsync);
            assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(numberOfModels + 1));
            assertThat(getTrainedModelsResponse.getCount(), equalTo(5L + 1));
        }
        {
            GetTrainedModelsResponse getTrainedModelsResponse = execute(
                new GetTrainedModelsRequest(modelIdPrefix + 4, modelIdPrefix + 2, modelIdPrefix + 3),
                machineLearningClient::getTrainedModels, machineLearningClient::getTrainedModelsAsync);
            assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(3));
            assertThat(getTrainedModelsResponse.getCount(), equalTo(3L));
        }
        {
            GetTrainedModelsResponse getTrainedModelsResponse = execute(
                new GetTrainedModelsRequest(modelIdPrefix + "*").setPageParams(new PageParams(1, 2)),
                machineLearningClient::getTrainedModels, machineLearningClient::getTrainedModelsAsync);
            assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(2));
            assertThat(getTrainedModelsResponse.getCount(), equalTo(5L));
            assertThat(
                getTrainedModelsResponse.getTrainedModels().stream().map(TrainedModelConfig::getModelId).collect(Collectors.toList()),
                containsInAnyOrder(modelIdPrefix + 1, modelIdPrefix + 2));
        }
    }

    public void testPutTrainedModel() throws Exception {
        String modelId = "test-put-trained-model";
        String modelIdCompressed = "test-put-trained-model-compressed-definition";

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION).build();
        TrainedModelConfig trainedModelConfig = TrainedModelConfig.builder()
            .setDefinition(definition)
            .setModelId(modelId)
            .setInferenceConfig(new RegressionConfig())
            .setInput(new TrainedModelInput(Arrays.asList("col1", "col2", "col3", "col4")))
            .setDescription("test model")
            .build();
        PutTrainedModelResponse putTrainedModelResponse = execute(new PutTrainedModelRequest(trainedModelConfig),
            machineLearningClient::putTrainedModel,
            machineLearningClient::putTrainedModelAsync);
        TrainedModelConfig createdModel = putTrainedModelResponse.getResponse();
        assertThat(createdModel.getModelId(), equalTo(modelId));

        definition = TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION).build();
        trainedModelConfig = TrainedModelConfig.builder()
            .setCompressedDefinition(InferenceToXContentCompressor.deflate(definition))
            .setModelId(modelIdCompressed)
            .setInferenceConfig(new RegressionConfig())
            .setInput(new TrainedModelInput(Arrays.asList("col1", "col2", "col3", "col4")))
            .setDescription("test model")
            .build();
        putTrainedModelResponse = execute(new PutTrainedModelRequest(trainedModelConfig),
            machineLearningClient::putTrainedModel,
            machineLearningClient::putTrainedModelAsync);
        createdModel = putTrainedModelResponse.getResponse();
        assertThat(createdModel.getModelId(), equalTo(modelIdCompressed));

        GetTrainedModelsResponse getTrainedModelsResponse = execute(
            new GetTrainedModelsRequest(modelIdCompressed).setDecompressDefinition(true).setIncludeDefinition(true),
            machineLearningClient::getTrainedModels,
            machineLearningClient::getTrainedModelsAsync);

        assertThat(getTrainedModelsResponse.getCount(), equalTo(1L));
        assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(1));
        assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getCompressedDefinition(), is(nullValue()));
        assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getDefinition(), is(not(nullValue())));
        assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getModelId(), equalTo(modelIdCompressed));
    }

    public void testGetTrainedModelsStats() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String modelIdPrefix = "a-get-trained-model-stats-";
        int numberOfModels = 5;
        for (int i = 0; i < numberOfModels; ++i) {
            String modelId = modelIdPrefix + i;
            putTrainedModel(modelId);
        }

        String regressionPipeline = "{" +
            "    \"processors\": [\n" +
            "      {\n" +
            "        \"inference\": {\n" +
            "          \"target_field\": \"regression_value\",\n" +
            "          \"model_id\": \"" + modelIdPrefix + 0 + "\",\n" +
            "          \"inference_config\": {\"regression\": {}},\n" +
            "          \"field_map\": {\n" +
            "            \"col1\": \"col1\",\n" +
            "            \"col2\": \"col2\",\n" +
            "            \"col3\": \"col3\",\n" +
            "            \"col4\": \"col4\"\n" +
            "          }\n" +
            "        }\n" +
            "      }]}\n";

        highLevelClient().ingest().putPipeline(
            new PutPipelineRequest("regression-stats-pipeline",
                new BytesArray(regressionPipeline.getBytes(StandardCharsets.UTF_8)),
                XContentType.JSON),
            RequestOptions.DEFAULT);
        {
            GetTrainedModelsStatsResponse getTrainedModelsStatsResponse = execute(
                GetTrainedModelsStatsRequest.getAllTrainedModelStatsRequest(),
                machineLearningClient::getTrainedModelsStats, machineLearningClient::getTrainedModelsStatsAsync);
            assertThat(getTrainedModelsStatsResponse.getTrainedModelStats(), hasSize(numberOfModels + 1));
            assertThat(getTrainedModelsStatsResponse.getCount(), equalTo(5L + 1));
            assertThat(getTrainedModelsStatsResponse.getTrainedModelStats().get(0).getPipelineCount(), equalTo(1));
            assertThat(getTrainedModelsStatsResponse.getTrainedModelStats().get(1).getPipelineCount(), equalTo(0));
        }
        {
            GetTrainedModelsStatsResponse getTrainedModelsStatsResponse = execute(
                new GetTrainedModelsStatsRequest(modelIdPrefix + 4, modelIdPrefix + 2, modelIdPrefix + 3),
                machineLearningClient::getTrainedModelsStats, machineLearningClient::getTrainedModelsStatsAsync);
            assertThat(getTrainedModelsStatsResponse.getTrainedModelStats(), hasSize(3));
            assertThat(getTrainedModelsStatsResponse.getCount(), equalTo(3L));
        }
        {
            GetTrainedModelsStatsResponse getTrainedModelsStatsResponse = execute(
                new GetTrainedModelsStatsRequest(modelIdPrefix + "*").setPageParams(new PageParams(1, 2)),
                machineLearningClient::getTrainedModelsStats, machineLearningClient::getTrainedModelsStatsAsync);
            assertThat(getTrainedModelsStatsResponse.getTrainedModelStats(), hasSize(2));
            assertThat(getTrainedModelsStatsResponse.getCount(), equalTo(5L));
            assertThat(
                getTrainedModelsStatsResponse.getTrainedModelStats()
                    .stream()
                    .map(TrainedModelStats::getModelId)
                    .collect(Collectors.toList()),
                containsInAnyOrder(modelIdPrefix + 1, modelIdPrefix + 2));
        }
    }

    public void testDeleteTrainedModel() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        String modelId = "delete-trained-model-test";
        putTrainedModel(modelId);

        GetTrainedModelsResponse getTrainedModelsResponse = execute(
            new GetTrainedModelsRequest(modelId + "*").setIncludeDefinition(false).setAllowNoMatch(true),
            machineLearningClient::getTrainedModels,
            machineLearningClient::getTrainedModelsAsync);

        assertThat(getTrainedModelsResponse.getCount(), equalTo(1L));
        assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(1));

        AcknowledgedResponse deleteTrainedModelResponse = execute(
            new DeleteTrainedModelRequest(modelId),
            machineLearningClient::deleteTrainedModel, machineLearningClient::deleteTrainedModelAsync);
        assertTrue(deleteTrainedModelResponse.isAcknowledged());

        getTrainedModelsResponse = execute(
            new GetTrainedModelsRequest(modelId + "*").setIncludeDefinition(false).setAllowNoMatch(true),
            machineLearningClient::getTrainedModels,
            machineLearningClient::getTrainedModelsAsync);

        assertThat(getTrainedModelsResponse.getCount(), equalTo(0L));
        assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(0));
    }

    public void testGetPrepackagedModels() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        GetTrainedModelsResponse getTrainedModelsResponse = execute(
            new GetTrainedModelsRequest("lang_ident_model_1").setIncludeDefinition(true),
            machineLearningClient::getTrainedModels,
            machineLearningClient::getTrainedModelsAsync);

        assertThat(getTrainedModelsResponse.getCount(), equalTo(1L));
        assertThat(getTrainedModelsResponse.getTrainedModels(), hasSize(1));
        assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getModelId(), equalTo("lang_ident_model_1"));
        assertThat(getTrainedModelsResponse.getTrainedModels().get(0).getDefinition().getTrainedModel(),
            instanceOf(LangIdentNeuralNetwork.class));
    }

    public void testPutFilter() throws Exception {
        String filterId = "filter-job-test";
        MlFilter mlFilter = MlFilter.builder(filterId)
            .setDescription(randomAlphaOfLength(10))
            .setItems(generateRandomStringArray(10, 10, false, false))
            .build();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        PutFilterResponse putFilterResponse = execute(new PutFilterRequest(mlFilter),
            machineLearningClient::putFilter,
            machineLearningClient::putFilterAsync);
        MlFilter createdFilter = putFilterResponse.getResponse();

        assertThat(createdFilter, equalTo(mlFilter));
    }

    public void testGetFilters() throws Exception {
        String filterId1 = "get-filter-test-1";
        String filterId2 = "get-filter-test-2";
        String filterId3 = "get-filter-test-3";
        MlFilter mlFilter1 = MlFilter.builder(filterId1)
            .setDescription(randomAlphaOfLength(10))
            .setItems(generateRandomStringArray(10, 10, false, false))
            .build();
        MlFilter mlFilter2 = MlFilter.builder(filterId2)
            .setDescription(randomAlphaOfLength(10))
            .setItems(generateRandomStringArray(10, 10, false, false))
            .build();
        MlFilter mlFilter3 = MlFilter.builder(filterId3)
            .setDescription(randomAlphaOfLength(10))
            .setItems(generateRandomStringArray(10, 10, false, false))
            .build();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putFilter(new PutFilterRequest(mlFilter1), RequestOptions.DEFAULT);
        machineLearningClient.putFilter(new PutFilterRequest(mlFilter2), RequestOptions.DEFAULT);
        machineLearningClient.putFilter(new PutFilterRequest(mlFilter3), RequestOptions.DEFAULT);

        {
            GetFiltersRequest getFiltersRequest = new GetFiltersRequest();
            getFiltersRequest.setFilterId(filterId1);

            GetFiltersResponse getFiltersResponse = execute(getFiltersRequest,
                machineLearningClient::getFilter,
                machineLearningClient::getFilterAsync);
            assertThat(getFiltersResponse.count(), equalTo(1L));
            assertThat(getFiltersResponse.filters().get(0), equalTo(mlFilter1));
        }
        {
            GetFiltersRequest getFiltersRequest = new GetFiltersRequest();

            getFiltersRequest.setFrom(1);
            getFiltersRequest.setSize(2);

            GetFiltersResponse getFiltersResponse = execute(getFiltersRequest,
                machineLearningClient::getFilter,
                machineLearningClient::getFilterAsync);
            assertThat(getFiltersResponse.count(), equalTo(3L));
            assertThat(getFiltersResponse.filters().size(), equalTo(2));
            assertThat(getFiltersResponse.filters().stream().map(MlFilter::getId).collect(Collectors.toList()),
                containsInAnyOrder("get-filter-test-2", "get-filter-test-3"));
        }
    }

    public void testUpdateFilter() throws Exception {
        String filterId = "update-filter-test";
        MlFilter mlFilter = MlFilter.builder(filterId)
            .setDescription("old description")
            .setItems(Arrays.asList("olditem1", "olditem2"))
            .build();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putFilter(new PutFilterRequest(mlFilter), RequestOptions.DEFAULT);

        UpdateFilterRequest updateFilterRequest = new UpdateFilterRequest(filterId);

        updateFilterRequest.setAddItems(Arrays.asList("newItem1", "newItem2"));
        updateFilterRequest.setRemoveItems(Collections.singletonList("olditem1"));
        updateFilterRequest.setDescription("new description");
        MlFilter filter = execute(updateFilterRequest,
            machineLearningClient::updateFilter,
            machineLearningClient::updateFilterAsync).getResponse();

        assertThat(filter.getDescription(), equalTo(updateFilterRequest.getDescription()));
        assertThat(filter.getItems(), contains("newItem1", "newItem2", "olditem2"));
    }

    public void testDeleteFilter() throws Exception {
        String filterId = "delete-filter-job-test";
        MlFilter mlFilter = MlFilter.builder(filterId)
            .setDescription(randomAlphaOfLength(10))
            .setItems(generateRandomStringArray(10, 10, false, false))
            .build();
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        PutFilterResponse putFilterResponse = execute(new PutFilterRequest(mlFilter),
            machineLearningClient::putFilter,
            machineLearningClient::putFilterAsync);
        MlFilter createdFilter = putFilterResponse.getResponse();

        assertThat(createdFilter, equalTo(mlFilter));

        DeleteFilterRequest deleteFilterRequest = new DeleteFilterRequest(filterId);
        AcknowledgedResponse response = execute(deleteFilterRequest, machineLearningClient::deleteFilter,
            machineLearningClient::deleteFilterAsync);
        assertTrue(response.isAcknowledged());

        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(deleteFilterRequest, machineLearningClient::deleteFilter,
                machineLearningClient::deleteFilterAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testGetMlInfo() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        MlInfoResponse infoResponse = execute(new MlInfoRequest(), machineLearningClient::getMlInfo, machineLearningClient::getMlInfoAsync);
        Map<String, Object> info = infoResponse.getInfo();
        assertThat(info, notNullValue());
        assertTrue(info.containsKey("defaults"));
        assertTrue(info.containsKey("limits"));
    }

    public static String randomValidJobId() {
        CodepointSetGenerator generator = new CodepointSetGenerator("abcdefghijklmnopqrstuvwxyz0123456789".toCharArray());
        return generator.ofCodePointsLength(random(), 10, 10);
    }

    private static Job buildJobForExpiredDataTests(String jobId) {
        Job.Builder builder = new Job.Builder(jobId);
        builder.setDescription(randomAlphaOfLength(10));

        Detector detector = new Detector.Builder()
            .setFunction("count")
            .setDetectorDescription(randomAlphaOfLength(10))
            .build();
        AnalysisConfig.Builder configBuilder = new AnalysisConfig.Builder(Collections.singletonList(detector));
        //should not be random, see:https://github.com/elastic/ml-cpp/issues/208
        configBuilder.setBucketSpan(new TimeValue(1, TimeUnit.HOURS));
        builder.setAnalysisConfig(configBuilder);
        builder.setModelSnapshotRetentionDays(1L);
        builder.setDailyModelSnapshotRetentionAfterDays(1L);

        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);
        dataDescription.setTimeField("timestamp");
        builder.setDataDescription(dataDescription);

        return builder.build();
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
        //should not be random, see:https://github.com/elastic/ml-cpp/issues/208
        configBuilder.setBucketSpan(new TimeValue(5, TimeUnit.SECONDS));
        builder.setAnalysisConfig(configBuilder);
        builder.setAnalysisLimits(new AnalysisLimits(512L, 4L));

        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);
        dataDescription.setTimeField("timestamp");
        builder.setDataDescription(dataDescription);

        return builder.build();
    }

    private void putJob(Job job) throws IOException {
        highLevelClient().machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);
    }

    private void openJob(Job job) throws IOException {
        highLevelClient().machineLearning().openJob(new OpenJobRequest(job.getId()), RequestOptions.DEFAULT);
    }

    private void putTrainedModel(String modelId) throws IOException {
        TrainedModelDefinition definition = TrainedModelDefinitionTests.createRandomBuilder(TargetType.REGRESSION).build();
        TrainedModelConfig trainedModelConfig = TrainedModelConfig.builder()
            .setDefinition(definition)
            .setModelId(modelId)
            .setInferenceConfig(new RegressionConfig())
            .setInput(new TrainedModelInput(Arrays.asList("col1", "col2", "col3", "col4")))
            .setDescription("test model")
            .build();
        highLevelClient().machineLearning().putTrainedModel(new PutTrainedModelRequest(trainedModelConfig), RequestOptions.DEFAULT);
    }

    private void waitForJobToClose(String jobId) throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        assertBusy(() -> {
            JobStats stats = machineLearningClient.getJobStats(new GetJobStatsRequest(jobId), RequestOptions.DEFAULT).jobStats().get(0);
            assertEquals(JobState.CLOSED, stats.getState());
        }, 30, TimeUnit.SECONDS);
    }

    private void startDatafeed(String datafeedId, String start, String end) throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        StartDatafeedRequest startDatafeedRequest = new StartDatafeedRequest(datafeedId);
        startDatafeedRequest.setStart(start);
        startDatafeedRequest.setEnd(end);
        StartDatafeedResponse response = execute(startDatafeedRequest,
            machineLearningClient::startDatafeed,
            machineLearningClient::startDatafeedAsync);

        assertTrue(response.isStarted());
    }

    private void updateModelSnapshotTimestamp(String jobId, String timestamp) throws Exception {

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        GetModelSnapshotsRequest getModelSnapshotsRequest = new GetModelSnapshotsRequest(jobId);
        GetModelSnapshotsResponse getModelSnapshotsResponse = execute(getModelSnapshotsRequest, machineLearningClient::getModelSnapshots,
            machineLearningClient::getModelSnapshotsAsync);

        assertThat(getModelSnapshotsResponse.count(), greaterThanOrEqualTo(1L));

        ModelSnapshot modelSnapshot = getModelSnapshotsResponse.snapshots().get(0);

        String snapshotId = modelSnapshot.getSnapshotId();
        String documentId = jobId + "_model_snapshot_" + snapshotId;

        String snapshotUpdate = "{ \"timestamp\": " + timestamp + "}";
        UpdateRequest updateSnapshotRequest = new UpdateRequest(".ml-anomalies-" + jobId, documentId);
        updateSnapshotRequest.doc(snapshotUpdate.getBytes(StandardCharsets.UTF_8), XContentType.JSON);
        highLevelClient().update(updateSnapshotRequest, RequestOptions.DEFAULT);
    }

    private String createAndPutDatafeed(String jobId, String indexName) throws IOException {
        String datafeedId = jobId + "-feed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, jobId)
            .setIndices(indexName)
            .setQueryDelay(TimeValue.timeValueSeconds(1))
            .setFrequency(TimeValue.timeValueSeconds(1)).build();
        highLevelClient().machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);
        return datafeedId;
    }

    public void createModelSnapshot(String jobId, String snapshotId) throws IOException {
        String documentId = jobId + "_model_snapshot_" + snapshotId;
        Job job = MachineLearningIT.buildJob(jobId);
        highLevelClient().machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared").id(documentId);
        indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        indexRequest.source("{\"job_id\":\"" + jobId + "\", \"timestamp\":1541587919000, " +
            "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
            "\"snapshot_id\":\"" + snapshotId + "\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
            "\"job_id\":\"" + jobId + "\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
            "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
            "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
            "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
            "\"latest_result_time_stamp\":1519930800000, \"retain\":false}", XContentType.JSON);

        highLevelClient().index(indexRequest, RequestOptions.DEFAULT);
    }

    public void createModelSnapshots(String jobId, List<String> snapshotIds) throws IOException {
        Job job = MachineLearningIT.buildJob(jobId);
        highLevelClient().machineLearning().putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        for(String snapshotId : snapshotIds) {
            String documentId = jobId + "_model_snapshot_" + snapshotId;
            IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared").id(documentId);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            indexRequest.source("{\"job_id\":\"" + jobId + "\", \"timestamp\":1541587919000, " +
                "\"description\":\"State persisted due to job close at 2018-11-07T10:51:59+0000\", " +
                "\"snapshot_id\":\"" + snapshotId + "\", \"snapshot_doc_count\":1, \"model_size_stats\":{" +
                "\"job_id\":\"" + jobId + "\", \"result_type\":\"model_size_stats\",\"model_bytes\":51722, " +
                "\"total_by_field_count\":3, \"total_over_field_count\":0, \"total_partition_field_count\":2," +
                "\"bucket_allocation_failures_count\":0, \"memory_status\":\"ok\", \"log_time\":1541587919000, " +
                "\"timestamp\":1519930800000}, \"latest_record_time_stamp\":1519931700000," +
                "\"latest_result_time_stamp\":1519930800000, \"retain\":false, " +
                "\"quantiles\":{\"job_id\":\""+jobId+"\", \"timestamp\":1541587919000, " +
                "\"quantile_state\":\"state\"}}", XContentType.JSON);
            highLevelClient().index(indexRequest, RequestOptions.DEFAULT);
        }
    }

    public void testDeleteModelSnapshot() throws IOException {
        String jobId = "test-delete-model-snapshot";
        String snapshotId = "1541587919";

        createModelSnapshot(jobId, snapshotId);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        DeleteModelSnapshotRequest request = new DeleteModelSnapshotRequest(jobId, snapshotId);

        AcknowledgedResponse response = execute(request, machineLearningClient::deleteModelSnapshot,
                machineLearningClient::deleteModelSnapshotAsync);

        assertTrue(response.isAcknowledged());
    }

    public void testUpdateModelSnapshot() throws Exception {
        String jobId = "test-update-model-snapshot";

        String snapshotId = "1541587919";
        createModelSnapshot(jobId, snapshotId);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        GetModelSnapshotsRequest getModelSnapshotsRequest = new GetModelSnapshotsRequest(jobId);

        GetModelSnapshotsResponse getModelSnapshotsResponse1 = execute(getModelSnapshotsRequest, machineLearningClient::getModelSnapshots,
            machineLearningClient::getModelSnapshotsAsync);

        assertEquals(getModelSnapshotsResponse1.count(), 1L);
        assertEquals("State persisted due to job close at 2018-11-07T10:51:59+0000",
            getModelSnapshotsResponse1.snapshots().get(0).getDescription());

        UpdateModelSnapshotRequest request = new UpdateModelSnapshotRequest(jobId, snapshotId);
        request.setDescription("Updated description");
        request.setRetain(true);

        UpdateModelSnapshotResponse response = execute(request, machineLearningClient::updateModelSnapshot,
            machineLearningClient::updateModelSnapshotAsync);

        assertTrue(response.getAcknowledged());
        assertEquals("Updated description", response.getModel().getDescription());
        assertTrue(response.getModel().getRetain());

        GetModelSnapshotsResponse getModelSnapshotsResponse2 = execute(getModelSnapshotsRequest, machineLearningClient::getModelSnapshots,
            machineLearningClient::getModelSnapshotsAsync);

        assertEquals(getModelSnapshotsResponse2.count(), 1L);
        assertEquals("Updated description",
            getModelSnapshotsResponse2.snapshots().get(0).getDescription());
    }

    public void testRevertModelSnapshot() throws IOException {
        String jobId = "test-revert-model-snapshot";

        List<String> snapshotIds = new ArrayList<>();

        String snapshotId1 = "1541587919";
        String snapshotId2 = "1541588919";
        String snapshotId3 = "1541589919";

        snapshotIds.add(snapshotId1);
        snapshotIds.add(snapshotId2);
        snapshotIds.add(snapshotId3);

        createModelSnapshots(jobId, snapshotIds);

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        for (String snapshotId : snapshotIds){
            RevertModelSnapshotRequest request = new RevertModelSnapshotRequest(jobId, snapshotId);
            if (randomBoolean()) {
                request.setDeleteInterveningResults(randomBoolean());
            }

            RevertModelSnapshotResponse response = execute(request, machineLearningClient::revertModelSnapshot,
                machineLearningClient::revertModelSnapshotAsync);

            ModelSnapshot model = response.getModel();

            assertEquals(snapshotId, model.getSnapshotId());
        }
    }

    public void testFindFileStructure() throws IOException {

        String sample = "{\"logger\":\"controller\",\"timestamp\":1478261151445,\"level\":\"INFO\"," +
                "\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 1\",\"class\":\"ml\"," +
                "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n" +
            "{\"logger\":\"controller\",\"timestamp\":1478261151445," +
                "\"level\":\"INFO\",\"pid\":42,\"thread\":\"0x7fff7d2a8000\",\"message\":\"message 2\",\"class\":\"ml\"," +
                "\"method\":\"core::SomeNoiseMaker\",\"file\":\"Noisemaker.cc\",\"line\":333}\n";

        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        FindFileStructureRequest request = new FindFileStructureRequest();
        request.setSample(sample.getBytes(StandardCharsets.UTF_8));

        FindFileStructureResponse response =
            execute(request, machineLearningClient::findFileStructure, machineLearningClient::findFileStructureAsync);

        FileStructure structure = response.getFileStructure();

        assertEquals(2, structure.getNumLinesAnalyzed());
        assertEquals(2, structure.getNumMessagesAnalyzed());
        assertEquals(sample, structure.getSampleStart());
        assertEquals(FileStructure.Format.NDJSON, structure.getFormat());
        assertEquals(StandardCharsets.UTF_8.displayName(Locale.ROOT), structure.getCharset());
        assertFalse(structure.getHasByteOrderMarker());
        assertNull(structure.getMultilineStartPattern());
        assertNull(structure.getExcludeLinesPattern());
        assertNull(structure.getColumnNames());
        assertNull(structure.getHasHeaderRow());
        assertNull(structure.getDelimiter());
        assertNull(structure.getQuote());
        assertNull(structure.getShouldTrimFields());
        assertNull(structure.getGrokPattern());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJavaTimestampFormats());
        assertEquals(Collections.singletonList("UNIX_MS"), structure.getJodaTimestampFormats());
        assertEquals("timestamp", structure.getTimestampField());
        assertFalse(structure.needClientTimezone());
    }

    public void testEnableUpgradeMode() throws Exception {
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();

        MlInfoResponse mlInfoResponse = machineLearningClient.getMlInfo(new MlInfoRequest(), RequestOptions.DEFAULT);
        assertThat(mlInfoResponse.getInfo().get("upgrade_mode"), equalTo(false));

        AcknowledgedResponse setUpgrademodeResponse = execute(new SetUpgradeModeRequest(true),
            machineLearningClient::setUpgradeMode,
            machineLearningClient::setUpgradeModeAsync);

        assertThat(setUpgrademodeResponse.isAcknowledged(), is(true));


        mlInfoResponse = machineLearningClient.getMlInfo(new MlInfoRequest(), RequestOptions.DEFAULT);
        assertThat(mlInfoResponse.getInfo().get("upgrade_mode"), equalTo(true));

        setUpgrademodeResponse = execute(new SetUpgradeModeRequest(false),
            machineLearningClient::setUpgradeMode,
            machineLearningClient::setUpgradeModeAsync);

        assertThat(setUpgrademodeResponse.isAcknowledged(), is(true));

        mlInfoResponse = machineLearningClient.getMlInfo(new MlInfoRequest(), RequestOptions.DEFAULT);
        assertThat(mlInfoResponse.getInfo().get("upgrade_mode"), equalTo(false));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
    }
}
