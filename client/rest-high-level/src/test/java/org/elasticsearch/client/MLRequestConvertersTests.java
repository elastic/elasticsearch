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
import org.elasticsearch.client.core.PageParams;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarEventRequest;
import org.elasticsearch.client.ml.DeleteCalendarJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarRequest;
import org.elasticsearch.client.ml.DeleteDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataRequest;
import org.elasticsearch.client.ml.DeleteFilterRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteModelSnapshotRequest;
import org.elasticsearch.client.ml.DeleteTrainedModelRequest;
import org.elasticsearch.client.ml.EstimateModelMemoryRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameRequest;
import org.elasticsearch.client.ml.EvaluateDataFrameRequestTests;
import org.elasticsearch.client.ml.ExplainDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.FindFileStructureRequest;
import org.elasticsearch.client.ml.FindFileStructureRequestTests;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.GetBucketsRequest;
import org.elasticsearch.client.ml.GetCalendarEventsRequest;
import org.elasticsearch.client.ml.GetCalendarsRequest;
import org.elasticsearch.client.ml.GetCategoriesRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.GetDataFrameAnalyticsStatsRequest;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedStatsRequest;
import org.elasticsearch.client.ml.GetFiltersRequest;
import org.elasticsearch.client.ml.GetInfluencersRequest;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetModelSnapshotsRequest;
import org.elasticsearch.client.ml.GetOverallBucketsRequest;
import org.elasticsearch.client.ml.GetRecordsRequest;
import org.elasticsearch.client.ml.GetTrainedModelsRequest;
import org.elasticsearch.client.ml.GetTrainedModelsStatsRequest;
import org.elasticsearch.client.ml.MlInfoRequest;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.PostCalendarEventRequest;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PreviewDatafeedRequest;
import org.elasticsearch.client.ml.PutCalendarJobRequest;
import org.elasticsearch.client.ml.PutCalendarRequest;
import org.elasticsearch.client.ml.PutDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutFilterRequest;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutTrainedModelRequest;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.SetUpgradeModeRequest;
import org.elasticsearch.client.ml.StartDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StartDatafeedRequest;
import org.elasticsearch.client.ml.StartDatafeedRequestTests;
import org.elasticsearch.client.ml.StopDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.UpdateDataFrameAnalyticsRequest;
import org.elasticsearch.client.ml.UpdateFilterRequest;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.UpdateModelSnapshotRequest;
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.client.ml.calendars.CalendarTests;
import org.elasticsearch.client.ml.calendars.ScheduledEvent;
import org.elasticsearch.client.ml.calendars.ScheduledEventTests;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.datafeed.DatafeedConfigTests;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfig;
import org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigUpdate;
import org.elasticsearch.client.ml.dataframe.MlDataFrameAnalysisNamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.evaluation.MlEvaluationNamedXContentProvider;
import org.elasticsearch.client.ml.dataframe.stats.AnalysisStatsNamedXContentProvider;
import org.elasticsearch.client.ml.filestructurefinder.FileStructure;
import org.elasticsearch.client.ml.inference.MlInferenceNamedXContentProvider;
import org.elasticsearch.client.ml.inference.TrainedModelConfig;
import org.elasticsearch.client.ml.inference.TrainedModelConfigTests;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.JobUpdateTests;
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.client.ml.job.config.MlFilterTests;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.test.ESTestCase;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigTests.randomDataFrameAnalyticsConfig;
import static org.elasticsearch.client.ml.dataframe.DataFrameAnalyticsConfigUpdateTests.randomDataFrameAnalyticsConfigUpdate;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.core.IsNull.nullValue;

public class MLRequestConvertersTests extends ESTestCase {

    public void testPutJob() throws IOException {
        Job job = createValidJob("foo");
        PutJobRequest putJobRequest = new PutJobRequest(job);

        Request request = MLRequestConverters.putJob(putJobRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/anomaly_detectors/foo"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            Job parsedJob = Job.PARSER.apply(parser, null).build();
            assertThat(parsedJob, equalTo(job));
        }
    }

    public void testGetJob() {
        GetJobRequest getJobRequest = new GetJobRequest();

        Request request = MLRequestConverters.getJob(getJobRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("allow_no_match"));

        getJobRequest = new GetJobRequest("job1", "jobs*");
        getJobRequest.setAllowNoMatch(true);
        request = MLRequestConverters.getJob(getJobRequest);

        assertEquals("/_ml/anomaly_detectors/job1,jobs*", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("allow_no_match"));
    }

    public void testGetJobStats() {
        GetJobStatsRequest getJobStatsRequestRequest = new GetJobStatsRequest();

        Request request = MLRequestConverters.getJobStats(getJobStatsRequestRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/_stats", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("allow_no_match"));

        getJobStatsRequestRequest = new GetJobStatsRequest("job1", "jobs*");
        getJobStatsRequestRequest.setAllowNoMatch(true);
        request = MLRequestConverters.getJobStats(getJobStatsRequestRequest);

        assertEquals("/_ml/anomaly_detectors/job1,jobs*/_stats", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("allow_no_match"));
    }

    public void testOpenJob() throws Exception {
        String jobId = "some-job-id";
        OpenJobRequest openJobRequest = new OpenJobRequest(jobId);
        openJobRequest.setTimeout(TimeValue.timeValueMinutes(10));

        Request request = MLRequestConverters.openJob(openJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_open", request.getEndpoint());
        assertEquals(requestEntityToString(request), "{\"job_id\":\""+ jobId +"\",\"timeout\":\"10m\"}");
    }

    public void testCloseJob() throws Exception {
        String jobId = "somejobid";
        CloseJobRequest closeJobRequest = new CloseJobRequest(jobId);

        Request request = MLRequestConverters.closeJob(closeJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_close", request.getEndpoint());
        assertEquals("{\"job_id\":\"somejobid\"}", requestEntityToString(request));

        closeJobRequest = new CloseJobRequest(jobId, "otherjobs*");
        closeJobRequest.setForce(true);
        closeJobRequest.setAllowNoMatch(false);
        closeJobRequest.setTimeout(TimeValue.timeValueMinutes(10));
        request = MLRequestConverters.closeJob(closeJobRequest);

        assertEquals("/_ml/anomaly_detectors/" + jobId + ",otherjobs*/_close", request.getEndpoint());
        assertEquals("{\"job_id\":\"somejobid,otherjobs*\",\"timeout\":\"10m\",\"force\":true,\"allow_no_match\":false}",
            requestEntityToString(request));
    }

    public void testDeleteExpiredData() throws Exception {
        float requestsPerSec = randomBoolean() ? -1.0f : (float)randomDoubleBetween(0.0, 100000.0, false);
        String jobId = randomBoolean() ? null : randomAlphaOfLength(8);
        DeleteExpiredDataRequest deleteExpiredDataRequest = new DeleteExpiredDataRequest(
            jobId,
            requestsPerSec,
            TimeValue.timeValueHours(1));

        Request request = MLRequestConverters.deleteExpiredData(deleteExpiredDataRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());

        String expectedPath = jobId == null ? "/_ml/_delete_expired_data" : "/_ml/_delete_expired_data/" + jobId;
        assertEquals(expectedPath, request.getEndpoint());
        if (jobId == null) {
            assertEquals("{\"requests_per_second\":" + requestsPerSec + ",\"timeout\":\"1h\"}", requestEntityToString(request));
        } else {
            assertEquals("{\"job_id\":\"" + jobId + "\",\"requests_per_second\":" + requestsPerSec + ",\"timeout\":\"1h\"}",
                requestEntityToString(request));
        }
    }

    public void testDeleteJob() {
        String jobId = randomAlphaOfLength(10);
        DeleteJobRequest deleteJobRequest = new DeleteJobRequest(jobId);

        Request request = MLRequestConverters.deleteJob(deleteJobRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId, request.getEndpoint());
        assertNull(request.getParameters().get("force"));
        assertNull(request.getParameters().get("wait_for_completion"));

        deleteJobRequest = new DeleteJobRequest(jobId);
        deleteJobRequest.setForce(true);
        request = MLRequestConverters.deleteJob(deleteJobRequest);
        assertEquals(Boolean.toString(true), request.getParameters().get("force"));

        deleteJobRequest = new DeleteJobRequest(jobId);
        deleteJobRequest.setWaitForCompletion(false);
        request = MLRequestConverters.deleteJob(deleteJobRequest);
        assertEquals(Boolean.toString(false), request.getParameters().get("wait_for_completion"));
    }

    public void testFlushJob() throws Exception {
        String jobId = randomAlphaOfLength(10);
        FlushJobRequest flushJobRequest = new FlushJobRequest(jobId);

        Request request = MLRequestConverters.flushJob(flushJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_flush", request.getEndpoint());
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

    public void testForecastJob() throws Exception {
        String jobId = randomAlphaOfLength(10);
        ForecastJobRequest forecastJobRequest = new ForecastJobRequest(jobId);

        forecastJobRequest.setDuration(TimeValue.timeValueHours(10));
        forecastJobRequest.setExpiresIn(TimeValue.timeValueHours(12));
        Request request = MLRequestConverters.forecastJob(forecastJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_forecast", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            ForecastJobRequest parsedRequest = ForecastJobRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(forecastJobRequest));
        }
    }

    public void testUpdateJob() throws Exception {
        String jobId = randomAlphaOfLength(10);
        JobUpdate updates = JobUpdateTests.createRandom(jobId);
        UpdateJobRequest updateJobRequest = new UpdateJobRequest(updates);

        Request request = MLRequestConverters.updateJob(updateJobRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_update", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            JobUpdate.Builder parsedRequest = JobUpdate.PARSER.apply(parser, null);
            assertThat(parsedRequest.build(), equalTo(updates));
        }
    }

    public void testPutDatafeed() throws IOException {
        DatafeedConfig datafeed = DatafeedConfigTests.createRandom();
        PutDatafeedRequest putDatafeedRequest = new PutDatafeedRequest(datafeed);

        Request request = MLRequestConverters.putDatafeed(putDatafeedRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/datafeeds/" + datafeed.getId()));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DatafeedConfig parsedDatafeed = DatafeedConfig.PARSER.apply(parser, null).build();
            assertThat(parsedDatafeed, equalTo(datafeed));
        }
    }

    public void testGetDatafeed() {
        GetDatafeedRequest getDatafeedRequest = new GetDatafeedRequest();

        Request request = MLRequestConverters.getDatafeed(getDatafeedRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/datafeeds", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("allow_no_match"));

        getDatafeedRequest = new GetDatafeedRequest("feed-1", "feed-*");
        getDatafeedRequest.setAllowNoMatch(true);
        request = MLRequestConverters.getDatafeed(getDatafeedRequest);

        assertEquals("/_ml/datafeeds/feed-1,feed-*", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("allow_no_match"));
    }

    public void testDeleteDatafeed() {
        String datafeedId = randomAlphaOfLength(10);
        DeleteDatafeedRequest deleteDatafeedRequest = new DeleteDatafeedRequest(datafeedId);

        Request request = MLRequestConverters.deleteDatafeed(deleteDatafeedRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/datafeeds/" + datafeedId, request.getEndpoint());
        assertFalse(request.getParameters().containsKey("force"));

        deleteDatafeedRequest.setForce(true);
        request = MLRequestConverters.deleteDatafeed(deleteDatafeedRequest);
        assertEquals(Boolean.toString(true), request.getParameters().get("force"));
    }

    public void testStartDatafeed() throws Exception {
        String datafeedId = DatafeedConfigTests.randomValidDatafeedId();
        StartDatafeedRequest datafeedRequest = StartDatafeedRequestTests.createRandomInstance(datafeedId);

        Request request = MLRequestConverters.startDatafeed(datafeedRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/datafeeds/" + datafeedId + "/_start", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            StartDatafeedRequest parsedDatafeedRequest = StartDatafeedRequest.PARSER.apply(parser, null);
            assertThat(parsedDatafeedRequest, equalTo(datafeedRequest));
        }
    }

    public void testStopDatafeed() throws Exception {
        StopDatafeedRequest datafeedRequest = new StopDatafeedRequest("datafeed_1", "datafeed_2");
        datafeedRequest.setForce(true);
        datafeedRequest.setTimeout(TimeValue.timeValueMinutes(10));
        datafeedRequest.setAllowNoMatch(true);
        Request request = MLRequestConverters.stopDatafeed(datafeedRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/datafeeds/" +
            Strings.collectionToCommaDelimitedString(datafeedRequest.getDatafeedIds()) +
            "/_stop", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            StopDatafeedRequest parsedDatafeedRequest = StopDatafeedRequest.PARSER.apply(parser, null);
            assertThat(parsedDatafeedRequest, equalTo(datafeedRequest));
        }
    }

    public void testGetDatafeedStats() {
        GetDatafeedStatsRequest getDatafeedStatsRequestRequest = new GetDatafeedStatsRequest();

        Request request = MLRequestConverters.getDatafeedStats(getDatafeedStatsRequestRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/datafeeds/_stats", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("allow_no_match"));

        getDatafeedStatsRequestRequest = new GetDatafeedStatsRequest("datafeed1", "datafeeds*");
        getDatafeedStatsRequestRequest.setAllowNoMatch(true);
        request = MLRequestConverters.getDatafeedStats(getDatafeedStatsRequestRequest);

        assertEquals("/_ml/datafeeds/datafeed1,datafeeds*/_stats", request.getEndpoint());
        assertEquals(Boolean.toString(true), request.getParameters().get("allow_no_match"));
    }

    public void testPreviewDatafeed() {
        PreviewDatafeedRequest datafeedRequest = new PreviewDatafeedRequest("datafeed_1");
        Request request = MLRequestConverters.previewDatafeed(datafeedRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/datafeeds/" + datafeedRequest.getDatafeedId() + "/_preview", request.getEndpoint());
    }

    public void testDeleteForecast() {
        String jobId = randomAlphaOfLength(10);
        DeleteForecastRequest deleteForecastRequest = new DeleteForecastRequest(jobId);

        Request request = MLRequestConverters.deleteForecast(deleteForecastRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_forecast", request.getEndpoint());
        assertFalse(request.getParameters().containsKey("timeout"));
        assertFalse(request.getParameters().containsKey("allow_no_forecasts"));

        deleteForecastRequest.setForecastIds(randomAlphaOfLength(10), randomAlphaOfLength(10));
        deleteForecastRequest.timeout("10s");
        deleteForecastRequest.setAllowNoForecasts(true);

        request = MLRequestConverters.deleteForecast(deleteForecastRequest);
        assertEquals(
            "/_ml/anomaly_detectors/" +
                jobId +
                "/_forecast/" +
                Strings.collectionToCommaDelimitedString(deleteForecastRequest.getForecastIds()),
            request.getEndpoint());
        assertEquals("10s",
            request.getParameters().get(DeleteForecastRequest.TIMEOUT.getPreferredName()));
        assertEquals(Boolean.toString(true),
            request.getParameters().get(DeleteForecastRequest.ALLOW_NO_FORECASTS.getPreferredName()));
    }

    public void testDeleteModelSnapshot() {
        String jobId = randomAlphaOfLength(10);
        String snapshotId = randomAlphaOfLength(10);
        DeleteModelSnapshotRequest deleteModelSnapshotRequest = new DeleteModelSnapshotRequest(jobId, snapshotId);

        Request request = MLRequestConverters.deleteModelSnapshot(deleteModelSnapshotRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/model_snapshots/" + snapshotId, request.getEndpoint());
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
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/results/buckets", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetBucketsRequest parsedRequest = GetBucketsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getBucketsRequest));
        }
    }

    public void testGetCategories() throws IOException {
        String jobId = randomAlphaOfLength(10);
        GetCategoriesRequest getCategoriesRequest = new GetCategoriesRequest(jobId);
        getCategoriesRequest.setPageParams(new PageParams(100, 300));


        Request request = MLRequestConverters.getCategories(getCategoriesRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/results/categories", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetCategoriesRequest parsedRequest = GetCategoriesRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getCategoriesRequest));
        }
    }

    public void testGetModelSnapshots() throws IOException {
        String jobId = randomAlphaOfLength(10);
        GetModelSnapshotsRequest getModelSnapshotsRequest = new GetModelSnapshotsRequest(jobId);
        getModelSnapshotsRequest.setPageParams(new PageParams(100, 300));


        Request request = MLRequestConverters.getModelSnapshots(getModelSnapshotsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/model_snapshots", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetModelSnapshotsRequest parsedRequest = GetModelSnapshotsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getModelSnapshotsRequest));
        }
    }

    public void testUpdateModelSnapshot() throws IOException {
        String jobId = randomAlphaOfLength(10);
        String snapshotId = randomAlphaOfLength(10);
        UpdateModelSnapshotRequest updateModelSnapshotRequest = new UpdateModelSnapshotRequest(jobId, snapshotId);
        updateModelSnapshotRequest.setDescription("My First Snapshot");
        updateModelSnapshotRequest.setRetain(true);

        Request request = MLRequestConverters.updateModelSnapshot(updateModelSnapshotRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/model_snapshots/" + snapshotId + "/_update", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            UpdateModelSnapshotRequest parsedRequest = UpdateModelSnapshotRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(updateModelSnapshotRequest));
        }
    }

    public void testRevertModelSnapshot() throws IOException {
        String jobId = randomAlphaOfLength(10);
        String snapshotId = randomAlphaOfLength(10);
        RevertModelSnapshotRequest revertModelSnapshotRequest = new RevertModelSnapshotRequest(jobId, snapshotId);
        if (randomBoolean()) {
            revertModelSnapshotRequest.setDeleteInterveningResults(randomBoolean());
        }

        Request request = MLRequestConverters.revertModelSnapshot(revertModelSnapshotRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/model_snapshots/" + snapshotId + "/_revert",
            request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            RevertModelSnapshotRequest parsedRequest = RevertModelSnapshotRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(revertModelSnapshotRequest));
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
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/results/overall_buckets", request.getEndpoint());
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
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/results/records", request.getEndpoint());
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
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/_data", request.getEndpoint());
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
        assertEquals("/_ml/anomaly_detectors/" + jobId + "/results/influencers", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetInfluencersRequest parsedRequest = GetInfluencersRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getInfluencersRequest));
        }
    }

    public void testPutCalendar() throws IOException {
        PutCalendarRequest putCalendarRequest = new PutCalendarRequest(CalendarTests.testInstance());
        Request request = MLRequestConverters.putCalendar(putCalendarRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + putCalendarRequest.getCalendar().getId(), request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            Calendar parsedCalendar = Calendar.PARSER.apply(parser, null);
            assertThat(parsedCalendar, equalTo(putCalendarRequest.getCalendar()));
        }
    }

    public void testPutCalendarJob() {
        String calendarId = randomAlphaOfLength(10);
        String job1 = randomAlphaOfLength(5);
        String job2 = randomAlphaOfLength(5);
        PutCalendarJobRequest putCalendarJobRequest = new PutCalendarJobRequest(calendarId, job1, job2);
        Request request = MLRequestConverters.putCalendarJob(putCalendarJobRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + calendarId + "/jobs/" + job1 + "," + job2, request.getEndpoint());
    }

    public void testDeleteCalendarJob() {
        String calendarId = randomAlphaOfLength(10);
        String job1 = randomAlphaOfLength(5);
        String job2 = randomAlphaOfLength(5);
        DeleteCalendarJobRequest deleteCalendarJobRequest = new DeleteCalendarJobRequest(calendarId, job1, job2);
        Request request = MLRequestConverters.deleteCalendarJob(deleteCalendarJobRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + calendarId + "/jobs/" + job1 + "," + job2, request.getEndpoint());
    }

    public void testGetCalendars() throws IOException {
        GetCalendarsRequest getCalendarsRequest = new GetCalendarsRequest();
        String expectedEndpoint = "/_ml/calendars";

        if (randomBoolean()) {
            String calendarId = randomAlphaOfLength(10);
            getCalendarsRequest.setCalendarId(calendarId);
            expectedEndpoint += "/" + calendarId;
        }
        if (randomBoolean()) {
            getCalendarsRequest.setPageParams(new PageParams(10, 20));
        }

        Request request = MLRequestConverters.getCalendars(getCalendarsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals(expectedEndpoint, request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetCalendarsRequest parsedRequest = GetCalendarsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getCalendarsRequest));
        }
    }

    public void testDeleteCalendar() {
        DeleteCalendarRequest deleteCalendarRequest = new DeleteCalendarRequest(randomAlphaOfLength(10));
        Request request = MLRequestConverters.deleteCalendar(deleteCalendarRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + deleteCalendarRequest.getCalendarId(), request.getEndpoint());
    }

    public void testGetCalendarEvents() throws IOException {
        String calendarId = randomAlphaOfLength(10);
        GetCalendarEventsRequest getCalendarEventsRequest = new GetCalendarEventsRequest(calendarId);
        getCalendarEventsRequest.setStart("2018-08-08T00:00:00Z");
        getCalendarEventsRequest.setEnd("2018-09-08T00:00:00Z");
        getCalendarEventsRequest.setPageParams(new PageParams(100, 300));
        getCalendarEventsRequest.setJobId(randomAlphaOfLength(10));

        Request request = MLRequestConverters.getCalendarEvents(getCalendarEventsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + calendarId + "/events", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            GetCalendarEventsRequest parsedRequest = GetCalendarEventsRequest.PARSER.apply(parser, null);
            assertThat(parsedRequest, equalTo(getCalendarEventsRequest));
        }
    }

    public void testPostCalendarEvent() throws Exception {
        String calendarId = randomAlphaOfLength(10);
        List<ScheduledEvent> events = Arrays.asList(ScheduledEventTests.testInstance(),
            ScheduledEventTests.testInstance(),
            ScheduledEventTests.testInstance());
        PostCalendarEventRequest postCalendarEventRequest = new PostCalendarEventRequest(calendarId, events);

        Request request = MLRequestConverters.postCalendarEvents(postCalendarEventRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + calendarId + "/events", request.getEndpoint());

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = postCalendarEventRequest.toXContent(builder, PostCalendarEventRequest.EXCLUDE_CALENDAR_ID_PARAMS);
        assertEquals(Strings.toString(builder), requestEntityToString(request));
    }

    public void testDeleteCalendarEvent() {
        String calendarId = randomAlphaOfLength(10);
        String eventId = randomAlphaOfLength(5);
        DeleteCalendarEventRequest deleteCalendarEventRequest = new DeleteCalendarEventRequest(calendarId, eventId);
        Request request = MLRequestConverters.deleteCalendarEvent(deleteCalendarEventRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/calendars/" + calendarId + "/events/" + eventId, request.getEndpoint());
    }

    public void testEstimateModelMemory() throws Exception {
        String byFieldName = randomAlphaOfLength(10);
        String influencerFieldName = randomAlphaOfLength(10);
        AnalysisConfig analysisConfig = AnalysisConfig.builder(
            Collections.singletonList(
                Detector.builder().setFunction("count").setByFieldName(byFieldName).build()
            )).setInfluencers(Collections.singletonList(influencerFieldName)).build();
        EstimateModelMemoryRequest estimateModelMemoryRequest = new EstimateModelMemoryRequest(analysisConfig);
        estimateModelMemoryRequest.setOverallCardinality(Collections.singletonMap(byFieldName, randomNonNegativeLong()));
        estimateModelMemoryRequest.setMaxBucketCardinality(Collections.singletonMap(influencerFieldName, randomNonNegativeLong()));
        Request request = MLRequestConverters.estimateModelMemory(estimateModelMemoryRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/anomaly_detectors/_estimate_model_memory", request.getEndpoint());

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder = estimateModelMemoryRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(Strings.toString(builder), requestEntityToString(request));
    }

    public void testPutDataFrameAnalytics() throws IOException {
        PutDataFrameAnalyticsRequest putRequest = new PutDataFrameAnalyticsRequest(randomDataFrameAnalyticsConfig());
        Request request = MLRequestConverters.putDataFrameAnalytics(putRequest);
        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + putRequest.getConfig().getId(), request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DataFrameAnalyticsConfig parsedConfig = DataFrameAnalyticsConfig.fromXContent(parser);
            assertThat(parsedConfig, equalTo(putRequest.getConfig()));
        }
    }

    public void testUpdateDataFrameAnalytics() throws IOException {
        UpdateDataFrameAnalyticsRequest updateRequest = new UpdateDataFrameAnalyticsRequest(randomDataFrameAnalyticsConfigUpdate());
        Request request = MLRequestConverters.updateDataFrameAnalytics(updateRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + updateRequest.getUpdate().getId() + "/_update", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            DataFrameAnalyticsConfigUpdate parsedUpdate = DataFrameAnalyticsConfigUpdate.fromXContent(parser);
            assertThat(parsedUpdate, equalTo(updateRequest.getUpdate()));
        }
    }

    public void testGetDataFrameAnalytics() {
        String configId1 = randomAlphaOfLength(10);
        String configId2 = randomAlphaOfLength(10);
        String configId3 = randomAlphaOfLength(10);
        GetDataFrameAnalyticsRequest getRequest = new GetDataFrameAnalyticsRequest(configId1, configId2, configId3)
            .setAllowNoMatch(false)
            .setPageParams(new PageParams(100, 300));

        Request request = MLRequestConverters.getDataFrameAnalytics(getRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + configId1 + "," + configId2 + "," + configId3, request.getEndpoint());
        assertThat(request.getParameters(), allOf(hasEntry("from", "100"), hasEntry("size", "300"), hasEntry("allow_no_match", "false")));
        assertNull(request.getEntity());
    }

    public void testGetDataFrameAnalyticsStats() {
        String configId1 = randomAlphaOfLength(10);
        String configId2 = randomAlphaOfLength(10);
        String configId3 = randomAlphaOfLength(10);
        GetDataFrameAnalyticsStatsRequest getStatsRequest = new GetDataFrameAnalyticsStatsRequest(configId1, configId2, configId3)
            .setAllowNoMatch(false)
            .setPageParams(new PageParams(100, 300));

        Request request = MLRequestConverters.getDataFrameAnalyticsStats(getStatsRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + configId1 + "," + configId2 + "," + configId3 + "/_stats", request.getEndpoint());
        assertThat(request.getParameters(), allOf(hasEntry("from", "100"), hasEntry("size", "300"), hasEntry("allow_no_match", "false")));
        assertNull(request.getEntity());
    }

    public void testStartDataFrameAnalytics() {
        StartDataFrameAnalyticsRequest startRequest = new StartDataFrameAnalyticsRequest(randomAlphaOfLength(10));
        Request request = MLRequestConverters.startDataFrameAnalytics(startRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + startRequest.getId() + "/_start", request.getEndpoint());
        assertNull(request.getEntity());
    }

    public void testStartDataFrameAnalytics_WithTimeout() {
        StartDataFrameAnalyticsRequest startRequest = new StartDataFrameAnalyticsRequest(randomAlphaOfLength(10))
            .setTimeout(TimeValue.timeValueMinutes(1));
        Request request = MLRequestConverters.startDataFrameAnalytics(startRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + startRequest.getId() + "/_start", request.getEndpoint());
        assertThat(request.getParameters(), hasEntry("timeout", "1m"));
        assertNull(request.getEntity());
    }

    public void testStopDataFrameAnalytics() {
        StopDataFrameAnalyticsRequest stopRequest = new StopDataFrameAnalyticsRequest(randomAlphaOfLength(10));
        Request request = MLRequestConverters.stopDataFrameAnalytics(stopRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + stopRequest.getId() + "/_stop", request.getEndpoint());
        assertNull(request.getEntity());
    }

    public void testStopDataFrameAnalytics_WithParams() {
        StopDataFrameAnalyticsRequest stopRequest = new StopDataFrameAnalyticsRequest(randomAlphaOfLength(10))
            .setTimeout(TimeValue.timeValueMinutes(1))
            .setAllowNoMatch(false)
            .setForce(true);
        Request request = MLRequestConverters.stopDataFrameAnalytics(stopRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + stopRequest.getId() + "/_stop", request.getEndpoint());
        assertThat(request.getParameters(), allOf(
            hasEntry("timeout", "1m"),
            hasEntry("allow_no_match", "false"),
            hasEntry("force", "true")));
        assertNull(request.getEntity());
    }

    public void testDeleteDataFrameAnalytics() {
        DeleteDataFrameAnalyticsRequest deleteRequest = new DeleteDataFrameAnalyticsRequest(randomAlphaOfLength(10));
        Request request = MLRequestConverters.deleteDataFrameAnalytics(deleteRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + deleteRequest.getId(), request.getEndpoint());
        assertNull(request.getEntity());
        assertThat(request.getParameters().isEmpty(), is(true));

        deleteRequest = new DeleteDataFrameAnalyticsRequest(randomAlphaOfLength(10));
        deleteRequest.setForce(true);
        request = MLRequestConverters.deleteDataFrameAnalytics(deleteRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + deleteRequest.getId(), request.getEndpoint());
        assertNull(request.getEntity());
        assertThat(request.getParameters().size(), equalTo(1));
        assertEquals(Boolean.toString(true), request.getParameters().get("force"));
    }

    public void testDeleteDataFrameAnalytics_WithTimeout() {
        DeleteDataFrameAnalyticsRequest deleteRequest = new DeleteDataFrameAnalyticsRequest(randomAlphaOfLength(10));
        deleteRequest.setTimeout(TimeValue.timeValueSeconds(10));
        Request request = MLRequestConverters.deleteDataFrameAnalytics(deleteRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/analytics/" + deleteRequest.getId(), request.getEndpoint());
        assertNull(request.getEntity());
        assertThat(request.getParameters().size(), equalTo(1));
        assertEquals(request.getParameters().get("timeout"), "10s");
    }

    public void testEvaluateDataFrame() throws IOException {
        EvaluateDataFrameRequest evaluateRequest = EvaluateDataFrameRequestTests.createRandom();
        Request request = MLRequestConverters.evaluateDataFrame(evaluateRequest);
        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/data_frame/_evaluate", request.getEndpoint());
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            EvaluateDataFrameRequest parsedRequest = EvaluateDataFrameRequest.fromXContent(parser);
            assertThat(parsedRequest, equalTo(evaluateRequest));
        }
    }

    public void testExplainDataFrameAnalytics() throws IOException {
        // Request with config
        {
            ExplainDataFrameAnalyticsRequest estimateRequest = new ExplainDataFrameAnalyticsRequest(randomDataFrameAnalyticsConfig());
            Request request = MLRequestConverters.explainDataFrameAnalytics(estimateRequest);
            assertEquals(HttpPost.METHOD_NAME, request.getMethod());
            assertEquals("/_ml/data_frame/analytics/_explain", request.getEndpoint());
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
                DataFrameAnalyticsConfig parsedConfig = DataFrameAnalyticsConfig.fromXContent(parser);
                assertThat(parsedConfig, equalTo(estimateRequest.getConfig()));
            }
        }
        // Request with id
        {
            ExplainDataFrameAnalyticsRequest estimateRequest = new ExplainDataFrameAnalyticsRequest("foo");
            Request request = MLRequestConverters.explainDataFrameAnalytics(estimateRequest);
            assertEquals(HttpPost.METHOD_NAME, request.getMethod());
            assertEquals("/_ml/data_frame/analytics/foo/_explain", request.getEndpoint());
            assertNull(request.getEntity());
        }
    }

    public void testGetTrainedModels() {
        String modelId1 = randomAlphaOfLength(10);
        String modelId2 = randomAlphaOfLength(10);
        String modelId3 = randomAlphaOfLength(10);
        GetTrainedModelsRequest getRequest = new GetTrainedModelsRequest(modelId1, modelId2, modelId3)
            .setAllowNoMatch(false)
            .setDecompressDefinition(true)
            .setIncludeDefinition(false)
            .setTags("tag1", "tag2")
            .setPageParams(new PageParams(100, 300));

        Request request = MLRequestConverters.getTrainedModels(getRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/inference/" + modelId1 + "," + modelId2 + "," + modelId3, request.getEndpoint());
        assertThat(request.getParameters(),
            allOf(
                hasEntry("from", "100"),
                hasEntry("size", "300"),
                hasEntry("allow_no_match", "false"),
                hasEntry("decompress_definition", "true"),
                hasEntry("tags", "tag1,tag2"),
                hasEntry("include_model_definition", "false")
            ));
        assertNull(request.getEntity());
    }

    public void testGetTrainedModelsStats() {
        String modelId1 = randomAlphaOfLength(10);
        String modelId2 = randomAlphaOfLength(10);
        String modelId3 = randomAlphaOfLength(10);
        GetTrainedModelsStatsRequest getRequest = new GetTrainedModelsStatsRequest(modelId1, modelId2, modelId3)
            .setAllowNoMatch(false)
            .setPageParams(new PageParams(100, 300));

        Request request = MLRequestConverters.getTrainedModelsStats(getRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/inference/" + modelId1 + "," + modelId2 + "," + modelId3 + "/_stats", request.getEndpoint());
        assertThat(request.getParameters(),
            allOf(
                hasEntry("from", "100"),
                hasEntry("size", "300"),
                hasEntry("allow_no_match", "false")
            ));
        assertNull(request.getEntity());
    }

    public void testDeleteTrainedModel() {
        DeleteTrainedModelRequest deleteRequest = new DeleteTrainedModelRequest(randomAlphaOfLength(10));
        Request request = MLRequestConverters.deleteTrainedModel(deleteRequest);
        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/inference/" + deleteRequest.getId(), request.getEndpoint());
        assertNull(request.getEntity());
    }

    public void testPutTrainedModel() throws IOException {
        TrainedModelConfig trainedModelConfig = TrainedModelConfigTests.createTestTrainedModelConfig();
        PutTrainedModelRequest putTrainedModelRequest = new PutTrainedModelRequest(trainedModelConfig);

        Request request = MLRequestConverters.putTrainedModel(putTrainedModelRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/inference/" + trainedModelConfig.getModelId()));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            TrainedModelConfig parsedTrainedModelConfig = TrainedModelConfig.PARSER.apply(parser, null).build();
            assertThat(parsedTrainedModelConfig, equalTo(trainedModelConfig));
        }
    }

    public void testPutFilter() throws IOException {
        MlFilter filter = MlFilterTests.createRandomBuilder("foo").build();
        PutFilterRequest putFilterRequest = new PutFilterRequest(filter);

        Request request = MLRequestConverters.putFilter(putFilterRequest);

        assertEquals(HttpPut.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/filters/foo"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            MlFilter parsedFilter = MlFilter.PARSER.apply(parser, null).build();
            assertThat(parsedFilter, equalTo(filter));
        }
    }

    public void testGetFilter() {
        String id = randomAlphaOfLength(10);
        GetFiltersRequest getFiltersRequest = new GetFiltersRequest();

        getFiltersRequest.setFilterId(id);

        Request request = MLRequestConverters.getFilter(getFiltersRequest);
        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/filters/" + id, request.getEndpoint());
        assertThat(request.getParameters().get(PageParams.FROM.getPreferredName()), is(nullValue()));
        assertThat(request.getParameters().get(PageParams.SIZE.getPreferredName()), is(nullValue()));

        getFiltersRequest.setFrom(1);
        getFiltersRequest.setSize(10);
        request = MLRequestConverters.getFilter(getFiltersRequest);
        assertThat(request.getParameters().get(PageParams.FROM.getPreferredName()), equalTo("1"));
        assertThat(request.getParameters().get(PageParams.SIZE.getPreferredName()), equalTo("10"));
    }

    public void testUpdateFilter() throws IOException {
        String filterId = randomAlphaOfLength(10);
        UpdateFilterRequest updateFilterRequest = new UpdateFilterRequest(filterId);
        updateFilterRequest.setDescription(randomAlphaOfLength(10));
        updateFilterRequest.setRemoveItems(Arrays.asList("item1", "item2"));
        updateFilterRequest.setAddItems(Arrays.asList("item3", "item5"));

        Request request = MLRequestConverters.updateFilter(updateFilterRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/filters/"+filterId+"/_update"));
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, request.getEntity().getContent())) {
            UpdateFilterRequest parsedFilterRequest = UpdateFilterRequest.PARSER.apply(parser, null);
            assertThat(parsedFilterRequest, equalTo(updateFilterRequest));
        }
    }

    public void testDeleteFilter() {
        MlFilter filter = MlFilterTests.createRandomBuilder("foo").build();
        DeleteFilterRequest deleteFilterRequest = new DeleteFilterRequest(filter.getId());

        Request request = MLRequestConverters.deleteFilter(deleteFilterRequest);

        assertEquals(HttpDelete.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/filters/foo"));
        assertNull(request.getEntity());
    }

    public void testMlInfo() {
        MlInfoRequest infoRequest = new MlInfoRequest();

        Request request = MLRequestConverters.mlInfo(infoRequest);

        assertEquals(HttpGet.METHOD_NAME, request.getMethod());
        assertThat(request.getEndpoint(), equalTo("/_ml/info"));
        assertNull(request.getEntity());
    }

    public void testFindFileStructure() throws Exception {

        String sample = randomAlphaOfLength(randomIntBetween(1000, 2000));
        FindFileStructureRequest findFileStructureRequest = FindFileStructureRequestTests.createTestRequestWithoutSample();
        findFileStructureRequest.setSample(sample.getBytes(StandardCharsets.UTF_8));
        Request request = MLRequestConverters.findFileStructure(findFileStructureRequest);

        assertEquals(HttpPost.METHOD_NAME, request.getMethod());
        assertEquals("/_ml/find_file_structure", request.getEndpoint());
        if (findFileStructureRequest.getLinesToSample() != null) {
            assertEquals(findFileStructureRequest.getLinesToSample(), Integer.valueOf(request.getParameters().get("lines_to_sample")));
        } else {
            assertNull(request.getParameters().get("lines_to_sample"));
        }
        if (findFileStructureRequest.getTimeout() != null) {
            assertEquals(findFileStructureRequest.getTimeout().toString(), request.getParameters().get("timeout"));
        } else {
            assertNull(request.getParameters().get("timeout"));
        }
        if (findFileStructureRequest.getCharset() != null) {
            assertEquals(findFileStructureRequest.getCharset(), request.getParameters().get("charset"));
        } else {
            assertNull(request.getParameters().get("charset"));
        }
        if (findFileStructureRequest.getFormat() != null) {
            assertEquals(findFileStructureRequest.getFormat(), FileStructure.Format.fromString(request.getParameters().get("format")));
        } else {
            assertNull(request.getParameters().get("format"));
        }
        if (findFileStructureRequest.getColumnNames() != null) {
            assertEquals(findFileStructureRequest.getColumnNames(),
                Arrays.asList(Strings.splitStringByCommaToArray(request.getParameters().get("column_names"))));
        } else {
            assertNull(request.getParameters().get("column_names"));
        }
        if (findFileStructureRequest.getHasHeaderRow() != null) {
            assertEquals(findFileStructureRequest.getHasHeaderRow(), Boolean.valueOf(request.getParameters().get("has_header_row")));
        } else {
            assertNull(request.getParameters().get("has_header_row"));
        }
        if (findFileStructureRequest.getDelimiter() != null) {
            assertEquals(findFileStructureRequest.getDelimiter().toString(), request.getParameters().get("delimiter"));
        } else {
            assertNull(request.getParameters().get("delimiter"));
        }
        if (findFileStructureRequest.getQuote() != null) {
            assertEquals(findFileStructureRequest.getQuote().toString(), request.getParameters().get("quote"));
        } else {
            assertNull(request.getParameters().get("quote"));
        }
        if (findFileStructureRequest.getShouldTrimFields() != null) {
            assertEquals(findFileStructureRequest.getShouldTrimFields(),
                Boolean.valueOf(request.getParameters().get("should_trim_fields")));
        } else {
            assertNull(request.getParameters().get("should_trim_fields"));
        }
        if (findFileStructureRequest.getGrokPattern() != null) {
            assertEquals(findFileStructureRequest.getGrokPattern(), request.getParameters().get("grok_pattern"));
        } else {
            assertNull(request.getParameters().get("grok_pattern"));
        }
        if (findFileStructureRequest.getTimestampFormat() != null) {
            assertEquals(findFileStructureRequest.getTimestampFormat(), request.getParameters().get("timestamp_format"));
        } else {
            assertNull(request.getParameters().get("timestamp_format"));
        }
        if (findFileStructureRequest.getTimestampField() != null) {
            assertEquals(findFileStructureRequest.getTimestampField(), request.getParameters().get("timestamp_field"));
        } else {
            assertNull(request.getParameters().get("timestamp_field"));
        }
        if (findFileStructureRequest.getExplain() != null) {
            assertEquals(findFileStructureRequest.getExplain(), Boolean.valueOf(request.getParameters().get("explain")));
        } else {
            assertNull(request.getParameters().get("explain"));
        }
        assertEquals(sample, requestEntityToString(request));
    }

    public void testSetUpgradeMode() {
        SetUpgradeModeRequest setUpgradeModeRequest = new SetUpgradeModeRequest(true);

        Request request = MLRequestConverters.setUpgradeMode(setUpgradeModeRequest);
        assertThat(request.getEndpoint(), equalTo("/_ml/set_upgrade_mode"));
        assertThat(request.getMethod(), equalTo(HttpPost.METHOD_NAME));
        assertThat(request.getParameters().get(SetUpgradeModeRequest.ENABLED.getPreferredName()), equalTo(Boolean.toString(true)));
        assertThat(request.getParameters().containsKey(SetUpgradeModeRequest.TIMEOUT.getPreferredName()), is(false));

        setUpgradeModeRequest.setTimeout(TimeValue.timeValueHours(1));
        setUpgradeModeRequest.setEnabled(false);
        request = MLRequestConverters.setUpgradeMode(setUpgradeModeRequest);
        assertThat(request.getParameters().get(SetUpgradeModeRequest.ENABLED.getPreferredName()), equalTo(Boolean.toString(false)));
        assertThat(request.getParameters().get(SetUpgradeModeRequest.TIMEOUT.getPreferredName()), is("1h"));
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        List<NamedXContentRegistry.Entry> namedXContent = new ArrayList<>();
        namedXContent.addAll(new SearchModule(Settings.EMPTY, Collections.emptyList()).getNamedXContents());
        namedXContent.addAll(new MlDataFrameAnalysisNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlEvaluationNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new MlInferenceNamedXContentProvider().getNamedXContentParsers());
        namedXContent.addAll(new AnalysisStatsNamedXContentProvider().getNamedXContentParsers());
        return new NamedXContentRegistry(namedXContent);
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
