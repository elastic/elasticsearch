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
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.DeleteCalendarEventRequest;
import org.elasticsearch.client.ml.DeleteCalendarJobRequest;
import org.elasticsearch.client.ml.DeleteCalendarRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataRequest;
import org.elasticsearch.client.ml.DeleteExpiredDataResponse;
import org.elasticsearch.client.ml.DeleteFilterRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.DeleteJobResponse;
import org.elasticsearch.client.ml.DeleteModelSnapshotRequest;
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
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutDatafeedResponse;
import org.elasticsearch.client.ml.PutFilterRequest;
import org.elasticsearch.client.ml.PutFilterResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.RevertModelSnapshotRequest;
import org.elasticsearch.client.ml.RevertModelSnapshotResponse;
import org.elasticsearch.client.ml.StartDatafeedRequest;
import org.elasticsearch.client.ml.StartDatafeedResponse;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.StopDatafeedResponse;
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
import org.elasticsearch.client.ml.filestructurefinder.FileStructure;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobState;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.config.MlFilter;
import org.elasticsearch.client.ml.job.process.ModelSnapshot;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.client.ml.job.util.PageParams;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItem;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

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

        // Test when allow_no_jobs is false
        final GetJobStatsRequest erroredRequest = new GetJobStatsRequest("jobs-that-do-not-exist*");
        erroredRequest.setAllowNoJobs(false);
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
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
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

        // Test get missing pattern with allow_no_datafeeds set to true
        {
            GetDatafeedRequest request = new GetDatafeedRequest("missing-*");

            GetDatafeedResponse response = execute(request, machineLearningClient::getDatafeed, machineLearningClient::getDatafeedAsync);

            assertThat(response.count(), equalTo(0L));
        }

        // Test get missing pattern with allow_no_datafeeds set to false
        {
            GetDatafeedRequest request = new GetDatafeedRequest("missing-*");
            request.setAllowNoDatafeeds(false);

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
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping("_doc", "timestamp", "type=date", "total", "type=long");
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        long now = (System.currentTimeMillis()/1000)*1000;
        long thePast = now - 60000;
        int i = 0;
        long pastCopy = thePast;
        while(pastCopy < now) {
            IndexRequest doc = new IndexRequest();
            doc.index(indexName);
            doc.type("_doc");
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
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping("_doc", "timestamp", "type=date", "total", "type=long");
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);

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
            request.setAllowNoDatafeeds(false);
            StopDatafeedResponse stopDatafeedResponse = execute(request,
                machineLearningClient::stopDatafeed,
                machineLearningClient::stopDatafeedAsync);
            assertTrue(stopDatafeedResponse.isStopped());
        }
        {
            StopDatafeedRequest request = new StopDatafeedRequest(datafeedId2, datafeedId3);
            request.setAllowNoDatafeeds(false);
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
            request.setAllowNoDatafeeds(false);
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
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping("_doc", "timestamp", "type=date", "total", "type=long");
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);

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

        // Test when allow_no_jobs is false
        final GetDatafeedStatsRequest erroredRequest = new GetDatafeedStatsRequest("datafeeds-that-do-not-exist*");
        erroredRequest.setAllowNoDatafeeds(false);
        ElasticsearchStatusException exception = expectThrows(ElasticsearchStatusException.class,
            () -> execute(erroredRequest, machineLearningClient::getDatafeedStats, machineLearningClient::getDatafeedStatsAsync));
        assertThat(exception.status().getStatus(), equalTo(404));
    }

    public void testPreviewDatafeed() throws Exception {
        String jobId = "test-preview-datafeed";
        String indexName = "preview_data_1";

        // Set up the index and docs
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexName);
        createIndexRequest.mapping("_doc", "timestamp", "type=date", "total", "type=long");
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
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
            doc.type("_doc");
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
        String indexId = jobId + "-data";
        // Set up the index and docs
        CreateIndexRequest createIndexRequest = new CreateIndexRequest(indexId);
        createIndexRequest.mapping("_doc", "timestamp", "type=date,format=epoch_millis", "total", "type=long");
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
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
                IndexRequest indexRequest = new IndexRequest(indexId, "_doc");
                indexRequest.source(XContentType.JSON, "timestamp", timestamp, "total", randomInt(1000));
                bulk.add(indexRequest);
            }
        }
        highLevelClient().bulk(bulk, RequestOptions.DEFAULT);

        {
            // Index a randomly named unused state document
            String docId = "non_existing_job_" + randomFrom("model_state_1234567#1", "quantiles", "categorizer_state#1");
            IndexRequest indexRequest = new IndexRequest(".ml-state", "_doc", docId);
            indexRequest.source(Collections.emptyMap(), XContentType.JSON);
            indexRequest.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
            highLevelClient().index(indexRequest, RequestOptions.DEFAULT);
        }

        Job job = buildJobForExpiredDataTests(jobId);
        putJob(job);
        openJob(job);
        String datafeedId = createAndPutDatafeed(jobId, indexId);

        startDatafeed(datafeedId, String.valueOf(0), String.valueOf(nowMillis - TimeValue.timeValueHours(24).getMillis()));

        waitForJobToClose(jobId);

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
        awaitBusy(() -> false, 1, TimeUnit.SECONDS);

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
            // Verify .ml-state contains the expected unused state document
            Iterable<SearchHit> hits = searchAll(".ml-state");
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

        awaitBusy(() -> false, 1, TimeUnit.SECONDS);

        GetModelSnapshotsRequest getModelSnapshotsRequest1 = new GetModelSnapshotsRequest(jobId);
        GetModelSnapshotsResponse getModelSnapshotsResponse1 = execute(getModelSnapshotsRequest1, machineLearningClient::getModelSnapshots,
            machineLearningClient::getModelSnapshotsAsync);

        assertEquals(1L, getModelSnapshotsResponse1.count());

        assertFalse(forecastExists(jobId, forecastId));

        {
            // Verify .ml-state doesn't contain unused state documents
            Iterable<SearchHit> hits = searchAll(".ml-state");
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
            assertThat(getFiltersResponse.count(), equalTo(2L));
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
        AnalysisConfig.Builder configBuilder = new AnalysisConfig.Builder(Arrays.asList(detector));
        //should not be random, see:https://github.com/elastic/ml-cpp/issues/208
        configBuilder.setBucketSpan(new TimeValue(1, TimeUnit.HOURS));
        builder.setAnalysisConfig(configBuilder);

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
        UpdateRequest updateSnapshotRequest = new UpdateRequest(".ml-anomalies-" + jobId, "_doc", documentId);
        updateSnapshotRequest.doc(snapshotUpdate.getBytes(StandardCharsets.UTF_8), XContentType.JSON);
        highLevelClient().update(updateSnapshotRequest, RequestOptions.DEFAULT);

        // Wait a second to ensure subsequent model snapshots will have a different ID (it depends on epoch seconds)
        awaitBusy(() -> false, 1, TimeUnit.SECONDS);
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

        IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "_doc", documentId);
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
            IndexRequest indexRequest = new IndexRequest(".ml-anomalies-shared", "_doc", documentId);
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
}
