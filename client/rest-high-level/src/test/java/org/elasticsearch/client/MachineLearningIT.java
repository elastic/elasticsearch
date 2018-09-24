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
import org.elasticsearch.client.ml.CloseJobRequest;
import org.elasticsearch.client.ml.CloseJobResponse;
import org.elasticsearch.client.ml.DeleteCalendarRequest;
import org.elasticsearch.client.ml.DeleteDatafeedRequest;
import org.elasticsearch.client.ml.DeleteForecastRequest;
import org.elasticsearch.client.ml.DeleteJobRequest;
import org.elasticsearch.client.ml.FlushJobRequest;
import org.elasticsearch.client.ml.FlushJobResponse;
import org.elasticsearch.client.ml.ForecastJobRequest;
import org.elasticsearch.client.ml.ForecastJobResponse;
import org.elasticsearch.client.ml.GetCalendarsRequest;
import org.elasticsearch.client.ml.GetCalendarsResponse;
import org.elasticsearch.client.ml.GetDatafeedRequest;
import org.elasticsearch.client.ml.GetDatafeedResponse;
import org.elasticsearch.client.ml.GetJobRequest;
import org.elasticsearch.client.ml.GetJobResponse;
import org.elasticsearch.client.ml.GetJobStatsRequest;
import org.elasticsearch.client.ml.GetJobStatsResponse;
import org.elasticsearch.client.ml.OpenJobRequest;
import org.elasticsearch.client.ml.OpenJobResponse;
import org.elasticsearch.client.ml.PostDataRequest;
import org.elasticsearch.client.ml.PostDataResponse;
import org.elasticsearch.client.ml.PutCalendarRequest;
import org.elasticsearch.client.ml.PutCalendarResponse;
import org.elasticsearch.client.ml.PutDatafeedRequest;
import org.elasticsearch.client.ml.PutDatafeedResponse;
import org.elasticsearch.client.ml.PutJobRequest;
import org.elasticsearch.client.ml.PutJobResponse;
import org.elasticsearch.client.ml.StartDatafeedRequest;
import org.elasticsearch.client.ml.StartDatafeedResponse;
import org.elasticsearch.client.ml.StopDatafeedRequest;
import org.elasticsearch.client.ml.StopDatafeedResponse;
import org.elasticsearch.client.ml.UpdateJobRequest;
import org.elasticsearch.client.ml.calendars.Calendar;
import org.elasticsearch.client.ml.calendars.CalendarTests;
import org.elasticsearch.client.ml.datafeed.DatafeedConfig;
import org.elasticsearch.client.ml.job.config.AnalysisConfig;
import org.elasticsearch.client.ml.job.config.DataDescription;
import org.elasticsearch.client.ml.job.config.Detector;
import org.elasticsearch.client.ml.job.config.Job;
import org.elasticsearch.client.ml.job.config.JobState;
import org.elasticsearch.client.ml.job.config.JobUpdate;
import org.elasticsearch.client.ml.job.stats.JobStats;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.rest.RestStatus;
import org.junit.After;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;

public class MachineLearningIT extends ESRestHighLevelClientTestCase {

    @After
    public void cleanUp() throws IOException {
        new MlRestTestStateCleaner(logger, client()).clearMlMetadata();
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

    public void testDeleteJob() throws Exception {
        String jobId = randomValidJobId();
        Job job = buildJob(jobId);
        MachineLearningClient machineLearningClient = highLevelClient().machineLearning();
        machineLearningClient.putJob(new PutJobRequest(job), RequestOptions.DEFAULT);

        AcknowledgedResponse response = execute(new DeleteJobRequest(jobId),
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
        createIndexRequest.mapping("doc", "timestamp", "type=date", "total", "type=long");
        highLevelClient().indices().create(createIndexRequest, RequestOptions.DEFAULT);
        BulkRequest bulk = new BulkRequest();
        bulk.setRefreshPolicy(WriteRequest.RefreshPolicy.IMMEDIATE);
        long now = System.currentTimeMillis();
        long oneDayAgo = now - 86400000;
        int i = 0;
        long dayAgoCopy = oneDayAgo;
        while(dayAgoCopy < now) {
            IndexRequest doc = new IndexRequest();
            doc.index(indexName);
            doc.type("doc");
            doc.id("id" + i);
            doc.source("{\"total\":" +randomInt(1000) + ",\"timestamp\":"+ dayAgoCopy +"}", XContentType.JSON);
            bulk.add(doc);
            dayAgoCopy += 1000000;
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
            .setTypes(Arrays.asList("doc"))
            .setFrequency(TimeValue.timeValueSeconds(1)).build();
        machineLearningClient.putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);


        StartDatafeedRequest startDatafeedRequest = new StartDatafeedRequest(datafeedId);
        startDatafeedRequest.setStart(String.valueOf(oneDayAgo));
        // Should only process two documents
        startDatafeedRequest.setEnd(String.valueOf(oneDayAgo + 2000000));
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
        createIndexRequest.mapping("doc", "timestamp", "type=date", "total", "type=long");
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

    private String createAndPutDatafeed(String jobId, String indexName) throws IOException {
        String datafeedId = jobId + "-feed";
        DatafeedConfig datafeed = DatafeedConfig.builder(datafeedId, jobId)
            .setIndices(indexName)
            .setQueryDelay(TimeValue.timeValueSeconds(1))
            .setTypes(Arrays.asList("doc"))
            .setFrequency(TimeValue.timeValueSeconds(1)).build();
        highLevelClient().machineLearning().putDatafeed(new PutDatafeedRequest(datafeed), RequestOptions.DEFAULT);
        return datafeedId;
    }
}
