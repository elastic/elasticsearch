/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.DeprecationHandler;
import org.elasticsearch.common.xcontent.NamedXContentRegistry;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.elasticsearch.xpack.core.action.util.PageParams;
import org.elasticsearch.xpack.core.ml.action.CloseJobAction;
import org.elasticsearch.xpack.core.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.core.ml.action.FlushJobAction;
import org.elasticsearch.xpack.core.ml.action.ForecastJobAction;
import org.elasticsearch.xpack.core.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.core.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsAction;
import org.elasticsearch.xpack.core.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.core.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.core.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.core.ml.action.OpenJobAction;
import org.elasticsearch.xpack.core.ml.action.PersistJobAction;
import org.elasticsearch.xpack.core.ml.action.PostCalendarEventsAction;
import org.elasticsearch.xpack.core.ml.action.PostDataAction;
import org.elasticsearch.xpack.core.ml.action.PutCalendarAction;
import org.elasticsearch.xpack.core.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.PutJobAction;
import org.elasticsearch.xpack.core.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.core.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateDatafeedAction;
import org.elasticsearch.xpack.core.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.core.ml.annotations.Annotation;
import org.elasticsearch.xpack.core.ml.annotations.AnnotationIndex;
import org.elasticsearch.xpack.core.ml.calendars.Calendar;
import org.elasticsearch.xpack.core.ml.calendars.ScheduledEvent;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.core.ml.datafeed.DatafeedUpdate;
import org.elasticsearch.xpack.core.ml.job.config.Job;
import org.elasticsearch.xpack.core.ml.job.config.JobState;
import org.elasticsearch.xpack.core.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.core.ml.job.persistence.AnomalyDetectorsIndex;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.core.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.core.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.core.ml.job.results.Bucket;
import org.elasticsearch.xpack.core.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.core.ml.job.results.Forecast;
import org.elasticsearch.xpack.core.ml.job.results.ForecastRequestStats;
import org.elasticsearch.xpack.core.ml.job.results.Result;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static org.elasticsearch.common.xcontent.json.JsonXContent.jsonXContent;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.in;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Base class of ML integration tests that use a native autodetect process
 */
abstract class MlNativeAutodetectIntegTestCase extends MlNativeIntegTestCase {

    private List<Job.Builder> jobs = new ArrayList<>();
    private List<DatafeedConfig> datafeeds = new ArrayList<>();

    @Override
    protected void cleanUpResources() {
        cleanUpDatafeeds();
        cleanUpJobs();
    }

    private void cleanUpDatafeeds() {
        for (DatafeedConfig datafeed : datafeeds) {
            try {
                stopDatafeed(datafeed.getId());
            } catch (Exception e) {
                // ignore
            }
            try {
                deleteDatafeed(datafeed.getId());
            } catch (Exception e) {
                // ignore
            }
        }
    }

    private void cleanUpJobs() {
        for (Job.Builder job : jobs) {
            try {
                closeJob(job.getId());
            } catch (Exception e) {
                // ignore
            }
            try {
                deleteJob(job.getId());
            } catch (Exception e) {
                // ignore
            }
        }
    }

    protected void registerJob(Job.Builder job) {
        if (jobs.add(job) == false) {
            throw new IllegalArgumentException("job [" + job.getId() + "] is already registered");
        }
    }

    protected void registerDatafeed(DatafeedConfig datafeed) {
        if (datafeeds.add(datafeed) == false) {
            throw new IllegalArgumentException("datafeed [" + datafeed.getId() + "] is already registered");
        }
    }

    protected List<Job.Builder> getJobs() {
        return jobs;
    }

    protected PutJobAction.Response putJob(Job.Builder job) {
        PutJobAction.Request request = new PutJobAction.Request(job);
        return client().execute(PutJobAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse openJob(String jobId) {
        OpenJobAction.Request request = new OpenJobAction.Request(jobId);
        return client().execute(OpenJobAction.INSTANCE, request).actionGet();
    }

    protected CloseJobAction.Response closeJob(String jobId) {
        return closeJob(jobId, false);
    }

    protected CloseJobAction.Response closeJob(String jobId, boolean force) {
        CloseJobAction.Request request = new CloseJobAction.Request(jobId);
        request.setForce(force);
        return client().execute(CloseJobAction.INSTANCE, request).actionGet();
    }

    protected FlushJobAction.Response flushJob(String jobId, boolean calcInterim) {
        FlushJobAction.Request request = new FlushJobAction.Request(jobId);
        request.setCalcInterim(calcInterim);
        return client().execute(FlushJobAction.INSTANCE, request).actionGet();
    }

    protected PutJobAction.Response updateJob(String jobId, JobUpdate update) {
        UpdateJobAction.Request request = new UpdateJobAction.Request(jobId, update);
        return client().execute(UpdateJobAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse deleteJob(String jobId) {
        DeleteJobAction.Request request = new DeleteJobAction.Request(jobId);
        return client().execute(DeleteJobAction.INSTANCE, request).actionGet();
    }

    protected PutDatafeedAction.Response putDatafeed(DatafeedConfig datafeed) {
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeed);
        return client().execute(PutDatafeedAction.INSTANCE, request).actionGet();
    }

    protected StopDatafeedAction.Response stopDatafeed(String datafeedId) {
        StopDatafeedAction.Request request = new StopDatafeedAction.Request(datafeedId);
        return client().execute(StopDatafeedAction.INSTANCE, request).actionGet();
    }

    protected PutDatafeedAction.Response updateDatafeed(DatafeedUpdate update) {
        UpdateDatafeedAction.Request request = new UpdateDatafeedAction.Request(update);
        return client().execute(UpdateDatafeedAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse deleteDatafeed(String datafeedId) {
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request(datafeedId);
        return client().execute(DeleteDatafeedAction.INSTANCE, request).actionGet();
    }

    protected AcknowledgedResponse startDatafeed(String datafeedId, long start, Long end) {
        StartDatafeedAction.Request request = new StartDatafeedAction.Request(datafeedId, start);
        request.getParams().setEndTime(end);
        return client().execute(StartDatafeedAction.INSTANCE, request).actionGet();
    }

    protected void waitUntilJobIsClosed(String jobId) throws Exception {
        waitUntilJobIsClosed(jobId, TimeValue.timeValueSeconds(30));
    }

    protected void waitUntilJobIsClosed(String jobId, TimeValue waitTime) throws Exception {
        assertBusy(() -> assertThat(getJobStats(jobId).get(0).getState(), equalTo(JobState.CLOSED)),
                waitTime.getMillis(), TimeUnit.MILLISECONDS);
    }

    protected List<Job> getJob(String jobId) {
        GetJobsAction.Request request = new GetJobsAction.Request(jobId);
        return client().execute(GetJobsAction.INSTANCE, request).actionGet().getResponse().results();
    }

    protected List<GetJobsStatsAction.Response.JobStats> getJobStats(String jobId) {
        GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
        GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).actionGet();
        return response.getResponse().results();
    }

    protected List<Bucket> getBuckets(String jobId) {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        return getBuckets(request);
    }

    protected List<Bucket> getBuckets(GetBucketsAction.Request request) {
        GetBucketsAction.Response response = client().execute(GetBucketsAction.INSTANCE, request).actionGet();
        return response.getBuckets().results();
    }

    protected List<AnomalyRecord> getRecords(String jobId) {
        GetRecordsAction.Request request = new GetRecordsAction.Request(jobId);
        return getRecords(request);
    }

    protected List<AnomalyRecord> getRecords(GetRecordsAction.Request request) {
        GetRecordsAction.Response response = client().execute(GetRecordsAction.INSTANCE, request).actionGet();
        return response.getRecords().results();
    }

    protected List<ModelSnapshot> getModelSnapshots(String jobId) {
        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        GetModelSnapshotsAction.Response response = client().execute(GetModelSnapshotsAction.INSTANCE, request).actionGet();
        return response.getPage().results();
    }

    protected RevertModelSnapshotAction.Response revertModelSnapshot(String jobId, String snapshotId, boolean deleteInterveningResults) {
        RevertModelSnapshotAction.Request request = new RevertModelSnapshotAction.Request(jobId, snapshotId);
        request.setDeleteInterveningResults(deleteInterveningResults);
        return client().execute(RevertModelSnapshotAction.INSTANCE, request).actionGet();
    }

    protected List<CategoryDefinition> getCategories(String jobId) {
        GetCategoriesAction.Request getCategoriesRequest =
                new GetCategoriesAction.Request(jobId);
        getCategoriesRequest.setPageParams(new PageParams());
        GetCategoriesAction.Response categoriesResponse = client().execute(GetCategoriesAction.INSTANCE, getCategoriesRequest).actionGet();
        return categoriesResponse.getResult().results();
    }

    protected DataCounts postData(String jobId, String data) {
        logger.debug("Posting data to job [{}]:\n{}", jobId, data);
        PostDataAction.Request request = new PostDataAction.Request(jobId);
        request.setContent(new BytesArray(data), XContentType.JSON);
        return client().execute(PostDataAction.INSTANCE, request).actionGet().getDataCounts();
    }

    protected String forecast(String jobId, TimeValue duration, TimeValue expiresIn) {
        return forecast(jobId, duration, expiresIn, null);
    }

    protected String forecast(String jobId, TimeValue duration, TimeValue expiresIn, Long maxMemory) {
        ForecastJobAction.Request request = new ForecastJobAction.Request(jobId);
        if (duration != null) {
            request.setDuration(duration.getStringRep());
        }
        if (expiresIn != null) {
            request.setExpiresIn(expiresIn.getStringRep());
        }
        if (maxMemory != null) {
            request.setMaxModelMemory(maxMemory);
        }
        return client().execute(ForecastJobAction.INSTANCE, request).actionGet().getForecastId();
    }

    protected void waitForecastToFinish(String jobId, String forecastId) throws Exception {
        waitForecastStatus(jobId, forecastId, ForecastRequestStats.ForecastRequestStatus.FINISHED);
    }

    protected void waitForecastStatus(String jobId,
                                      String forecastId,
                                      ForecastRequestStats.ForecastRequestStatus... status) throws Exception {
        assertBusy(() -> {
            ForecastRequestStats forecastRequestStats = getForecastStats(jobId, forecastId);
            assertThat(forecastRequestStats, is(notNullValue()));
            assertThat(forecastRequestStats.getStatus(), in(status));
        }, 30, TimeUnit.SECONDS);
    }

    protected void assertThatNumberOfAnnotationsIsEqualTo(int expectedNumberOfAnnotations) throws IOException {
        // Refresh the annotations index so that recently indexed annotation docs are visible.
        client().admin().indices().prepareRefresh(AnnotationIndex.INDEX_NAME)
            .setIndicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN)
            .execute()
            .actionGet();

        SearchRequest searchRequest =
            new SearchRequest(AnnotationIndex.READ_ALIAS_NAME).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN_CLOSED_HIDDEN);
        SearchResponse searchResponse = client().search(searchRequest).actionGet();
        List<Annotation> annotations = new ArrayList<>();
        for (SearchHit hit : searchResponse.getHits().getHits()) {
            try (XContentParser parser = createParser(jsonXContent, hit.getSourceRef())) {
                annotations.add(Annotation.fromXContent(parser, null));
            }
        }
        assertThat("Annotations were: " + annotations, annotations, hasSize(expectedNumberOfAnnotations));
    }

    protected ForecastRequestStats getForecastStats(String jobId, String forecastId) {
        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobResultsAliasedName(jobId))
            .setQuery(QueryBuilders.idsQuery().addIds(ForecastRequestStats.documentId(jobId, forecastId)))
            .get();

        if (searchResponse.getHits().getHits().length == 0) {
            return null;
        }

        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        try (XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                    NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                    searchResponse.getHits().getHits()[0].getSourceRef().streamInput())) {
            return ForecastRequestStats.STRICT_PARSER.apply(parser, null);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected List<ForecastRequestStats> getForecastStats() {
        List<ForecastRequestStats> forecastStats = new ArrayList<>();

        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*")
                .setSize(1000)
                .setQuery(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), ForecastRequestStats.RESULT_TYPE_VALUE)))
                .execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            try {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                        NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION, hit.getSourceRef().streamInput());
                forecastStats.add(ForecastRequestStats.STRICT_PARSER.apply(parser, null));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return forecastStats;
    }

    protected long countForecastDocs(String jobId, String forecastId) {
        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*")
                .setQuery(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), Forecast.RESULT_TYPE_VALUE))
                        .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                        .filter(QueryBuilders.termQuery(Forecast.FORECAST_ID.getPreferredName(), forecastId)))
                .execute().actionGet();
        return searchResponse.getHits().getTotalHits().value;
    }

    protected List<Forecast> getForecasts(String jobId, ForecastRequestStats forecastRequestStats) {
        List<Forecast> forecasts = new ArrayList<>();
        SearchResponse searchResponse = client().prepareSearch(AnomalyDetectorsIndex.jobResultsIndexPrefix() + "*")
                .setSize((int) forecastRequestStats.getRecordCount())
                .setQuery(QueryBuilders.boolQuery()
                        .filter(QueryBuilders.termQuery(Result.RESULT_TYPE.getPreferredName(), Forecast.RESULT_TYPE_VALUE))
                        .filter(QueryBuilders.termQuery(Job.ID.getPreferredName(), jobId))
                        .filter(QueryBuilders.termQuery(Forecast.FORECAST_ID.getPreferredName(), forecastRequestStats.getForecastId())))
                .addSort(SortBuilders.fieldSort(Result.TIMESTAMP.getPreferredName()).order(SortOrder.ASC))
                .execute().actionGet();
        SearchHits hits = searchResponse.getHits();
        for (SearchHit hit : hits) {
            try {
                XContentParser parser = XContentFactory.xContent(XContentType.JSON).createParser(
                        NamedXContentRegistry.EMPTY, DeprecationHandler.THROW_UNSUPPORTED_OPERATION,
                        hit.getSourceRef().streamInput());
                forecasts.add(Forecast.STRICT_PARSER.apply(parser, null));
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        }
        return forecasts;
    }

    protected PutCalendarAction.Response putCalendar(String calendarId, List<String> jobIds, String description) {
        PutCalendarAction.Request request = new PutCalendarAction.Request(new Calendar(calendarId, jobIds, description));
        return client().execute(PutCalendarAction.INSTANCE, request).actionGet();
    }

    protected PostCalendarEventsAction.Response postScheduledEvents(String calendarId, List<ScheduledEvent> events) {
        PostCalendarEventsAction.Request request = new PostCalendarEventsAction.Request(calendarId, events);
        return client().execute(PostCalendarEventsAction.INSTANCE, request).actionGet();
    }

    protected PersistJobAction.Response persistJob(String jobId) {
        PersistJobAction.Request request = new PersistJobAction.Request(jobId);
        return client().execute(PersistJobAction.INSTANCE, request).actionGet();
    }

    protected List<String> generateData(long timestamp, TimeValue bucketSpan, int bucketCount,
                                      Function<Integer, Integer> timeToCountFunction) throws IOException {
        List<String> data = new ArrayList<>();
        long now = timestamp;
        for (int bucketIndex = 0; bucketIndex < bucketCount; bucketIndex++) {
            for (int count = 0; count < timeToCountFunction.apply(bucketIndex); count++) {
                Map<String, Object> record = new HashMap<>();
                record.put("time", now);
                data.add(createJsonRecord(record));
            }
            now += bucketSpan.getMillis();
        }
        return data;
    }

    protected static String createJsonRecord(Map<String, Object> keyValueMap) throws IOException {
        return Strings.toString(JsonXContent.contentBuilder().map(keyValueMap)) + "\n";
    }
}
