/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.security.Security;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;

/**
 * Base class of ML integration tests that use a native autodetect process
 */
abstract class MlNativeAutodetectIntegTestCase extends SecurityIntegTestCase {

    private List<Job.Builder> jobs = new ArrayList<>();
    private List<DatafeedConfig> datafeeds = new ArrayList<>();

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder builder = Settings.builder();
        builder.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
        builder.put(Security.USER_SETTING.getKey(), "elastic:changeme");
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true);
        return builder.build();
    }

    protected void cleanUp() {
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

    protected void putJob(Job.Builder job) throws Exception {
        PutJobAction.Request request = new PutJobAction.Request(job);
        client().execute(PutJobAction.INSTANCE, request).get();
    }

    protected void openJob(String jobId) throws Exception {
        OpenJobAction.Request request = new OpenJobAction.Request(jobId);
        client().execute(OpenJobAction.INSTANCE, request).get();
    }

    protected void closeJob(String jobId) throws Exception {
        CloseJobAction.Request request = new CloseJobAction.Request(jobId);
        client().execute(CloseJobAction.INSTANCE, request).get();
    }

    protected void deleteJob(String jobId) throws Exception {
        DeleteJobAction.Request request = new DeleteJobAction.Request(jobId);
        client().execute(DeleteJobAction.INSTANCE, request).get();
    }

    protected void putDatafeed(DatafeedConfig datafeed) throws Exception {
        PutDatafeedAction.Request request = new PutDatafeedAction.Request(datafeed);
        client().execute(PutDatafeedAction.INSTANCE, request).get();
    }

    protected void stopDatafeed(String datafeedId) throws Exception {
        StopDatafeedAction.Request request = new StopDatafeedAction.Request(datafeedId);
        client().execute(StopDatafeedAction.INSTANCE, request).get();
    }

    protected void deleteDatafeed(String datafeedId) throws Exception {
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request(datafeedId);
        client().execute(DeleteDatafeedAction.INSTANCE, request).get();
    }

    protected void startDatafeed(String datafeedId, long start, long end) throws Exception {
        StartDatafeedAction.Request request = new StartDatafeedAction.Request(datafeedId, start);
        request.setEndTime(end);
        client().execute(StartDatafeedAction.INSTANCE, request).get();
    }

    protected void waitUntilJobIsClosed(String jobId) throws Exception {
        assertBusy(() -> {
            try {
                GetJobsStatsAction.Request request = new GetJobsStatsAction.Request(jobId);
                GetJobsStatsAction.Response response = client().execute(GetJobsStatsAction.INSTANCE, request).get();
                assertThat(response.getResponse().results().get(0).getState(), equalTo(JobState.CLOSED));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
    }

    protected List<Bucket> getBuckets(String jobId) throws Exception {
        GetBucketsAction.Request request = new GetBucketsAction.Request(jobId);
        GetBucketsAction.Response response = client().execute(GetBucketsAction.INSTANCE, request).get();
        return response.getBuckets().results();
    }

    protected List<AnomalyRecord> getRecords(String jobId) throws Exception {
        GetRecordsAction.Request request = new GetRecordsAction.Request(jobId);
        GetRecordsAction.Response response = client().execute(GetRecordsAction.INSTANCE, request).get();
        return response.getRecords().results();
    }

    protected List<ModelSnapshot> getModelSnapshots(String jobId) throws Exception {
        GetModelSnapshotsAction.Request request = new GetModelSnapshotsAction.Request(jobId, null);
        GetModelSnapshotsAction.Response response = client().execute(GetModelSnapshotsAction.INSTANCE, request).get();
        return response.getPage().results();
    }

    protected List<CategoryDefinition> getCategories(String jobId) throws Exception {
        GetCategoriesAction.Request getCategoriesRequest =
                new GetCategoriesAction.Request(jobId);
        getCategoriesRequest.setPageParams(new PageParams());
        GetCategoriesAction.Response categoriesResponse = client().execute(
                GetCategoriesAction.INSTANCE, getCategoriesRequest).get();
        return categoriesResponse.getResult().results();
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        // this method in ESIntegTestCase is not plugin-friendly - it does not account for plugin NamedWritableRegistries
    }
}
