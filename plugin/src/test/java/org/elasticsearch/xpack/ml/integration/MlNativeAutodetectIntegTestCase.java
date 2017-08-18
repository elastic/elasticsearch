/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.action.admin.cluster.node.tasks.list.ListTasksRequest;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.elasticsearch.common.xcontent.json.JsonXContent;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.test.SecuritySettingsSource;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.FlushJobAction;
import org.elasticsearch.xpack.ml.action.GetBucketsAction;
import org.elasticsearch.xpack.ml.action.GetCategoriesAction;
import org.elasticsearch.xpack.ml.action.GetJobsAction;
import org.elasticsearch.xpack.ml.action.GetJobsStatsAction;
import org.elasticsearch.xpack.ml.action.GetModelSnapshotsAction;
import org.elasticsearch.xpack.ml.action.GetRecordsAction;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PostDataAction;
import org.elasticsearch.xpack.ml.action.PutDatafeedAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.RevertModelSnapshotAction;
import org.elasticsearch.xpack.ml.action.StartDatafeedAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.action.UpdateJobAction;
import org.elasticsearch.xpack.ml.action.util.PageParams;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.config.JobTaskStatus;
import org.elasticsearch.xpack.ml.job.config.JobUpdate;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.ModelSnapshot;
import org.elasticsearch.xpack.ml.job.results.AnomalyRecord;
import org.elasticsearch.xpack.ml.job.results.Bucket;
import org.elasticsearch.xpack.ml.job.results.CategoryDefinition;
import org.elasticsearch.xpack.persistent.PersistentTaskParams;
import org.elasticsearch.xpack.persistent.PersistentTasksCustomMetaData;
import org.elasticsearch.xpack.persistent.PersistentTasksNodeService;
import org.elasticsearch.xpack.security.Security;
import org.elasticsearch.xpack.security.authc.TokenMetaData;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
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
        builder.put(Security.USER_SETTING.getKey(), "x_pack_rest_user:" + SecuritySettingsSource.TEST_PASSWORD_SECURE_STRING);
        builder.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true);
        return builder.build();
    }

    protected void cleanUp() {
        cleanUpDatafeeds();
        cleanUpJobs();
        waitForPendingTasks();
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

    private void waitForPendingTasks() {
        ListTasksRequest listTasksRequest = new ListTasksRequest();
        listTasksRequest.setWaitForCompletion(true);
        listTasksRequest.setDetailed(true);
        listTasksRequest.setTimeout(TimeValue.timeValueSeconds(10));
        try {
            admin().cluster().listTasks(listTasksRequest).get();
        } catch (Exception e) {
            throw new AssertionError("Failed to wait for pending tasks to complete", e);
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

    protected OpenJobAction.Response openJob(String jobId) {
        OpenJobAction.Request request = new OpenJobAction.Request(jobId);
        return client().execute(OpenJobAction.INSTANCE, request).actionGet();
    }

    protected CloseJobAction.Response closeJob(String jobId) {
        CloseJobAction.Request request = new CloseJobAction.Request(jobId);
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

    protected DeleteJobAction.Response deleteJob(String jobId) {
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

    protected DeleteDatafeedAction.Response deleteDatafeed(String datafeedId) {
        DeleteDatafeedAction.Request request = new DeleteDatafeedAction.Request(datafeedId);
        return client().execute(DeleteDatafeedAction.INSTANCE, request).actionGet();
    }

    protected StartDatafeedAction.Response startDatafeed(String datafeedId, long start, Long end) {
        StartDatafeedAction.Request request = new StartDatafeedAction.Request(datafeedId, start);
        request.getParams().setEndTime(end);
        return client().execute(StartDatafeedAction.INSTANCE, request).actionGet();
    }

    protected void waitUntilJobIsClosed(String jobId) throws Exception {
        assertBusy(() -> assertThat(getJobStats(jobId).get(0).getState(), equalTo(JobState.CLOSED)), 30, TimeUnit.SECONDS);
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

    protected RevertModelSnapshotAction.Response revertModelSnapshot(String jobId, String snapshotId) {
        RevertModelSnapshotAction.Request request = new RevertModelSnapshotAction.Request(jobId, snapshotId);
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

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedWriteables());
            entries.addAll(new SearchModule(Settings.EMPTY, true, Collections.emptyList()).getNamedWriteables());
            entries.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new));
            entries.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, PersistentTasksCustomMetaData.TYPE,
                    PersistentTasksCustomMetaData::new));
            entries.add(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, StartDatafeedAction.TASK_NAME,
                    StartDatafeedAction.DatafeedParams::new));
            entries.add(new NamedWriteableRegistry.Entry(PersistentTaskParams.class, OpenJobAction.TASK_NAME,
                    OpenJobAction.JobParams::new));
            entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, PersistentTasksNodeService.Status.NAME,
                    PersistentTasksNodeService.Status::new));
            entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, JobTaskStatus.NAME, JobTaskStatus::new));
            entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, DatafeedState.NAME, DatafeedState::fromStream));
            entries.add(new NamedWriteableRegistry.Entry(ClusterState.Custom.class, TokenMetaData.TYPE, TokenMetaData::new));
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(entries);
            ClusterState masterClusterState = client().admin().cluster().prepareState().all().get().getState();
            byte[] masterClusterStateBytes = ClusterState.Builder.toBytes(masterClusterState);
            // remove local node reference
            masterClusterState = ClusterState.Builder.fromBytes(masterClusterStateBytes, null, namedWriteableRegistry);
            Map<String, Object> masterStateMap = convertToMap(masterClusterState);
            int masterClusterStateSize = ClusterState.Builder.toBytes(masterClusterState).length;
            String masterId = masterClusterState.nodes().getMasterNodeId();
            for (Client client : cluster().getClients()) {
                ClusterState localClusterState = client.admin().cluster().prepareState().all().setLocal(true).get().getState();
                byte[] localClusterStateBytes = ClusterState.Builder.toBytes(localClusterState);
                // remove local node reference
                localClusterState = ClusterState.Builder.fromBytes(localClusterStateBytes, null, namedWriteableRegistry);
                final Map<String, Object> localStateMap = convertToMap(localClusterState);
                final int localClusterStateSize = ClusterState.Builder.toBytes(localClusterState).length;
                // Check that the non-master node has the same version of the cluster state as the master and
                // that the master node matches the master (otherwise there is no requirement for the cluster state to match)
                if (masterClusterState.version() == localClusterState.version() &&
                        masterId.equals(localClusterState.nodes().getMasterNodeId())) {
                    try {
                        assertEquals("clusterstate UUID does not match", masterClusterState.stateUUID(), localClusterState.stateUUID());
                        // We cannot compare serialization bytes since serialization order of maps is not guaranteed
                        // but we can compare serialization sizes - they should be the same
                        assertEquals("clusterstate size does not match", masterClusterStateSize, localClusterStateSize);
                        // Compare JSON serialization
                        assertNull("clusterstate JSON serialization does not match",
                                differenceBetweenMapsIgnoringArrayOrder(masterStateMap, localStateMap));
                    } catch (AssertionError error) {
                        logger.error("Cluster state from master:\n{}\nLocal cluster state:\n{}",
                                masterClusterState.toString(), localClusterState.toString());
                        throw error;
                    }
                }
            }
        }
    }

    protected static String createJsonRecord(Map<String, Object> keyValueMap) throws IOException {
        return JsonXContent.contentBuilder().map(keyValueMap).string() + "\n";
    }
}
