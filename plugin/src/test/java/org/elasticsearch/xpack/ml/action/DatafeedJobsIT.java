/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.action;

import org.elasticsearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.elasticsearch.action.admin.cluster.node.hotthreads.NodesHotThreadsResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.network.NetworkModule;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.search.SearchModule;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.XPackSettings;
import org.elasticsearch.xpack.ml.MlMetadata;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.config.JobState;
import org.elasticsearch.xpack.ml.job.process.autodetect.state.DataCounts;
import org.elasticsearch.xpack.ml.support.BaseMlIntegTestCase;
import org.elasticsearch.xpack.persistent.PersistentActionCoordinator;
import org.elasticsearch.xpack.persistent.PersistentActionRequest;
import org.elasticsearch.xpack.persistent.PersistentActionResponse;
import org.elasticsearch.xpack.persistent.PersistentTasks;
import org.elasticsearch.xpack.security.Security;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.hamcrest.Matchers.equalTo;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.SUITE, numDataNodes = 0, numClientNodes = 0,
        transportClientRatio = 0, supportsDedicatedMasters = false)
public class DatafeedJobsIT extends BaseMlIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return false;
    }

    @Override
    protected Settings externalClusterClientSettings() {
        Settings.Builder settings = Settings.builder()
                .put(Security.USER_SETTING.getKey(), "elastic:changeme");
        settings.put(XPackSettings.MACHINE_LEARNING_ENABLED.getKey(), true);
        settings.put(NetworkModule.TRANSPORT_TYPE_KEY, Security.NAME4);
        return settings.build();
    }

    public void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>(ClusterModule.getNamedWriteables());
            entries.addAll(new SearchModule(Settings.EMPTY, true, Collections.emptyList()).getNamedWriteables());
            entries.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new));
            entries.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, PersistentTasks.TYPE, PersistentTasks::new));
            entries.add(new NamedWriteableRegistry.Entry(PersistentActionRequest.class, StartDatafeedAction.NAME,
                    StartDatafeedAction.Request::new));
            entries.add(new NamedWriteableRegistry.Entry(PersistentActionRequest.class, OpenJobAction.NAME, OpenJobAction.Request::new));
            entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, PersistentActionCoordinator.Status.NAME,
                    PersistentActionCoordinator.Status::new));
            entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, JobState.NAME, JobState::fromStream));
            entries.add(new NamedWriteableRegistry.Entry(Task.Status.class, DatafeedState.NAME, DatafeedState::fromStream));
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

    public void testLookbackOnly() throws Exception {
        client().admin().indices().prepareCreate("data-1")
        .addMapping("type", "time", "type=date")
        .get();
        long numDocs = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long oneWeekAgo = now - 604800000;
        long twoWeeksAgo = oneWeekAgo - 604800000;
        indexDocs("data-1", numDocs, twoWeeksAgo, oneWeekAgo);

        client().admin().indices().prepareCreate("data-2")
                .addMapping("type", "time", "type=date")
                .get();
        long numDocs2 = randomIntBetween(32, 2048);
        indexDocs("data-2", numDocs2, oneWeekAgo, now);

        Job.Builder job = createScheduledJob("lookback-job");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data-*"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(datafeedConfig);
        PutDatafeedAction.Response putDatafeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        assertTrue(putDatafeedResponse.isAcknowledged());

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedConfig.getId(), 0L);
        startDatafeedRequest.setEndTime(now);
        client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));

            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

    public void testRealtime() throws Exception {
        client().admin().indices().prepareCreate("data")
        .addMapping("type", "time", "type=date")
        .get();
        long numDocs1 = randomIntBetween(32, 2048);
        long now = System.currentTimeMillis();
        long lastWeek = now - 604800000;
        indexDocs("data", numDocs1, lastWeek, now);

        Job.Builder job = createScheduledJob("realtime-job");
        PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build());
        PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
        assertTrue(putJobResponse.isAcknowledged());
        client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()));
        assertBusy(() -> {
            GetJobsStatsAction.Response statsResponse =
                    client().execute(GetJobsStatsAction.INSTANCE, new GetJobsStatsAction.Request(job.getId())).actionGet();
            assertEquals(statsResponse.getResponse().results().get(0).getState(), JobState.OPENED);
        });

        DatafeedConfig datafeedConfig = createDatafeed(job.getId() + "-datafeed", job.getId(), Collections.singletonList("data"));
        PutDatafeedAction.Request putDatafeedRequest = new PutDatafeedAction.Request(datafeedConfig);
        PutDatafeedAction.Response putDatafeedResponse = client().execute(PutDatafeedAction.INSTANCE, putDatafeedRequest).get();
        assertTrue(putDatafeedResponse.isAcknowledged());

        StartDatafeedAction.Request startDatafeedRequest = new StartDatafeedAction.Request(datafeedConfig.getId(), 0L);
        PersistentActionResponse startDatafeedResponse =
                client().execute(StartDatafeedAction.INSTANCE, startDatafeedRequest).get();
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        });

        long numDocs2 = randomIntBetween(2, 64);
        now = System.currentTimeMillis();
        indexDocs("data", numDocs2, now + 5000, now + 6000);
        assertBusy(() -> {
            DataCounts dataCounts = getDataCounts(job.getId());
            assertThat(dataCounts.getProcessedRecordCount(), equalTo(numDocs1 + numDocs2));
            assertThat(dataCounts.getOutOfOrderTimeStampCount(), equalTo(0L));
        }, 30, TimeUnit.SECONDS);

        StopDatafeedAction.Request stopDatafeedRequest = new StopDatafeedAction.Request(datafeedConfig.getId());
        try {
            StopDatafeedAction.Response stopJobResponse = client().execute(StopDatafeedAction.INSTANCE, stopDatafeedRequest).get();
            assertTrue(stopJobResponse.isStopped());
        } catch (Exception e) {
            NodesHotThreadsResponse nodesHotThreadsResponse = client().admin().cluster().prepareNodesHotThreads().get();
            int i = 0;
            for (NodeHotThreads nodeHotThreads : nodesHotThreadsResponse.getNodes()) {
                logger.info(i++ + ":\n" +nodeHotThreads.getHotThreads());
            }
            throw e;
        }
        assertBusy(() -> {
            GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedConfig.getId());
            GetDatafeedsStatsAction.Response response = client().execute(GetDatafeedsStatsAction.INSTANCE, request).actionGet();
            assertThat(response.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
        });
    }

}
