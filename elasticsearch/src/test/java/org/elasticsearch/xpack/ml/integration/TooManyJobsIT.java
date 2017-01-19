/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.integration;

import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.ClusterModule;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.OpenJobAction;
import org.elasticsearch.xpack.ml.action.PutJobAction;
import org.elasticsearch.xpack.ml.action.DatafeedJobsIT;
import org.elasticsearch.xpack.ml.job.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.DataDescription;
import org.elasticsearch.xpack.ml.job.Detector;
import org.elasticsearch.xpack.ml.job.Job;
import org.elasticsearch.xpack.ml.job.manager.AutodetectProcessManager;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.junit.After;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.elasticsearch.test.XContentTestUtils.convertToMap;
import static org.elasticsearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;

@ESIntegTestCase.ClusterScope(numDataNodes = 1)
public class TooManyJobsIT extends ESIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(MlPlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> transportClientPlugins() {
        return nodePlugins();
    }

    @After
    public void clearMlMetadata() throws Exception {
        DatafeedJobsIT.clearMlMetadata(client());
    }

    public void testCannotStartTooManyAnalyticalProcesses() throws Exception {
        int maxRunningJobsPerNode = AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getDefault(Settings.EMPTY);
        logger.info("[{}] is [{}]", AutodetectProcessManager.MAX_RUNNING_JOBS_PER_NODE.getKey(), maxRunningJobsPerNode);
        for (int i = 1; i <= (maxRunningJobsPerNode + 1); i++) {
            Job.Builder job = createJob(Integer.toString(i));
            PutJobAction.Request putJobRequest = new PutJobAction.Request(job.build(true, job.getId()));
            PutJobAction.Response putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
            assertTrue(putJobResponse.isAcknowledged());

            try {
                OpenJobAction.Request openJobRequest = new OpenJobAction.Request(job.getId());
                openJobRequest.setOpenTimeout(TimeValue.timeValueSeconds(10));
                OpenJobAction.Response openJobResponse = client().execute(OpenJobAction.INSTANCE, openJobRequest)
                        .get();
                assertTrue(openJobResponse.isAcknowledged());
                logger.info("Opened {}th job", i);
            } catch (Exception e) {
                Throwable cause = ExceptionsHelper.unwrapCause(e.getCause());
                if (ElasticsearchStatusException.class.equals(cause.getClass()) == false) {
                    logger.warn("Unexpected cause", e);
                }
                assertEquals(ElasticsearchStatusException.class, cause.getClass());
                assertEquals(RestStatus.CONFLICT, ((ElasticsearchStatusException) cause).status());
                assertEquals("[" + (maxRunningJobsPerNode + 1) + "] expected job status [OPENED], but got [FAILED], reason " +
                        "[failed to open, max running job capacity [" + maxRunningJobsPerNode + "] reached]", cause.getMessage());
                logger.info("good news everybody --> reached maximum number of allowed opened jobs, after trying to open the {}th job", i);

                // now manually clean things up and see if we can succeed to run one new job
                clearMlMetadata();
                putJobResponse = client().execute(PutJobAction.INSTANCE, putJobRequest).get();
                assertTrue(putJobResponse.isAcknowledged());
                OpenJobAction.Response openJobResponse = client().execute(OpenJobAction.INSTANCE, new OpenJobAction.Request(job.getId()))
                        .get();
                assertTrue(openJobResponse.isAcknowledged());
                return;
            }
        }

        fail("shouldn't be able to add more than [" + maxRunningJobsPerNode + "] jobs");
    }

    private Job.Builder createJob(String id) {
        DataDescription.Builder dataDescription = new DataDescription.Builder();
        dataDescription.setFormat(DataDescription.DataFormat.JSON);
        dataDescription.setTimeFormat(DataDescription.EPOCH_MS);

        Detector.Builder d = new Detector.Builder("count", null);
        AnalysisConfig.Builder analysisConfig = new AnalysisConfig.Builder(Collections.singletonList(d.build()));

        Job.Builder builder = new Job.Builder();
        builder.setId(id);

        builder.setAnalysisConfig(analysisConfig);
        builder.setDataDescription(dataDescription);
        return builder;
    }

    @Override
    protected void ensureClusterStateConsistency() throws IOException {
        ensureClusterStateConsistencyWorkAround();
    }

    // TODO: Fix in ES. In ESIntegTestCase we should get all NamedWriteableRegistry.Entry entries from ESIntegTestCase#nodePlugins()
    public static void ensureClusterStateConsistencyWorkAround() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            List<NamedWriteableRegistry.Entry> namedWritables = new ArrayList<>(ClusterModule.getNamedWriteables());
            namedWritables.add(new NamedWriteableRegistry.Entry(MetaData.Custom.class, "ml", MlMetadata::new));
            final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(namedWritables);
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
                        assertEquals("clusterstate UUID does not match", masterClusterState.stateUUID(),
                                localClusterState.stateUUID());
                        // We cannot compare serialization bytes since serialization order of maps is not guaranteed
                        // but we can compare serialization sizes - they should be the same
                        assertEquals("clusterstate size does not match", masterClusterStateSize, localClusterStateSize);
                        // Compare JSON serialization
                        assertNull("clusterstate JSON serialization does not match",
                                differenceBetweenMapsIgnoringArrayOrder(masterStateMap, localStateMap));
                    } catch (AssertionError error) {
                        fail("Cluster state from master:\n" + masterClusterState.toString() + "\nLocal cluster state:\n" +
                                localClusterState.toString());
                        throw error;
                    }
                }
            }
        }

    }

}
