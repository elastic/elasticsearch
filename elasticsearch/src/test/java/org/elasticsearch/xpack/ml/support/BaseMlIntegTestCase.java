/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.ml.support;

import org.elasticsearch.ExceptionsHelper;
import org.elasticsearch.client.Client;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.SecurityIntegTestCase;
import org.elasticsearch.xpack.ml.MlPlugin;
import org.elasticsearch.xpack.ml.action.CloseJobAction;
import org.elasticsearch.xpack.ml.action.DeleteDatafeedAction;
import org.elasticsearch.xpack.ml.action.DeleteJobAction;
import org.elasticsearch.xpack.ml.action.GetDatafeedsStatsAction;
import org.elasticsearch.xpack.ml.action.StopDatafeedAction;
import org.elasticsearch.xpack.ml.datafeed.DatafeedConfig;
import org.elasticsearch.xpack.ml.datafeed.DatafeedState;
import org.elasticsearch.xpack.ml.job.config.AnalysisConfig;
import org.elasticsearch.xpack.ml.job.config.DataDescription;
import org.elasticsearch.xpack.ml.job.config.Detector;
import org.elasticsearch.xpack.ml.job.config.Job;
import org.elasticsearch.xpack.ml.job.metadata.MlMetadata;
import org.elasticsearch.xpack.persistent.RemovePersistentTaskAction;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import static org.hamcrest.Matchers.equalTo;

/**
 * A base class for testing datafeed and job lifecycle specifics.
 *
 * Note for other type of integration tests you should use the external test cluster created by the Gradle integTest task.
 * For example tests extending this base class test with the non native autodetect process.
 */
@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 0,
        transportClientRatio = 0, supportsDedicatedMasters = false)
public abstract class BaseMlIntegTestCase extends SecurityIntegTestCase {

    @Override
    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal));
        settings.put(MlPlugin.USE_NATIVE_PROCESS_OPTION.getKey(), false);
        settings.put(MlPlugin.ML_ENABLED.getKey(), true);
        return settings.build();
    }

    protected Job.Builder createJob(String id) {
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

    // Due to the fact that ml plugin creates the state, notifications and meta indices automatically
    // when the test framework removes all indices then ml plugin adds them back. Causing validation to fail
    // we should move to templates instead as that will fix the test problem
    protected void cleanupWorkaround(int numNodes) throws Exception {
        deleteAllDatafeeds(client());
        deleteAllJobs(client());
        for (int i = 0; i < numNodes; i++) {
            internalCluster().stopRandomDataNode();
        }
        internalCluster().startNode(Settings.builder().put(MlPlugin.ML_ENABLED.getKey(), false));
    }

    private void deleteAllDatafeeds(Client client) throws Exception {
        MetaData metaData = client.admin().cluster().prepareState().get().getState().getMetaData();
        MlMetadata mlMetadata = metaData.custom(MlMetadata.TYPE);
        for (DatafeedConfig datafeed : mlMetadata.getDatafeeds().values()) {
            String datafeedId = datafeed.getId();
            try {
                RemovePersistentTaskAction.Response stopResponse =
                        client.execute(StopDatafeedAction.INSTANCE, new StopDatafeedAction.Request(datafeedId)).get();
                assertTrue(stopResponse.isAcknowledged());
            } catch (ExecutionException e) {
                // CONFLICT is ok, as it means the datafeed has already stopped, which isn't an issue at all.
                if (RestStatus.CONFLICT != ExceptionsHelper.status(e.getCause())) {
                    throw new RuntimeException(e);
                }
            }
            assertBusy(() -> {
                try {
                    GetDatafeedsStatsAction.Request request = new GetDatafeedsStatsAction.Request(datafeedId);
                    GetDatafeedsStatsAction.Response r = client.execute(GetDatafeedsStatsAction.INSTANCE, request).get();
                    assertThat(r.getResponse().results().get(0).getDatafeedState(), equalTo(DatafeedState.STOPPED));
                } catch (InterruptedException | ExecutionException e) {
                    throw new RuntimeException(e);
                }
            });
            DeleteDatafeedAction.Response deleteResponse =
                    client.execute(DeleteDatafeedAction.INSTANCE, new DeleteDatafeedAction.Request(datafeedId)).get();
            assertTrue(deleteResponse.isAcknowledged());
        }
    }

    private void deleteAllJobs(Client client) throws Exception {
        MetaData metaData = client.admin().cluster().prepareState().get().getState().getMetaData();
        MlMetadata mlMetadata = metaData.custom(MlMetadata.TYPE);
        for (Map.Entry<String, Job> entry : mlMetadata.getJobs().entrySet()) {
            String jobId = entry.getKey();
            try {
                CloseJobAction.Response response =
                        client.execute(CloseJobAction.INSTANCE, new CloseJobAction.Request(jobId)).get();
                assertTrue(response.isClosed());
            } catch (Exception e) {
                logger.warn("Job [" + jobId + "] couldn't be closed", e);
            }
            DeleteJobAction.Response response =
                    client.execute(DeleteJobAction.INSTANCE, new DeleteJobAction.Request(jobId)).get();
            assertTrue(response.isAcknowledged());
        }
    }

}
