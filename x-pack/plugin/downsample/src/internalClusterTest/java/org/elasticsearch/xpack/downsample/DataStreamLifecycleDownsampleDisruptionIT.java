/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ClusterServiceUtils;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;

import java.util.List;

import static org.elasticsearch.cluster.metadata.IndexMetadata.INDEX_DOWNSAMPLE_STATUS;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 4)
public class DataStreamLifecycleDownsampleDisruptionIT extends DownsamplingIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleDownsampleDisruptionIT.class);
    public static final int DOC_COUNT = 25_000;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    public void testDataStreamLifecycleDownsampleRollingRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String dataStreamName = "metrics-foo";
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsampling(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(0),
                        new DownsampleConfig(new DateHistogramInterval("5m"))
                    )
                )
            )
            .buildTemplate();
        setupTSDBDataStreamAndIngestDocs(
            dataStreamName,
            "1986-01-08T23:40:53.384Z",
            "2022-01-08T23:40:53.384Z",
            lifecycle,
            DOC_COUNT,
            "1990-09-09T18:00:00"
        );

        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);
        safeGet(client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)));
        String sourceIndex = getDataStreamBackingIndexNames(dataStreamName).get(0);
        final String targetIndex = "downsample-5m-" + sourceIndex;

        /**
         * DLM runs every second and it has to tail forcemerge the index (2 seconds) and mark it as read-only (2s) before it starts
         * downsampling. We try to detect if the downsampling has started by checking the downsample status in the target index.
         */
        logger.info("-> Waiting for the data stream lifecycle to start the downsampling operation before starting the disruption.");
        ensureDownsamplingStatus(targetIndex, IndexMetadata.DownsampleTaskStatus.STARTED, TimeValue.timeValueSeconds(8));

        logger.info("-> Starting the disruption.");
        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback());

        ensureDownsamplingStatus(targetIndex, IndexMetadata.DownsampleTaskStatus.SUCCESS, TimeValue.timeValueSeconds(120));
        ensureGreen(targetIndex);
        logger.info("-> Relocation has finished");
    }

    private void ensureDownsamplingStatus(String downsampledIndex, IndexMetadata.DownsampleTaskStatus expectedStatus, TimeValue timeout) {
        final var clusterService = internalCluster().getCurrentMasterNodeInstance(ClusterService.class);
        final var listener = ClusterServiceUtils.addTemporaryStateListener(clusterService, clusterState -> {
            final var indexMetadata = clusterState.metadata().index(downsampledIndex);
            if (indexMetadata == null) {
                return false;
            }
            var downsamplingStatus = INDEX_DOWNSAMPLE_STATUS.get(indexMetadata.getSettings());
            if (expectedStatus == downsamplingStatus) {
                logger.info("-> Downsampling status for index [{}] is [{}]", downsampledIndex, downsamplingStatus);
                return true;
            }
            return false;
        }, timeout);
        safeAwait(listener, timeout);
    }
}
