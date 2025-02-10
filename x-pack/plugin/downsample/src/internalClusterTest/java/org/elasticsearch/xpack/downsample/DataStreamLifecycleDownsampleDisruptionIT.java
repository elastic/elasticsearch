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
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.InternalTestCluster;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.xpack.downsample.DataStreamLifecycleDriver.getBackingIndices;
import static org.elasticsearch.xpack.downsample.DataStreamLifecycleDriver.putTSDBIndexTemplate;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

@ESIntegTestCase.ClusterScope(scope = ESIntegTestCase.Scope.TEST, numDataNodes = 0, numClientNodes = 4)
public class DataStreamLifecycleDownsampleDisruptionIT extends ESIntegTestCase {
    private static final Logger logger = LogManager.getLogger(DataStreamLifecycleDownsampleDisruptionIT.class);
    public static final int DOC_COUNT = 50_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDataStreamLifecycleDownsampleRollingRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        cluster.startMasterOnlyNodes(1);
        cluster.startDataOnlyNodes(3);
        ensureStableCluster(cluster.size());
        ensureGreen();

        final String dataStreamName = "metrics-foo";
        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new DataStreamLifecycle.Downsampling(
                    List.of(
                        new DataStreamLifecycle.Downsampling.Round(
                            TimeValue.timeValueMillis(0),
                            new DownsampleConfig(new DateHistogramInterval("5m"))
                        )
                    )
                )
            )
            .build();
        DataStreamLifecycleDriver.setupTSDBDataStreamAndIngestDocs(
            client(),
            dataStreamName,
            "1986-01-08T23:40:53.384Z",
            "2022-01-08T23:40:53.384Z",
            lifecycle,
            DOC_COUNT,
            "1990-09-09T18:00:00"
        );

        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(client(), dataStreamName, null, null, lifecycle);
        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        // DSL runs every second and it has to tail forcemerge the index (2 seconds) and mark it as read-only (2s) before it starts
        // downsampling. This sleep here tries to get as close as possible to having disruption during the downsample execution.
        long sleepTime = randomLongBetween(3000, 4500);
        logger.info("-> giving data stream lifecycle [{}] millis to make some progress before starting the disruption", sleepTime);
        Thread.sleep(sleepTime);
        List<String> backingIndices = getBackingIndices(client(), dataStreamName);
        // first generation index
        String sourceIndex = backingIndices.get(0);

        internalCluster().rollingRestart(new InternalTestCluster.RestartCallback() {
        });

        // if the source index has already been downsampled and moved into the data stream just use its name directly
        final String targetIndex = sourceIndex.startsWith("downsample-5m-") ? sourceIndex : "downsample-5m-" + sourceIndex;
        assertBusy(() -> {
            try {
                GetSettingsResponse getSettingsResponse = cluster.client()
                    .admin()
                    .indices()
                    .getSettings(new GetSettingsRequest().indices(targetIndex).indicesOptions(IndicesOptions.LENIENT_EXPAND_OPEN))
                    .actionGet();
                Settings indexSettings = getSettingsResponse.getIndexToSettings().get(targetIndex);
                assertThat(indexSettings, is(notNullValue()));
                assertThat(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexSettings), is(IndexMetadata.DownsampleTaskStatus.SUCCESS));
                assertEquals("5m", IndexMetadata.INDEX_DOWNSAMPLE_INTERVAL.get(indexSettings));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 120, TimeUnit.SECONDS);
        ensureGreen(targetIndex);
    }
}
