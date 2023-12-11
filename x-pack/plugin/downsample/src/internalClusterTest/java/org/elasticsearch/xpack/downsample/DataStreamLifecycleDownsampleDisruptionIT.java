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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

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

    @AwaitsFix(bugUrl = "https://github.com/elastic/elasticsearch/issues/99520")
    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDataStreamLifecycleDownsampleRollingRestart() throws Exception {
        final InternalTestCluster cluster = internalCluster();
        final List<String> masterNodes = cluster.startMasterOnlyNodes(1);
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
        final CountDownLatch disruptionStart = new CountDownLatch(1);
        final CountDownLatch disruptionEnd = new CountDownLatch(1);
        List<String> backingIndices = getBackingIndices(client(), dataStreamName);
        // first generation index
        String sourceIndex = backingIndices.get(0);
        new Thread(new Disruptor(cluster, sourceIndex, new DisruptionListener() {
            @Override
            public void disruptionStart() {
                disruptionStart.countDown();
            }

            @Override
            public void disruptionEnd() {
                disruptionEnd.countDown();
            }
        }, masterNodes.get(0), (ignored) -> {
            try {
                cluster.rollingRestart(new InternalTestCluster.RestartCallback() {
                    @Override
                    public boolean validateClusterForming() {
                        return true;
                    }
                });
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        })).start();

        waitUntil(
            () -> cluster.client().admin().cluster().preparePendingClusterTasks().get().pendingTasks().isEmpty(),
            60,
            TimeUnit.SECONDS
        );
        ensureStableCluster(cluster.numDataAndMasterNodes());

        final String targetIndex = "downsample-5m-" + sourceIndex;
        assertBusy(() -> {
            try {
                GetSettingsResponse getSettingsResponse = client().admin()
                    .indices()
                    .getSettings(new GetSettingsRequest().indices(targetIndex))
                    .actionGet();
                Settings indexSettings = getSettingsResponse.getIndexToSettings().get(targetIndex);
                assertThat(indexSettings, is(notNullValue()));
                assertThat(IndexMetadata.INDEX_DOWNSAMPLE_STATUS.get(indexSettings), is(IndexMetadata.DownsampleTaskStatus.SUCCESS));
            } catch (Exception e) {
                throw new AssertionError(e);
            }
        }, 60, TimeUnit.SECONDS);
    }

    interface DisruptionListener {
        void disruptionStart();

        void disruptionEnd();
    }

    private class Disruptor implements Runnable {
        final InternalTestCluster cluster;
        private final String sourceIndex;
        private final DisruptionListener listener;
        private final String clientNode;
        private final Consumer<String> disruption;

        private Disruptor(
            final InternalTestCluster cluster,
            final String sourceIndex,
            final DisruptionListener listener,
            final String clientNode,
            final Consumer<String> disruption
        ) {
            this.cluster = cluster;
            this.sourceIndex = sourceIndex;
            this.listener = listener;
            this.clientNode = clientNode;
            this.disruption = disruption;
        }

        @Override
        public void run() {
            listener.disruptionStart();
            try {
                final String candidateNode = cluster.client(clientNode)
                    .admin()
                    .cluster()
                    .prepareSearchShards(sourceIndex)
                    .get()
                    .getNodes()[0].getName();
                logger.info("Candidate node [" + candidateNode + "]");
                disruption.accept(candidateNode);
                ensureGreen(sourceIndex);
                ensureStableCluster(cluster.numDataAndMasterNodes(), clientNode);

            } catch (Exception e) {
                logger.error("Ignoring Error while injecting disruption [" + e.getMessage() + "]");
            } finally {
                listener.disruptionEnd();
            }
        }
    }
}
