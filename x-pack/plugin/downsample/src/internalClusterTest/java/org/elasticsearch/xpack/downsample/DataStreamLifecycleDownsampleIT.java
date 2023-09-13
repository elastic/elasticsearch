/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle.Downsampling;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.DataStreamsPlugin;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.datastreams.lifecycle.action.PutDataStreamLifecycleAction;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.ESIntegTestCase;
import org.elasticsearch.test.junit.annotations.TestLogging;
import org.elasticsearch.xpack.aggregatemetric.AggregateMetricMapperPlugin;
import org.elasticsearch.xpack.core.LocalStateCompositeXPackPlugin;

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.xpack.downsample.DataStreamLifecycleDriver.getBackingIndices;
import static org.hamcrest.Matchers.is;

public class DataStreamLifecycleDownsampleIT extends ESIntegTestCase {
    public static final int DOC_COUNT = 50_000;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(DataStreamsPlugin.class, LocalStateCompositeXPackPlugin.class, Downsample.class, AggregateMetricMapperPlugin.class);
    }

    protected boolean ignoreExternalCluster() {
        return true;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsampling() throws Exception {
        String dataStreamName = "metrics-foo";

        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new Downsampling(
                    List.of(
                        new Downsampling.Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("1s"))),
                        new Downsampling.Round(TimeValue.timeValueSeconds(10), new DownsampleConfig(new DateHistogramInterval("10s")))
                    )
                )
            )
            .build();

        DataStreamLifecycleDriver.setupDataStreamAndIngestDocs(client(), dataStreamName, lifecycle, DOC_COUNT);

        List<String> backingIndices = getBackingIndices(client(), dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0);
        String oneSecondDownsampleIndex = "downsample-1s-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10s-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (event.indicesCreated().contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (event.indicesCreated().contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });

        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            // first downsampling round
            assertThat(witnessedDownsamplingIndices.contains(oneSecondDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(2));
            assertThat(witnessedDownsamplingIndices.contains(oneSecondDownsampleIndex), is(true));
            assertThat(witnessedDownsamplingIndices.contains(tenSecondsDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getBackingIndices(client(), dataStreamName);

            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            // the last downsampling round must remain in the data stream
            assertThat(dsBackingIndices.get(0), is(tenSecondsDownsampleIndex));
            assertThat(indexExists(oneSecondDownsampleIndex), is(false));
        }, 30, TimeUnit.SECONDS);
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsamplingOnlyExecutesTheLastMatchingRound() throws Exception {
        String dataStreamName = "metrics-bar";

        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new Downsampling(
                    List.of(
                        new Downsampling.Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("1s"))),
                        // data stream lifecycle runs every 1 second, so by the time we forcemerge the backing index it would've been at
                        // least 2 seconds since rollover. only the 10 seconds round should be executed.
                        new Downsampling.Round(TimeValue.timeValueMillis(10), new DownsampleConfig(new DateHistogramInterval("10s")))
                    )
                )
            )
            .build();
        DataStreamLifecycleDriver.setupDataStreamAndIngestDocs(client(), dataStreamName, lifecycle, DOC_COUNT);

        List<String> backingIndices = getBackingIndices(client(), dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0);
        String oneSecondDownsampleIndex = "downsample-1s-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10s-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (event.indicesCreated().contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (event.indicesCreated().contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });

        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(1));
            // only the ten seconds downsample round should've been executed
            assertThat(witnessedDownsamplingIndices.contains(tenSecondsDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getBackingIndices(client(), dataStreamName);

            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(dsBackingIndices.get(0), is(tenSecondsDownsampleIndex));
        }, 30, TimeUnit.SECONDS);
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testUpdateDownsampleRound() throws Exception {
        // we'll test updating the data lifecycle to add an earlier downsampling round to an already executed lifecycle
        // we expect the earlier round to be ignored
        String dataStreamName = "metrics-baz";

        DataStreamLifecycle lifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new Downsampling(
                    List.of(
                        new Downsampling.Round(TimeValue.timeValueMillis(0), new DownsampleConfig(new DateHistogramInterval("1s"))),
                        // data stream lifecycle runs every 1 second, so by the time we forcemerge the backing index it would've been at
                        // least 2 seconds since rollover. only the 10 seconds round should be executed.
                        new Downsampling.Round(TimeValue.timeValueMillis(10), new DownsampleConfig(new DateHistogramInterval("10s")))
                    )
                )
            )
            .build();

        DataStreamLifecycleDriver.setupDataStreamAndIngestDocs(client(), dataStreamName, lifecycle, DOC_COUNT);

        List<String> backingIndices = getBackingIndices(client(), dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0);
        String oneSecondDownsampleIndex = "downsample-1s-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10s-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (event.indicesCreated().contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (event.indicesCreated().contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });

        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(1));
            // only the ten seconds downsample round should've been executed
            assertThat(witnessedDownsamplingIndices.contains(tenSecondsDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getBackingIndices(client(), dataStreamName);
            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(dsBackingIndices.get(0), is(tenSecondsDownsampleIndex));
        }, 30, TimeUnit.SECONDS);

        // update the lifecycle so that it only has one round, for the same `after` parameter as before, but a different interval
        // the different interval should yield a different downsample index name so we expect the data stream lifecycle to get the previous
        // `10s` interval downsample index, downsample it to `30s` and replace it in the data stream instead of the `10s` one.
        DataStreamLifecycle updatedLifecycle = DataStreamLifecycle.newBuilder()
            .downsampling(
                new Downsampling(
                    List.of(new Downsampling.Round(TimeValue.timeValueMillis(10), new DownsampleConfig(new DateHistogramInterval("30s"))))
                )
            )
            .build();

        client().execute(
            PutDataStreamLifecycleAction.INSTANCE,
            new PutDataStreamLifecycleAction.Request(new String[] { dataStreamName }, updatedLifecycle)
        );

        String thirtySecondsDownsampleIndex = "downsample-30s-" + firstGenerationBackingIndex;

        assertBusy(() -> {
            assertThat(indexExists(tenSecondsDownsampleIndex), is(false));

            List<String> dsBackingIndices = getBackingIndices(client(), dataStreamName);
            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(dsBackingIndices.get(0), is(thirtySecondsDownsampleIndex));
        }, 30, TimeUnit.SECONDS);
    }
}
