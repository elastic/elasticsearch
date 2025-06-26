/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.test.junit.annotations.TestLogging;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static org.elasticsearch.cluster.metadata.ClusterChangedEventUtils.indicesCreated;
import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.backingIndexEqualTo;
import static org.elasticsearch.test.hamcrest.ElasticsearchAssertions.assertAcked;
import static org.hamcrest.Matchers.is;

public class DataStreamLifecycleDownsampleIT extends DownsamplingIntegTestCase {
    public static final int DOC_COUNT = 50_000;

    @Override
    protected Settings nodeSettings(int nodeOrdinal, Settings otherSettings) {
        Settings.Builder settings = Settings.builder().put(super.nodeSettings(nodeOrdinal, otherSettings));
        settings.put(DataStreamLifecycleService.DATA_STREAM_LIFECYCLE_POLL_INTERVAL, "1s");
        return settings.build();
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsampling() throws Exception {
        String dataStreamName = "metrics-foo";

        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsampling(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(0),
                        new DownsampleConfig(new DateHistogramInterval("5m"))
                    ),
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueSeconds(10),
                        new DownsampleConfig(new DateHistogramInterval("10m"))
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

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0);
        String oneSecondDownsampleIndex = "downsample-5m-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (indicesCreated(event).contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });
        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);

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
            List<String> dsBackingIndices = getDataStreamBackingIndexNames(dataStreamName);

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

        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsampling(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(0),
                        new DownsampleConfig(new DateHistogramInterval("5m"))
                    ),
                    // data stream lifecycle runs every 1 second, so by the time we forcemerge the backing index it would've been at
                    // least 2 seconds since rollover. only the 10 seconds round should be executed.
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(10),
                        new DownsampleConfig(new DateHistogramInterval("10m"))
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

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0);
        String oneSecondDownsampleIndex = "downsample-5m-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (indicesCreated(event).contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });
        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);
        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(1));
            // only the ten seconds downsample round should've been executed
            assertThat(witnessedDownsamplingIndices.contains(tenSecondsDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getDataStreamBackingIndexNames(dataStreamName);

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

        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsampling(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(0),
                        new DownsampleConfig(new DateHistogramInterval("5m"))
                    ),
                    // data stream lifecycle runs every 1 second, so by the time we forcemerge the backing index it would've been at
                    // least 2 seconds since rollover. only the 10 seconds round should be executed.
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(10),
                        new DownsampleConfig(new DateHistogramInterval("10m"))
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

        List<String> backingIndices = getDataStreamBackingIndexNames(dataStreamName);
        String firstGenerationBackingIndex = backingIndices.get(0);
        String oneSecondDownsampleIndex = "downsample-5m-" + firstGenerationBackingIndex;
        String tenSecondsDownsampleIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(oneSecondDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(oneSecondDownsampleIndex))) {
                witnessedDownsamplingIndices.add(oneSecondDownsampleIndex);
            }
            if (indicesCreated(event).contains(tenSecondsDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenSecondsDownsampleIndex);
            }
        });
        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);
        safeGet(client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)));

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(1));
            // only the ten seconds downsample round should've been executed
            assertThat(witnessedDownsamplingIndices.contains(tenSecondsDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getDataStreamBackingIndexNames(dataStreamName);
            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(dsBackingIndices.get(0), is(tenSecondsDownsampleIndex));
        }, 30, TimeUnit.SECONDS);

        // update the lifecycle so that it only has one round, for the same `after` parameter as before, but a different interval
        // the different interval should yield a different downsample index name so we expect the data stream lifecycle to get the previous
        // `10s` interval downsample index, downsample it to `20m` and replace it in the data stream instead of the `10s` one.
        DataStreamLifecycle updatedLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsampling(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(
                        TimeValue.timeValueMillis(10),
                        new DownsampleConfig(new DateHistogramInterval("20m"))
                    )
                )
            )
            .build();
        assertAcked(
            client().execute(
                PutDataStreamLifecycleAction.INSTANCE,
                new PutDataStreamLifecycleAction.Request(
                    TEST_REQUEST_TIMEOUT,
                    TEST_REQUEST_TIMEOUT,
                    new String[] { dataStreamName },
                    updatedLifecycle
                )
            )
        );

        String thirtySecondsDownsampleIndex = "downsample-20m-" + firstGenerationBackingIndex;

        assertBusy(() -> {
            assertThat(indexExists(tenSecondsDownsampleIndex), is(false));

            List<String> dsBackingIndices = getDataStreamBackingIndexNames(dataStreamName);
            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(dsBackingIndices.get(0), is(thirtySecondsDownsampleIndex));
        }, 30, TimeUnit.SECONDS);
    }
}
