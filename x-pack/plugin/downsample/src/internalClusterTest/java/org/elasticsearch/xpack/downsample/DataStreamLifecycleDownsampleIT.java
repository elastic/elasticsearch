/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.downsample;

import org.elasticsearch.action.admin.indices.rollover.RolloverAction;
import org.elasticsearch.action.admin.indices.rollover.RolloverRequest;
import org.elasticsearch.action.admin.indices.rollover.RolloverResponse;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.elasticsearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.elasticsearch.action.datastreams.lifecycle.PutDataStreamLifecycleAction;
import org.elasticsearch.action.downsample.DownsampleConfig;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
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
import static org.hamcrest.Matchers.equalTo;
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

        DownsampleConfig.SamplingMethod downsamplingMethod = randomSamplingMethod();
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingMethod(downsamplingMethod)
            .downsamplingRounds(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m")),
                    new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueSeconds(10), new DateHistogramInterval("10m"))
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
        String fiveMinuteDownsampleIndex = "downsample-5m-" + firstGenerationBackingIndex;
        String tenMinuteDownsampleIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(fiveMinuteDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(fiveMinuteDownsampleIndex))) {
                witnessedDownsamplingIndices.add(fiveMinuteDownsampleIndex);
            }
            if (indicesCreated(event).contains(tenMinuteDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenMinuteDownsampleIndex);
            }
        });
        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing, so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);

        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            // first downsampling round
            assertThat(witnessedDownsamplingIndices.contains(fiveMinuteDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(2));
            assertThat(witnessedDownsamplingIndices.contains(fiveMinuteDownsampleIndex), is(true));
            assertThat(witnessedDownsamplingIndices.contains(tenMinuteDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getDataStreamBackingIndexNames(dataStreamName);

            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            // the last downsampling round must remain in the data stream
            assertThat(dsBackingIndices.get(0), is(tenMinuteDownsampleIndex));
            assertThat(indexExists(fiveMinuteDownsampleIndex), is(false));
        }, 30, TimeUnit.SECONDS);
        assertDownsamplingMethod(downsamplingMethod, tenMinuteDownsampleIndex);
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testDownsamplingOnlyExecutesTheLastMatchingRound() throws Exception {
        String dataStreamName = "metrics-bar";

        DownsampleConfig.SamplingMethod downsamplingMethod = randomSamplingMethod();
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingMethod(downsamplingMethod)
            .downsamplingRounds(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m")),
                    // data stream lifecycle runs every 1 second, so by the time we forcemerge the backing index it would've been at
                    // least 2 seconds since rollover. only the 10 seconds round should be executed.
                    new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(10), new DateHistogramInterval("10m"))
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
        String fiveMinuteDownsampleIndex = "downsample-5m-" + firstGenerationBackingIndex;
        String tenMinuteDownsampleIndex = "downsample-10m-" + firstGenerationBackingIndex;

        Set<String> witnessedDownsamplingIndices = new HashSet<>();
        clusterService().addListener(event -> {
            if (indicesCreated(event).contains(fiveMinuteDownsampleIndex)
                || event.indicesDeleted().stream().anyMatch(index -> index.getName().equals(fiveMinuteDownsampleIndex))) {
                witnessedDownsamplingIndices.add(fiveMinuteDownsampleIndex);
            }
            if (indicesCreated(event).contains(tenMinuteDownsampleIndex)) {
                witnessedDownsamplingIndices.add(tenMinuteDownsampleIndex);
            }
        });
        // before we rollover we update the index template to remove the start/end time boundaries (they're there just to ease with
        // testing, so DSL doesn't have to wait for the end_time to lapse)
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);
        client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)).actionGet();

        assertBusy(() -> {
            assertThat(witnessedDownsamplingIndices.size(), is(1));
            // only the ten seconds downsample round should've been executed
            assertThat(witnessedDownsamplingIndices.contains(tenMinuteDownsampleIndex), is(true));
        }, 30, TimeUnit.SECONDS);

        assertBusy(() -> {
            List<String> dsBackingIndices = getDataStreamBackingIndexNames(dataStreamName);

            assertThat(dsBackingIndices.size(), is(2));
            String writeIndex = dsBackingIndices.get(1);
            assertThat(writeIndex, backingIndexEqualTo(dataStreamName, 2));
            assertThat(dsBackingIndices.get(0), is(tenMinuteDownsampleIndex));
        }, 30, TimeUnit.SECONDS);
        assertDownsamplingMethod(downsamplingMethod, tenMinuteDownsampleIndex);
    }

    @TestLogging(value = "org.elasticsearch.datastreams.lifecycle:TRACE", reason = "debugging")
    public void testUpdateDownsampleRound() throws Exception {
        // we'll test updating the data lifecycle to add an earlier downsampling round to an already executed lifecycle
        // we expect the earlier round to be ignored
        String dataStreamName = "metrics-baz";

        DownsampleConfig.SamplingMethod downsamplingMethod = randomSamplingMethod();
        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingMethod(downsamplingMethod)
            .downsamplingRounds(
                List.of(
                    new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(0), new DateHistogramInterval("5m")),
                    // data stream lifecycle runs every 1 second, so by the time we forcemerge the backing index it would've been at
                    // least 2 seconds since rollover. Only the 10 seconds round should be executed.
                    new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(10), new DateHistogramInterval("10m"))
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
        // `10m` interval downsample index, downsample it to `20m` and replace it in the data stream instead of the `10m` one.
        DataStreamLifecycle updatedLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingRounds(
                List.of(new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(10), new DateHistogramInterval("20m")))
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
        assertDownsamplingMethod(downsamplingMethod, thirtySecondsDownsampleIndex);
    }

    /**
     * This test ensures that when we change the sampling method, the already downsampled indices will use the original sampling method,
     * while the raw data ones will be downsampled with the most recent configuration.
     * To achieve that, we set the following test:
     * 1. Create a data stream that is downsampled with a sampling method.
     * 2. Rollover and wait for the downsampling to occur
     * 3. Double the downsample interval (so it can downsample the first index as well) and change the sampling method.
     * 4. Rollover and wait for both indices to be downsampled with the new interval
     * 5. Check that the indices have been downsampled with the correct method.
     */
    public void testUpdateDownsampleSamplingMode() throws Exception {
        String dataStreamName = "metrics-baz";
        DownsampleConfig.SamplingMethod initialSamplingMethod = randomBoolean()
            ? null
            : randomFrom(DownsampleConfig.SamplingMethod.values());
        DownsampleConfig.SamplingMethod updatedSamplingMethod = initialSamplingMethod == DownsampleConfig.SamplingMethod.LAST_VALUE
            ? (randomBoolean() ? null : DownsampleConfig.SamplingMethod.AGGREGATE)
            : DownsampleConfig.SamplingMethod.LAST_VALUE;

        DataStreamLifecycle.Template lifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingMethod(initialSamplingMethod)
            .downsamplingRounds(
                List.of(new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(10), new DateHistogramInterval("5m")))
            )
            .buildTemplate();

        // Start and end time there just to ease with testing, so DLM doesn't have to wait for the end_time to lapse
        // Creating the first backing index.
        setupTSDBDataStreamAndIngestDocs(
            dataStreamName,
            "1986-01-08T23:40:53.384Z",
            "2022-01-08T23:40:53.384Z",
            lifecycle,
            DOC_COUNT,
            "1990-09-09T18:00:00"
        );

        // before we roll over, we update the index template to have new start/end time boundaries
        // Creating the second backing index.
        putTSDBIndexTemplate(dataStreamName, "2022-01-08T23:40:53.384Z", "2023-01-08T23:40:53.384Z", lifecycle);
        RolloverResponse rolloverResponse = safeGet(client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)));
        assertTrue(rolloverResponse.isRolledOver());
        String firstBackingIndex = rolloverResponse.getOldIndex();
        String secondBackingIndex = rolloverResponse.getNewIndex();
        indexDocuments(dataStreamName, randomIntBetween(1, 1000), "2022-01-08T23:50:00");

        // Ensure that the first backing index has been downsampled
        awaitClusterState(clusterState -> {
            final var dataStream = clusterState.metadata().getProject().dataStreams().get(dataStreamName);
            if (dataStream == null) {
                return false;
            }
            return dataStream.getIndices().size() > 1 && dataStream.getIndices().getFirst().getName().startsWith("downsample-");
        });
        assertDownsamplingMethod(initialSamplingMethod, "downsample-5m-" + firstBackingIndex);
        // We change the sampling method, but also we double the downsampling interval. We expect the data stream lifecycle to get the
        // previous `5m` interval downsampled index, downsample it to `10m` and replace it in the data stream with the `5m` one.
        DataStreamLifecycle updatedLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .downsamplingMethod(updatedSamplingMethod)
            .downsamplingRounds(
                List.of(new DataStreamLifecycle.DownsamplingRound(TimeValue.timeValueMillis(5), new DateHistogramInterval("10m")))
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

        // We roll over one more time, so the second backing index will be eligible for downsampling
        putTSDBIndexTemplate(dataStreamName, null, null, lifecycle);
        rolloverResponse = safeGet(client().execute(RolloverAction.INSTANCE, new RolloverRequest(dataStreamName, null)));
        assertTrue(rolloverResponse.isRolledOver());
        String downsampledPrefix = "downsample-10m-";
        awaitClusterState(clusterState -> {
            ProjectMetadata projectMetadata = clusterState.metadata().getProject();
            final var dataStream = projectMetadata.dataStreams().get(dataStreamName);
            if (dataStream == null) {
                return false;
            }
            return dataStream.getIndices().size() > 2
                && dataStream.getIndices().stream().filter(index -> index.getName().startsWith(downsampledPrefix)).count() == 2;
        });
        assertDownsamplingMethod(initialSamplingMethod, downsampledPrefix + firstBackingIndex);
        assertDownsamplingMethod(updatedSamplingMethod, downsampledPrefix + secondBackingIndex);
    }

    private void assertDownsamplingMethod(DownsampleConfig.SamplingMethod downsamplingMethod, String... indexNames) {
        String expected = DownsampleConfig.SamplingMethod.getOrDefault(downsamplingMethod).toString();
        GetSettingsResponse response = safeGet(
            client().admin().indices().getSettings(new GetSettingsRequest(TimeValue.THIRTY_SECONDS).indices(indexNames))
        );
        for (String indexName : indexNames) {
            assertThat(response.getSetting(indexName, IndexMetadata.INDEX_DOWNSAMPLE_METHOD_KEY), equalTo(expected));
        }
    }
}
