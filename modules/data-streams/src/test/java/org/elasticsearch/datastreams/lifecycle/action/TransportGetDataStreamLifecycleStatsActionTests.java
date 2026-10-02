/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.admin.indices.rollover.MaxAgeCondition;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycleSettings;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleService;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.time.Clock;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.elasticsearch.datastreams.lifecycle.DataStreamLifecycleFixtures.createDataStream;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportGetDataStreamLifecycleStatsActionTests extends ESTestCase {

    private final DataStreamLifecycleService dataStreamLifecycleService = mock(DataStreamLifecycleService.class);
    private final DataStreamLifecycleErrorStore errorStore = mock(DataStreamLifecycleErrorStore.class);
    private final TransportGetDataStreamLifecycleStatsAction action = new TransportGetDataStreamLifecycleStatsAction(
        mock(TransportService.class),
        mock(ClusterService.class),
        mock(ThreadPool.class),
        mock(ActionFilters.class),
        dataStreamLifecycleService,
        TestProjectResolvers.alwaysThrow(),
        DataStreamLifecycleSettings.create(ClusterSettings.createBuiltInClusterSettings())
    );
    private Long lastRunDuration;
    private Long timeBetweenStarts;

    @Before
    public void initMocks() throws Exception {
        lastRunDuration = randomBoolean() ? randomLongBetween(0, 100000) : null;
        timeBetweenStarts = randomBoolean() ? randomLongBetween(0, 100000) : null;
        when(dataStreamLifecycleService.getLastRunDuration()).thenReturn(lastRunDuration);
        when(dataStreamLifecycleService.getTimeBetweenStarts()).thenReturn(timeBetweenStarts);
        when(dataStreamLifecycleService.getErrorStore()).thenReturn(errorStore);
        when(errorStore.getAllIndices(any())).thenReturn(Set.of());
    }

    public void testEmptyClusterState() {
        GetDataStreamLifecycleStatsAction.Response response = action.collectStats(
            ProjectMetadata.builder(randomUniqueProjectId()).build(),
            randomBoolean()
        );
        assertThat(response.getRunDuration(), is(lastRunDuration));
        assertThat(response.getTimeBetweenStarts(), is(timeBetweenStarts));
        assertThat(response.getDataStreamStats().isEmpty(), is(true));
    }

    public void testMixedDataStreams() {
        Set<Index> indicesInError = new HashSet<>();
        int numBackingIndices = 3;
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream ilmDataStream = createDataStream(
            builder,
            "ilm-managed-index",
            numBackingIndices,
            Settings.builder()
                .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()),
            null,
            Clock.systemUTC().millis()
        );
        builder.put(ilmDataStream);
        DataStream dslDataStream = createDataStream(
            builder,
            "dsl-managed-index",
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(10)).build(),
            Clock.systemUTC().millis()
        );
        indicesInError.add(dslDataStream.getIndices().get(randomInt(numBackingIndices - 1)));
        builder.put(dslDataStream);
        {
            String dataStreamName = "mixed";
            final List<Index> backingIndices = new ArrayList<>();
            for (int k = 1; k <= 2; k++) {
                IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, k))
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy")
                            .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    )
                    .numberOfShards(1)
                    .numberOfReplicas(1)
                    .creationDate(Clock.systemUTC().millis());

                IndexMetadata indexMetadata = indexMetaBuilder.build();
                builder.put(indexMetadata, false);
                backingIndices.add(indexMetadata.getIndex());
            }
            // DSL managed write index
            IndexMetadata.Builder indexMetaBuilder = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 3))
                .settings(Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current()))
                .numberOfShards(1)
                .numberOfReplicas(1)
                .creationDate(Clock.systemUTC().millis());
            MaxAgeCondition rolloverCondition = new MaxAgeCondition(TimeValue.timeValueMillis(Clock.systemUTC().millis() - 2000L));
            indexMetaBuilder.putRolloverInfo(
                new RolloverInfo(dataStreamName, List.of(rolloverCondition), Clock.systemUTC().millis() - 2000L)
            );
            IndexMetadata indexMetadata = indexMetaBuilder.build();
            builder.put(indexMetadata, false);
            backingIndices.add(indexMetadata.getIndex());
            builder.put(newInstance(dataStreamName, backingIndices, 3, null, false, DataStreamLifecycle.dataLifecycleBuilder().build()));
        }
        ProjectMetadata project = builder.build();
        when(errorStore.getAllIndices(project.id())).thenReturn(indicesInError);
        // none of the data streams are time series, so the default lifecycle for time series has no effect
        GetDataStreamLifecycleStatsAction.Response response = action.collectStats(project, randomBoolean());
        assertThat(response.getRunDuration(), is(lastRunDuration));
        assertThat(response.getTimeBetweenStarts(), is(timeBetweenStarts));
        assertThat(response.getDataStreamStats().size(), is(2));
        for (GetDataStreamLifecycleStatsAction.Response.DataStreamStats stats : response.getDataStreamStats()) {
            if (stats.dataStreamName().equals("dsl-managed-index")) {
                assertThat(stats.backingIndicesInTotal(), is(3));
                assertThat(stats.backingIndicesInError(), is(1));
            }
            if (stats.dataStreamName().equals("mixed")) {
                assertThat(stats.backingIndicesInTotal(), is(1));
                assertThat(stats.backingIndicesInError(), is(0));
            }
        }
    }

    /**
     * Time series data streams without a configured lifecycle are managed by the default lifecycle only when the default lifecycle for
     * time series is enabled, so they should only be reported in the stats in that case. Whether the default lifecycle applies depends
     * on the index mode of the data stream, so the backing indices use the standard index mode to avoid having to configure
     * non-overlapping time series ranges.
     */
    public void testTimeSeriesDataStreamsWithoutLifecycle() {
        Set<Index> indicesInError = new HashSet<>();
        int numBackingIndices = 3;
        long now = Clock.systemUTC().millis();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        DataStream tsdsWithoutLifecycle = createDataStream(
            builder,
            "tsds-without-lifecycle",
            numBackingIndices,
            settings(IndexVersion.current()),
            null,
            now
        ).copy().setIndexMode(IndexMode.TIME_SERIES).build();
        indicesInError.add(tsdsWithoutLifecycle.getIndices().get(randomInt(numBackingIndices - 1)));
        builder.put(tsdsWithoutLifecycle);
        // the backing indices have an ILM policy, and ILM is preferred, so none of them are managed by the default lifecycle
        DataStream tsdsWithIlm = createDataStream(
            builder,
            "tsds-with-ilm",
            numBackingIndices,
            settings(IndexVersion.current()).put(IndexMetadata.LIFECYCLE_NAME, "ILM_policy"),
            null,
            now
        ).copy().setIndexMode(IndexMode.TIME_SERIES).build();
        builder.put(tsdsWithIlm);
        // a configured lifecycle takes precedence over the default lifecycle
        DataStream tsdsWithDisabledLifecycle = createDataStream(
            builder,
            "tsds-with-disabled-lifecycle",
            numBackingIndices,
            settings(IndexVersion.current()),
            DataStreamLifecycle.dataLifecycleBuilder().enabled(false).build(),
            now
        ).copy().setIndexMode(IndexMode.TIME_SERIES).build();
        builder.put(tsdsWithDisabledLifecycle);
        // the default lifecycle applies only to time series data streams
        DataStream standardWithoutLifecycle = createDataStream(
            builder,
            "standard-without-lifecycle",
            numBackingIndices,
            settings(IndexVersion.current()),
            null,
            now
        );
        builder.put(standardWithoutLifecycle);
        ProjectMetadata project = builder.build();
        when(errorStore.getAllIndices(project.id())).thenReturn(indicesInError);

        {
            GetDataStreamLifecycleStatsAction.Response response = action.collectStats(project, false);
            assertThat(response.getRunDuration(), is(lastRunDuration));
            assertThat(response.getTimeBetweenStarts(), is(timeBetweenStarts));
            assertThat(response.getDataStreamStats().isEmpty(), is(true));
        }

        {
            GetDataStreamLifecycleStatsAction.Response response = action.collectStats(project, true);
            assertThat(response.getRunDuration(), is(lastRunDuration));
            assertThat(response.getTimeBetweenStarts(), is(timeBetweenStarts));
            assertThat(
                response.getDataStreamStats(),
                is(
                    List.of(
                        new GetDataStreamLifecycleStatsAction.Response.DataStreamStats("tsds-with-ilm", 0, 0),
                        new GetDataStreamLifecycleStatsAction.Response.DataStreamStats("tsds-without-lifecycle", numBackingIndices, 1)
                    )
                )
            );
        }
    }
}
