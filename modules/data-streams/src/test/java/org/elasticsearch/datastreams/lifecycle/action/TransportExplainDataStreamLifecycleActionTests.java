/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.datastreams.lifecycle.action;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.rollover.RolloverInfo;
import org.elasticsearch.action.datastreams.lifecycle.ExplainDataStreamLifecycleAction;
import org.elasticsearch.action.datastreams.lifecycle.ExplainIndexDataStreamLifecycle;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
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
import org.elasticsearch.datastreams.lifecycle.FrozenTransitionInfoProvider;
import org.elasticsearch.dlm.DataStreamLifecycleErrorStore;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportExplainDataStreamLifecycleActionTests extends ESTestCase {

    private TransportExplainDataStreamLifecycleAction testAction;
    private final DataStreamLifecycleSettings dataStreamLifecycleSettings = DataStreamLifecycleSettings.create(
        ClusterSettings.createBuiltInClusterSettings()
    );

    @Before
    public void setUpAction() {
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        testAction = new TransportExplainDataStreamLifecycleAction(
            mock(TransportService.class),
            clusterService,
            mock(ThreadPool.class),
            mock(ActionFilters.class),
            TestProjectResolvers.alwaysThrow(),
            TestIndexNameExpressionResolver.newInstance(),
            mock(DataStreamLifecycleErrorStore.class),
            dataStreamLifecycleSettings,
            FrozenTransitionInfoProvider.noop()
        );
    }

    public void testLookupIndicesAreSkipped() throws Exception {
        String dataStreamName = "test-data-stream";
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        long now = System.currentTimeMillis();

        // Rolled-over backing index — managed by lifecycle
        IndexMetadata regularIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3000L)
            .putRolloverInfo(new RolloverInfo(dataStreamName, List.of(), now - 2000L))
            .build();
        builder.put(regularIndex, false);

        // Backing index with LOOKUP mode — must be reported as not managed
        IndexMetadata lookupIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 2))
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName())
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3000L)
            .putRolloverInfo(new RolloverInfo(dataStreamName, List.of(), now - 2000L))
            .build();
        builder.put(lookupIndex, false);

        // Current write index — managed by lifecycle
        IndexMetadata writeIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 3))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 1000L)
            .build();
        builder.put(writeIndex, false);

        List<Index> backingIndices = new ArrayList<>();
        backingIndices.add(regularIndex.getIndex());
        backingIndices.add(lookupIndex.getIndex());
        backingIndices.add(writeIndex.getIndex());

        DataStream dataStream = newInstance(
            dataStreamName,
            backingIndices,
            3,
            Map.of(),
            false,
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(30)).build()
        );
        builder.put(dataStream);

        ProjectMetadata projectMetadata = builder.build();
        ProjectState projectState = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(projectMetadata)
            .build()
            .projectState(projectMetadata.id());

        ExplainDataStreamLifecycleAction.Request request = new ExplainDataStreamLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { regularIndex.getIndex().getName(), lookupIndex.getIndex().getName(), writeIndex.getIndex().getName() }
        );

        AtomicReference<ExplainDataStreamLifecycleAction.Response> responseRef = new AtomicReference<>();
        testAction.masterOperation(
            mock(Task.class),
            request,
            projectState,
            ActionListener.wrap(responseRef::set, e -> fail(e.getMessage()))
        );

        ExplainDataStreamLifecycleAction.Response response = responseRef.get();
        assertNotNull(response);
        assertThat(response.getIndices().size(), equalTo(3));

        for (ExplainIndexDataStreamLifecycle explain : response.getIndices()) {
            boolean isLookup = explain.getIndex().equals(lookupIndex.getIndex().getName());
            assertThat(
                "lookup index should not be managed by lifecycle, regular and write indices should be",
                explain.isManagedByLifecycle(),
                is(isLookup == false)
            );
        }
    }

    public void testLookupWriteIndexIsSkipped() throws Exception {
        String dataStreamName = "test-data-stream";
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        long now = System.currentTimeMillis();

        // Current write index — managed by lifecycle
        IndexMetadata writeIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 3))
            .settings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, IndexVersion.current())
                    .put(IndexSettings.MODE.getKey(), IndexMode.LOOKUP.getName())
            )
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 1000L)
            .build();
        builder.put(writeIndex, false);

        List<Index> backingIndices = new ArrayList<>();
        backingIndices.add(writeIndex.getIndex());

        DataStream dataStream = newInstance(
            dataStreamName,
            backingIndices,
            3,
            Map.of(),
            false,
            DataStreamLifecycle.dataLifecycleBuilder().dataRetention(TimeValue.timeValueDays(30)).build()
        );
        builder.put(dataStream);

        ProjectMetadata projectMetadata = builder.build();
        ProjectState projectState = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(projectMetadata)
            .build()
            .projectState(projectMetadata.id());

        ExplainDataStreamLifecycleAction.Request request = new ExplainDataStreamLifecycleAction.Request(
            TEST_REQUEST_TIMEOUT,
            new String[] { writeIndex.getIndex().getName() }
        );

        AtomicReference<ExplainDataStreamLifecycleAction.Response> responseRef = new AtomicReference<>();
        testAction.masterOperation(
            mock(Task.class),
            request,
            projectState,
            ActionListener.wrap(responseRef::set, e -> fail(e.getMessage()))
        );

        ExplainDataStreamLifecycleAction.Response response = responseRef.get();
        assertNotNull(response);
        assertThat(response.getIndices().size(), equalTo(1));

        for (ExplainIndexDataStreamLifecycle explain : response.getIndices()) {
            assertThat(
                "lookup index should not be managed by lifecycle, regular and write indices should be",
                explain.isManagedByLifecycle(),
                is(false)
            );
        }

        // Access via the data stream name
        request = new ExplainDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { dataStreamName });
        responseRef = new AtomicReference<>();
        testAction.masterOperation(
            mock(Task.class),
            request,
            projectState,
            ActionListener.wrap(responseRef::set, e -> fail(e.getMessage()))
        );

        response = responseRef.get();
        assertNotNull(response);
        assertThat(response.getIndices().size(), equalTo(1));

        for (ExplainIndexDataStreamLifecycle explain : response.getIndices()) {
            assertThat(
                "lookup index should not be managed by lifecycle, regular and write indices should be",
                explain.isManagedByLifecycle(),
                is(false)
            );
        }
    }

    /**
     * Time series data streams without a configured lifecycle are managed by the default lifecycle only when the default lifecycle for
     * time series is enabled. Their indices are then reported as managed, with the lifecycle enabled by default and without a configured
     * lifecycle. Data streams with a configured lifecycle and non time series data streams are not affected.
     */
    public void testDefaultLifecycleForTimeSeries() throws Exception {
        long now = System.currentTimeMillis();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());

        String tsdsName = "tsds-without-lifecycle";
        IndexMetadata tsdsRolledOverIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(tsdsName, 1))
            .settings(timeSeriesSettings(now - 7200_000L, now - 3600_000L))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 7200_000L)
            .putRolloverInfo(new RolloverInfo(tsdsName, List.of(), now - 3600_000L))
            .build();
        builder.put(tsdsRolledOverIndex, false);
        IndexMetadata tsdsWriteIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(tsdsName, 2))
            .settings(timeSeriesSettings(now - 3600_000L, now + 3600_000L))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3600_000L)
            .build();
        builder.put(tsdsWriteIndex, false);
        builder.put(
            newInstance(tsdsName, List.of(tsdsRolledOverIndex.getIndex(), tsdsWriteIndex.getIndex()), 2, Map.of(), false, null).copy()
                .setIndexMode(IndexMode.TIME_SERIES)
                .build()
        );

        String tsdsWithLifecycleName = "tsds-with-lifecycle";
        DataStreamLifecycle configuredLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .dataRetention(TimeValue.timeValueDays(30))
            .build();
        IndexMetadata tsdsWithLifecycleIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(tsdsWithLifecycleName, 1))
            .settings(timeSeriesSettings(now - 3600_000L, now + 3600_000L))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3600_000L)
            .build();
        builder.put(tsdsWithLifecycleIndex, false);
        builder.put(
            newInstance(tsdsWithLifecycleName, List.of(tsdsWithLifecycleIndex.getIndex()), 1, Map.of(), false, configuredLifecycle).copy()
                .setIndexMode(IndexMode.TIME_SERIES)
                .build()
        );

        String standardName = "standard-without-lifecycle";
        IndexMetadata standardIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(standardName, 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .creationDate(now - 3600_000L)
            .build();
        builder.put(standardIndex, false);
        builder.put(newInstance(standardName, List.of(standardIndex.getIndex()), 1, Map.of(), false, null));

        ProjectMetadata projectMetadata = builder.build();
        ProjectState projectState = ClusterState.builder(new ClusterName("_name"))
            .putProjectMetadata(projectMetadata)
            .build()
            .projectState(projectMetadata.id());

        for (boolean defaultLifecycleForTimeSeriesEnabled : new boolean[] { false, true }) {
            // The default lifecycle for time series cannot be enabled via the cluster settings yet, so we spy on real settings and stub
            // only this method. This should be replaced with the cluster setting once it is available.
            dataStreamLifecycleSettings.setDefaultLifecycleForTimeSeriesEnabled(defaultLifecycleForTimeSeriesEnabled);

            ExplainDataStreamLifecycleAction.Request request = new ExplainDataStreamLifecycleAction.Request(
                TEST_REQUEST_TIMEOUT,
                new String[] { tsdsName, tsdsWithLifecycleName, standardName }
            );
            AtomicReference<ExplainDataStreamLifecycleAction.Response> responseRef = new AtomicReference<>();
            testAction.masterOperation(
                mock(Task.class),
                request,
                projectState,
                ActionListener.wrap(responseRef::set, e -> fail(e.getMessage()))
            );

            ExplainDataStreamLifecycleAction.Response response = responseRef.get();
            assertNotNull(response);
            Map<String, ExplainIndexDataStreamLifecycle> explainByIndex = response.getIndices()
                .stream()
                .collect(Collectors.toMap(ExplainIndexDataStreamLifecycle::getIndex, Function.identity()));
            assertThat(explainByIndex.size(), equalTo(4));

            for (IndexMetadata tsdsIndex : List.of(tsdsRolledOverIndex, tsdsWriteIndex)) {
                ExplainIndexDataStreamLifecycle explain = explainByIndex.get(tsdsIndex.getIndex().getName());
                assertThat(explain.isManagedByLifecycle(), is(defaultLifecycleForTimeSeriesEnabled));
                assertThat(explain.isLifecycleEnabledByDefault(), is(defaultLifecycleForTimeSeriesEnabled));
                // the default lifecycle is not reported as a configured lifecycle
                assertThat(explain.getLifecycle(), is(nullValue()));
            }
            if (defaultLifecycleForTimeSeriesEnabled) {
                ExplainIndexDataStreamLifecycle explain = explainByIndex.get(tsdsRolledOverIndex.getIndex().getName());
                assertThat(explain.getIndexCreationDate(), is(tsdsRolledOverIndex.getCreationDate()));
                assertThat(explain.getRolloverDate(), is(now - 3600_000L));
            }

            ExplainIndexDataStreamLifecycle withLifecycle = explainByIndex.get(tsdsWithLifecycleIndex.getIndex().getName());
            assertThat(withLifecycle.isManagedByLifecycle(), is(true));
            assertThat(withLifecycle.isLifecycleEnabledByDefault(), is(false));
            assertThat(withLifecycle.getLifecycle(), equalTo(configuredLifecycle));

            ExplainIndexDataStreamLifecycle standard = explainByIndex.get(standardIndex.getIndex().getName());
            assertThat(standard.isManagedByLifecycle(), is(false));
            assertThat(standard.isLifecycleEnabledByDefault(), is(false));
        }
    }

    private static Settings timeSeriesSettings(long startTimeMillis, long endTimeMillis) {
        return settings(IndexVersion.current()).put(IndexSettings.MODE.getKey(), IndexMode.TIME_SERIES.getName())
            .put(IndexMetadata.INDEX_ROUTING_PATH.getKey(), "host")
            .put(IndexSettings.TIME_SERIES_START_TIME.getKey(), Instant.ofEpochMilli(startTimeMillis).toString())
            .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), Instant.ofEpochMilli(endTimeMillis).toString())
            .build();
    }
}
