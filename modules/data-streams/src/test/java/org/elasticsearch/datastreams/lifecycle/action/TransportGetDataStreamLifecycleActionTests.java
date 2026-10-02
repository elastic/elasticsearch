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
import org.elasticsearch.action.datastreams.lifecycle.GetDataStreamLifecycleAction;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ProjectState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetention;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
import org.elasticsearch.cluster.metadata.DataStreamLifecycleSettings;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.ClusterSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Nullable;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.CancellableTask;
import org.elasticsearch.tasks.TaskId;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.transport.TransportService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class TransportGetDataStreamLifecycleActionTests extends ESTestCase {

    /**
     * Time series data streams without a configured lifecycle are managed by the default lifecycle only when the default lifecycle for
     * time series is enabled, in which case they are reported as having the lifecycle enabled by default. The default lifecycle is not
     * reported as a configured lifecycle. Data streams with a configured lifecycle and non time series data streams are not affected.
     */
    public void testDefaultLifecycleForTimeSeries() {
        DataStreamLifecycle configuredLifecycle = DataStreamLifecycle.dataLifecycleBuilder()
            .enabled(randomBoolean())
            .dataRetention(TimeValue.timeValueDays(30))
            .build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addDataStream(builder, "tsds-without-lifecycle", IndexMode.TIME_SERIES, null);
        addDataStream(builder, "tsds-with-lifecycle", IndexMode.TIME_SERIES, configuredLifecycle);
        addDataStream(builder, "standard-without-lifecycle", randomFrom(IndexMode.STANDARD, IndexMode.LOGSDB, null), null);
        ProjectMetadata projectMetadata = builder.build();
        ProjectState projectState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadata)
            .build()
            .projectState(projectMetadata.id());

        for (boolean defaultLifecycleForTimeSeriesEnabled : new boolean[] { false, true }) {
            GetDataStreamLifecycleAction.Response response = getDataStreamLifecycle(
                defaultLifecycleForTimeSeriesEnabled,
                Settings.EMPTY,
                projectState
            );
            assertThat(
                response.getDataStreamLifecycles(),
                equalTo(
                    List.of(
                        new GetDataStreamLifecycleAction.Response.DataStreamLifecycle("standard-without-lifecycle", null, false, false),
                        new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(
                            "tsds-with-lifecycle",
                            configuredLifecycle,
                            false,
                            false
                        ),
                        new GetDataStreamLifecycleAction.Response.DataStreamLifecycle(
                            "tsds-without-lifecycle",
                            null,
                            false,
                            defaultLifecycleForTimeSeriesEnabled
                        )
                    )
                )
            );
        }
    }

    /**
     * The global retention is reported at the response level regardless of the default lifecycle for time series, but the data streams
     * managed by the default lifecycle have no configured lifecycle, so no effective retention is reported for them.
     */
    public void testGlobalRetentionWithDefaultLifecycleForTimeSeries() {
        TimeValue globalDefaultRetention = TimeValue.timeValueDays(10);
        TimeValue globalMaxRetention = TimeValue.timeValueDays(50);
        Settings globalRetentionSettings = Settings.builder()
            .put(DataStreamLifecycleSettings.DATA_STREAMS_DEFAULT_RETENTION_SETTING.getKey(), globalDefaultRetention)
            .put(DataStreamLifecycleSettings.DATA_STREAMS_MAX_RETENTION_SETTING.getKey(), globalMaxRetention)
            .build();
        ProjectMetadata.Builder builder = ProjectMetadata.builder(randomProjectIdOrDefault());
        addDataStream(builder, "tsds-without-lifecycle", IndexMode.TIME_SERIES, null);
        ProjectMetadata projectMetadata = builder.build();
        ProjectState projectState = ClusterState.builder(ClusterName.DEFAULT)
            .putProjectMetadata(projectMetadata)
            .build()
            .projectState(projectMetadata.id());

        GetDataStreamLifecycleAction.Response response = getDataStreamLifecycle(true, globalRetentionSettings, projectState);
        assertThat(response.getGlobalRetention(), equalTo(new DataStreamGlobalRetention(globalDefaultRetention, globalMaxRetention)));
        assertThat(
            response.getDataStreamLifecycles(),
            equalTo(List.of(new GetDataStreamLifecycleAction.Response.DataStreamLifecycle("tsds-without-lifecycle", null, false, true)))
        );
    }

    private static void addDataStream(
        ProjectMetadata.Builder builder,
        String dataStreamName,
        @Nullable IndexMode indexMode,
        @Nullable DataStreamLifecycle lifecycle
    ) {
        // Only the index mode of the data stream determines if the default lifecycle applies, so the backing index uses the standard
        // index mode to avoid having to configure the time series bounds.
        IndexMetadata backingIndex = IndexMetadata.builder(DataStream.getDefaultBackingIndexName(dataStreamName, 1))
            .settings(settings(IndexVersion.current()))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        builder.put(backingIndex, false);
        builder.put(
            DataStream.builder(dataStreamName, List.of(backingIndex.getIndex()))
                .setGeneration(1)
                .setIndexMode(indexMode)
                .setLifecycle(lifecycle)
                .build()
        );
    }

    private static GetDataStreamLifecycleAction.Response getDataStreamLifecycle(
        boolean defaultLifecycleForTimeSeriesEnabled,
        Settings globalRetentionSettings,
        ProjectState projectState
    ) {
        // The default lifecycle for time series cannot be enabled via the cluster settings yet, so we spy on real settings and stub
        // only this method. This should be replaced with the cluster setting once it is available.
        DataStreamLifecycleSettings dataStreamLifecycleSettings = spy(
            DataStreamLifecycleSettings.create(ClusterSettings.createBuiltInClusterSettings(globalRetentionSettings))
        );
        when(dataStreamLifecycleSettings.defaultLifecycleForTimeSeriesEnabled()).thenReturn(defaultLifecycleForTimeSeriesEnabled);
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getClusterSettings()).thenReturn(ClusterSettings.createBuiltInClusterSettings());
        TransportGetDataStreamLifecycleAction action = new TransportGetDataStreamLifecycleAction(
            mock(TransportService.class),
            clusterService,
            mock(ActionFilters.class),
            TestProjectResolvers.alwaysThrow(),
            TestIndexNameExpressionResolver.newInstance(),
            dataStreamLifecycleSettings
        );

        GetDataStreamLifecycleAction.Request request = new GetDataStreamLifecycleAction.Request(TEST_REQUEST_TIMEOUT, new String[] { "*" });
        AtomicReference<GetDataStreamLifecycleAction.Response> responseRef = new AtomicReference<>();
        action.localClusterStateOperation(
            new CancellableTask(
                randomNonNegativeLong(),
                "test",
                GetDataStreamLifecycleAction.INSTANCE.name(),
                "",
                TaskId.EMPTY_TASK_ID,
                Map.of()
            ),
            request,
            projectState,
            ActionListener.wrap(responseRef::set, e -> fail(e.getMessage()))
        );
        GetDataStreamLifecycleAction.Response response = responseRef.get();
        assertThat(response, is(notNullValue()));
        return response;
    }
}
