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
import org.elasticsearch.cluster.metadata.DataStreamGlobalRetentionSettings;
import org.elasticsearch.cluster.metadata.DataStreamLifecycle;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.elasticsearch.cluster.metadata.DataStreamTestHelper.newInstance;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TransportExplainDataStreamLifecycleActionTests extends ESTestCase {

    private TransportExplainDataStreamLifecycleAction testAction;
    private final DataStreamGlobalRetentionSettings globalRetentionSettings = DataStreamGlobalRetentionSettings.create(
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
            globalRetentionSettings,
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
}
