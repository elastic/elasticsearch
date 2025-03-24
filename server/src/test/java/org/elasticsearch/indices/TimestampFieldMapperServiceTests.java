/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.indices;

import org.elasticsearch.cluster.ClusterChangedEvent;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.DataStream;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.core.Tuple;
import org.elasticsearch.index.IndexMode;
import org.elasticsearch.index.IndexService;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.index.mapper.DateFieldMapper;
import org.elasticsearch.index.mapper.MapperService;
import org.elasticsearch.node.Node;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TimestampFieldMapperServiceTests extends ESTestCase {

    private ThreadPool threadPool;
    private IndicesService indicesService;
    private TimestampFieldMapperService timestampService;

    @Before
    public void instantiate() {
        threadPool = new TestThreadPool("test");
        indicesService = mock(IndicesService.class);
        timestampService = new TimestampFieldMapperService(
            Settings.builder().put(Node.NODE_NAME_SETTING.getKey(), "node-name").build(),
            threadPool,
            indicesService
        );
    }

    @After
    public void shutdown() throws Exception {
        if (threadPool != null) {
            threadPool.shutdownNow();
        }
        threadPool = null;
    }

    public void testApplyingClusterStateWithMultipleProjects() {
        final var initialState = initialClusterState();
        final var indices = randomList(1, 5, () -> createIndex(true));
        final var metadataBuilder = Metadata.builder(initialState.metadata());
        // Add indices to the builder and set up mocks
        for (IndexMetadata index : indices) {
            var mapperService = mock(MapperService.class);
            when(mapperService.fieldType(DataStream.TIMESTAMP_FIELD_NAME)).thenReturn(new DateFieldMapper.DateFieldType("@timestamp"));
            var indexService = mock(IndexService.class);
            when(indexService.mapperService()).thenReturn(mapperService);
            when(indicesService.indexService(index.getIndex())).thenReturn(indexService);
            metadataBuilder.put(
                metadataBuilder.getProject(randomFrom(initialState.metadata().projects().keySet())).put(index, false).build()
            );
        }
        final var newState = ClusterState.builder(initialState).metadata(metadataBuilder.build()).build();
        final var event = new ClusterChangedEvent("source", newState, initialState);
        timestampService.applyClusterState(event);
        for (IndexMetadata index : indices) {
            assertNotNull(timestampService.getTimestampFieldTypeInfo(index.getIndex()));
        }
    }

    private static ClusterState initialClusterState() {
        final var projects = randomMap(1, 5, () -> {
            ProjectMetadata.Builder builder = ProjectMetadata.builder(randomUniqueProjectId());
            randomList(5, () -> createIndex(randomBoolean())).forEach(index -> builder.put(index, false));
            return Tuple.tuple(builder.getId(), builder.build());
        });
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().projectMetadata(projects).build()).build();
    }

    private static IndexMetadata createIndex(boolean isTimeSeries) {
        return IndexMetadata.builder(randomAlphaOfLength(5))
            .settings(
                indexSettings(IndexVersion.current(), 1, 0).put(IndexSettings.TIME_SERIES_START_TIME.getKey(), "2021-04-28T00:00:00Z")
                    .put(IndexSettings.TIME_SERIES_END_TIME.getKey(), "2021-04-29T00:00:00Z")
                    .put(IndexSettings.MODE.getKey(), isTimeSeries ? IndexMode.TIME_SERIES.getName() : IndexMode.STANDARD.getName())
            )
            .build();
    }
}
