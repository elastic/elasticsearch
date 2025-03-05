/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndexDescriptorUtils;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.indices.TestIndexNameExpressionResolver;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportUpdateSettingsActionTests extends ESTestCase {

    private static final String SYSTEM_INDEX_NAME = ".my-system";
    private static final SystemIndices SYSTEM_INDICES = new SystemIndices(
        List.of(
            new SystemIndices.Feature(
                "test-feature",
                "a test feature",
                List.of(SystemIndexDescriptorUtils.createUnmanaged(SYSTEM_INDEX_NAME + "*", "test"))
            )
        )
    );

    private final ProjectId projectId = randomProjectIdOrDefault();

    private TransportUpdateSettingsAction action;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        final var projectResolver = TestProjectResolvers.singleProject(projectId);
        IndexNameExpressionResolver indexNameExpressionResolver = TestIndexNameExpressionResolver.newInstance(
            SYSTEM_INDICES,
            projectResolver
        );
        MetadataUpdateSettingsService metadataUpdateSettingsService = mock(MetadataUpdateSettingsService.class);

        final ThreadPool threadPool = mock(ThreadPool.class);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        this.action = new TransportUpdateSettingsAction(
            transportService,
            mock(ClusterService.class),
            threadPool,
            metadataUpdateSettingsService,
            mock(ActionFilters.class),
            projectResolver,
            indexNameExpressionResolver,
            SYSTEM_INDICES
        );
    }

    public void testSystemIndicesCannotBeSetToVisible() {
        UpdateSettingsRequest request = new UpdateSettingsRequest(
            Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build()
        );
        request.indices(SYSTEM_INDEX_NAME);

        @SuppressWarnings("unchecked")
        ActionListener<AcknowledgedResponse> mockListener = mock(ActionListener.class);

        final ClusterState clusterState = ClusterState.builder(new ClusterName("test"))
            .putProjectMetadata(
                ProjectMetadata.builder(projectId)
                    .put(
                        IndexMetadata.builder(".my-system").system(true).settings(indexSettings(IndexVersion.current(), 1, 0)).build(),
                        true
                    )
                    .build()
            )
            .build();
        action.masterOperation(mock(Task.class), request, clusterState, mockListener);

        ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockListener, times(0)).onResponse(any());
        verify(mockListener, times(1)).onFailure(exceptionArgumentCaptor.capture());

        Exception e = exceptionArgumentCaptor.getValue();
        assertThat(e.getMessage(), equalTo("Cannot set [index.hidden] to 'false' on system indices: [.my-system]"));
    }
}
