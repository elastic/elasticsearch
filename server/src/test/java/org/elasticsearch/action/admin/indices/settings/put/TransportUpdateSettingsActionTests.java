/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.settings.put;

import org.elasticsearch.Version;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataUpdateSettingsService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
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

    private static final ClusterState CLUSTER_STATE = ClusterState.builder(new ClusterName("test"))
        .metadata(
            Metadata.builder()
                .put(
                    IndexMetadata.builder(".my-system")
                        .system(true)
                        .settings(
                            Settings.builder()
                                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
                                .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                                .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                                .build()
                        )
                        .build(),
                    true
                )
                .build()
        )
        .build();

    private static final String SYSTEM_INDEX_NAME = ".my-system";
    private static final SystemIndices SYSTEM_INDICES = new SystemIndices(
        List.of(
            new SystemIndices.Feature("test-feature", "a test feature", List.of(new SystemIndexDescriptor(SYSTEM_INDEX_NAME + "*", "test")))
        )
    );

    private TransportUpdateSettingsAction action;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext, SYSTEM_INDICES);
        MetadataUpdateSettingsService metadataUpdateSettingsService = mock(MetadataUpdateSettingsService.class);
        this.action = new TransportUpdateSettingsAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            null,
            metadataUpdateSettingsService,
            mock(ActionFilters.class),
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

        action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener);

        ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockListener, times(0)).onResponse(any());
        verify(mockListener, times(1)).onFailure(exceptionArgumentCaptor.capture());

        Exception e = exceptionArgumentCaptor.getValue();
        assertThat(e.getMessage(), equalTo("Cannot set [index.hidden] to 'false' on system indices: [.my-system]"));
    }
}
