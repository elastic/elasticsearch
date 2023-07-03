/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.IndexNameExpressionResolver;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.settings.PublicSettings;
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

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class TransportCreateIndexActionTests extends ESTestCase {

    private static final ClusterState CLUSTER_STATE = ClusterState.builder(new ClusterName("test"))
        .metadata(Metadata.builder().build())
        .build();

    private static final String UNMANAGED_SYSTEM_INDEX_NAME = ".my-system";
    private static final String MANAGED_SYSTEM_INDEX_NAME = ".my-managed";
    private static final String SYSTEM_ALIAS_NAME = ".my-alias";
    private static final SystemIndices SYSTEM_INDICES = new SystemIndices(
        List.of(
            new SystemIndices.Feature(
                "test-feature",
                "a test feature",
                List.of(
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(UNMANAGED_SYSTEM_INDEX_NAME + "*")
                        .setType(SystemIndexDescriptor.Type.INTERNAL_UNMANAGED)
                        .setAliasName(SYSTEM_ALIAS_NAME)
                        .build(),
                    SystemIndexDescriptor.builder()
                        .setIndexPattern(MANAGED_SYSTEM_INDEX_NAME + "*")
                        .setPrimaryIndex(MANAGED_SYSTEM_INDEX_NAME + "-primary")
                        .setType(SystemIndexDescriptor.Type.INTERNAL_MANAGED)
                        .setSettings(SystemIndexDescriptor.DEFAULT_SETTINGS)
                        .setMappings("{\"_meta\":  {\"version\":  \"1.0.0\"}}")
                        .setVersionMetaKey("version")
                        .setOrigin("origin")
                        .build()
                )
            )
        )
    );

    private MetadataCreateIndexService metadataCreateIndexService;
    private TransportCreateIndexAction action;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        IndexNameExpressionResolver indexNameExpressionResolver = new IndexNameExpressionResolver(threadContext, SYSTEM_INDICES);
        this.metadataCreateIndexService = mock(MetadataCreateIndexService.class);
        this.action = new TransportCreateIndexAction(
            mock(TransportService.class),
            mock(ClusterService.class),
            null,
            metadataCreateIndexService,
            mock(ActionFilters.class),
            indexNameExpressionResolver,
            SYSTEM_INDICES,
            new PublicSettings.DefaultPublicSettings()
        );
    }

    public void testSystemIndicesCannotBeCreatedUnhidden() {
        CreateIndexRequest request = new CreateIndexRequest();
        request.settings(Settings.builder().put(IndexMetadata.SETTING_INDEX_HIDDEN, false).build());
        request.index(UNMANAGED_SYSTEM_INDEX_NAME);

        @SuppressWarnings("unchecked")
        ActionListener<CreateIndexResponse> mockListener = mock(ActionListener.class);

        action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener);

        ArgumentCaptor<Exception> exceptionArgumentCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(mockListener, times(0)).onResponse(any());
        verify(metadataCreateIndexService, times(0)).createIndex(any(), any());
        verify(mockListener, times(1)).onFailure(exceptionArgumentCaptor.capture());

        Exception e = exceptionArgumentCaptor.getValue();
        assertThat(e.getMessage(), equalTo("Cannot create system index [.my-system] with [index.hidden] set to 'false'"));
    }

    public void testSystemIndicesCreatedHiddenByDefault() {
        CreateIndexRequest request = new CreateIndexRequest();
        request.index(UNMANAGED_SYSTEM_INDEX_NAME);

        @SuppressWarnings("unchecked")
        ActionListener<CreateIndexResponse> mockListener = mock(ActionListener.class);

        action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener);

        ArgumentCaptor<CreateIndexClusterStateUpdateRequest> createRequestArgumentCaptor = ArgumentCaptor.forClass(
            CreateIndexClusterStateUpdateRequest.class
        );
        verify(mockListener, times(0)).onFailure(any());
        verify(metadataCreateIndexService, times(1)).createIndex(createRequestArgumentCaptor.capture(), any());

        CreateIndexClusterStateUpdateRequest processedRequest = createRequestArgumentCaptor.getValue();
        assertTrue(processedRequest.settings().getAsBoolean(SETTING_INDEX_HIDDEN, false));
    }

    public void testSystemAliasCreatedHiddenByDefault() {
        CreateIndexRequest request = new CreateIndexRequest();
        request.index(UNMANAGED_SYSTEM_INDEX_NAME);
        request.alias(new Alias(SYSTEM_ALIAS_NAME));

        @SuppressWarnings("unchecked")
        ActionListener<CreateIndexResponse> mockListener = mock(ActionListener.class);

        action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener);

        ArgumentCaptor<CreateIndexClusterStateUpdateRequest> createRequestArgumentCaptor = ArgumentCaptor.forClass(
            CreateIndexClusterStateUpdateRequest.class
        );
        verify(mockListener, times(0)).onFailure(any());
        verify(metadataCreateIndexService, times(1)).createIndex(createRequestArgumentCaptor.capture(), any());

        CreateIndexClusterStateUpdateRequest processedRequest = createRequestArgumentCaptor.getValue();
        assertTrue(processedRequest.aliases().contains(new Alias(SYSTEM_ALIAS_NAME).isHidden(true)));
    }

    public void testErrorWhenCreatingNonPrimarySystemIndex() {
        CreateIndexRequest request = new CreateIndexRequest();
        request.index(MANAGED_SYSTEM_INDEX_NAME + "-alternate");

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.masterOperation(mock(Task.class), request, CLUSTER_STATE, ActionListener.noop())
        );

        assertThat(
            e.getMessage(),
            equalTo("Cannot create system index with name .my-managed-alternate; descriptor primary index is .my-managed-primary")
        );
    }

}
