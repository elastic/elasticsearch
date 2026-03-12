/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.action.admin.indices.create;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.alias.Alias;
import org.elasticsearch.action.support.ActionFilters;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.MetadataCreateIndexService;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.cluster.node.DiscoveryNodeRole;
import org.elasticsearch.cluster.node.DiscoveryNodeUtils;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.project.TestProjectResolvers;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.cluster.version.CompatibilityVersions;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.indices.SystemIndices;
import org.elasticsearch.tasks.Task;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.MockUtils;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.transport.TransportService;
import org.junit.Before;
import org.mockito.ArgumentCaptor;

import java.net.InetAddress;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import static org.elasticsearch.cluster.metadata.IndexMetadata.SETTING_INDEX_HIDDEN;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class TransportCreateIndexActionTests extends ESTestCase {

    private static final String UNMANAGED_SYSTEM_INDEX_NAME = ".my-system";
    private static final String MANAGED_SYSTEM_INDEX_NAME = ".my-managed";
    private static final String SYSTEM_ALIAS_NAME = ".my-alias";
    private static final ProjectId PROJECT_ID = ProjectId.fromId("test_project_id");
    private static final ClusterState CLUSTER_STATE = ClusterState.builder(new ClusterName("test"))
        .metadata(Metadata.builder().build())
        .putProjectMetadata(ProjectMetadata.builder(PROJECT_ID).build())
        .nodes(
            DiscoveryNodes.builder()
                .add(
                    DiscoveryNodeUtils.builder("node-1")
                        .name("node-1")
                        .address(new TransportAddress(InetAddress.getLoopbackAddress(), 9300))
                        .roles(Set.of(DiscoveryNodeRole.DATA_ROLE))
                        .build()
                )
                .build()
        )
        .nodeIdsToCompatibilityVersions(
            Map.of(
                "node-1",
                new CompatibilityVersions(
                    TransportVersion.current(),
                    Map.of(MANAGED_SYSTEM_INDEX_NAME + "-primary", new SystemIndexDescriptor.MappingsVersion(1, 1))
                )
            )
        )
        .build();

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
                        .setMappings(String.format(Locale.ROOT, """
                            {
                              "_meta": {
                                "version": "1.0.0",
                                "%s": 0
                              }
                            }"
                            """, SystemIndexDescriptor.VERSION_META_KEY))
                        .setOrigin("origin")
                        .build()
                )
            )
        )
    );

    private MetadataCreateIndexService metadataCreateIndexService;
    private TransportCreateIndexAction action;
    private ThreadContext threadContext;

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadContext = new ThreadContext(Settings.EMPTY);
        final var projectResolver = TestProjectResolvers.usingRequestHeader(threadContext);
        this.metadataCreateIndexService = mock(MetadataCreateIndexService.class);

        final ThreadPool threadPool = mock(ThreadPool.class);
        when(threadPool.getThreadContext()).thenReturn(threadContext);
        TransportService transportService = MockUtils.setupTransportServiceWithThreadpoolExecutor(threadPool);
        ClusterService mockClusterService = mock(ClusterService.class);
        when(mockClusterService.state()).thenReturn(CLUSTER_STATE);
        this.action = new TransportCreateIndexAction(
            transportService,
            mockClusterService,
            threadPool,
            metadataCreateIndexService,
            mock(ActionFilters.class),
            SYSTEM_INDICES,
            projectResolver
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
        verify(metadataCreateIndexService, times(0)).createIndex(
            any(TimeValue.class),
            any(TimeValue.class),
            any(TimeValue.class),
            any(),
            any()
        );
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
        verify(metadataCreateIndexService, times(1)).createIndex(
            any(TimeValue.class),
            any(TimeValue.class),
            any(TimeValue.class),
            createRequestArgumentCaptor.capture(),
            any()
        );

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
        verify(metadataCreateIndexService, times(1)).createIndex(
            any(TimeValue.class),
            any(TimeValue.class),
            any(TimeValue.class),
            createRequestArgumentCaptor.capture(),
            any()
        );

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

    public void testCreateIndexUnderRequestedProject() {
        threadContext.putHeader(Map.of(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, PROJECT_ID.id()));

        @SuppressWarnings("unchecked")
        ActionListener<CreateIndexResponse> mockListener = mock(ActionListener.class);

        CreateIndexRequest request = new CreateIndexRequest();
        request.index(randomFrom(UNMANAGED_SYSTEM_INDEX_NAME, MANAGED_SYSTEM_INDEX_NAME + "-primary"));
        action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener);
        ArgumentCaptor<CreateIndexClusterStateUpdateRequest> createRequestArgumentCaptor = ArgumentCaptor.forClass(
            CreateIndexClusterStateUpdateRequest.class
        );
        verify(mockListener, times(0)).onFailure(any());
        verify(metadataCreateIndexService, times(1)).createIndex(
            any(TimeValue.class),
            any(TimeValue.class),
            any(TimeValue.class),
            createRequestArgumentCaptor.capture(),
            any()
        );

        CreateIndexClusterStateUpdateRequest processedRequest = createRequestArgumentCaptor.getValue();
        assertThat(processedRequest.projectId(), is(PROJECT_ID));
    }

    public void testCreateIndexUnderMissingProject() {
        threadContext.putHeader(Map.of(Task.X_ELASTIC_PROJECT_ID_HTTP_HEADER, "unknown_project_id"));

        @SuppressWarnings("unchecked")
        ActionListener<CreateIndexResponse> mockListener = mock(ActionListener.class);

        CreateIndexRequest request = new CreateIndexRequest();
        request.index(randomFrom(UNMANAGED_SYSTEM_INDEX_NAME, MANAGED_SYSTEM_INDEX_NAME + "-primary"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener)
        );
        assertThat(e.getMessage(), containsString("Could not find project with id [unknown_project_id]"));
    }

    public void testCreatingSystemIndexForMigration() {
        CreateIndexRequest request = new CreateIndexRequest();
        String path = "/test";  // just to test that we pass settings
        Settings settings = Settings.builder().put(SETTING_INDEX_HIDDEN, true).put(IndexMetadata.SETTING_DATA_PATH, path).build();
        request.index(MANAGED_SYSTEM_INDEX_NAME + SystemIndices.UPGRADED_INDEX_SUFFIX)
            .cause(SystemIndices.MIGRATE_SYSTEM_INDEX_CAUSE)
            .settings(settings);

        @SuppressWarnings("unchecked")
        ActionListener<CreateIndexResponse> mockListener = mock(ActionListener.class);

        action.masterOperation(mock(Task.class), request, CLUSTER_STATE, mockListener);

        ArgumentCaptor<CreateIndexClusterStateUpdateRequest> createRequestArgumentCaptor = ArgumentCaptor.forClass(
            CreateIndexClusterStateUpdateRequest.class
        );
        verify(mockListener, times(0)).onFailure(any());
        verify(metadataCreateIndexService, times(1)).createIndex(
            any(TimeValue.class),
            any(TimeValue.class),
            any(TimeValue.class),
            createRequestArgumentCaptor.capture(),
            any()
        );

        CreateIndexClusterStateUpdateRequest processedRequest = createRequestArgumentCaptor.getValue();
        assertTrue(processedRequest.settings().getAsBoolean(SETTING_INDEX_HIDDEN, false));
        assertThat(processedRequest.settings().get(IndexMetadata.SETTING_DATA_PATH, ""), is(path));
    }
}
