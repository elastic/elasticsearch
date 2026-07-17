/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.inference;

import org.elasticsearch.ResourceAlreadyExistsException;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest;
import org.elasticsearch.action.admin.indices.create.CreateIndexResponse;
import org.elasticsearch.action.admin.indices.create.TransportCreateIndexAction;
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.elasticsearch.action.admin.indices.mapping.put.TransportPutMappingAction;
import org.elasticsearch.action.support.master.AcknowledgedResponse;
import org.elasticsearch.client.internal.Client;
import org.elasticsearch.cluster.ClusterName;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.AliasMetadata;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.MappingMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.ProjectId;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.index.IndexVersion;
import org.elasticsearch.indices.SystemIndexDescriptor;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.threadpool.ThreadPool;
import org.elasticsearch.xcontent.XContentType;
import org.mockito.ArgumentCaptor;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class InferenceIndexMappingManagerTests extends ESTestCase {

    private Client mockClient;
    private SystemIndexDescriptor descriptor;

    @Override
    public void setUp() throws Exception {
        super.setUp();

        // Client is mocked — see class Javadoc for the rationale.
        mockClient = mock(Client.class);
        ThreadPool mockThreadPool = mock(ThreadPool.class);
        when(mockClient.threadPool()).thenReturn(mockThreadPool);
        when(mockThreadPool.getThreadContext()).thenReturn(new ThreadContext(Settings.EMPTY));
        when(mockClient.settings()).thenReturn(Settings.EMPTY);

        descriptor = InferencePlugin.createInferenceIndexDescriptor(InferenceIndex.settings());
    }

    /** Returns a cluster state that contains no {@code .inference} index. */
    private static ClusterState emptyClusterState() {
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();
    }

    /**
     * Returns a cluster state where the concrete index {@code .inference} exists and carries the
     * supplied mappings JSON.
     */
    private static ClusterState clusterStateWithIndex(String indexName, String mappingsJson) {
        IndexMetadata indexMeta = IndexMetadata.builder(indexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(mappingsJson)))
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(indexMeta, false).build();
        return ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> jsonToMap(String json) {
        Map<String, Object> doc = (Map<String, Object>) XContentHelper.convertToMap(new BytesArray(json), false, XContentType.JSON)
            .v2()
            .get("_doc");
        assertNotNull("mapping JSON must have a top-level '_doc' key", doc);
        return doc;
    }

    public void testIndexDoesNotExist_createsIndex() {
        ClusterState clusterState = emptyClusterState();
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<CreateIndexResponse>> capturedListeners = new ArrayList<>();

        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("CreateIndex should have been requested exactly once", capturedListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        // Verify the CreateIndexRequest was issued with the descriptor's mappings and settings.
        ArgumentCaptor<CreateIndexRequest> createCaptor = ArgumentCaptor.forClass(CreateIndexRequest.class);
        verify(mockClient).execute(eq(TransportCreateIndexAction.TYPE), createCaptor.capture(), any());
        CreateIndexRequest capturedRequest = createCaptor.getValue();
        assertThat(
            "CreateIndexRequest must carry the descriptor's mappings",
            jsonToMap(capturedRequest.mappings()),
            equalTo(jsonToMap(descriptor.getMappings()))
        );
        assertThat(
            "CreateIndexRequest must carry the descriptor's settings",
            capturedRequest.settings(),
            equalTo(descriptor.getSettings())
        );

        // Simulate an acknowledged create response.
        capturedListeners.get(0).onResponse(new CreateIndexResponse(true, true, InferenceIndex.INDEX_NAME));

        assertTrue("Caller listener must be notified after successful create", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testIndexAtCurrentVersion_immediateCallbackNoIO() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV4());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must be notified synchronously on the fast path", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
        verify(mockClient, never()).execute(any(), any(), any());
    }

    public void testIndexWithOutdatedMappings_putsMappings() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> capturedListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should have been requested exactly once", capturedListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        // Verify the PutMappingRequest was issued with the descriptor's latest mappings.
        ArgumentCaptor<PutMappingRequest> putMappingCaptor = ArgumentCaptor.forClass(PutMappingRequest.class);
        verify(mockClient).execute(eq(TransportPutMappingAction.TYPE), putMappingCaptor.capture(), any());
        assertThat(
            "PutMappingRequest must carry the descriptor's latest mappings",
            jsonToMap(putMappingCaptor.getValue().source()),
            equalTo(jsonToMap(descriptor.getMappings()))
        );

        // Simulate an acknowledged put-mapping response.
        capturedListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
        // Verify no CreateIndex was issued.
        verify(mockClient, never()).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
    }

    public void testResourceAlreadyExistsOnCreate_fallsThroughToPutMapping() {
        ClusterState clusterState = emptyClusterState();
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> putMappingListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<CreateIndexResponse> listener = invocation.getArgument(2);
            listener.onFailure(new ResourceAlreadyExistsException(InferenceIndex.INDEX_NAME));
            return null;
        }).when(mockClient).execute(eq(TransportCreateIndexAction.TYPE), any(), any());
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            putMappingListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should be issued after ResourceAlreadyExistsException", putMappingListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        putMappingListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testPutMappingFailure_propagatesToListener() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        RuntimeException mappingError = new RuntimeException("simulated put-mapping failure");
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            listener.onFailure(mappingError);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must receive the failure", callerListener.completed);
        assertSame("The original exception must be forwarded unchanged", mappingError, callerListener.failure);
    }

    public void testAliasResolution_currentMappings_immediateCallback() {
        // Simulates: ".inference" is an alias pointing to ".inference-reindexed-for-10"
        // (the pattern produced by SystemIndexMigrator).
        String migratedIndexName = ".inference-reindexed-for-10";
        IndexMetadata migratedIndex = IndexMetadata.builder(migratedIndexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(InferenceIndex.mappingsV4())))
            .putAlias(AliasMetadata.builder(InferenceIndex.INDEX_NAME).build())
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(migratedIndex, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();

        // Confirm the alias setup: projectMetadata.index(".inference") must return null so
        // InferenceIndexMappingManager falls through to the indicesLookup resolution path.
        assertNull("'.inference' should not resolve as a concrete index name", project.index(InferenceIndex.INDEX_NAME));

        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertTrue("Caller listener must be notified synchronously — mappings are already current", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
        verify(mockClient, never()).execute(any(), any(), any());
    }

    public void testAliasResolution_staleMappings_putsMappings() {
        // Simulates: ".inference" is an alias pointing to ".inference-reindexed-for-10"
        // (the pattern produced by SystemIndexMigrator), but the concrete index still
        // has v3 (stale) mappings — e.g. the migration happened before the mapping upgrade.
        String migratedIndexName = ".inference-reindexed-for-10";
        IndexMetadata migratedIndex = IndexMetadata.builder(migratedIndexName)
            .settings(indexSettings(IndexVersion.current(), 1, 0))
            .putMapping(new MappingMetadata("_doc", jsonToMap(InferenceIndex.mappingsV3())))
            .putAlias(AliasMetadata.builder(InferenceIndex.INDEX_NAME).build())
            .build();
        var project = ProjectMetadata.builder(ProjectId.DEFAULT).put(migratedIndex, false).build();
        ClusterState clusterState = ClusterState.builder(ClusterName.DEFAULT).metadata(Metadata.builder().put(project).build()).build();

        // Confirm the alias setup: projectMetadata.index(".inference") must return null so
        // InferenceIndexMappingManager falls through to the indicesLookup resolution path.
        assertNull("'.inference' should not resolve as a concrete index name", project.index(InferenceIndex.INDEX_NAME));

        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> capturedListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener callerListener = new TestActionListener();
        manager.withUpToDateMappings(clusterState, callerListener);

        assertThat("PutMapping should have been requested exactly once", capturedListeners, hasSize(1));
        assertFalse("Caller listener should not be notified until the I/O completes", callerListener.completed);

        // Verify no CreateIndex was issued — the index exists (behind the alias), only mappings need updating.
        verify(mockClient, never()).execute(eq(TransportCreateIndexAction.TYPE), any(), any());

        // Simulate an acknowledged put-mapping response.
        capturedListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller listener must be notified after successful put-mapping", callerListener.completed);
        assertNull("No failure should be reported", callerListener.failure);
    }

    public void testConcurrentCalls_singleUpdateNotifiesAllCallers() {
        ClusterState clusterState = clusterStateWithIndex(InferenceIndex.INDEX_NAME, InferenceIndex.mappingsV3());
        InferenceIndexMappingManager manager = new InferenceIndexMappingManager(mockClient, descriptor);

        List<ActionListener<AcknowledgedResponse>> capturedListeners = new ArrayList<>();
        doAnswer(invocation -> {
            @SuppressWarnings("unchecked")
            ActionListener<AcknowledgedResponse> listener = invocation.getArgument(2);
            capturedListeners.add(listener);
            return null;
        }).when(mockClient).execute(eq(TransportPutMappingAction.TYPE), any(), any());

        TestActionListener caller1 = new TestActionListener();
        TestActionListener caller2 = new TestActionListener();

        // First call acquires the in-flight guard and issues the PutMapping request.
        manager.withUpToDateMappings(clusterState, caller1);
        // Second call arrives while the update is in-flight; it must be queued, not issue a new request.
        manager.withUpToDateMappings(clusterState, caller2);

        assertThat("Exactly one PutMapping must be issued for both concurrent callers", capturedListeners, hasSize(1));
        assertFalse("Caller 1 must not be notified yet", caller1.completed);
        assertFalse("Caller 2 must not be notified yet", caller2.completed);

        // Completing the single in-flight update must notify both callers.
        capturedListeners.get(0).onResponse(AcknowledgedResponse.of(true));

        assertTrue("Caller 1 must be notified after the update completes", caller1.completed);
        assertNull("Caller 1 must not see a failure", caller1.failure);
        assertTrue("Caller 2 must be notified after the update completes", caller2.completed);
        assertNull("Caller 2 must not see a failure", caller2.failure);
    }

    /** Simple {@link ActionListener} that records whether it was called and with what outcome. */
    private static class TestActionListener implements ActionListener<Void> {
        boolean completed = false;
        Exception failure = null;

        @Override
        public void onResponse(Void v) {
            completed = true;
        }

        @Override
        public void onFailure(Exception e) {
            completed = true;
            failure = e;
        }
    }
}
